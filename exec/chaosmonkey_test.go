// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/base/retry"
	"github.com/grailbio/bigmachine/testsystem"
	"github.com/grailbio/bigslice"
	"golang.org/x/sync/errgroup"
)

var chaos = flag.Bool("chaos", false, "run chaos monkey tests")

// Victim is a bigslice.Func that produces a simple bigslice operation that
// is about the simplest, sharded op that requires inter-node communication.
// The victim op sleeps with random durations (exponentially distributed)
// to give the chaos monkey time to act.
var victim = bigslice.Func(func() (slice bigslice.Slice) {
	data := make([]int, 1000)
	for i := range data {
		data[i] = rand.Int()
	}
	slice = bigslice.Const(10, data)
	slice = bigslice.Map(slice, func(i int) (int, int) {
		return i % 15, i
	})
	slice = bigslice.Reduce(slice, func(a, e int) int {
		// This gives us on average 10s runs, when free of failures.
		time.Sleep(time.Duration(60*rand.ExpFloat64()) * time.Millisecond)
		return a + e
	})
	return
})

func TestChaosMonkey(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos monkey tests disable with -short")
	}
	// The nature of this test is highly nondeterministic, and there are
	// always corner cases that need to be handled still. Further, a failed
	// test usually manifests in running forever. Currently the test is in
	// place to test and exercise code paths manually, not as part of
	// CI testing.
	if !*chaos {
		t.Skip("chaos monkey tests disabled; pass -chaos to enable")
	}
	// This test takes way too long to recover with the default probation timeouts.
	save := ProbationTimeout
	ProbationTimeout = time.Second
	defer func() {
		ProbationTimeout = save
	}()
	system := testsystem.New()
	system.Machineprocs = 2
	system.KeepalivePeriod = time.Second
	system.KeepaliveTimeout = 2 * time.Second
	system.KeepaliveRpcTimeout = time.Second

	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	sess := Start(Bigmachine(system), Parallelism(10))
	var g errgroup.Group
	g.Go(func() error {
		// Aggressively kill machines in the beginning, and then back off
		// so that we have a chance to actually recover.
		var (
			wait        = time.Second
			killerStart = time.Now()
		)
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(wait):
				wait += time.Duration(500+rand.Intn(2000)) * time.Millisecond
				log.Printf("activating next chaos monkey in %s", wait)
			}
			if system.Kill(nil) {
				log.Print("the simian army claimed yet another victim!")
			}
			if time.Since(killerStart) > time.Minute {
				return nil
			}
		}
	})
	_, err := sess.Run(ctx, victim)
	cancel()
	t.Logf("victim ran in %s", time.Since(start))
	if err != nil {
		t.Error(err)
	}
	if err = g.Wait(); err != nil {
		t.Fatal(err)
	}
}

// TestDiscardChaos verifies that execution is robust to concurrent evaluation
// and discarding. It does this by repeatedly concurrently evaluating and
// discarding the same task graph and verifying that a final evaluation
// produces correct results.
func TestDiscardChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos monkey tests disable with -short")
	}
	if !*chaos {
		t.Skip("chaos monkey tests disabled; pass -chaos to enable")
	}
	origEnableMaxConsecutiveLost := enableMaxConsecutiveLost
	enableMaxConsecutiveLost = false
	origRetryPolicy := retryPolicy
	retryPolicy = retry.MaxRetries(retry.Backoff(100*time.Millisecond, 1*time.Second, 2), 5)
	defer func() {
		enableMaxConsecutiveLost = origEnableMaxConsecutiveLost
		retryPolicy = origRetryPolicy
	}()
	const Nshard = 10
	const N = Nshard * 100
	// Niter is the number of stress test iterations. Each iteration
	// concurrently runs and discards a task graph, then verifies that a final
	// evaluation produces correct results.
	const Niter = 5
	f := bigslice.Func(func() bigslice.Slice {
		vs := make([]int, N)
		for i := range vs {
			vs[i] = i
		}
		slice := bigslice.Const(Nshard, vs, append([]int{}, vs...))
		slice = bigslice.Reduce(slice, func(x, y int) int {
			time.Sleep(1 * time.Millisecond)
			return x + y
		})
		return slice
	})
	id := bigslice.Func(func(result *Result) bigslice.Slice {
		return result
	})
	for j := 0; j < Niter; j++ {
		system := testsystem.New()
		system.Machineprocs = 2
		system.KeepalivePeriod = 500 * time.Millisecond
		system.KeepaliveTimeout = 1 * time.Second
		system.KeepaliveRpcTimeout = 500 * time.Millisecond
		sess := Start(Bigmachine(system), Parallelism(8))
		ctx := context.Background()
		result, err := sess.Run(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
		ctxRace, cancelRace := context.WithTimeout(ctx, 20*time.Second)
		// Start two goroutines, one which continually evaluates and one
		// which continually discards results.
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			wait := 10 * time.Millisecond
			for {
				select {
				case <-ctxRace.Done():
					return
				case <-time.After(wait):
					_, runErr := sess.Run(ctxRace, id, result)
					if runErr != nil && ctxRace.Err() == nil {
						t.Error(runErr)
					}
				}
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			wait := 10 * time.Millisecond
			for {
				select {
				case <-ctxRace.Done():
					return
				case <-time.After(wait):
					wait += time.Duration(rand.Intn(10)) * time.Millisecond
					result.Discard(ctxRace)
				}
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			wait := time.Duration(rand.Intn(250)) * time.Millisecond
			for {
				select {
				case <-ctxRace.Done():
					return
				case <-time.After(wait):
					wait += time.Duration(10+rand.Intn(250)) * time.Millisecond
					log.Printf("activating next chaos monkey in %s", wait)
					if system.Kill(nil) {
						log.Print("the simian army claimed yet another victim!")
					}
				}
			}
		}()
		wg.Wait()
		cancelRace()
		// Do one final evaluation, the result of which we verify for
		// correctness.
		result, err = sess.Run(ctx, id, result)
		if err != nil {
			t.Fatal(err)
		}
		s := result.Scanner()
		x := rand.Int()
		var count, i, j int
		for s.Scan(ctx, &i, &j) {
			count++
			if i != j {
				t.Error("result computed incorrectly")
				break
			}
		}
		if scanErr := s.Err(); scanErr != nil {
			t.Fatal(scanErr)
		}
		if got, want := count, N; got != want {
			t.Errorf("%v got %v, want %v", x, got, want)
		}
	}
}
