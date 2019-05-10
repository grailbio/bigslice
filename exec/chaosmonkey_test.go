// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec_test

import (
	"context"
	"flag"
	"math/rand"
	"testing"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/bigmachine/testsystem"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/exec"
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
	system := testsystem.New()
	system.Machineprocs = 2
	system.KeepalivePeriod = time.Second
	system.KeepaliveTimeout = 2 * time.Second
	system.KeepaliveRpcTimeout = time.Second

	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	sess := exec.Start(exec.Bigmachine(system), exec.Parallelism(10))
	var g errgroup.Group
	g.Go(func() error {
		// Aggressively kill machines in the beginning, and then back off
		// so that we have a chance to actually recover.
		var (
			wait  = time.Second
			start = time.Now()
		)
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(wait):
				wait += time.Duration(100+rand.Intn(900)) * time.Millisecond
			}
			if system.Kill(nil) {
				t.Log("the simian army claimed yet another victim!")
			}
			if time.Since(start) > time.Minute {
				return errors.New("test took more than a minute to recover")
			}
		}
	})
	_, err := sess.Run(ctx, victim)
	cancel()
	t.Logf("victim ran in %s", time.Since(start))
	if err != nil {
		t.Error(err)
	}
	if err := g.Wait(); err != nil {
		t.Error(err)
	}
}
