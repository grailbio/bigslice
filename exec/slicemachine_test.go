// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigmachine/testsystem"
)

func TestSlicemachineLoad(t *testing.T) {
	for _, maxLoad := range []float64{0.5, 0.90, 1.5} {
		t.Run(fmt.Sprint("maxLoad=", maxLoad), func(t *testing.T) {
			const (
				Nproc = 100
				Nmach = 10
			)
			ntask := int(maxLoad * Nproc * Nmach)
			system, _, mgr, cancel := startTestSystem(
				Nproc,
				ntask,
				maxLoad,
			)
			defer cancel()

			if got, want := system.N(), 0; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			ctx := context.Background()
			ms := getMachines(ctx, mgr, 1)
			if got, want := system.Wait(1), 1; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			ms = append(ms, getMachines(ctx, mgr, ntask-1)...)
			if got, want := system.Wait(Nmach), Nmach; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			mustUnavailable(t, mgr)
			if got, want := system.Wait(Nmach), Nmach; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			// Machines should be balanced, and allow maxLoad load.
			loads := make(map[*sliceMachine]int)
			for i := range ms {
				if ms[i] != nil {
					loads[ms[i]]++
				}
			}
			if got, want := len(loads), Nmach; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			for m, v := range loads {
				if got, want := v, int(Nproc*maxLoad); got != want {
					t.Errorf("%s: got %v, want %v", m, got, want)
				}
			}
		})
	}
}

func TestSlicemachineExclusive(t *testing.T) {
	var (
		system, _, mgr, cancel = startTestSystem(32, 64, 0)
		ctx                    = context.Background()
	)
	getMachines(ctx, mgr, 1)
	if got, want := system.Wait(1), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	getMachines(ctx, mgr, 1)
	mustUnavailable(t, mgr)
	if got, want := system.Wait(2), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	cancel()
	if got, want := system.N(), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSlicemachineProbation(t *testing.T) {
	system, _, mgr, cancel := startTestSystem(2, 4, 1.0)
	defer cancel()

	ctx := context.Background()
	ms := getMachines(ctx, mgr, 4)
	if got, want := system.N(), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	ms[0].Done(1, errors.New("some error"))
	mustUnavailable(t, mgr)
	if got, want := ms[0].health, machineProbation; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	ms[1].Done(1, nil)
	ns := getMachines(ctx, mgr, 2)
	if got, want := ns[0], ms[0]; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := ns[1], ms[1]; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := ms[0].health, machineOk; got != want {
		t.Errorf("got %v, want %v", ms[0].health, want)
	}
}

// TestSlicemachineProbationTimeout verifies that machines that have been put on
// probation and do not experience further errors are removed from probation.
func TestSlicemachineProbationTimeout(t *testing.T) {
	const machinep = 2
	const maxp = 16
	if maxp < machinep*4 {
		panic("maxp not big enough")
	}
	// This test takes way too long to recover with the default probation
	// timeout.
	save := ProbationTimeout
	ProbationTimeout = time.Second
	defer func() {
		ProbationTimeout = save
	}()
	_, _, mgr, cancel := startTestSystem(machinep, maxp, 1.0)
	defer cancel()
	ctx := context.Background()
	ms := getMachines(ctx, mgr, maxp)
	for i := range ms {
		if i%machinep != 0 {
			continue
		}
		ms[i].Done(1, errors.New("some error"))
	}
	// Bring two machines back from probation with successful completions to
	// make sure there's no surprising interaction with timeouts.
	ms[0*machinep].Done(1, nil)
	ms[2*machinep].Done(1, nil)
	ctx, ctxcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer ctxcancel()
	for {
		select {
		case <-ctx.Done():
			t.Fatal("took too long")
		default:
		}
		<-time.After(100 * time.Millisecond)
		var healthyCount int
		for i := range ms {
			if i%machinep != 0 {
				continue
			}
			if ms[i].health == machineOk {
				healthyCount++
			}
		}
		if healthyCount == maxp/machinep {
			break
		}
	}
}

func TestSlicemachineLost(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	system, _, mgr, cancel := startTestSystem(2, 4, 1.0)
	defer cancel()

	ctx := context.Background()
	ms := getMachines(ctx, mgr, 4)
	system.Kill(ms[0].Machine)
	for ms[0].health != machineLost {
		<-time.After(10 * time.Millisecond)
	}
	if got, want := system.Wait(2), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

// TestSlicemachinePriority verifies that higher-priority requests are serviced
// before lower-priority requests.
func TestSlicemachinePriority(t *testing.T) {
	const maxp = 16
	_, _, mgr, cancel := startTestSystem(2, maxp, 1.0)
	defer cancel()

	ctx, ctxcancel := context.WithCancel(context.Background())
	defer ctxcancel()
	// Get machines up to our maximum parallelism. Any requests made afterwards
	// will need to be queued until these offers are returned.
	ms := getMachines(ctx, mgr, maxp)
	sema := make(chan struct{})
	c := make(chan int)
	// Queue up many offer requests with distinct priorities in [0, maxp*4).
	// We'll expect that the offer requests with priorities in [0, maxp) will be
	// serviced first. Queue in descending priority value in case requests are
	// serviced in FIFO order.
	for i := (maxp * 4) - 1; i >= 0; i-- {
		i := i
		go func() {
			offerc, _ := mgr.Offer(i, 1)
			sema <- struct{}{}
			select {
			case <-offerc:
			case <-ctx.Done():
				return
			}
			c <- i
		}()
		// Wait for the goroutine offer request to be queued.
		<-sema
	}
	// Return the original machines/procs to allow the machines to be offered to
	// our blocked requests.
	for _, m := range ms {
		m.Done(1, nil)
	}
	for j := 0; j < maxp; j++ {
		i := <-c
		if i >= maxp {
			t.Error("did not respect priority")
		}
	}
}

// TestSlicemachineNonblockingExclusive verifies that the scheduling
// algorithm does not allow an exclusive task to block progress on
// non-exclusive tasks while we wait to schedule it.
func TestSlicemachineNonblockingExclusive(t *testing.T) {
	const (
		maxp      = 128
		machprocs = maxp / 2
	)
	_, _, mgr, cancel := startTestSystem(machprocs, maxp, 1.0)
	defer cancel()

	ctx, ctxcancel := context.WithCancel(context.Background())
	defer ctxcancel()

	ms := getMachines(ctx, mgr, maxp)
	// Return about half of the machines/procs back to the pool immediately.
	// Occupy the other half indefinitely, making it impossible to successfully
	// schedule an "exclusive" task.
	r := rand.New(rand.NewSource(0))
	for _, m := range ms {
		if r.Float64() < 0.5 {
			m.Done(1, nil)
			continue
		}
	}
	var wg sync.WaitGroup
	// Attempt to schedule an exclusive task. We expect this to be impossible
	// to schedule.
	wg.Add(1)
	go func() {
		offerc, _ := mgr.Offer(1, machprocs)
		wg.Done()
		select {
		case <-offerc:
			// This means that we were able to schedule the exclusive task. We
			// shouldn't get here, as we should have been able to use one of
			// our half-loaded machines to schedule all of our lower priority
			// requests first.
			panic("impossible scheduling")
		case <-ctx.Done():
			return
		}
	}()
	wg.Wait()
	// Schedule a bunch of lower priority (2) tasks. These should all be
	// successfully scheduled to run on one of our machines while the other is
	// reserved for the exclusive task.
	wg.Add(maxp)
	for i := 0; i < maxp; i++ {
		go func() {
			defer wg.Done()
			offerc, _ := mgr.Offer(2, 1)
			select {
			case m := <-offerc:
				m.Done(1, nil)
			case <-ctx.Done():
				return
			}
		}()
	}
	wg.Wait()
	// Returning means that the test passes. If we're blocked on scheduling the
	// exclusive task, we'll never return, and the test will time out.
}

func startTestSystem(machinep, maxp int, maxLoad float64) (system *testsystem.System, b *bigmachine.B, m *machineManager, cancel func()) {
	system = testsystem.New()
	system.Machineprocs = machinep
	// Customize timeouts so that tests run faster.
	system.KeepalivePeriod = time.Second
	system.KeepaliveTimeout = 5 * time.Second
	system.KeepaliveRpcTimeout = time.Second
	b = bigmachine.Start(system)
	ctx, ctxcancel := context.WithCancel(context.Background())
	m = newMachineManager(b, nil, nil, maxp, maxLoad, &worker{MachineCombiners: false})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		m.Do(ctx)
		wg.Done()
	}()
	cancel = func() {
		ctxcancel()
		b.Shutdown()
		wg.Wait()
	}
	return
}

// getMachines gets n machines from mgr and returns them.
func getMachines(ctx context.Context, mgr *machineManager, n int) []*sliceMachine {
	ms := make([]*sliceMachine, n)
	for i := range ms {
		offerc, _ := mgr.Offer(0, 1)
		ms[i] = <-offerc
	}
	return ms
}

// mustUnavailable asserts that no machine is immediately available from mgr.
func mustUnavailable(t *testing.T, mgr *machineManager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	offerc, cancel := mgr.Offer(0, 1)
	select {
	case <-offerc:
		t.Fatal("unexpected machine available")
	case <-ctx.Done():
		cancel()
	}
}
