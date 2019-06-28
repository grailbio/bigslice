// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
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
			mgr.Need(1)
			if got, want := system.Wait(1), 1; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			mgr.Need(ntask - 1)
			if got, want := system.Wait(Nmach), Nmach; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			// (This is racy.)
			mgr.Need(ntask)
			time.Sleep(10 * time.Millisecond)
			if got, want := system.Wait(Nmach), Nmach; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			// Machines should be balanced, and allow maxLoad load.
			loads := make(map[*sliceMachine]int)
			for i := 0; i < ntask; i++ {
				loads[<-mgr.Offer()]++
			}
			select {
			case m := <-mgr.Offer():
				t.Errorf("offered machine %s", m)
			default:
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
	system, _, mgr, cancel := startTestSystem(
		32,
		64,
		0,
	)
	mgr.Need(1)
	if got, want := system.Wait(1), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	mgr.Need(10)
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

	ms := make([]*sliceMachine, 4)
	for i := range ms {
		// To make sure we get m0, m0, m1, m1
		mgr.Need(1)
		ms[i] = <-mgr.Offer()
	}
	if got, want := system.N(), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	ms[0].Done(errors.New("some error"))
	mgr.Need(0)
	select {
	case <-mgr.Offer():
		t.Fatal("did not expect an offer")
	default:
	}
	if got, want := ms[0].health, machineProbation; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	ms[1].Done(nil)
	mgr.Need(0)
	if got, want := ms[0].health, machineOk; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := <-mgr.Offer(), ms[0]; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := <-mgr.Offer(), ms[1]; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSlicemachineLost(t *testing.T) {
	system, _, mgr, cancel := startTestSystem(2, 4, 1.0)
	defer cancel()

	mgr.Need(4)
	m := <-mgr.Offer()
	system.Kill(m.Machine)
	for m.health != machineLost {
		mgr.Need(0)
	}
	if got, want := system.Wait(2), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestMachineQ(t *testing.T) {
	q := machineQ{
		{Machine: &bigmachine.Machine{Maxprocs: 2}, curprocs: 1},
		{Machine: &bigmachine.Machine{Maxprocs: 4}, curprocs: 1},
		{Machine: &bigmachine.Machine{Maxprocs: 3}, curprocs: 0},
	}
	heap.Init(&q)
	if got, want := q[0].Maxprocs, 3; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	q[0].curprocs++
	heap.Fix(&q, 0)
	expect := []int{4, 3, 2}
	for _, procs := range expect {
		if got, want := q[0].Maxprocs, procs; got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		heap.Pop(&q)
	}
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
	m = newMachineManager(b, nil, maxp, maxLoad, &worker{MachineCombiners: false})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		m.Do(ctx)
		wg.Done()
	}()
	cancel = func() {
		ctxcancel()
		wg.Wait()
	}
	return
}
