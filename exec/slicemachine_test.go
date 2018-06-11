// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"container/heap"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigmachine/testsystem"
)

func TestSlicemachineLoad(t *testing.T) {
	system, _, needc, offerc, cancel := startTestSystem(100, 1000)
	defer cancel()

	if got, want := system.N(), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	needc <- 1
	if got, want := system.Wait(1), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	needc <- 999
	if got, want := system.Wait(10), 10; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	// (This is racy.)
	needc <- 1000
	time.Sleep(10 * time.Millisecond)
	if got, want := system.Wait(10), 10; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	// Machines should be balanced, and allow 95% load.
	loads := make(map[*sliceMachine]int)
	for i := 0; i < 950; i++ {
		loads[<-offerc]++
	}
	select {
	case m := <-offerc:
		t.Errorf("offered machine %s", m)
	default:
	}
	if got, want := len(loads), 10; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	for m, v := range loads {
		if got, want := v, 95; got != want {
			t.Errorf("%s: got %v, want %v", m, got, want)
		}
	}
}

func TestSlicemachineProbation(t *testing.T) {
	system, _, needc, offerc, cancel := startTestSystem(2, 4)
	defer cancel()

	ms := make([]*sliceMachine, 4)
	for i := range ms {
		// To make sure we get m0, m0, m1, m1
		needc <- 1
		ms[i] = <-offerc
	}
	if got, want := system.N(), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	ms[0].Done(errors.New("some error"))
	needc <- 0
	select {
	case <-offerc:
		t.Fatal("did not expect an offer")
	default:
	}
	if got, want := ms[0].health, machineProbation; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	ms[1].Done(nil)
	needc <- 0
	if got, want := ms[0].health, machineOk; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := <-offerc, ms[0]; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := <-offerc, ms[1]; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSlicemachineLost(t *testing.T) {
	system, _, needc, offerc, cancel := startTestSystem(2, 4)
	defer cancel()

	needc <- 4
	m := <-offerc
	system.Kill(m.Machine)
	for m.health != machineLost {
		needc <- 0
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

func startTestSystem(machinep, maxp int) (system *testsystem.System, b *bigmachine.B, needc chan int, offerc chan *sliceMachine, cancel func()) {
	system = testsystem.New()
	system.Machineprocs = machinep
	// Customize timeouts so that tests run faster.
	system.KeepalivePeriod = time.Second
	system.KeepaliveTimeout = 5 * time.Second
	system.KeepaliveRpcTimeout = time.Second
	b = bigmachine.Start(system)
	var ctx context.Context
	ctx, cancel = context.WithCancel(context.Background())
	needc = make(chan int)
	offerc = make(chan *sliceMachine)
	go manageMachines(ctx, b, nil, maxp, needc, offerc)
	return
}
