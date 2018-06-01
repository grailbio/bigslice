// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"container/heap"
	"context"
	"testing"

	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigmachine/testsystem"
	"github.com/grailbio/bigslice"
)

func TestMachineQ(t *testing.T) {
	q := machineQ{
		{Machine: &bigmachine.Machine{Maxprocs: 2}, Curprocs: 1},
		{Machine: &bigmachine.Machine{Maxprocs: 4}, Curprocs: 1},
		{Machine: &bigmachine.Machine{Maxprocs: 3}, Curprocs: 0},
	}
	heap.Init(&q)
	if got, want := q[0].Maxprocs, 3; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	q[0].Curprocs++
	heap.Fix(&q, 0)
	expect := []int{4, 3, 2}
	for _, procs := range expect {
		if got, want := q[0].Maxprocs, procs; got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
		heap.Pop(&q)
	}
}

func TestBigmachineExecutor(t *testing.T) {
	x, stop := testExecutor()
	defer stop()

	gate := make(chan struct{}, 1)
	gate <- struct{}{} // one for the local invocation.
	tasks, _, _ := compileFunc(func() bigslice.Slice {
		<-gate
		return bigslice.Const(1, []int{})
	})
	if got, want := len(tasks), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	task := tasks[0]

	// Runnable is idempotent.
	x.Runnable(task)
	x.Runnable(task)
	ctx := context.Background()
	task.Lock()
	if got, want := task.state, TaskWaiting; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	gate <- struct{}{}
	for task.state <= TaskRunning {
		if err := task.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := task.state, TaskOk; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	task.Unlock()

	// If we run it again, it should first enter waiting/running state, and
	// then Ok again. There should not be a new invocation (p=1).
	x.Runnable(task)
	task.Lock()
	for task.state <= TaskRunning {
		if err := task.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := task.state, TaskOk; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	task.Unlock()
}

func TestBigmachineExecutorError(t *testing.T) {
	x, stop := testExecutor()
	defer stop()

	var count int
	tasks, _, _ := compileFunc(func() bigslice.Slice {
		count++
		if count == 2 {
			panic("hello")
		}
		return bigslice.Const(1, []int{})
	})
	if got, want := len(tasks), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	task := tasks[0]
	ctx := context.Background()
	x.Runnable(task)
	task.Lock()
	for task.state <= TaskRunning {
		if err := task.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := task.state, TaskErr; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if task.err == nil {
		t.Error("expected error")
	}
	task.err = nil
	task.Unlock()
}

func TestBigmachineCompiler(t *testing.T) {
	x, stop := testExecutor()
	defer stop()

	tasks, slice, inv := compileFunc(func() bigslice.Slice {
		return bigslice.Const(1, []int{})
	})
	run(t, x, tasks)
	tasks, _, _ = compileFunc(func() bigslice.Slice {
		return bigslice.Map(&Result{Slice: slice, inv: inv}, func(i int) int { return i * 2 })
	})
	run(t, x, tasks)
}

func run(t *testing.T, x Executor, tasks []*Task) {
	t.Helper()
	for _, task := range tasks {
		x.Runnable(task)
	}
	for _, task := range tasks {
		task.WaitState(context.Background(), TaskOk)
		task.Lock()
		if task.state != TaskOk {
			t.Fatalf("task %v not ok", task)
		}
		task.Unlock()
	}
}

func testExecutor() (exec *bigmachineExecutor, stop func()) {
	x := newBigmachineExecutor(testsystem.New())
	return x, x.Start(&Session{
		Context: context.Background(),
		p:       1,
	})
}

func compileFunc(f func() bigslice.Slice) ([]*Task, bigslice.Slice, bigslice.Invocation) {
	fn := bigslice.Func(f)
	inv := fn.Invocation()
	slice := inv.Invoke()
	tasks, err := compile(make(taskNamer), inv, slice)
	if err != nil {
		panic(err)
	}
	return tasks, slice, inv
}
