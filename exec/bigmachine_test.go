// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"context"
	"sync"
	"testing"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/bigmachine/testsystem"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/sliceio"
)

func TestBigmachineExecutor(t *testing.T) {
	x, stop := bigmachineTestExecutor()
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

func TestBigmachineExecutorExclusive(t *testing.T) {
	x, stop := bigmachineTestExecutor()
	defer stop()
	var wg sync.WaitGroup
	fn := bigslice.Func(func(i int) bigslice.Slice {
		wg.Done()
		return bigslice.Const(1, []int{})
	})
	fn = fn.Exclusive()

	const N = 5
	var maxIndex int
	wg.Add(2 * N) //one for local invocation; one for remote
	for i := 0; i < N; i++ {
		inv := fn.Invocation("", i)
		if ix := int(inv.Index); ix > maxIndex {
			maxIndex = ix
		}
		slice := inv.Invoke()
		tasks, err := compile(make(taskNamer), inv, slice)
		if err != nil {
			t.Fatal(err)
		}
		x.Runnable(tasks[0])
	}
	wg.Wait()
	if got, want := len(x.managers), maxIndex+1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	var n int
	for i := 1; i < maxIndex+1; i++ {
		if x.managers[i] != nil {
			n++
		}
	}
	if got, want := n, N; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestBigmachineExecutorPanicCompile(t *testing.T) {
	x, stop := bigmachineTestExecutor()
	defer stop()

	var count int
	tasks, _, _ := compileFunc(func() bigslice.Slice {
		count++
		if count == 2 {
			panic("hello")
		}
		return bigslice.Const(1, []int{})
	})
	run(t, x, tasks, TaskErr)
}

func TestBigmachineExecutorPanicRun(t *testing.T) {
	x, stop := bigmachineTestExecutor()
	defer stop()

	tasks, _, _ := compileFunc(func() bigslice.Slice {
		slice := bigslice.Const(1, []int{123})
		return bigslice.Map(slice, func(i int) int {
			panic(i)
		})
	})
	run(t, x, tasks, TaskErr)
	if err := tasks[0].Err(); !errors.Match(fatalErr, err) {
		t.Errorf("expected fatal error, got %v", err)
	}
}

type errorSlice struct {
	bigslice.Slice
	err error
}

func (r *errorSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader {
	return sliceio.ErrReader(r.err)
}

func TestBigmachineExecutorErrorRun(t *testing.T) {
	x, stop := bigmachineTestExecutor()
	defer stop()

	tasks, _, _ := compileFunc(func() bigslice.Slice {
		return &errorSlice{bigslice.Const(1, []int{123}), errors.New("some error")}
	})
	run(t, x, tasks, TaskLost)
}

func TestBigmachineExecutorFatalErrorRun(t *testing.T) {
	x, stop := bigmachineTestExecutor()
	defer stop()

	err := errors.E(errors.Fatal, "a fatal error")
	tasks, _, _ := compileFunc(func() bigslice.Slice {
		return &errorSlice{bigslice.Const(1, []int{123}), err}
	})
	run(t, x, tasks, TaskErr)
	if got, want := tasks[0].Err(), err; !errors.Match(want, got) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestBigmachineCompiler(t *testing.T) {
	x, stop := bigmachineTestExecutor()
	defer stop()

	tasks, slice, inv := compileFunc(func() bigslice.Slice {
		return bigslice.Const(10, []int{})
	})
	firstTasks := tasks
	run(t, x, tasks, TaskOk)
	tasks, _, _ = compileFunc(func() bigslice.Slice {
		return bigslice.Map(&Result{Slice: slice, inv: inv, tasks: firstTasks}, func(i int) int { return i * 2 })
	})
	run(t, x, tasks, TaskOk)
}

func run(t *testing.T, x Executor, tasks []*Task, expect TaskState) {
	t.Helper()
	for _, task := range tasks {
		x.Runnable(task)
	}
	for _, task := range tasks {
		task.WaitState(context.Background(), expect)
		task.Lock()
		if got, want := task.state, expect; got != want {
			t.Fatalf("task %v: got %v, want %v", task, got, want)
		}
		task.Unlock()
	}
}

func bigmachineTestExecutor() (exec *bigmachineExecutor, stop func()) {
	x := newBigmachineExecutor(testsystem.New())
	ctx, cancel := context.WithCancel(context.Background())
	shutdown := x.Start(&Session{
		Context: ctx,
		p:       1,
		maxLoad: 1,
	})
	return x, func() {
		cancel()
		shutdown()
	}
}

func compileFunc(f func() bigslice.Slice) ([]*Task, bigslice.Slice, bigslice.Invocation) {
	fn := bigslice.Func(f)
	inv := fn.Invocation("")
	slice := inv.Invoke()
	tasks, err := compile(make(taskNamer), inv, slice)
	if err != nil {
		panic(err)
	}
	return tasks, slice, inv
}
