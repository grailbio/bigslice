// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/bigmachine/testsystem"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/sliceio"
)

func TestBigmachineExecutor(t *testing.T) {
	x, stop := bigmachineTestExecutor(1)
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

	go x.Run(task)
	ctx := context.Background()
	task.Lock()
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
	go x.Run(task)
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
	x, stop := bigmachineTestExecutor(1)
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
		inv := fn.Invocation("<test>", i)
		if ix := int(inv.Index); ix > maxIndex {
			maxIndex = ix
		}
		slice := inv.Invoke()
		tasks, _, err := compile(make(taskNamer), inv, slice, false)
		if err != nil {
			t.Fatal(err)
		}
		go x.Run(tasks[0])
	}
	wg.Wait()
	var n int
	for i := 1; i < 2*maxIndex+1; i++ {
		if x.managers[i] != nil {
			n++
		}
	}
	if got, want := n, N; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestBigmachineExecutorTaskExclusive(t *testing.T) {
	ctx := context.Background()
	x, stop := bigmachineTestExecutor(2)
	defer stop()
	var called, replied sync.WaitGroup
	fn := bigslice.Func(func() bigslice.Slice {
		var once sync.Once
		slice := bigslice.Const(2, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 10})
		slice = bigslice.Map(slice, func(i int) int {
			once.Do(func() {
				called.Done()
				replied.Wait()
			})
			return i
		}, bigslice.Exclusive)
		return slice
	})
	inv := fn.Invocation("<test>")
	slice := inv.Invoke()
	tasks, _, err := compile(make(taskNamer), inv, slice, false)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(tasks), 2; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	for _, task := range tasks {
		if !task.Pragma.Exclusive() {
			t.Fatalf("task %v not bigslice.Exclusive", task)
		}
	}
	called.Add(2)
	replied.Add(1)
	go x.Run(tasks[0])
	go x.Run(tasks[1])
	called.Wait()
	if got, want := tasks[0].State(), TaskRunning; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := tasks[1].State(), TaskRunning; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	replied.Done()
	state, err := tasks[0].WaitState(ctx, TaskOk)
	if err != nil || state != TaskOk {
		t.Fatal(state, err)
	}
	state, err = tasks[1].WaitState(ctx, TaskOk)
	if err != nil || state != TaskOk {
		t.Fatal(state, err)
	}
}

func TestBigmachineExecutorPanicCompile(t *testing.T) {
	x, stop := bigmachineTestExecutor(1)
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
	x, stop := bigmachineTestExecutor(1)
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

func TestBigmachineExecutorLost(t *testing.T) {
	if testing.Short() {
		t.Skip("lost executor test disabled for -short")
	}
	system := testsystem.New()
	system.Machineprocs = 1
	system.KeepalivePeriod = time.Second
	system.KeepaliveTimeout = 2 * time.Second
	system.KeepaliveRpcTimeout = time.Second

	ctx, cancel := context.WithCancel(context.Background())
	x := newBigmachineExecutor(system)
	shutdown := x.Start(&Session{
		Context: ctx,
		p:       100,
		maxLoad: 1,
	})
	defer shutdown()
	defer cancel()

	// Make sure to produce enough data that requires multiple calls to
	// get. Currently the batch size is 1024. We mark it as exclusive
	// to ensure that the task is executed on different machine from
	// the subsequent reduce (after shuffle).
	readerTasks, readerSlice, _ := compileFuncExclusive(func() bigslice.Slice {
		return bigslice.ReaderFunc(1, func(shard int, n *int, col []int) (int, error) {
			const N = 10000
			if *n >= N {
				return 0, sliceio.EOF
			}
			for i := range col {
				col[i] = *n + i
			}
			*n += len(col)
			return len(col), nil
		}, bigslice.Exclusive)
	})
	readerTask := readerTasks[0]
	// We need to use a result, not a regular slice, so that the tasks
	// are reused across Func invocations.
	readerResult := &Result{
		Slice: readerSlice,
		tasks: readerTasks,
	}
	go x.Run(readerTask)
	system.Wait(1)
	readerTask.Lock()
	for readerTask.state != TaskOk {
		if err := readerTask.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	readerTask.Unlock()

	if !system.Kill(system.Index(0)) {
		t.Fatal("could not kill machine")
	}
	mapTasks, _, _ := compileFunc(func() bigslice.Slice {
		return bigslice.Map(readerResult, func(v int) int { return v })
	})
	mapTask := mapTasks[0]
	go x.Run(mapTask)
	if state, err := mapTask.WaitState(ctx, TaskLost); err != nil {
		t.Fatal(err)
	} else if state != TaskLost {
		t.Fatal(state)
	}

	// Resubmit the task: Now it should recompute successfully
	// (while allocating a new machine for it). We may have to submit
	// it multiple times before the worker is marked down. (We're racing
	// with the failure detector.)
	readerTask.Lock()
	readerTask.state = TaskInit
	for readerTask.state != TaskOk {
		readerTask.state = TaskInit
		readerTask.Unlock()
		go x.Run(readerTask)
		readerTask.Lock()
		if err := readerTask.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	readerTask.Unlock()

	// Now do the same for the map task. We never killed the system
	// it gets allocated on so no retries. This can take a few seconds as
	// we wait for machine probation to expire.
	mapTask.Set(TaskInit)
	go x.Run(mapTask)
	if state, err := mapTask.WaitState(ctx, TaskOk); err != nil {
		t.Fatal(err)
	} else if state != TaskOk {
		t.Fatal(state)
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
	x, stop := bigmachineTestExecutor(1)
	defer stop()

	tasks, _, _ := compileFunc(func() bigslice.Slice {
		return &errorSlice{bigslice.Const(1, []int{123}), errors.New("some error")}
	})
	run(t, x, tasks, TaskLost)
}

func TestBigmachineExecutorFatalErrorRun(t *testing.T) {
	x, stop := bigmachineTestExecutor(1)
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
	x, stop := bigmachineTestExecutor(1)
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

func run(t *testing.T, x *bigmachineExecutor, tasks []*Task, expect TaskState) {
	t.Helper()
	for _, task := range tasks {
		go x.Run(task)
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

func bigmachineTestExecutor(p int) (exec *bigmachineExecutor, stop func()) {
	x := newBigmachineExecutor(testsystem.New())
	ctx, cancel := context.WithCancel(context.Background())
	shutdown := x.Start(&Session{
		Context: ctx,
		p:       p,
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
	tasks, _, err := compile(make(taskNamer), inv, slice, false)
	if err != nil {
		panic(err)
	}
	return tasks, slice, inv
}

func compileFuncExclusive(f func() bigslice.Slice) ([]*Task, bigslice.Slice, bigslice.Invocation) {
	fn := bigslice.Func(f).Exclusive()
	inv := fn.Invocation("")
	slice := inv.Invoke()
	tasks, _, err := compile(make(taskNamer), inv, slice, false)
	if err != nil {
		panic(err)
	}
	return tasks, slice, inv
}
