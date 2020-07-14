// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/bigmachine/testsystem"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/metrics"
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

	ctx := context.Background()
	go x.Run(ctx, task)
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
	go x.Run(ctx, task)
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

	ctx := context.Background()
	const N = 5
	var maxIndex int
	wg.Add(2 * N) //one for local invocation; one for remote
	for i := 0; i < N; i++ {
		inv := makeExecInvocation(fn.Invocation("<test>", i))
		if ix := int(inv.Index); ix > maxIndex {
			maxIndex = ix
		}
		slice := inv.Invoke()
		tasks, err := compile(inv, slice, false)
		if err != nil {
			t.Fatal(err)
		}
		go x.Run(ctx, tasks[0])
	}
	wg.Wait()
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
	inv := makeExecInvocation(fn.Invocation("<test>"))
	slice := inv.Invoke()
	tasks, err := compile(inv, slice, false)
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
	go x.Run(ctx, tasks[0])
	go x.Run(ctx, tasks[1])
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

// TestBigmachineExecutorProcs verifies that using the Procs pragma properly
// affects machine/proc allocation.
func TestBigmachineExecutorProcs(t *testing.T) {
	// Set up the test with:
	// - a slice with 8 tasks
	// - Procs(2) so that two procs are allocated for each task
	// - a system with 12 procs, 4 procs per machine
	system := testsystem.New()
	system.Machineprocs = 4
	ctx, cancel := context.WithCancel(context.Background())
	x := newBigmachineExecutor(system)
	shutdown := x.Start(&Session{
		Context: ctx,
		p:       12, // 3 machines
		maxLoad: 1,
	})
	defer shutdown()
	defer cancel()

	// We use blockc to block completion of tasks, controlling execution for our
	// test. All tasks block until we close blockc.
	blockc := make(chan struct{})
	fn := bigslice.Func(func() bigslice.Slice {
		is := make([]int, 100)
		for i := range is {
			is[i] = i
		}
		slice := bigslice.ReaderFunc(8, func(shard int, x *int, xs []int) (int, error) {
			<-blockc
			const N = 10
			var i int
			for *x < N && i < len(xs) {
				xs[i] = (shard * N) + *x
				i++
				*x++
			}
			if *x == N {
				return i, sliceio.EOF
			}
			return i, nil
		}, bigslice.Procs(1)) // Exercise Procs composition.
		// Add an identity mapping to exercise pipelining.
		slice = bigslice.Map(slice, func(i int) int {
			return i
		}, bigslice.Procs(2))
		return slice
	})
	inv := makeExecInvocation(fn.Invocation("<test>"))
	slice := inv.Invoke()
	tasks, err := compile(inv, slice, false)
	if err != nil {
		t.Fatal(err)
	}
	// Verify that there is one task per shard.
	if got, want := len(tasks), 8; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	// Verify that the proc need is propagated through the pipeline.
	for _, task := range tasks {
		if got, want := task.Pragma.Procs(), 2; got != want {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	// Run three tasks (needing 6 procs), and verify that two machines have been
	// started on which to run them.
	for _, task := range tasks[:3] {
		go x.Run(ctx, task)
		state, err := task.WaitState(ctx, TaskRunning)
		if err != nil || state != TaskRunning {
			t.Fatal(state, err)
		}
	}
	if got, want := system.N(), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	// Run the rest of the tasks, and verify that the remaining machines have
	// been started on which to run them.
	//
	// Note: this is racy, as we don't have a way of knowing that the executor
	// has blocked because it cannot acquire a machine on which to run a task.
	// If this is a problem, we'll need a better solution.
	for _, task := range tasks[3:] {
		go x.Run(ctx, task)
		func() {
			stateCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()
			state, err := task.WaitState(stateCtx, TaskRunning)
			if stateCtx.Err() != nil {
				// We expect some tasks to not reach TaskRunning, as there are
				// not enough procs to service them.
				return
			}
			if err != nil || state != TaskRunning {
				t.Fatal(state, err)
			}
		}()
	}
	if got, want := system.N(), 3; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	// Only 6 of the 8 shards should be running at this point, occupying all
	// procs.
	var running int
	for _, task := range tasks {
		if task.State() == TaskRunning {
			running++
		}
	}
	if got, want := running, 6; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	// Verify that everything runs to completion.
	close(blockc)
	for _, task := range tasks {
		state, err := task.WaitState(ctx, TaskOk)
		if err != nil || state != TaskOk {
			t.Fatal(state, err)
		}
	}
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
	go x.Run(ctx, readerTask)
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
	go x.Run(ctx, mapTask)
	if state, err := mapTask.WaitState(ctx, TaskOk); err != nil {
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
		go x.Run(ctx, readerTask)
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
	go x.Run(ctx, mapTask)
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
	if got, want := errors.Recover(tasks[0].Err()).Severity, errors.Fatal; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

// TestBigmachineExecutorFatalCombinerErrorRun verifies that fatal errors in
// execution of tasks that have combiners are propagated and put the task in
// TaskErr state.
func TestBigmachineExecutorFatalCombinerErrorRun(t *testing.T) {
	x, stop := bigmachineTestExecutor(1)
	defer stop()

	err := errors.E(errors.Fatal, "a fatal error")
	tasks, _, _ := compileFunc(func() bigslice.Slice {
		var slice bigslice.Slice
		slice = &errorSlice{bigslice.Const(1, []int{123}, []int{456}), err}
		// This Reduce causes the tasks compiled from the error slice to use a
		// combiner.
		slice = bigslice.Reduce(slice, func(i, j int) int { return i + j })
		return slice
	})
	// depTask will be a task of the errorSlice, which has a combiner.
	depTask := tasks[0].Deps[0].Head
	run(t, x, []*Task{depTask}, TaskErr)
	if got, want := errors.Recover(depTask.Err()).Severity, errors.Fatal; got != want {
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
		return bigslice.Map(
			&Result{Slice: slice, invIndex: inv.Index, tasks: firstTasks},
			func(i int) int { return i * 2 },
		)
	})
	run(t, x, tasks, TaskOk)
}

// TestReadRetries verifies that the reader used to read task data from worker
// machines correctly retries on failures.
func TestReadRetries(t *testing.T) {
	const N = 100
	bs := make([]byte, N)
	for i := range bs {
		bs[i] = byte(i)
	}
	// The real retry policy makes this test way too slow.
	origPolicy := retryPolicy
	retryPolicy = retry.Backoff(1*time.Nanosecond, 10*time.Second, 2)
	defer func() {
		retryPolicy = origPolicy
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Given a reader that systematically produces errors, we make sure that we
	// still read correctly and that our retry behavior is reasonable.
	var (
		errorReader = newErrorReader(bytes.NewReader(bs))
		openerAt    = readSeekerOpenerAt{r: errorReader}
		r           = newRetryReader(ctx, openerAt)
		// p is our per-read buffer.
		p     = make([]byte, 1)
		total int
		data  []byte
	)
	for {
		n, err := r.Read(p)
		if err == io.EOF {
			break
		}
		if err, ok := err.(*errors.Error); ok && err.Kind == errors.Timeout {
			// If we're properly resetting our retry frequency on successful
			// reads, this test should finish quickly. If not, it will exceed
			// the context timeout.
			t.Fatalf("took too long; check retry behavior")
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		total += n
		data = append(data, p...)
	}
	if got, want := total, N; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := data, bs; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestBigmachineMetrics(t *testing.T) {
	counter := metrics.NewCounter()

	x, stop := bigmachineTestExecutor(1)
	defer stop()

	tasks, _, _ := compileFunc(func() (slice bigslice.Slice) {
		slice = bigslice.Const(1, []int{1, 2, 3})
		slice = bigslice.Map(slice, func(ctx context.Context, i int) int {
			scope := metrics.ContextScope(ctx)
			counter.Incr(scope, int64(i))
			return i
		})
		return
	})
	task := tasks[0]
	ctx := context.Background()
	go x.Run(ctx, task)
	if _, err := task.WaitState(ctx, TaskOk); err != nil {
		t.Fatal(err)
	}
	if got, want := counter.Value(&task.Scope), int64(6); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

// readSeekerOpenerAt wraps an io.ReadSeeker to implement the openerAt
// interface. It simply seeks to the desired open offset.
type readSeekerOpenerAt struct {
	r io.ReadSeeker
}

func (o readSeekerOpenerAt) OpenAt(ctx context.Context, offset int64) (io.ReadCloser, error) {
	if _, err := o.r.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	return ioutil.NopCloser(o.r), nil
}

// errorReader wraps a io.ReadSeeker and systematically returns errors when
// reading.
type errorReader struct {
	r     io.ReadSeeker
	nread int
}

func (r *errorReader) Read(data []byte) (int, error) {
	r.nread++
	if r.nread%3 != 0 {
		return 0, errors.New("some error")
	}
	return r.r.Read(data)
}

func (r *errorReader) Seek(offset int64, whence int) (int64, error) {
	return r.r.Seek(offset, whence)
}

func newErrorReader(r io.ReadSeeker) *errorReader {
	return &errorReader{r: r}
}

func run(t *testing.T, x *bigmachineExecutor, tasks []*Task, expect TaskState) {
	t.Helper()
	ctx := context.Background()
	for _, task := range tasks {
		go x.Run(ctx, task)
	}
	for _, task := range tasks {
		if _, err := task.WaitState(context.Background(), expect); err != nil {
			t.Fatalf("error waiting for state %v: %v", expect, err)
		}
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

func compileFunc(f func() bigslice.Slice) ([]*Task, bigslice.Slice, execInvocation) {
	fn := bigslice.Func(f)
	inv := makeExecInvocation(fn.Invocation(""))
	slice := inv.Invoke()
	tasks, err := compile(inv, slice, false)
	if err != nil {
		panic(err)
	}
	return tasks, slice, inv
}

func compileFuncExclusive(f func() bigslice.Slice) ([]*Task, bigslice.Slice, execInvocation) {
	fn := bigslice.Func(f).Exclusive()
	inv := makeExecInvocation(fn.Invocation(""))
	slice := inv.Invoke()
	tasks, err := compile(inv, slice, false)
	if err != nil {
		panic(err)
	}
	return tasks, slice, inv
}
