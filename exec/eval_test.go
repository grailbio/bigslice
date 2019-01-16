// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/sliceio"
)

type testExecutor struct{ *testing.T }

func (testExecutor) Start(*Session) (shutdown func()) {
	return func() {}
}

func (t testExecutor) Runnable(task *Task) {
	task.Lock()
	switch task.state {
	case TaskWaiting, TaskRunning:
		t.Fatalf("invalid task state %s", task.state)
	}
	task.state = TaskRunning
	task.Broadcast()
	task.Unlock()
}

func (testExecutor) Reader(context.Context, *Task, int) sliceio.Reader {
	panic("not implemented")
}

func (testExecutor) HandleDebug(handler *http.ServeMux) {
	panic("not implemented")
}

// SimpleEvalTest sets up a simple, 2-node task graph.
type simpleEvalTest struct {
	Tasks []*Task
	Inv   bigslice.Invocation

	ConstTask, CogroupTask *Task

	wg      sync.WaitGroup
	evalErr error
}

func (s *simpleEvalTest) Go(t *testing.T) {
	t.Helper()
	s.Tasks, _, s.Inv = compileFunc(func() bigslice.Slice {
		slice := bigslice.Const(1, []int{1, 2, 3})
		slice = bigslice.Cogroup(slice)
		return slice
	})
	s.ConstTask = s.Tasks[0].Deps[0].Tasks[0]
	s.CogroupTask = s.Tasks[0]
	ctx := context.Background()
	s.wg.Add(1)
	go func() {
		s.evalErr = Eval(ctx, testExecutor{t}, s.Inv, s.Tasks, nil)
		s.wg.Done()
	}()
}

func (s *simpleEvalTest) Wait() error {
	s.wg.Wait()
	return s.evalErr
}

func TestEvalErr(t *testing.T) {
	var (
		test simpleEvalTest
		ctx  = context.Background()
	)
	test.Go(t)
	state, err := test.ConstTask.WaitState(ctx, TaskRunning)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := state, TaskRunning; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := test.CogroupTask.State(), TaskInit; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	constErr := errors.New("const task error")
	test.ConstTask.Error(constErr)

	if got, want := test.Wait(), constErr; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := test.CogroupTask.State(), TaskInit; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestResubmitLostTask(t *testing.T) {
	var (
		test simpleEvalTest
		ctx  = context.Background()
	)
	test.Go(t)
	var (
		fst = test.ConstTask
		snd = test.CogroupTask
	)
	fst.Lock()
	for fst.state != TaskRunning {
		if err := fst.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	fst.state = TaskLost
	fst.Broadcast()
	for fst.state == TaskLost {
		if err := fst.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// The evaluator should have resubmitted it.
	if got, want := fst.state, TaskRunning; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	// Now we lose both of them while the second is running.
	// The evaluator should resubmit both.
	fst.state = TaskOk
	fst.Broadcast()
	fst.Unlock()

	snd.Lock()
	for snd.state != TaskRunning {
		if err := snd.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	fst.Lock()
	snd.state = TaskLost
	snd.Broadcast()
	snd.Unlock()
	fst.state = TaskLost
	fst.Broadcast()

	for fst.state < TaskRunning {
		if err := fst.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := snd.State(), TaskLost; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	fst.state = TaskOk
	fst.Broadcast()
	fst.Unlock()

	snd.Lock()
	for snd.state < TaskRunning {
		if err := snd.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	snd.state = TaskOk
	snd.Broadcast()
	snd.Unlock()

	if err := test.Wait(); err != nil {
		t.Fatal(err)
	}
}

type benchExecutor struct{ *testing.B }

func (benchExecutor) Start(*Session) (shutdown func()) {
	return func() {}
}

func (b benchExecutor) Runnable(task *Task) {
	task.Lock()
	switch task.state {
	case TaskWaiting, TaskRunning:
		b.Fatalf("invalid task state %s", task.state)
	}
	// Go directly to done to let the scheduler do its work.
	task.state = TaskOk
	task.Broadcast()
	task.Unlock()
}

func (benchExecutor) Reader(context.Context, *Task, int) sliceio.Reader {
	panic("not implemented")
}

func (benchExecutor) HandleDebug(handler *http.ServeMux) {
	panic("not implemented")
}

func BenchmarkEval(b *testing.B) {
	compile := func() ([]*Task, bigslice.Invocation) {
		tasks, _, inv := compileFunc(func() bigslice.Slice {
			const (
				Nstage = 5
				Nshard = 1000
			)
			keys := make([]string, Nshard*2)
			for i := range keys {
				keys[i] = fmt.Sprint(i)
			}
			values := make([]int, Nshard*2)
			for i := range values {
				values[i] = i
			}

			slice := bigslice.Const(Nshard, keys, values)
			for stage := 0; stage < Nstage; stage++ {
				slice = bigslice.Reduce(slice, func(i, j int) int { return i + j })
			}
			return slice
		})
		return tasks, inv
	}
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		tasks, inv := compile()
		if i == 0 {
			b.Log("ntask=", len(tasks))
		}
		if err := Eval(ctx, benchExecutor{b}, inv, tasks, nil); err != nil {
			b.Fatal(err)
		}
	}
}
