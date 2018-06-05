// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"context"
	"errors"
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
