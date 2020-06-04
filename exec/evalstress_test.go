// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"context"
	"math/rand"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/base/eventlog"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/sliceio"
)

// stressExecutor implements Executor with randomized task loss to stress the
// correctness of evaluation.
type stressExecutor struct {
	wg sync.WaitGroup
	// tasksOk is the set of tasks that this executor has ever successfully
	// completed. We use this to verify that once Eval returns, all root tasks
	// have, at least at some point, completed successfully.
	tasksOk map[*Task]bool
	// cTasksOk is sent tasks to be recorded in tasksOk.
	cTasksOk chan *Task
	// cReadTaskOk is sent requests to read whether a task a task has ever been
	// completed successfully by this executor.
	cReadTaskOk chan msgReadTaskOk
	errorRate   float64
}

type msgReadTaskOk struct {
	task *Task
	c    chan bool
}

func newStressExecutor() *stressExecutor {
	e := &stressExecutor{
		tasksOk:     make(map[*Task]bool),
		cTasksOk:    make(chan *Task),
		cReadTaskOk: make(chan msgReadTaskOk),
	}
	return e
}

func (*stressExecutor) Name() string {
	return "stress"
}

func (e *stressExecutor) Start(*Session) func() {
	ctx, cancel := context.WithCancel(context.Background())
	go e.loop(ctx)
	return func() {
		cancel()
		e.wg.Wait()
	}
}

// delay delays for a random duration to exercise different sequencing.
func delay() {
	delayMS := time.Duration(rand.Intn(50) + 10)
	<-time.After(delayMS * time.Millisecond)
}

func (e *stressExecutor) Run(task *Task) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		delay()
		task.Set(TaskRunning)
		completionState := TaskOk
		if rand.Float64() < e.errorRate {
			completionState = TaskLost
		}
		delay()
		if completionState == TaskOk {
			e.cTasksOk <- task
		}
		task.Set(completionState)
	}()
}

func (*stressExecutor) Reader(*Task, int) sliceio.ReadCloser {
	panic("not implemented")
}

func (*stressExecutor) Discard(ctx context.Context, task *Task) {
	panic("not implemented")
}

func (*stressExecutor) Eventer() eventlog.Eventer {
	return eventlog.Nop{}
}

func (*stressExecutor) HandleDebug(handler *http.ServeMux) {
	panic("not implemented")
}

func (e *stressExecutor) loop(ctx context.Context) {
	for {
		select {
		case task := <-e.cTasksOk:
			e.tasksOk[task] = true
			if rand.Float64() < e.errorRate {
				// Simulate spontaneous task loss.
				e.wg.Add(1)
				go func() {
					defer e.wg.Done()
					delay()
					task.Set(TaskLost)
				}()
			}
		case m := <-e.cReadTaskOk:
			m.c <- e.tasksOk[m.task]
		case <-ctx.Done():
			return
		}
	}
}

func (e *stressExecutor) taskOk(task *Task) bool {
	c := make(chan bool)
	e.cReadTaskOk <- msgReadTaskOk{task: task, c: c}
	return <-c
}

func (e *stressExecutor) setErrorRate(errorRate float64) {
	e.errorRate = errorRate
}

// TestEvalStress verifies that evaluation behaves properly in the face of task
// loss.
func TestEvalStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	// Disable handling of consecutive lost tasks so that the evaluator tries
	// indefinitely. We will be stressing the evaluator with very high error
	// rates and are verifying its behavior, even though in practice we would
	// give up, as error rates this high would generally mean some systematic
	// problem that needs to be fixed.
	origEnableMaxConsecutiveLost := enableMaxConsecutiveLost
	enableMaxConsecutiveLost = false
	defer func() {
		enableMaxConsecutiveLost = origEnableMaxConsecutiveLost
	}()
	var (
		executor = newStressExecutor()
		shutdown = executor.Start(nil)
		ctx      = context.Background()
	)
	for i := 0; i < 10; i++ {
		tasks, _, _ := compileFunc(func() bigslice.Slice {
			vs := make([]int, 1000)
			for i := range vs {
				vs[i] = i
			}
			slice := bigslice.Const(100, vs)
			slice2 := bigslice.Const(100, vs)
			slice = bigslice.Cogroup(slice)
			slice = bigslice.Map(slice, func(i int) int { return i + 1 })
			slice = bigslice.Cogroup(slice, slice2)
			slice = bigslice.Map(slice, func(i int) (int, int) { return i, i + 1 })
			slice = bigslice.Reduce(slice, func(a, b int) int { return a + b })
			return slice
		})
		executor.setErrorRate(rand.Float64() * 0.2)
		err := Eval(ctx, executor, tasks, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Verify that all root tasks have been computed successfully at some
		// point.
		for _, task := range tasks {
			if !executor.taskOk(task) {
				t.Errorf("task not evaluated: %v", task)
			}
		}
	}
	shutdown()
}
