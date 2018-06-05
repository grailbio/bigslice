// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package exec implements compilation, evaluation, and execution of
// Bigslice slice operations.
package exec

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/status"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/sliceio"
)

const defaultChunksize = 1024

// Executor defines an interface used to provide implementations of
// task runners. An Executor is responsible for running single tasks,
// partitioning their outputs, and instantiating readers to retrieve the
// output of any given task.
type Executor interface {
	// Start starts the executor. It is called before evaluation has started
	// and after all funcs have been registered. Start need not return:
	// for example, the Bigmachine implementation of Executor uses
	// Start as an entry point for worker processes.
	Start(*Session) (shutdown func())

	// Runnable marks the task as runnable. After a call to Runnable,
	// the Task should have state >= TaskWaiting. The executor owns
	// the task after calling Runnable, and only the executor should
	// modify the task's state.
	Runnable(*Task)

	// Reader returns a locally accessible reader for the requested task.
	Reader(context.Context, *Task, int) sliceio.Reader

	// HandleDebug adds executor-specific debug handlers to the provided
	// http.ServeMux. This is used to serve diagnostic information relating
	// to the executor.
	HandleDebug(handler *http.ServeMux)
}

// Eval simultaneously evaluates a set of task graphs from the
// provided set of roots. Eval uses the provided executor to dispatch
// tasks when their dependencies have been satisfied. Eval returns on
// evaluation error or else when all roots are fully evaluated.
//
// TODO(marius): consider including the invocation in the task definitions
// themselves. This way, a task's name is entirely self contained and can
// be interpreted without an accompanying invocation.
// TODO(marius): we can often stream across shuffle boundaries. This would
// complicate scheduling, but may be worth doing.
func Eval(ctx context.Context, executor Executor, inv bigslice.Invocation, roots []*Task, group *status.Group) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	tasks := make(map[*Task]bool)
	for _, task := range roots {
		task.all(tasks)
	}
	var (
		donec   = make(chan struct{})
		errc    = make(chan error)
		running int
	)
	for {
		todo := make(map[*Task]bool)
		for _, task := range roots {
			task.Lock()
			err := addReady(todo, task)
			task.Unlock()
			if err != nil {
				return err
			}
		}
		if len(todo) == 0 && running == 0 {
			break
		}

		// Mark each ready task as runnable and keep track of them.
		// The executor manages parallelism.
		for task := range todo {
			log.Debug.Printf("runnable: %s", task)
			// TODO(marius): this will result in multiple task entries when there is
			// concurrent evaluation, perhaps status should be managed by Task.
			task.Status = group.Startf("%s(%x)", task.Name, inv.Index)
			executor.Runnable(task)
			running++
			go func(task *Task) {
				state, err := task.WaitState(ctx, TaskOk)
				if err != nil {
					errc <- err
					return
				}
				log.Debug.Printf("done task %v", task)
				task.Status.Done()
				switch state {
				default:
					err = fmt.Errorf("unexpected task state %v", task)
				case TaskOk:
				case TaskErr:
					err = task.err
				case TaskLost:
					log.Error.Printf("lost task %s", task.Name)
				}
				if err != nil {
					errc <- err
				} else {
					donec <- struct{}{}
				}
			}(task)
		}

		var stateCounts [maxState]int
		for task := range tasks {
			task.Lock()
			stateCounts[task.state]++
			task.Unlock()
		}
		states := make([]string, maxState)
		for state, count := range stateCounts {
			states[state] = fmt.Sprintf("%s=%d", TaskState(state), count)
		}
		group.Printf("tasks: %s", strings.Join(states, " "))
		select {
		case <-donec:
			running--
		case err := <-errc:
			return err
		}
	}
	return nil
}

// AddReady  adds all tasks that are runnable but not yet running to
// the provided tasks set. AddReady requires that task is locked on
// entry.
//
// AddReady locks sub-tasks while traversing the graph. Since task
// graphs are DAGs and children are always traversed in the same
// order, concurrent addReady invocations will not deadlock.
func addReady(tasks map[*Task]bool, task *Task) error {
	if tasks[task] {
		return nil
	}
	switch task.state {
	case TaskInit:
	case TaskWaiting, TaskRunning, TaskOk:
		// We only add back lost tasks after they've been acknowledged
		// by the main evaluation loop.
		return nil
	case TaskLost:
		// If we encounter a lost task, we re-initialize it.
		if task.CombineKey != "" {
			return fmt.Errorf("unrecoverable combine task %v lost", task)
		}
		task.state = TaskInit
	case TaskErr:
		return task.err
	default:
		panic("unhandled task state")
	}

	ready := true
	for _, dep := range task.Deps {
		for _, deptask := range dep.Tasks {
			deptask.Lock()
			err := addReady(tasks, deptask)
			ready = ready && deptask.state == TaskOk
			deptask.Unlock()
			if err != nil {
				return err
			}
		}
	}
	if ready {
		tasks[task] = true
	}
	return nil
}
