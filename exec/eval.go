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

	// First initialize scheduler state. We build three maps and a todo list:
	var (
		// Tasks is the full set of tasks that will be run by this evaluation.
		// They do not include tasks that are handed to us from a previous
		// evaluation, for example as a dependency introduced during
		// an iterative Bigslice session.
		tasks = make(map[*Task]bool)
		// Pending stores the number of pending dependencies for a
		// particular task. When the count reaches 0, the task is then
		// runnable.
		pending = make(map[*Task]int)
		// Deps stores the set of dependees for a given task: it is the
		// reverse set of edges for a given task in the graph. This is used
		// to maintain the completion counts stored in complete.
		deps = make(map[*Task][]*Task)
		// Todo is the current list of tasks that are ready. They should be
		// marked runnable by the scheduler.
		todo []*Task
	)
	for _, task := range roots {
		initialize(task, tasks, pending, &todo, deps)
	}
	var (
		donec   = make(chan *Task, 8)
		errc    = make(chan error)
		running int
	)
	for len(todo) > 0 || running > 0 {
		group.Printf("tasks: runnable: %d/%d", running, len(tasks))
	drain:
		for len(todo) == 0 && running > 0 {
			select {
			case err := <-errc:
				if err == nil {
					panic("not nil err")
				}
				return err
			case task := <-donec:
				running--
				task.Lock()
				if task.state == TaskLost {
					// Resubmit lost tasks when we can.
					if task.CombineKey != "" {
						return fmt.Errorf("unrecoverable combine task %v lost", task)
					}
					task.state = TaskInit
					todo = append(todo, task)
					task.Unlock()
					continue drain
				}
				if task.state != TaskOk {
					panic("exec.Eval: invalid task state " + task.state.String())
				}
				task.Unlock()
				for _, deptask := range deps[task] {
					pending[deptask]--
					if pending[deptask] == 0 {
						todo = append(todo, deptask)
					}
				}
			}
		}

		// Mark each ready task as runnable and keep track of them.
		// The executor manages parallelism.
		for _, task := range todo {
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
					donec <- task
				}
			}(task)
		}
		todo = todo[:0]
	}
	return nil
}

// Initialize initializes the evaluator run list state required for scheduling.
// See the comment in Eval for a detailed explanation of the function of
// these lists.
func initialize(task *Task, tasks map[*Task]bool, pending map[*Task]int, todo *[]*Task, reverse map[*Task][]*Task) bool {
	// This can be true when we're handed tasks from a previous run,
	// e.g., during an iterative session.
	if task.state != TaskInit {
		return false
	}
	if tasks[task] {
		return true
	}
	tasks[task] = true
	ready := true
	for _, dep := range task.Deps {
		for _, deptask := range dep.Tasks {
			reverse[deptask] = append(reverse[deptask], task)
			pending[task]++
			if initialize(deptask, tasks, pending, todo, reverse) {
				ready = false
			}
		}
	}
	if ready {
		*todo = append(*todo, task)
	}
	return true
}
