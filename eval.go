// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/status"
)

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

	// Run runs the given task on the executor. It returns an error if
	// the task fails. Run is called only when a task is ready to be run
	// (i.e., its dependents are all complete). When Run succeeds,
	// the outputs of the task are available in the executor.
	Run(context.Context, Invocation, *Task) error

	// Reader returns a locally accessible reader for the requested task.
	Reader(context.Context, *Task, int) Reader

	// Maxprocs returns the number of available processors in this executor.
	// It determines the amount of available physical parallelism.
	Maxprocs() int

	// HandleDebug adds executor-specific debug handlers to the provided
	// http.ServeMux. This is used to serve diagnostic information relating
	// to the executor.
	HandleDebug(handler *http.ServeMux)
}

// Eval simultaneously evaluates a set of task graphs from the
// provided set of roots. Eval uses the provided executor to dispatch
// tasks when their dependencies have been satisfied. Eval never
// schedules more than the number of procs available from the
// executor. Eval returns on evaluation error or else when all roots
// are fully evaluated.
//
// TODO(marius): consider including the invocation in the task definitions
// themselves. This way, a task's name is entirely self contained and can
// be interpreted without an accompanying invocation.
// TODO(marius): we can often stream across shuffle boundaries. This would
// complicate scheduling, but may be worth doing.
func Eval(ctx context.Context, executor Executor, p int, inv Invocation, roots []*Task, group *status.Group) error {
	if p == 0 {
		return errors.New("cannot evaluate with 0 parallelism")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	tasks := make(map[*Task]bool)
	for _, task := range roots {
		task.all(tasks)
	}
	var (
		state   = make(map[*Task]taskState)
		donec   = make(chan *Task)
		errc    = make(chan error)
		running int
	)
	for {
		done := true
		for _, task := range roots {
			todo(state, task)
			done = done && state[task] == taskDone
		}
		if done {
			break
		}
		// Kick off ready tasks as long as we have space.
		// Also count tasks by state.
		var stateCounts [maxState]int
		for task, taskState := range state {
			if running >= p || taskState != taskReady {
				stateCounts[taskState]++
				continue
			}
			stateCounts[taskRunning]++
			running++
			state[task] = taskRunning
			task.Status = group.Start(task.Name)
			go func(task *Task) {
				if err := executor.Run(ctx, inv, task); err != nil {
					task.Status.Printf("error %v", err)
					errc <- err
				} else {
					donec <- task
				}
				task.Status.Done()
			}(task)
		}
		// DEBUG: print all task states.
		if false {
			log.Print("task states:")
			for task, taskState := range state {
				log.Printf("task %s state %s", task.Name, taskState)
			}
		}
		states := make([]string, maxState)
		for state := range states {
			states[state] = fmt.Sprintf("%s=%d", taskState(state), stateCounts[state])
		}
		group.Printf("tasks: %s", strings.Join(states, " "))
		select {
		case task := <-donec:
			running--
			state[task] = taskDone
		case err := <-errc:
			return err
		}
	}
	return nil
}

func todo(state map[*Task]taskState, task *Task) {
	if state[task] >= taskReady {
		return
	}
	for _, dep := range task.Deps {
		for _, deptask := range dep.Tasks {
			todo(state, deptask)
		}
	}
	for _, dep := range task.Deps {
		for _, deptask := range dep.Tasks {
			if state[deptask] < taskDone {
				return
			}
		}
	}
	state[task] = taskReady
}

type taskState int

const (
	taskInit taskState = iota
	taskReady
	taskRunning
	taskDone

	maxState
)

func (t taskState) String() string {
	switch t {
	default:
		panic("unrecognized task state")
	case taskInit:
		return "INIT"
	case taskReady:
		return "READY"
	case taskRunning:
		return "RUNNING"
	case taskDone:
		return "DONE"
	}
}
