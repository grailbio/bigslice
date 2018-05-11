// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/grailbio/base/status"
)

// ErrTaskLost indicates that a Task was in TaskLost state.
var ErrTaskLost = errors.New("task was lost")

// TaskState represents the runtime state of a Task. TaskState
// values are defined so that their magnitudes correspond with
// task progression.
type TaskState int

const (
	// TaskInit is the initial state of a task. Tasks in state TaskInit
	// have usually not yet been seen by an executor.
	TaskInit TaskState = iota
	// TaskWaiting indicates that a task has been scheduled for
	// execution (it is runnable) but has not yet been allocated
	// resources by the executor.
	TaskWaiting
	// TaskRunning is the state of a task that's currently being run.
	// After a task is in state TaskRunning, it can only enter a
	// larger-valued state.
	TaskRunning

	// TaskOk indicates that a task has successfully completed;
	// the task's results are available to dependent tasks.
	//
	// All TaskState values greater than TaskOk indicate task
	// errors.
	TaskOk

	// TaskErr indicates that the task experienced a failure while
	// running.
	TaskErr
	// TaskLost indicates that the task was lost, usually because
	// the machine to which the task was assigned failed.
	TaskLost

	maxState
)

var states = [...]string{
	TaskInit:    "INIT",
	TaskWaiting: "WAITING",
	TaskRunning: "RUNNING",
	TaskOk:      "OK",
	TaskErr:     "ERROR",
	TaskLost:    "LOST",
}

// String returns the task's state as an upper-case string.
func (s TaskState) String() string {
	return states[s]
}

// A TaskDep describes a single dependency for a task. A dependency
// comprises one or more tasks and the partition number of the task
// set that must be read at run time.
type TaskDep struct {
	Tasks     []*Task
	Partition int
}

// A Task represents a concrete computational task. Tasks form graphs
// through dependencies; task graphs are compiled from slices.
//
// Tasks also maintain executor state, and are used to coordinate
// execution between concurrent evaluators and a single executor
// (which may be evaluating many tasks concurrently). Tasks thus
// embed a mutex for coordination and provide a context-aware
// conditional variable to coordinate runtime state changes.
type Task struct {
	Type
	// Invocation is the task's invocation, i.e. the Func invocation
	// from which this task was compiled.
	Invocation Invocation
	// Name is the name of the task. Tasks are named universally: they
	// should be unique among all possible tasks in a bigslice session.
	Name string
	// Do starts computation for this task, returning a reader that
	// computes batches of values on demand. Do is invoked with readers
	// for the task's dependencies.
	Do func([]Reader) Reader
	// Deps are the task's dependencies. See TaskDep for details.
	Deps []TaskDep
	// NumPartition is the number of partitions that are output by this task.
	// If NumPartition > 1, then the task must also define a partitioner.
	NumPartition int
	// Hasher is used to compute hashes of Frame rows, used to partition
	// a Frame's output.
	Hasher FrameHasher

	// The following are used to coordinate runtime execution.

	sync.Mutex
	waitc chan struct{}

	// State is the task's state. It is protected by the task's lock
	// and state changes are also broadcast on the task's condition
	// variable.
	state TaskState
	// Err is defines when state == TaskErr.
	err error

	// Status is a status object to which task status is reported.
	Status *status.Task
}

// String returns a short, human-readable string describing the
// task's state.
func (t *Task) String() string {
	// We play fast-and-loose with concurrency here (we read state and
	// err without holding the task's mutex) so that it is safe to call
	// String even when the lock is held.
	var b bytes.Buffer
	fmt.Fprintf(&b, "task %s(%x) %s", t.Name, t.Invocation.Index, t.state)
	if t.err != nil {
		fmt.Fprintf(&b, ": %v", t.err)
	}
	return b.String()
}

// State sets the task's state to the provided state and notifies
// any waiters.
func (t *Task) State(state TaskState) {
	t.Lock()
	t.state = state
	t.Broadcast()
	t.Unlock()
}

// Error sets the task's state to TaskErr and its error to the
// provided error. Waiters are notified.
func (t *Task) Error(err error) {
	t.Lock()
	t.state = TaskErr
	t.err = err
	t.Status.Printf(err.Error())
	t.Broadcast()
	t.Unlock()
}

// Errorf formats an error message using fmt.Errorf, sets the task's
// state to TaskErr and its err to the resulting error message.
func (t *Task) Errorf(format string, v ...interface{}) {
	t.Error(fmt.Errorf(format, v...))
}

// Err returns an error if the task's state is >= TaskErr. When the
// state is > TaskErr, Err returns an error describing the task's
// failed state, otherwise, t.err is returned.
func (t *Task) Err() error {
	t.Lock()
	defer t.Unlock()
	switch t.state {
	case TaskErr:
		if t.err == nil {
			panic("TaskErr without an err")
		}
		return t.err
	case TaskLost:
		return ErrTaskLost
	}
	if t.state >= TaskErr {
		panic("unhandled state")
	}
	return nil
}

// Broadcast notifies waiters of a state change. Broadcast must only
// be called while the task's lock is held.
func (t *Task) Broadcast() {
	if t.waitc != nil {
		close(t.waitc)
		t.waitc = nil
	}
}

// Wait returns after the next call to Broadcast, or if the context
// is complete. The task's lock must be held when calling Wait.
func (t *Task) Wait(ctx context.Context) error {
	if t.waitc == nil {
		t.waitc = make(chan struct{})
	}
	waitc := t.waitc
	t.Unlock()
	var err error
	select {
	case <-waitc:
	case <-ctx.Done():
		err = ctx.Err()
	}
	t.Lock()
	return err
}

// GraphString returns a schematic string of the task graph rooted at t.
func (t *Task) GraphString() string {
	var b bytes.Buffer
	t.WriteGraph(&b)
	return b.String()
}

// WriteGraph writes a schematic string of the task graph rooted at t into w.
func (t *Task) WriteGraph(w io.Writer) {
	var tw tabwriter.Writer
	tw.Init(w, 4, 4, 1, ' ', 0)
	fmt.Fprintln(&tw, "tasks:")
	for _, task := range t.All() {
		out := make([]string, task.NumOut())
		for i := range out {
			out[i] = fmt.Sprint(task.Out(i))
		}
		outstr := strings.Join(out, ",")
		fmt.Fprintf(&tw, "\t%s\t%s\t%d\n", task.Name, outstr, task.NumPartition)
	}
	tw.Flush()
	fmt.Fprintln(&tw, "dependencies:")
	t.writeDeps(&tw)
	tw.Flush()
}

func (t *Task) writeDeps(w io.Writer) {
	for _, dep := range t.Deps {
		for _, task := range dep.Tasks {
			fmt.Fprintf(w, "\t%s:\t%s[%d]\n", t.Name, task.Name, dep.Partition)
			task.writeDeps(w)
		}
	}
}

// All returns all tasks reachable from t. The returned
// set of tasks is unique.
func (t *Task) All() []*Task {
	all := make(map[*Task]bool)
	t.all(all)
	var tasks []*Task
	for task := range all {
		tasks = append(tasks, task)
	}
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].Name < tasks[j].Name
	})
	return tasks
}

func (t *Task) all(tasks map[*Task]bool) {
	if tasks[t] {
		return
	}
	tasks[t] = true
	for _, dep := range t.Deps {
		for _, task := range dep.Tasks {
			task.all(tasks)
		}
	}
}
