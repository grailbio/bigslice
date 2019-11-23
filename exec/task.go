// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

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
	"github.com/grailbio/base/sync/ctxsync"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/metrics"
	"github.com/grailbio/bigslice/slicefunc"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/slicetype"
)

func init() {
	close(closedc)
}

// closedc is closed in init which can be used any time we just want a closed
// channel (i.e. a channel that is always ready and receives a zero value).
var closedc = make(chan struct{})

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
	// Head holds the underlying task that represents this dependency.
	// For shuffle dependencies, that task is the head task of the
	// phase, and the evaluator must expand the phase.
	Head      *Task
	Partition int

	// Expand indicates that the task's dependencies for a given
	// partition should not be merged, but rather passed individually to
	// the task implementation.
	Expand bool

	// CombineKey is an optional label that names the combination key to
	// be used by this dependency. It is used to name a single combiner
	// buffer from which is read a number of combined tasks.
	//
	// CombineKeys must be provided to tasks that contain combiners.
	CombineKey string
}

// NumTask returns the number of tasks that are comprised by this dependency.
func (d TaskDep) NumTask() int {
	if d.Head == nil {
		return 0
	}
	if n := len(d.Head.Group); n > 0 {
		return n
	}
	return 1
}

// Task returns the i'th task comprised by this dependency.
func (d TaskDep) Task(i int) *Task {
	if i == 0 {
		return d.Head
	}
	return d.Head.Group[i]
}

// A TaskName uniquely names a task by its constituent components.
// Tasks with 0 shards are taken to be combiner tasks: they are
// machine-local buffers of combiner outputs for some (non-overlapping)
// subset of shards for a task.
type TaskName struct {
	// Op is a unique string describing the operation that is provided
	// by the task.
	Op string
	// Shard and NumShard describe the shard processed by this task
	// and the total number of shards to be processed.
	Shard, NumShard int
}

// String returns a canonical representation of the task name,
// formatted as:
//
//	{n.Op}@{n.NumShard}:{n.Shard}
//	{n.Op}_combiner
func (n TaskName) String() string {
	if n.NumShard == 0 {
		return n.Op + "_combiner"
	}
	return fmt.Sprintf("%s@%d:%d", n.Op, n.NumShard, n.Shard)
}

// IsCombiner returns whether the named task is a combiner task.
func (n TaskName) IsCombiner() bool {
	return n.NumShard == 0
}

// TaskSubscriber is subscribed to a Task using Subscribe. It is then notified
// whenever the Task state changes. This is useful for efficiently observing the
// state changes of many tasks.
type TaskSubscriber struct {
	sync.Mutex
	cond *ctxsync.Cond

	// tasks holds the set of tasks that has changed since the last call to
	// Tasks.
	tasks map[*Task]struct{}
}

// NewTaskSubscriber returns a new TaskSubscriber. It needs to be subscribed to
// a Task with Subscribe for it to be notified of task state changes.
func NewTaskSubscriber() *TaskSubscriber {
	s := &TaskSubscriber{tasks: make(map[*Task]struct{})}
	s.cond = ctxsync.NewCond(s)
	return s
}

// Notify notifies s of a task whose state has changed.
func (s *TaskSubscriber) Notify(task *Task) {
	s.Lock()
	defer s.Unlock()
	s.tasks[task] = struct{}{}
	s.cond.Broadcast()
}

// Ready returns a channel that is closed if a subsequent call to Tasks will
// return a non-nil slice.
func (s *TaskSubscriber) Ready() <-chan struct{} {
	s.Lock()
	if len(s.tasks) > 0 {
		s.Unlock()
		return closedc
	}
	return s.cond.Done()
}

// Tasks returns the tasks whose state has changed since the last call to Tasks.
func (s *TaskSubscriber) Tasks() []*Task {
	s.Lock()
	defer s.Unlock()
	tasks := make([]*Task, 0, len(s.tasks))
	for task := range s.tasks {
		tasks = append(tasks, task)
	}
	s.tasks = make(map[*Task]struct{})
	return tasks
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
	slicetype.Type
	// Invocation is the task's invocation, i.e. the Func invocation
	// from which this task was compiled.
	Invocation bigslice.Invocation
	// Name is the name of the task. Tasks are named uniquely inside each
	// Bigslice session.
	Name TaskName
	// Do starts computation for this task, returning a reader that
	// computes batches of values on demand. Do is invoked with readers
	// for the task's dependencies.
	Do func([]sliceio.Reader) sliceio.Reader
	// Deps are the task's dependencies. See TaskDep for details.
	Deps []TaskDep
	// NumPartition is the number of partitions that are output by this task.
	// If NumPartition > 1, then the task must also define a partitioner.
	NumPartition int

	// Combiner specifies an (optional) combiner to use for this task's output.
	// If a Combiner is not Nil, CombineKey names the combine buffer used:
	// each combine buffer contains combiner outputs from multiple tasks.
	// If CombineKey is not set, then per-task buffers are used instead.
	Combiner   slicefunc.Func
	CombineKey string

	// Pragma comprises the pragmas of all slice operations that
	// are pipelined into this task.
	bigslice.Pragma

	// Slices is the set of slices to which this task directly contributes.
	Slices []bigslice.Slice

	// Group stores an ordered list of peer tasks. If Group is nonempty,
	// it is guaranteed that these sets of tasks constitute a shuffle
	// dependency, and share a set of shuffle dependencies. This allows
	// the evaluator to perform optimizations while tracking such
	// dependencies.
	Group []*Task

	// Scopes is the metrics scope for this task. It is populated with the
	// metrics produced during execution of this task.
	Scope metrics.Scope

	// subs is the set of subscribers to which this task will be sent whenever
	// its state changes.
	subs []*TaskSubscriber

	// The following are used to coordinate runtime execution.

	sync.Mutex
	waitc chan struct{}

	// State is the task's state. It is protected by the task's lock
	// and state changes are also broadcast on the task's condition
	// variable.
	state TaskState
	// Err is defines when state == TaskErr.
	err error

	// consecutiveLost is the number of times this task has been run and lost
	// consecutively. See maxConsecutiveLost.
	consecutiveLost int

	// Status is a status object to which task status is reported.
	Status *status.Task
}

// Phase returns the phase to which this task belongs.
func (t *Task) Phase() []*Task {
	if len(t.Group) == 0 {
		return []*Task{t}
	}
	return t.Group
}

// Head returns the head task of this task's phase. If the task does
// not belong to a phase, Head returns the task t.
func (t *Task) Head() *Task {
	if len(t.Group) == 0 {
		return t
	}
	return t.Group[0]
}

// String returns a short, human-readable string describing the
// task's state.
func (t *Task) String() string {
	// We play fast-and-loose with concurrency here (we read state and
	// err without holding the task's mutex) so that it is safe to call
	// String even when the lock is held.
	var b bytes.Buffer
	fmt.Fprintf(&b, "task %s [%d] %s", t.Name, t.Invocation.Index, t.state)
	if t.err != nil {
		fmt.Fprintf(&b, ": %v", t.err)
	}
	return b.String()
}

// Set sets the task's state to the provided state and notifies
// any waiters.
func (t *Task) Set(state TaskState) {
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

// State returns the task's current state.
func (t *Task) State() TaskState {
	t.Lock()
	state := t.state
	t.Unlock()
	return state
}

// Broadcast notifies waiters of a state change. Broadcast must only
// be called while the task's lock is held.
func (t *Task) Broadcast() {
	if t.waitc != nil {
		close(t.waitc)
		t.waitc = nil
	}
	for _, sub := range t.subs {
		sub.Notify(t)
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

// WaitState returns when the task's state is at least the provided state,
// or else when the context is done.
func (t *Task) WaitState(ctx context.Context, state TaskState) (TaskState, error) {
	t.Lock()
	defer t.Unlock()
	var err error
	for t.state < state && err == nil {
		err = t.Wait(ctx)
	}
	return t.state, err
}

// Subscribe subscribes s to be notified of any changes to t's state. If s has
// already been subscribed, no-op.
func (t *Task) Subscribe(s *TaskSubscriber) {
	t.Lock()
	defer t.Unlock()
	for _, sub := range t.subs {
		if s == sub {
			// It is already registered.
			return
		}
	}
	t.subs = append(t.subs, s)
}

// Unsubscribe unsubscribes previously subscribe s. s will on longer receive
// task state change notifications. No-op if s was never subscribed.
func (t *Task) Unsubscribe(s *TaskSubscriber) {
	t.Lock()
	defer t.Unlock()
	subs := t.subs[:0]
	for _, sub := range t.subs {
		if s == sub {
			continue
		}
		subs = append(subs, sub)
	}
	t.subs = subs
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
		fmt.Fprintf(&tw, "\t%s\t%s\t%d [%s]\n", task.Name, outstr, task.NumPartition, task.State())
	}
	tw.Flush()
	fmt.Fprintln(&tw, "dependencies:")
	t.writeDeps(&tw)
	tw.Flush()
}

func (t *Task) writeDeps(w io.Writer) {
	for _, dep := range t.Deps {
		for i := 0; i < dep.NumTask(); i++ {
			task := dep.Task(i)
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
		return tasks[i].Name.String() < tasks[j].Name.String()
	})
	return tasks
}

func (t *Task) all(tasks map[*Task]bool) {
	if tasks[t] {
		return
	}
	tasks[t] = true
	for _, dep := range t.Deps {
		for i := 0; i < dep.NumTask(); i++ {
			dep.Task(i).all(tasks)
		}
	}
}
