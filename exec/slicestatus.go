// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"context"

	"github.com/grailbio/base/status"
	"github.com/grailbio/bigslice"
)

// sliceStatus is the information directly used to print a slice's status (in a
// *status.Task).
type sliceStatus struct {
	// sliceName is the name of the slice to which this status information
	// applies.
	sliceName bigslice.Name
	// counts is the count of tasks of the slice, by TaskState.
	counts [maxState]int32
}

// idleCount returns the number of tasks considered idle for status display.
func (s sliceStatus) idleCount() int32 {
	return s.counts[TaskInit] + s.counts[TaskWaiting]
}

// printTo prints s to t, translating our slice status information to a
// status.Task update.
func (s sliceStatus) printTo(t *status.Task) {
	if s.counts[TaskLost] > 0 || s.counts[TaskErr] > 0 {
		// Provide a more detailed view if there are tasks that are lost or in
		// error.
		t.Printf("tasks idle/running/done(lost)/error: %d/%d/%d(%d)/%d",
			s.idleCount(), s.counts[TaskRunning], s.counts[TaskOk],
			s.counts[TaskLost], s.counts[TaskErr])
		return
	}
	t.Printf("tasks idle/running/done: %d/%d/%d", s.idleCount(),
		s.counts[TaskRunning], s.counts[TaskOk])
}

// iterTasks calls f for each task in the full graph specified by tasks. It is
// post-order DFS so that tasks are visited in a valid execution order. We use
// this property to display slice status in a sensible order.
func iterTasks(tasks []*Task, f func(*Task)) {
	visited := make(map[*Task]struct{})
	var walk func([]*Task)
	walk = func(tasks []*Task) {
		if len(tasks) == 0 {
			return
		}
		// This optimization to only use the first task as a marker for
		// visitation is safe because task slices that result from compilation
		// are either identical or mutually exclusive.
		if _, ok := visited[tasks[0]]; ok {
			return
		}
		visited[tasks[0]] = struct{}{}
		for _, t := range tasks {
			for _, d := range t.Deps {
				walk(d.Tasks)
			}
			f(t)
		}
	}
	walk(tasks)
}

// maintainSliceGroup maintains a status.Group that tracks the evaluation status
// of the slices computed by tasks. This is usually called in a goroutine and
// returns only when ctx is done.
func maintainSliceGroup(ctx context.Context, tasks []*Task, group *status.Group) {
	sliceToStatusTask := make(map[bigslice.Name]*status.Task)
	// We set up a status.Task for each slice computed by the given task graph.
	iterTasks(tasks, func(t *Task) {
		for i := len(t.Slices) - 1; i >= 0; i-- {
			// The slices are in dependency order, so we visit them in reverse
			// to get them in execution order.
			s := t.Slices[i]
			if _, ok := sliceToStatusTask[s.Name()]; !ok {
				sliceToStatusTask[s.Name()] = group.Start(s.Name().String())
			}
		}
	})
	group.Printf("count: %d", len(sliceToStatusTask))
	statusc := make(chan sliceStatus)
	go monitorSliceStatus(ctx, tasks, group, statusc)
	for status := range statusc {
		status.printTo(sliceToStatusTask[status.sliceName])
	}
	for _, statusTask := range sliceToStatusTask {
		statusTask.Printf("tasks done")
		statusTask.Done()
	}
	group.Printf("count: %d; done", len(sliceToStatusTask))
}

// monitorSliceStatus continually sends sliceStatus to statusc as the states of
// tasks are updated. It will only return only when ctx is done.
func monitorSliceStatus(ctx context.Context, tasks []*Task, group *status.Group, statusc chan<- sliceStatus) {
	sub := NewTaskSubscriber()
	taskToLastState := make(map[*Task]TaskState)
	sliceToStatus := make(map[bigslice.Name]sliceStatus)
	iterTasks(tasks, func(t *Task) {
		// Subscribe to updates before we grab the initial state so that we
		// are guaranteed to see every subsequent update.
		t.Subscribe(sub)
		taskState := t.State()
		taskToLastState[t] = taskState
		for _, s := range t.Slices {
			status := sliceToStatus[s.Name()]
			status.sliceName = s.Name()
			status.counts[taskState]++
			sliceToStatus[s.Name()] = status
			statusc <- status
		}
	})
	defer func() {
		iterTasks(tasks, func(t *Task) {
			t.Unsubscribe(sub)
		})
	}()
	// Initial state is ready. Observe updates.
	for {
		select {
		case <-sub.Ready():
			for _, task := range sub.Tasks() {
				lastState := taskToLastState[task]
				state := task.State()
				for _, s := range task.Slices {
					status := sliceToStatus[s.Name()]
					status.counts[lastState]--
					status.counts[state]++
					sliceToStatus[s.Name()] = status
					statusc <- status
				}
				taskToLastState[task] = state
			}
		case <-ctx.Done():
			close(statusc)
			return
		}
	}
}
