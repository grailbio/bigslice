// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"context"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/status"
	"github.com/grailbio/bigslice"
)

// stateCounts is a snapshot of the counts of tasks in the states that we
// display in status.
type stateCounts struct {
	// sliceName is the name of the slice to which these counts apply.
	sliceName bigslice.Name
	idle      int32
	running   int32
	done      int32
	lost      int32
	error     int32
}

// Adds n to the count for state. n may be negative.
func (c *stateCounts) add(state TaskState, n int32) {
	switch state {
	case TaskInit, TaskWaiting:
		c.idle += n
	case TaskRunning:
		c.running += n
	case TaskOk:
		c.done += n
	case TaskLost:
		c.lost += n
	case TaskErr:
		c.error += n
	default:
		log.Panicf("unhandled task state: %v", state)
	}
}

// printTo prints the counts of c to t.
func (c stateCounts) printTo(t *status.Task) {
	if c.lost > 0 || c.error > 0 {
		// Provide a more detailed view if there are tasks that are lost or in
		// error.
		t.Printf("tasks idle/running/done(lost)/error: %d/%d/%d(%d)/%d",
			c.idle, c.running, c.done, c.lost, c.error)
		return
	}
	t.Printf("tasks idle/running/done: %d/%d/%d", c.idle, c.running, c.done)
}

// iterTasks calls f for each task in full graph specified by tasks.
func iterTasks(tasks []*Task, f func(*Task)) {
	for _, t := range tasks {
		f(t)
		for _, d := range t.Deps {
			iterTasks(d.Tasks, f)
		}
	}
}

// maintainSliceGroup maintains a status.Group that tracks the evaluation status
// of the slices computed by tasks. This is usually called in a goroutine and
// returns only when ctx is done.
func maintainSliceGroup(ctx context.Context, tasks []*Task, group *status.Group) {
	sliceToStatusTask := make(map[bigslice.Name]*status.Task)
	iterTasks(tasks, func(t *Task) {
		for _, s := range t.Slices {
			name := s.Name()
			if _, ok := sliceToStatusTask[name]; !ok {
				sliceToStatusTask[name] = group.Start(name.String())
			}
		}
		group.Printf("count: %d", len(sliceToStatusTask))
	})
	cCounts := make(chan stateCounts)
	go monitorStateCounts(ctx, tasks, group, cCounts)
	for counts := range cCounts {
		counts.printTo(sliceToStatusTask[counts.sliceName])
	}
	for _, statusTask := range sliceToStatusTask {
		statusTask.Printf("tasks done")
		statusTask.Done()
	}
	group.Printf("count: %d; done", len(sliceToStatusTask))
}

// monitorStateCounts continually sends stateCounts to out as the states of
// tasks are updated.
func monitorStateCounts(ctx context.Context, tasks []*Task, group *status.Group, out chan<- stateCounts) {
	// Buffer to stay out of the way of the actual task-running machinery.
	c := make(chan *Task, 128)
	taskToLastState := make(map[*Task]TaskState)
	sliceToCounts := make(map[bigslice.Name]stateCounts)
	iterTasks(tasks, func(t *Task) {
		// Subscribe to updates before we grab the initial state so that we
		// are guaranteed to see every subsequent update.
		t.Subscribe(c)
		taskState := t.State()
		taskToLastState[t] = taskState
		for _, s := range t.Slices {
			counts := sliceToCounts[s.Name()]
			counts.sliceName = s.Name()
			counts.add(taskState, 1)
			sliceToCounts[s.Name()] = counts
			out <- counts
		}
	})
	defer func() {
		iterTasks(tasks, func(t *Task) {
			t.Unsubscribe(c)
		})
	}()
	// Initial state is ready. Observe updates.
loop:
	for {
		select {
		case task := <-c:
			lastState := taskToLastState[task]
			state := task.State()
			for _, s := range task.Slices {
				counts := sliceToCounts[s.Name()]
				counts.add(lastState, -1)
				counts.add(state, 1)
				sliceToCounts[s.Name()] = counts
				out <- counts
			}
			taskToLastState[task] = state
		case <-ctx.Done():
			break loop
		}
	}
	close(out)
}
