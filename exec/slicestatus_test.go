// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"context"
	"math/rand"
	"testing"

	"github.com/grailbio/base/status"
	"github.com/grailbio/bigslice"
)

func makeFakeTasks() []*Task {
	N := 1000
	tasks := make([]*Task, N)
	for i := range tasks {
		tasks[i] = &Task{}
	}
	return tasks
}

func makeFakeSlices() []bigslice.Slice {
	N := 10
	slices := make([]bigslice.Slice, N)
	for i := range slices {
		slices[i] = bigslice.Const(1, []int{})
	}
	return slices
}

func associate(r *rand.Rand, tasks []*Task, slices []bigslice.Slice) {
	shuffled := make([]int, len(slices))
	for i := range shuffled {
		shuffled[i] = i
	}
	for _, task := range tasks {
		r.Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})
		for i := 0; i < r.Intn(len(slices)-1)+1; i++ {
			task.Slices = append(task.Slices, slices[shuffled[i]])
		}
	}
}

func simulateTasks(n int, r *rand.Rand, tasks []*Task) {
	for i := 0; i < n; i++ {
		// Pick a task randomly, and update it to a random state.
		itask := r.Intn(len(tasks))
		state := r.Intn(int(maxState))
		tasks[itask].Set(TaskState(state))
	}
}

// TestMonitorStateCounts verifies that sendStateCounts behaves reasonably with
// simulated tasks.
func TestMonitorStateCounts(t *testing.T) {
	// N is the number of task state changes to simulate.
	const N = 50000
	r := rand.New(rand.NewSource(123))
	// Set up simulation of slices and tasks.
	tasks := makeFakeTasks()
	slices := makeFakeSlices()
	associate(r, tasks, slices)
	taskCounts := make(map[bigslice.Name]int32)
	iterTasks(tasks, func(t *Task) {
		for _, slice := range t.Slices {
			taskCounts[slice.Name()]++
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	var s status.Status
	group := s.Group("slice status")
	out := make(chan stateCounts)
	go monitorStateCounts(ctx, tasks, group, out)
	go func() {
		simulateTasks(N, r, tasks)
		// We are done simulating. Cancelling the context brings everything
		// down.
		cancel()
	}()
	// Remember the last stateCounts we saw for a slice to count how often they
	// change.
	lastCounts := make(map[bigslice.Name]stateCounts)
	sliceChanges := make(map[bigslice.Name]int)
	i := 0
	for counts := range out {
		var total int32
		total += counts.idle
		total += counts.running
		total += counts.done
		total += counts.lost
		total += counts.error
		if counts != lastCounts[counts.sliceName] {
			sliceChanges[counts.sliceName]++
		}
		lastCounts[counts.sliceName] = counts
		if got, want := total < 0, false; got != want {
			// If we got a status message, there must exist at least one task:
			// the task that triggered the message.
			t.Errorf("got %v, want %v", got, want)
			continue
		}
		if got, want := taskCounts[counts.sliceName] < total, false; got != want {
			// The slice status counts more tasks than were associated with the
			// slice.
			t.Errorf("got %v, want %v", got, want)
			continue
		}
		i++
		if i > N/2 {
			if got, want := taskCounts[counts.sliceName] != total, false; got != want {
				// By now, we expect to have seen every task for every slice.
				t.Logf("slice counts total: %d, expected: %d", total, taskCounts[counts.sliceName])
				t.Errorf("got %v, want %v", got, want)
				continue
			}
		}
	}
	for _, changes := range sliceChanges {
		if got, want := changes < N/10, false; got != want {
			// We expect some changes in the task state counts.
			t.Logf("num changes: %d", changes)
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
