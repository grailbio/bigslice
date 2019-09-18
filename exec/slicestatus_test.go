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

// sample returns a slice of task sets randomly chosen from tasks, without
// repeat. This is used to build a task DAG for testing.
func sample(r *rand.Rand, tasks [][]*Task, n int) [][]*Task {
	tasks = append([][]*Task{}, tasks...)
	samples := make([][]*Task, n)
	for i := range samples {
		j := r.Intn(len(tasks))
		samples[i] = tasks[j]
		tasks[j] = tasks[len(tasks)-1]
		tasks = tasks[:len(tasks)-1]
	}
	return samples
}

// setUpDep sets dst to be a dependency of src.
func setUpDep(src, dst []*Task) {
	if len(src) != len(dst) {
		panic("src, dst mismatch")
	}
	for i := range src {
		src[i].Deps = append(src[i].Deps, TaskDep{Head: dst[i]})
	}
}

// makeFakeGraph builds a fake task graph, returning its roots.
func makeFakeGraph(r *rand.Rand, numSlices, numTasksPerSlice int) []*Task {
	// We build the graph by making some fake slices with fake tasks that
	// ostensibly compute them. We then a build a DAG out of these
	// slices/tasks. This is our rough approximation of what happens during
	// compilation.
	slices := make([]bigslice.Slice, numSlices)
	for i := range slices {
		slices[i] = bigslice.Const(1, []int{})
	}
	tasks := make([][]*Task, numSlices)
	for i := range tasks {
		tasks[i] = make([]*Task, numTasksPerSlice)
		for j := range tasks[i] {
			tasks[i][j] = &Task{Slices: []bigslice.Slice{slices[i]}}
		}
	}
	// We build a DAG by setting up dependencies, only ever depending on tasks
	// with a greater index in the tasks slice.
	for i := range tasks {
		if i != len(tasks)-1 {
			// Each tasks[i] depends on tasks[i+1]. This guarantees that every
			// slice/task is used in the graph.
			setUpDep(tasks[i], tasks[i+1])
		}
		// We limit the number of dependencies to vaguely mimic reality and
		// prevent an explosion in the number of edges in the fake graph.
		maxDeps := len(tasks) - i - 2
		if maxDeps <= 0 {
			continue
		}
		if maxDeps > 3 {
			maxDeps = 3
		}
		numDeps := 1 + r.Intn(maxDeps)
		deps := sample(r, tasks[i+2:], numDeps)
		for _, dep := range deps {
			setUpDep(tasks[i], dep)
		}
	}
	return tasks[0]
}

// simulateTasks simulates tasks changing states. It will simulate until ctx is
// done.
func simulateTasks(ctx context.Context, r *rand.Rand, tasks []*Task) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// Pick a task randomly, and update it to a random state.
		itask := r.Intn(len(tasks))
		currState := tasks[itask].State()
		nextState := TaskState(r.Intn(int(maxState)))
		// Spin until we get a new state. This is the only place that task state
		// is updated, so we don't have to worry about racing.
		for currState == nextState {
			nextState = TaskState(r.Intn(int(maxState)))
		}
		tasks[itask].Set(nextState)
	}
}

// TestMonitorStateCounts verifies that monitorSliceStatus behaves reasonably
// with simulated tasks.
func TestMonitorSliceStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	const numSlices = 100
	const numTasksPerSlice = 10 * 1000
	// The number of status messages to consume for the test.
	const numStatuses = 10 * 1000 * 1000
	r := rand.New(rand.NewSource(123))
	// Set up simulation of slices and tasks.
	tasks := makeFakeGraph(r, numSlices, numTasksPerSlice)
	taskCounts := make(map[bigslice.Name]int32)
	var allTasks []*Task
	var numTasks int
	iterTasks(tasks, func(t *Task) {
		numTasks++
		allTasks = append(allTasks, t)
		for _, slice := range t.Slices {
			taskCounts[slice.Name()]++
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	var s status.Status
	group := s.Group("slice status")
	statusc := make(chan sliceStatus)
	go monitorSliceStatus(ctx, tasks, group, statusc)
	go simulateTasks(ctx, r, allTasks)
	// Remember the last sliceStatus we saw for a slice to count how often it
	// changes.
	lastStatuses := make(map[bigslice.Name]sliceStatus)
	// Count the number of status changes we see per slice to verify later.
	sliceChanges := make(map[bigslice.Name]int)
	for i := 0; i < numStatuses; i++ {
		s := <-statusc
		var total int32
		for _, count := range s.counts {
			total += count
		}
		if s != lastStatuses[s.sliceName] {
			sliceChanges[s.sliceName]++
		}
		lastStatuses[s.sliceName] = s
		if got, want := total < 1, false; got != want {
			// If we got a status message, there must exist at least one task:
			// the task that triggered the message.
			t.Errorf("got %v, want %v", got, want)
			continue
		}
		if got, want := numTasksPerSlice < total, false; got != want {
			// The slice status counts more tasks than were associated with the
			// slice.
			t.Errorf("got %v, want %v", got, want)
			continue
		}
		if i > (numSlices * numTasksPerSlice) {
			if got, want := numTasksPerSlice != total, false; got != want {
				// By now, we expect to have seen every task for every slice.
				t.Logf("slice counts total: %d, expected: %d", total, taskCounts[s.sliceName])
				t.Errorf("got %v, want %v", got, want)
				continue
			}
		}
	}
	cancel()
	var totalChanges int
	for _, changes := range sliceChanges {
		totalChanges += changes
		if got, want := changes < numStatuses/numSlices/2, false; got != want {
			// We expect some changes in the task state counts.
			t.Logf("num changes: %d", changes)
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
