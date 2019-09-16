// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"testing"

	"github.com/grailbio/bigslice"
)

// TestMaterialize verifies that the Materialize pragma interrupts pipeline,
// which subsequently causes task results to be materialized.
func TestMaterialize(t *testing.T) {
	const N = 100
	f := bigslice.Func(func() (slice bigslice.Slice) {
		slice = makeMaterializeReader(N)
		slice = bigslice.Map(slice, func(i int) int { return i })
		return
	})
	// Expect N*2 tasks:
	// N for the reader
	// N for the map
	//
	// Without the Materialize pragma, this would have N tasks, as the reader
	// and map would be pipelined together.
	if got, want := countTasks(t, f), N*2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

// TestReuse verifies that results from a slice with the Materialize pragma are
// reused (i.e. downstream tasks refer to the same tasks with materialized
// results).
func TestReuse(t *testing.T) {
	const N = 100
	diamond := bigslice.Func(func() (slice bigslice.Slice) {
		slice = makeMaterializeReader(N)
		slice = bigslice.Map(slice, func(i int) int { return i })
		lhsSlice := bigslice.Map(slice, func(i int) int { return i })
		rhsSlice := bigslice.Map(slice, func(i int) int { return i })
		slice = bigslice.Cogroup(lhsSlice, rhsSlice)
		return
	})
	inv := diamond.Invocation("<unknown>")
	slice := inv.Invoke()
	tasks, _, err := compile(make(taskNamer), inv, slice, false)
	if err != nil {
		t.Fatalf("compilation failed")
	}
	var numTasks int
	iterTasks(tasks, func(task *Task) {
		numTasks++
	})
	// Expect N*4 tasks:
	// N for the cogroup
	// N for each of the two cogrouped slices
	// N for the reader
	//
	// Without the Materialize pragma, this would have N*3 tasks, as the two
	// middle map slices would each be independently pipelined with the root
	// reader.
	if got, want := numTasks, N*4; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func makeMaterializeReader(numShards int) bigslice.Slice {
	return bigslice.ReaderFunc(numShards, func(shard int, x *int, xs []int) (int, error) {
		var i int
		for i < len(xs) && *x < 1000 {
			xs[i] = i
			i++
			*x++
		}
		return i, nil
	}, bigslice.ExperimentalMaterialize)
}

func countTasks(t *testing.T, f *bigslice.FuncValue) int {
	t.Helper()
	inv := f.Invocation("<unknown>")
	slice := inv.Invoke()
	tasks, _, err := compile(make(taskNamer), inv, slice, false)
	if err != nil {
		t.Fatal("compilation failed")
	}
	var numTasks int
	iterTasks(tasks, func(task *Task) {
		numTasks++
	})
	return numTasks
}
