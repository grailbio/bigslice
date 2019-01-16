// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"fmt"
	"strings"

	"github.com/grailbio/base/log"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/sliceio"
)

// Pipeline returns the sequence of slices that may be pipelined
// starting from slice. Slices that do not have shuffle dependencies
// may be pipelined together.
func pipeline(slice bigslice.Slice) (slices []bigslice.Slice) {
	for {
		// Stop at *Results, so we can re-use previous tasks.
		if _, ok := slice.(*Result); ok {
			return
		}
		slices = append(slices, slice)
		if slice.NumDep() != 1 {
			return
		}
		dep := slice.Dep(0)
		if dep.Shuffle {
			return
		}
		slice = dep.Slice
	}
}

// Compile compiles the provided slice into a set of task graphs,
// each representing the computation for one shard of the slice. The
// slice is produced by the provided invocation. Compile coalesces
// slice operations that can be pipelined into single tasks, creating
// wide dependencies only at shuffle boundaries. The provided namer
// must mint names that are unique to the session. The order in which
// the namer is invoked is guaranteed to be deterministic.
//
// TODO(marius): we don't currently reuse tasks across compilations,
// even though this could sometimes safely be done (when the number
// of partitions and the kind of partitioner matches at shuffle
// boundaries). We should at least support this use case to avoid
// redundant computations.
//
// TODO(marius): an alternative model for propagating invocations is
// to provide each actual invocation with a "root" slice from where
// all other slices must be derived. This simplifies the
// implementation but may make the API a little confusing.
func compile(namer taskNamer, inv bigslice.Invocation, slice bigslice.Slice) ([]*Task, error) {
	// Reuse tasks from a previous invocation.
	if result, ok := slice.(*Result); ok {
		return result.tasks, nil
	}
	// Pipeline slices and create a task for each underlying shard,
	// pipelining the eligible computations.
	tasks := make([]*Task, slice.NumShard())
	slices := pipeline(slice)
	ops := make([]string, 0, len(slices)+1)
	ops = append(ops, fmt.Sprintf("inv%x", inv.Index))
	for i := len(slices) - 1; i >= 0; i-- {
		ops = append(ops, slices[i].Op())
	}
	opName := namer.New(strings.Join(ops, "_"))
	for i := range tasks {
		tasks[i] = &Task{
			Type:         slices[0],
			Name:         TaskName{Op: opName, Shard: i, NumShard: len(tasks)},
			Invocation:   inv,
			NumPartition: 1,
		}
	}
	// Pipeline execution, folding multiple frame operations
	// into a single task by composing their readers.
	for i := len(slices) - 1; i >= 0; i-- {
		for shard := range tasks {
			var (
				shard  = shard
				reader = slices[i].Reader
				prev   = tasks[shard].Do
			)
			if prev == nil {
				// First frame reads the input directly.
				tasks[shard].Do = func(readers []sliceio.Reader) sliceio.Reader {
					return reader(shard, readers)
				}
			} else {
				// Subsequent frames read the previous frame's output.
				tasks[shard].Do = func(readers []sliceio.Reader) sliceio.Reader {
					return reader(shard, []sliceio.Reader{prev(readers)})
				}
			}
		}
	}
	// Now capture the dependencies for this task set;
	// they are encoded in the last slice.
	lastSlice := slices[len(slices)-1]
	for i := 0; i < lastSlice.NumDep(); i++ {
		dep := lastSlice.Dep(i)
		deptasks, err := compile(namer, inv, dep.Slice)
		if err != nil {
			return nil, err
		}
		// These needn't be shuffle deps, for example if we terminated
		// pipelining early because we're reusing a result or because we're
		// doing a shuffle-free join.
		if !dep.Shuffle {
			if len(tasks) != len(deptasks) {
				log.Panicf("tasks:%d deptasks:%d", len(tasks), len(deptasks))
			}
			for shard := range tasks {
				tasks[shard].Deps = append(tasks[shard].Deps,
					TaskDep{[]*Task{deptasks[shard]}, 0, dep.Expand, ""})
			}
			continue
		}

		var combineKey string
		if lastSlice.Combiner() != nil {
			combineKey = opName
		}
		// Assign a partitioner and partition width our dependencies, so that
		// these are properly partitioned at the time of computation.
		for _, task := range deptasks {
			task.NumPartition = slice.NumShard()
			// Assign a combine key that's based on the root name of the task.
			// This is a name that's unique in the task namespace and is used to
			// coalesce combiners on a single machine.
			task.Combiner = lastSlice.Combiner()
			task.CombineKey = combineKey
		}

		// Each shard reads different partitions from all of the previous tasks's shards.
		for partition := range tasks {
			tasks[partition].Deps = append(tasks[partition].Deps,
				TaskDep{deptasks, partition, dep.Expand, combineKey})
		}
	}
	return tasks, nil
}

type taskNamer map[string]int

func (n taskNamer) New(name string) string {
	c := n[name]
	n[name]++
	if c == 0 {
		return name
	}
	return fmt.Sprintf("%s%d", name, c)
}
