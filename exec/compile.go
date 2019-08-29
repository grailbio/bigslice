// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"fmt"
	"strings"

	"github.com/grailbio/base/log"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/internal/slicecache"
	"github.com/grailbio/bigslice/sliceio"
)

// Pipeline returns the sequence of slices that may be pipelined
// starting from slice. Slices that do not have shuffle dependencies
// may be pipelined together: slices[0] depends on slices[1], and so on.
func pipeline(slice bigslice.Slice) (slices []bigslice.Slice) {
	for {
		// Stop at *Results, so we can re-use previous tasks.
		if _, ok := bigslice.Unwrap(slice).(*Result); ok {
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
func compile(namer taskNamer, inv bigslice.Invocation, slice bigslice.Slice, machineCombiners bool) (tasks []*Task, reused bool, err error) {
	// Reuse tasks from a previous invocation.
	if result, ok := bigslice.Unwrap(slice).(*Result); ok {
		return result.tasks, true, nil
	}
	// Pipeline slices and create a task for each underlying shard,
	// pipelining the eligible computations.
	tasks = make([]*Task, slice.NumShard())
	slices := pipeline(slice)
	ops := make([]string, 0, len(slices)+1)
	ops = append(ops, fmt.Sprintf("inv%x", inv.Index))
	var pragmas bigslice.Pragmas
	for i := len(slices) - 1; i >= 0; i-- {
		op, _, _ := slices[i].Op()
		ops = append(ops, op)
		if pragma, ok := slices[i].(bigslice.Pragma); ok {
			pragmas = append(pragmas, pragma)
		}
	}
	opName := namer.New(strings.Join(ops, "_"))
	for i := range tasks {
		tasks[i] = &Task{
			Type:         slices[0],
			Name:         TaskName{Op: opName, Shard: i, NumShard: len(tasks)},
			Invocation:   inv,
			NumPartition: 1,
			Pragma:       pragmas,
		}
	}
	// Capture the dependencies for this task set;
	// they are encoded in the last slice.
	lastSlice := slices[len(slices)-1]
	for i := 0; i < lastSlice.NumDep(); i++ {
		dep := lastSlice.Dep(i)
		deptasks, reused, err := compile(namer, inv, dep.Slice, machineCombiners)
		if err != nil {
			return nil, false, err
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

		// In the case where we are reusing slice results and require a
		// shuffle, we have to insert an explicit shuffle stage. This is
		// done by creating a pass-thru task for each dependency task.
		// These tasks then receive a shuffle dependency from the task set
		// we are compiling. This in turn will induce shuffling and local
		// combining at runtime.
		if reused {
			for _, task := range deptasks {
				if task.Combiner != nil {
					// TODO(marius): we may consider supporting this, but it should
					// be very rare, since it requires the user to explicitly reuse
					// an intermediate slice, which is impossible via the current
					// API.
					return nil, false, fmt.Errorf("cannot reuse task %s with combine key %s", task, task.CombineKey)
				}
			}
			newDeps := make([]*Task, len(deptasks))
			// We now insert a set of tasks whose only purpose is (re-)shuffling
			// the output from the previously completed task.
			for shard, task := range deptasks {
				newDeps[shard] = &Task{
					Type:       dep.Slice,
					Invocation: inv,
					Name: TaskName{
						Op:       fmt.Sprintf("%s_shuffle_%d", opName, i),
						Shard:    shard,
						NumShard: len(deptasks),
					},
					Do:   func(readers []sliceio.Reader) sliceio.Reader { return readers[0] },
					Deps: []TaskDep{{[]*Task{task}, 0, false, ""}},
				}
			}
			deptasks = newDeps
		}

		var combineKey string
		if lastSlice.Combiner() != nil && machineCombiners {
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

		// Each shard reads different partitions from all of the previous slice's shards.
		for partition := range tasks {
			tasks[partition].Deps = append(tasks[partition].Deps,
				TaskDep{deptasks, partition, dep.Expand, combineKey})
		}
	}
	// Pipeline execution, folding multiple frame operations
	// into a single task by composing their readers.
	// Use cache when configured.
	for i := len(slices) - 1; i >= 0; i-- {
		sliceOp, sliceFile, sliceLine := slices[i].Op()
		pprofLabel := fmt.Sprintf("%s:%s:%d(%s)", sliceOp, sliceFile, sliceLine, inv.Location)

		var shardCache *slicecache.ShardCache
		if c, ok := bigslice.Unwrap(slices[i]).(slicecache.Cacheable); ok {
			shardCache = c.Cache()
		}

		for shard := range tasks {
			var (
				shard  = shard
				reader = slices[i].Reader
				prev   = tasks[shard].Do
			)
			if prev == nil {
				// First, read the input directly.
				tasks[shard].Do = func(readers []sliceio.Reader) sliceio.Reader {
					r := reader(shard, readers)
					r = shardCache.Reader(shard, r)
					return &sliceio.PprofReader{r, pprofLabel}
				}
			} else {
				// Subsequently, read the previous pipelined slice's output.
				tasks[shard].Do = func(readers []sliceio.Reader) sliceio.Reader {
					r := reader(shard, []sliceio.Reader{prev(readers)})
					r = shardCache.Reader(shard, r)
					return &sliceio.PprofReader{r, pprofLabel}
				}
			}
			// Forget task dependencies for cached shards because we'll
			// read from the cache file.
			if shardCache.IsCached(shard) {
				tasks[shard].Deps = nil
			}
		}
	}
	return tasks, false, nil
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
