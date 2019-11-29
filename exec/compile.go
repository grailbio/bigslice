// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"fmt"
	"strings"

	"github.com/grailbio/base/log"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/internal/slicecache"
	"github.com/grailbio/bigslice/slicefunc"
	"github.com/grailbio/bigslice/sliceio"
)

func defaultPartitioner(frame frame.Frame, nshard int, shards []int) {
	for i := range shards {
		shards[i] = int(frame.Hash(i) % uint32(nshard))
	}
}

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
		if pragma, ok := dep.Slice.(bigslice.Pragma); ok && pragma.Materialize() {
			return
		}
		slice = dep.Slice
	}
}

// memoKey is the memo key for memoized slice compilations.
type memoKey struct {
	slice bigslice.Slice
	// numPartition is the number of partitions in the output of the memoized
	// compiled tasks.
	numPartition int
}

// partitioner configures the output partitioning of compiled tasks. The zero
// value indicates that the output of the tasks are not for a shuffle
// dependency.
type partitioner struct {
	// numPartition is the number of partitions in the output for a shuffle
	// dependency, if >1. If 0, the output is not used by a shuffle.
	numPartition int
	partitioner  bigslice.Partitioner
	Combiner     slicefunc.Func
	CombineKey   string
}

// IsShuffle returns whether the task output is used by a shuffle dependency.
func (p partitioner) IsShuffle() bool {
	return p.numPartition != 0
}

// Partitioner returns the partitioner to be used to partition the output of
// this task.
func (p partitioner) Partitioner() bigslice.Partitioner {
	if p.partitioner == nil {
		return defaultPartitioner
	}
	return p.partitioner
}

// NumPartition returns the number of partitions that the task output should
// have. If this is not a shuffle dependency, returns 1.
func (p partitioner) NumPartition() int {
	if p.numPartition == 0 {
		return 1
	}
	return p.numPartition
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
func compile(slice bigslice.Slice, inv bigslice.Invocation, machineCombiners bool) (tasks []*Task, err error) {
	c := compiler{make(taskNamer), inv, machineCombiners, make(map[memoKey][]*Task)}
	// Top-level compilation always produces tasks that write single partitions,
	// as they are materialized and will not be used as direct shuffle
	// dependencies.
	tasks, err = c.compile(slice, partitioner{})
	return
}

type compiler struct {
	namer            taskNamer
	inv              bigslice.Invocation
	machineCombiners bool
	memo             map[memoKey][]*Task
}

// compile compiles the provided slice into a set of task graphs, memoizing the
// compilation so that tasks can be reused within the invocation.
func (c *compiler) compile(slice bigslice.Slice, part partitioner) (tasks []*Task, err error) {
	// We never reuse combiner tasks, as we currently don't have a way of
	// identifying equivalent combiner functions.
	if part.Combiner.IsNil() {
		// TODO(jcharumilind): Repartition already-computed data instead of
		// forcing recomputation of the slice if we get a different
		// numPartition.
		key := memoKey{slice: slice, numPartition: part.numPartition}
		if memoTasks, ok := c.memo[key]; ok {
			// We're compiling the same slice with the same number of partitions
			// (and no combiner), so we can safely reuse the tasks.
			return memoTasks, nil
		}
		defer func() {
			if err != nil {
				return
			}
			c.memo[key] = tasks
		}()
	}
	// Beyond this point, any tasks used for shuffles are new and need to have
	// task groups set up for phasic evaluation.
	defer func() {
		if part.IsShuffle() {
			for _, task := range tasks {
				task.Group = tasks
			}
		}
	}()
	// Reuse tasks from a previous invocation.
	if result, ok := bigslice.Unwrap(slice).(*Result); ok {
		for _, task := range result.tasks {
			if !task.Combiner.IsNil() {
				// TODO(marius): we may consider supporting this, but it should
				// be very rare, since it requires the user to explicitly reuse
				// an intermediate slice, which is impossible via the current
				// API.
				return nil, fmt.Errorf("cannot reuse task %s with combine key %s", task, task.CombineKey)
			}
		}
		if !part.IsShuffle() {
			tasks = result.tasks
			return
		}
		// We now insert a set of tasks whose only purpose is (re-)shuffling
		// the output from the previously completed task.
		shuffleOpName := c.namer.New(fmt.Sprintf("%s_shuffle", result.tasks[0].Name.Op))
		tasks = make([]*Task, len(result.tasks))
		for shard, task := range result.tasks {
			tasks[shard] = &Task{
				Type:       slice,
				Invocation: c.inv,
				Name: TaskName{
					Op:       shuffleOpName,
					Shard:    shard,
					NumShard: len(result.tasks),
				},
				Do:     func(readers []sliceio.Reader) sliceio.Reader { return readers[0] },
				Deps:   []TaskDep{{task, 0, false, ""}},
				Pragma: task.Pragma,
				Slices: task.Slices,
			}
		}
		return
	}
	// Pipeline slices and create a task for each underlying shard, pipelining
	// the eligible computations.
	slices := pipeline(slice)
	defer func() {
		for _, task := range tasks {
			task.Slices = slices
		}
	}()
	var pragmas bigslice.Pragmas
	ops := make([]string, 0, len(slices)+1)
	ops = append(ops, fmt.Sprintf("inv%d", c.inv.Index))
	for i := len(slices) - 1; i >= 0; i-- {
		ops = append(ops, slices[i].Name().Op)
		if pragma, ok := slices[i].(bigslice.Pragma); ok {
			pragmas = append(pragmas, pragma)
		}
	}
	opName := c.namer.New(strings.Join(ops, "_"))
	tasks = make([]*Task, slice.NumShard())
	for i := range tasks {
		tasks[i] = &Task{
			Type:         slices[0],
			Name:         TaskName{Op: opName, Shard: i, NumShard: len(tasks)},
			Invocation:   c.inv,
			Pragma:       pragmas,
			NumPartition: part.NumPartition(),
			Partitioner:  part.Partitioner(),
			Combiner:     part.Combiner,
			CombineKey:   part.CombineKey,
		}
	}
	// Capture the dependencies for this task set; they are encoded in the last
	// slice.
	lastSlice := slices[len(slices)-1]
	for i := 0; i < lastSlice.NumDep(); i++ {
		dep := lastSlice.Dep(i)
		if !dep.Shuffle {
			depTasks, err := c.compile(dep.Slice, partitioner{})
			if err != nil {
				return nil, err
			}
			if len(tasks) != len(depTasks) {
				log.Panicf("tasks:%d deptasks:%d", len(tasks), len(depTasks))
			}
			for shard := range tasks {
				tasks[shard].Deps = append(tasks[shard].Deps,
					TaskDep{depTasks[shard], 0, dep.Expand, ""})
			}
			continue
		}
		var combineKey string
		if !lastSlice.Combiner().IsNil() && c.machineCombiners {
			combineKey = opName
		}
		depPart := partitioner{
			slice.NumShard(), dep.Partitioner,
			lastSlice.Combiner(), combineKey,
		}
		depTasks, err := c.compile(dep.Slice, depPart)
		if err != nil {
			return nil, err
		}
		// Each shard reads different partitions from all of the previous slice's shards.
		for partition := range tasks {
			tasks[partition].Deps = append(tasks[partition].Deps,
				TaskDep{depTasks[0], partition, dep.Expand, combineKey})
		}
	}
	// Pipeline execution, folding multiple frame operations
	// into a single task by composing their readers.
	// Use cache when configured.
	for i := len(slices) - 1; i >= 0; i-- {
		var (
			pprofLabel = fmt.Sprintf("%s(%s)", slices[i].Name(), c.inv.Location)
			reader     = slices[i].Reader
			shardCache *slicecache.ShardCache
		)
		if c, ok := bigslice.Unwrap(slices[i]).(slicecache.Cacheable); ok {
			shardCache = c.Cache()
		}
		for shard := range tasks {
			var (
				shard = shard
				prev  = tasks[shard].Do
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
	return
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
