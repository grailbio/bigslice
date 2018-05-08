// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/grailbio/base/status"
)

// A TaskDep describes a single dependency for a task. A dependency
// comprises one or more tasks and the partition number of the task
// set that must be read at run time.
type TaskDep struct {
	Tasks     []*Task
	Partition int
}

// A Task represents a concrete computational task. Tasks
// form graphs through dependencies; task graphs are compiled
// from slices.
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
	// Status is a status object to which task status is reported.
	Status *status.Task
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

// Pipeline returns the sequence of slices that may be pipelined
// starting from slice. Slices that do not have shuffle dependencies
// may be pipelined together.
func pipeline(slice Slice) (slices []Slice) {
	for {
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
func compile(namer taskNamer, inv Invocation, slice Slice) ([]*Task, error) {
	// Pipeline slices and create a task for each underlying shard,
	// pipelining the eligible computations.
	tasks := make([]*Task, slice.NumShard())
	slices := pipeline(slice)
	var ops []string
	for i := len(slices) - 1; i >= 0; i-- {
		ops = append(ops, slices[i].Op())
	}
	name := namer.New(strings.Join(ops, "_"))
	for i := range tasks {
		tasks[i] = &Task{
			Type:         slices[0],
			Name:         fmt.Sprintf("%s@%d:%d", name, len(tasks), i),
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
				tasks[shard].Do = func(readers []Reader) Reader {
					return reader(shard, readers)
				}
			} else {
				// Subsequent frames read the previous frame's output.
				tasks[shard].Do = func(readers []Reader) Reader {
					return reader(shard, []Reader{prev(readers)})
				}
			}
		}
	}
	// Now capture the dependencies; they are encoded in the last slice.
	lastSlice := slices[len(slices)-1]
	for i := 0; i < lastSlice.NumDep(); i++ {
		dep := lastSlice.Dep(i)
		deptasks, err := compile(namer, inv, dep)
		if err != nil {
			return nil, err
		}
		if !dep.Shuffle {
			panic("non-pipelined non-shuffle dependency")
		}
		// Assign a partitioner and partition width our dependencies, so that
		// these are properly partitioned at the time of computation.
		for _, task := range deptasks {
			task.NumPartition = slice.NumShard()
			task.Hasher = lastSlice.Hasher()
		}
		// Each shard reads different partitions from all of the previous tasks's shards.
		for partition := range tasks {
			tasks[partition].Deps = append(tasks[partition].Deps, TaskDep{deptasks, partition})
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
