// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"sort"
	"strings"
	"testing"

	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/internal/slicecache"
	"github.com/grailbio/bigslice/slicefunc"
	"github.com/grailbio/bigslice/sliceio"
)

func TestCompile(t *testing.T) {
	for _, c := range []struct {
		name string
		f    func() bigslice.Slice
	}{
		{
			"trivial",
			func() (slice bigslice.Slice) {
				slice = bigslice.Const(3, []int{})
				return
			},
		},
		{
			"shuffle",
			func() (slice bigslice.Slice) {
				slice = bigslice.Const(3, []int{}, []float64{})
				slice = bigslice.Reduce(slice, func(v0, v1 float64) float64 { return v0 + v1 })
				return
			},
		},
		{
			// Branch where both branches pipeline with the subsequent maps.
			"branch",
			func() (slice bigslice.Slice) {
				slice = bigslice.Const(3, []int{})
				slice = bigslice.Map(slice, func(i int) int { return i })
				slice0 := bigslice.Map(slice, func(i int) int { return i })
				slice1 := bigslice.Map(slice, func(i int) int { return i })
				slice = bigslice.Cogroup(slice0, slice1)
				return
			},
		},
		{
			// Branch from a materialized slice, so the subsequent maps are not
			// pipelined through the materialized tasks.
			"branch-materialize",
			func() (slice bigslice.Slice) {
				slice = bigslice.Const(3, []int{})
				slice = bigslice.Map(slice, func(i int) int { return i }, bigslice.ExperimentalMaterialize)
				slice0 := bigslice.Map(slice, func(i int) int { return i })
				slice1 := bigslice.Map(slice, func(i int) int { return i })
				slice = bigslice.Cogroup(slice0, slice1)
				return
			},
		},
		{
			// Branch the const slice with a reduce, which introduces its own
			// shuffle/combiner, so the const slice tasks cannot be reused.
			"branch-shuffle",
			func() (slice bigslice.Slice) {
				slice = bigslice.Const(3, []int{}, []float64{})
				slice0 := bigslice.Reduce(slice, func(v0, v1 float64) float64 { return v0 + v1 })
				slice = bigslice.Cogroup(slice, slice0)
				return
			},
		},
		{
			// Branch where each branch demands the same partition number from
			// the branch point slice. In this case, the branch point tasks can
			// be reused.
			"branch-same-partitions",
			func() (slice bigslice.Slice) {
				slice = bigslice.Const(3, []int{})
				slice = bigslice.Map(slice, func(i int) int { return i })
				slice0 := bigslice.Reshard(slice, 2)
				slice1 := bigslice.Reshard(slice, 2)
				slice = bigslice.Cogroup(slice0, slice1)
				return
			},
		},
		{
			// Branch where each branch demands different partition numbers from
			// the branch point slice. In this case, the branch point tasks
			// cannot be reused.
			"branch-different-partitions",
			func() (slice bigslice.Slice) {
				slice = bigslice.Const(3, []int{})
				slice = bigslice.Map(slice, func(i int) int { return i })
				slice0 := bigslice.Reshard(slice, 1)
				slice1 := bigslice.Reshard(slice, 2)
				slice = bigslice.Cogroup(slice0, slice1)
				return
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := bigslice.Func(c.f)
			inv := makeExecInvocation(f.Invocation("<unknown>"))
			inv.Index = 1
			slice := inv.Invoke()
			tasks, err := compile(inv, slice, false)
			if err != nil {
				t.Fatalf("compilation failed")
			}
			_ = iterTasks(tasks, func(task *Task) error {
				if task.Pragma == nil {
					t.Errorf("%v has nil task.Pragma", task)
				}
				return nil
			})
			g := makeGraph(tasks)
			want, err := ioutil.ReadFile("testdata/" + c.name + ".graph")
			if err != nil {
				t.Fatalf("error reading graph: %v", err)
			}
			d := lineDiff(g.String(), string(want))
			if d != "" {
				t.Errorf("differs from %s.graph:\n%s", c.name, d)
			}
		})
	}
}

// TestCompileEnv verifies that the compileEnv is used and behaves properly,
// specifically verifying that compilation correctly writes to writable
// environments and reads from non-writable environments.
func TestCompileEnv(t *testing.T) {
	const Nshard = 8

	// cachedShards is set up just before we invoke the Func. It represents the
	// fake cache state from the perspective of that invocation.
	var cachedShards []int
	f := bigslice.Func(func() bigslice.Slice {
		slice := bigslice.Const(Nshard, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
		// Break the pipeline, as we use this to detect for which compiled tasks
		// compilation considered the cache valid. If the cache is valid, the
		// compiled root task will have no dependencies.
		slice = bigslice.Reshuffle(slice)
		slice = fakeCache(slice, cachedShards)
		return slice
	})
	inv := makeExecInvocation(f.Invocation("<unknown>"))
	inv.Index = 0

	cachedShardsFrozen := []int{1, 4, 5}
	cachedShards = cachedShardsFrozen
	slice0 := inv.Invoke()
	tasks, err := compile(inv, slice0, false)
	if err != nil {
		t.Fatalf("compilation failed")
	}
	for _, task := range tasks {
		var cached bool
		for _, shard := range cachedShardsFrozen {
			if shard == task.Name.Shard {
				cached = true
			}
		}
		// Verify that the resulting tasks reflect the cache state.
		if got, want := len(task.Deps) == 0, cached; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	// Freeze the environment, and verify that compilation uses the environment
	// and not the current cache state.
	inv.Env.Freeze()
	cachedShards = []int{2, 4, 7} // different cache state from above.
	slice1 := inv.Invoke()
	tasks, err = compile(inv, slice1, false)
	if err != nil {
		t.Fatalf("compilation failed")
	}
	for _, task := range tasks {
		var cached bool
		for _, shard := range cachedShardsFrozen {
			if shard == task.Name.Shard {
				cached = true
			}
		}
		// Verify that the tasks are compiled according to the environment that
		// reflects cachedShardsFrozen, and not the current cache state in
		// cachedShards.
		if got, want := len(task.Deps) == 0, cached; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

// TestPipelinedCache verifies that cacheable slices that are pipelined for
// execution behave as we expect.
func TestPipelinedCache(t *testing.T) {
	const Nshard = 8
	f := bigslice.Func(func() bigslice.Slice {
		slice := bigslice.Const(Nshard, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
		// Break the pipeline, as we use this to detect for which compiled tasks
		// compilation considered the cache valid. If the cache is valid, the
		// compiled root task will have no dependencies.
		slice = bigslice.Reshuffle(slice)
		id := func(i int) int { return i }
		// These slices will be pipelined.  We set it up with different shards
		// cached in different slices, with some shards not cached at all.
		// When we examine the resulting dependencies of the (pipelined) task,
		// we should only see dependencies for shards without any cache, as
		// only those need the upstream results.
		slice = fakeCache(bigslice.Map(slice, id), []int{0, 2})
		slice = bigslice.Map(slice, id)
		slice = fakeCache(bigslice.Map(slice, id), []int{5, 7})
		slice = bigslice.Map(slice, id)
		return slice
	})
	inv := makeExecInvocation(f.Invocation("<unknown>"))
	inv.Index = 0
	slice0 := inv.Invoke()
	tasks, err := compile(inv, slice0, false)
	if err != nil {
		t.Fatalf("compilation failed")
	}
	// These are all the shards that we expect to be computable without
	// dependencies, as some part of the (pipelined) computation is cached.
	// This is the union of the shards cached in our fakeCache slices.
	noDeps := []int{0, 2, 5, 7}
	for _, task := range tasks {
		var inNoDeps bool
		for _, shard := range noDeps {
			if shard == task.Name.Shard {
				inNoDeps = true
			}
		}
		// Verify that the resulting tasks reflect the cache state.
		if got, want := len(task.Deps) == 0, inNoDeps; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		// Invoke Do to verify that we can construct our pipelined computation.
		// There have been bugs for which this call would panic.  Note that
		// this is somewhat fragile, as we assume that Do does not access the
		// input readers, instead only composing readers to represent the
		// pipeline.
		task.Do([]sliceio.Reader{sliceio.EmptyReader{}})
	}
}

// makeGraph returns a graph representation of the task graph roots that is
// convenient for printing and comparing. We use this to verify (and debug)
// compilation results.
func makeGraph(roots []*Task) graph {
	var (
		visited = make(map[*Task]bool)
		g       graph
		walk    func(tasks []*Task)
	)
	walk = func(tasks []*Task) {
		if len(tasks) == 0 {
			return
		}
		for _, t := range tasks {
			if visited[t] {
				continue
			}
			visited[t] = true
			g.nodes = append(g.nodes, t.Name.String())
			for _, d := range t.Deps {
				for i := 0; i < d.NumTask(); i++ {
					edge := edge{t.Name.String(), d.Task(i).Name.String()}
					g.edges = append(g.edges, edge)
					walk([]*Task{d.Task(i)})
				}
			}

		}
	}
	walk(roots)
	g.Sort()
	return g
}

type edge struct {
	src string
	dst string
}

type graph struct {
	nodes []string
	edges []edge
}

func (g graph) Sort() {
	sort.Strings(g.nodes)
	sort.Slice(g.edges, func(i, j int) bool {
		if g.edges[i].src != g.edges[j].src {
			return g.edges[i].src < g.edges[j].src
		}
		return g.edges[i].dst < g.edges[j].dst
	})
}

func (g graph) String() string {
	var b bytes.Buffer
	for _, n := range g.nodes {
		fmt.Fprintf(&b, "%s\n", n)
	}
	for _, e := range g.edges {
		fmt.Fprintf(&b, "%s -> %s\n", e.src, e.dst)
	}
	return b.String()
}

func lineDiff(lhs, rhs string) string {
	lhsLines := strings.Split(lhs, "\n")
	rhsLines := strings.Split(rhs, "\n")

	// This is a vanilla Levenshtein distance implementation.
	const (
		editNone = iota
		editAdd
		editDel
		editRep
	)
	type cell struct {
		edit int
		cost int
	}
	cells := make([][]cell, len(lhsLines)+1)
	for i := range cells {
		cells[i] = make([]cell, len(rhsLines)+1)
	}
	for i := 1; i < len(lhsLines)+1; i++ {
		cells[i][0].edit = editDel
		cells[i][0].cost = i
	}
	for j := 1; j < len(rhsLines)+1; j++ {
		cells[0][j].edit = editAdd
		cells[0][j].cost = j
	}
	for i := 1; i < len(lhsLines)+1; i++ {
		for j := 1; j < len(rhsLines)+1; j++ {
			if lhsLines[i-1] == rhsLines[j-1] {
				cells[i][j].cost = cells[i-1][j-1].cost
				continue
			}
			repCost := cells[i-1][j-1].cost + 1
			minCost := repCost
			delCost := cells[i-1][j].cost + 1
			if delCost < minCost {
				minCost = delCost
			}
			addCost := cells[i][j-1].cost + 1
			if addCost < minCost {
				minCost = addCost
			}
			cells[i][j].cost = minCost
			switch minCost {
			case repCost:
				cells[i][j].edit = editRep
			case addCost:
				cells[i][j].edit = editAdd
			case delCost:
				cells[i][j].edit = editDel
			}
		}
	}
	var (
		d      []string
		differ bool
	)
	for i, j := len(lhsLines), len(rhsLines); i > 0 || j > 0; {
		switch cells[i][j].edit {
		case editNone:
			d = append(d, lhsLines[i-1])
			i--
			j--
		case editAdd:
			d = append(d, "+ "+rhsLines[j-1])
			j--
			differ = true
		case editDel:
			d = append(d, "- "+lhsLines[i-1])
			i--
			differ = true
		case editRep:
			d = append(d, "+ "+rhsLines[j-1])
			d = append(d, "- "+lhsLines[i-1])
			i--
			j--
			differ = true
		}
	}
	if !differ {
		return ""
	}
	for i := len(d)/2 - 1; i >= 0; i-- {
		opp := len(d) - 1 - i
		d[i], d[opp] = d[opp], d[i]
	}
	var b bytes.Buffer
	for _, dLine := range d {
		b.WriteString(dLine + "\n")
	}
	return b.String()
}

type fakeShardCache struct {
	cachedSet map[int]bool
}

func (c fakeShardCache) IsCached(shard int) bool { return c.cachedSet[shard] }
func (fakeShardCache) WritethroughReader(shard int, reader sliceio.Reader) sliceio.Reader {
	return reader
}
func (fakeShardCache) CacheReader(shard int) sliceio.Reader {
	return emptyReader{}
}

type emptyReader struct{}

func (emptyReader) Read(ctx context.Context, frame frame.Frame) (int, error) {
	return 0, sliceio.EOF
}

type fakeCacheSlice struct {
	name bigslice.Name
	bigslice.Slice
	cache slicecache.ShardCache
}

func (c *fakeCacheSlice) Name() bigslice.Name { return c.name }
func (c *fakeCacheSlice) NumDep() int         { return 1 }
func (c *fakeCacheSlice) Dep(i int) bigslice.Dep {
	return bigslice.Dep{
		Slice:       c.Slice,
		Shuffle:     false,
		Partitioner: nil,
		Expand:      false,
	}
}
func (*fakeCacheSlice) Combiner() slicefunc.Func                                 { return slicefunc.Nil }
func (c *fakeCacheSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader { return deps[0] }
func (c *fakeCacheSlice) Cache() slicecache.ShardCache                           { return c.cache }

func fakeCache(slice bigslice.Slice, cachedShards []int) bigslice.Slice {
	cachedSet := make(map[int]bool)
	for _, shard := range cachedShards {
		cachedSet[shard] = true
	}
	return &fakeCacheSlice{bigslice.MakeName("testcache"), slice, fakeShardCache{cachedSet}}
}
