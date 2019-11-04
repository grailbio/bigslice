// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"sort"
	"strings"
	"testing"

	"github.com/grailbio/bigslice"
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
			inv := bigslice.Func(c.f).Invocation("<unknown>")
			inv.Index = 1
			slice := inv.Invoke()
			tasks, err := compile(slice, inv, false)
			if err != nil {
				t.Fatalf("compilation failed")
			}
			iterTasks(tasks, func(task *Task) {
				if task.Pragma == nil {
					t.Errorf("%v has nil task.Pragma", task)
				}
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
	sort.Sort(sort.StringSlice(g.nodes))
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
