// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import "container/heap"

type topIndex struct{ Index, Count int }

type topHeap []topIndex

func (q topHeap) Len() int           { return len(q) }
func (q topHeap) Less(i, j int) bool { return q[i].Count < q[j].Count }
func (q topHeap) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func (q *topHeap) Push(x interface{}) {
	t := x.(topIndex)
	*q = append(*q, t)
}

func (q *topHeap) Pop() interface{} {
	old := *q
	n := len(old)
	x := old[n-1]
	*q = old[:n-1]
	return x
}

func topn(counts []int, n int) []int {
	if m := len(counts); m < n {
		n = m
	}
	var q topHeap
	for i, count := range counts {
		if len(q) < n {
			heap.Push(&q, topIndex{i, count})
		} else if count > q[0].Count {
			heap.Pop(&q)
			heap.Push(&q, topIndex{i, count})
		}
	}
	indices := make([]int, n)
	for i := range indices {
		indices[i] = q[i].Index
	}
	return indices
}
