// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"sort"
	"testing"
	"time"

	fuzz "github.com/google/gofuzz"
)

// TestComputeQuartiles verifies that quartiles are computed correctly for
// select test cases.
func TestComputeQuartiles(t *testing.T) {
	for _, c := range []struct {
		name       string
		ds         []int64
		q1, q2, q3 int64
	}{
		{
			name: "OneElement",
			ds:   []int64{0},
			q1:   0,
			q2:   0,
			q3:   0,
		},
		{
			name: "TwoElementAllSame",
			ds:   []int64{0, 0},
			q1:   0,
			q2:   0,
			q3:   0,
		},
		{
			name: "TwoElement",
			ds:   []int64{0, 100},
			q1:   0,
			q2:   50,
			q3:   100,
		},
		{
			name: "ThreeElementAllSame",
			ds:   []int64{0, 0, 0},
			q1:   0,
			q2:   0,
			q3:   0,
		},
		{
			name: "ThreeElementLowSame",
			ds:   []int64{0, 0, 200},
			q1:   0,
			q2:   0,
			q3:   100,
		},
		{
			name: "ThreeElementHighSame",
			ds:   []int64{0, 200, 200},
			q1:   100,
			q2:   200,
			q3:   200,
		},
		{
			name: "ThreeElement",
			ds:   []int64{0, 100, 200},
			q1:   50,
			q2:   100,
			q3:   150,
		},
		{
			name: "FourElementAllSame",
			ds:   []int64{0, 0, 0, 0},
			q1:   0,
			q2:   0,
			q3:   0,
		},
		{
			name: "FourElementSomeSame",
			ds:   []int64{0, 100, 100, 200},
			q1:   50,
			q2:   100,
			q3:   150,
		},
		{
			name: "FourElement",
			ds:   []int64{0, 100, 200, 300},
			q1:   50,
			q2:   150,
			q3:   250,
		},
		{
			name: "FiveElementAllSame",
			ds:   []int64{0, 0, 0, 0, 0},
			q1:   0,
			q2:   0,
			q3:   0,
		},
		{
			name: "FiveElementSomeSame",
			ds:   []int64{0, 100, 100, 100, 200},
			q1:   100,
			q2:   100,
			q3:   100,
		},
		{
			name: "FiveElement",
			ds:   []int64{0, 100, 200, 300, 400},
			q1:   100,
			q2:   200,
			q3:   300,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			ds := make([]time.Duration, len(c.ds))
			for i := range ds {
				ds[i] = time.Duration(c.ds[i])
			}
			q1, q2, q3 := computeQuartiles(ds)
			if got, want := q1, time.Duration(c.q1); got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			if got, want := q2, time.Duration(c.q2); got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			if got, want := q3, time.Duration(c.q3); got != want {
				t.Errorf("got %v, want %v", got, want)
			}
		})
	}
}

// TestComputeQuartilesFuzz verifies that the quartiles computed from fuzzed
// values are within [min, max] and monotonically increasing.
func TestComputeQuartilesFuzz(t *testing.T) {
	const N = 10000
	f := fuzz.New()
	for i := 0; i < N; i++ {
		var ds []time.Duration
		f.Fuzz(&ds)
		if len(ds) == 0 {
			// computeQuartiles does not handle empty slices.
			continue
		}
		sort.Slice(ds, func(i, j int) bool {
			return ds[i] < ds[j]
		})
		var (
			min = time.Duration(1<<63 - 1)
			max = time.Duration(-1 << 63)
		)
		for _, d := range ds {
			if d < min {
				min = d
			}
			if max < d {
				max = d
			}
		}
		q1, q2, q3 := computeQuartiles(ds)
		t.Logf("test durations: %v", ds)
		if q1 < min {
			t.Errorf("%v(q1) < %v(min)", q1, min)
		}
		if q2 < q1 {
			t.Errorf("%v(q2) < %v(q1)", q2, q1)
		}
		if q3 < q2 {
			t.Errorf("%v(q3) < %v(q2)", q3, q2)
		}
		if max < q3 {
			t.Errorf("%v(max) < %v(q3)", max, q3)
		}
	}
}
