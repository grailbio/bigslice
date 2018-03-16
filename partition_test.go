// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"reflect"
	"testing"

	fuzz "github.com/google/gofuzz"
)

var (
	typeOfString  = reflect.TypeOf("")
	typeOfInt     = reflect.TypeOf(int(0))
	typeOfInt64   = reflect.TypeOf(int64(0))
	typeOfFloat64 = reflect.TypeOf(float64(0))

	partitionableTypes = []reflect.Type{typeOfString, typeOfInt, typeOfInt64}
)

func TestMakePartitioner(t *testing.T) {
	for _, typ := range partitionableTypes {
		if makePartitioner(typ, 0) == nil {
			t.Errorf("expected non-nil partitioner for type %v", typ)
		}
	}
	if makePartitioner(typeOfFloat64, 0) != nil {
		t.Error("expected nil partitioner for type float64")
	}
}

func TestPartitioner(t *testing.T) {
	const (
		N = 100000
		W = 10
	)
	fz := fuzz.NewWithSeed(N * W)
	// Never generate empty slices
	fz.NilChance(0)
	// Always generate slices of length N
	fz.NumElements(N, N)
	for _, typ := range partitionableTypes {
		p := makePartitioner(typ, 0)
		ptr := reflect.New(reflect.SliceOf(typ))
		fz.Fuzz(ptr.Interface())
		col := ptr.Elem()
		p1, p2 := make([]int, N, N), make([]int, N, N)
		p.Partition([]reflect.Value{col}, p1, N, W)
		p.Partition([]reflect.Value{col}, p2, N, W)
		// Test a few invariants.
		if !reflect.DeepEqual(p1, p2) {
			t.Errorf("nondeterministic partitioning for type %s", typ)
		}
		histo := make([]int, W, W)
		for i, part := range p1 {
			if part > W || part < 0 {
				t.Errorf("invalid partition %d for record %d", part, i)
			}
			histo[part]++
		}
		// Make sure they are reasonably balanced.
		var sum int
		for _, count := range histo {
			sum += count
		}
		mean := float64(sum) / float64(len(histo))
		for i, count := range histo {
			// Empty strings land partition 1 and they are generated
			// disproportionately by gofuzz.
			if typ == typeOfString && i == 1 {
				continue
			}
			prop := float64(count) / mean
			if prop < 0.9 || prop > 1.1 {
				t.Errorf("type %s partition %d is unbalanced: %g of mean", typ, i, prop)
			}
		}
	}
}
