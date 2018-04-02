// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"context"
	"reflect"
	"testing"

	fuzz "github.com/google/gofuzz"
)

type testStruct struct{ A, B, C int }

var typeOfTestStruct = reflect.TypeOf((*testStruct)(nil)).Elem()

func TestTaskBuffer(t *testing.T) {
	var batches [][]string
	fz := fuzz.New()
	fz.NilChance(0)
	fz.Fuzz(&batches)
	b := make(taskBuffer, 1)
	for _, batch := range batches {
		col := reflect.ValueOf(batch)
		b[0] = append(b[0], []reflect.Value{col})
	}
	s := &Scanner{
		readers: []Reader{b.Reader(0)},
		out:     []reflect.Type{typeOfString},
	}
	var (
		i   int
		str string
		q   = batches
	)
	ctx := context.Background()
	for s.Scan(ctx, &str) {
		for len(q) > 0 && len(q[0]) == 0 {
			q = q[1:]
		}
		if len(q) == 0 {
			t.Error("long read")
			break
		}
		if got, want := str, q[0][0]; got != want {
			t.Errorf("element %d: got %v, want %v", i, got, want)
		}
		q[0] = q[0][1:]
		i++
	}
	if err := s.Err(); err != nil {
		t.Error(err)
	}
}

func TestSliceBuffer(t *testing.T) {
	const N = 100
	fz := fuzz.New()
	fz.NilChance(0)
	fz.NumElements(N, N)
	var (
		c0 []string
		c1 []testStruct
	)
	fz.Fuzz(&c0)
	fz.Fuzz(&c1)

	var buf sliceBuffer
	in := []reflect.Value{reflect.ValueOf(c0), reflect.ValueOf(c1)}
	if err := buf.WriteColumns(in...); err != nil {
		t.Fatal(err)
	}
	out := []reflect.Value{
		reflect.New(reflect.SliceOf(typeOfString)),
		reflect.New(reflect.SliceOf(typeOfTestStruct)),
	}
	if err := buf.ReadColumns(out...); err != nil {
		t.Fatal(err)
	}
	for i := range in {
		if !reflect.DeepEqual(in[i].Interface(), out[i].Elem().Interface()) {
			t.Errorf("column %d mismatch", i)
		}
	}
	// Make sure we don't reallocate if we're providing slices with enough
	// capacity already.
	outptrs := make([]uintptr, len(out))
	for i := range out {
		outptrs[i] = out[i].Pointer() // points to the slice header's data
	}
	if err := buf.WriteColumns(in...); err != nil {
		t.Fatal(err)
	}
	if err := buf.ReadColumns(out...); err != nil {
		t.Fatal(err)
	}
	for i := range out {
		if outptrs[i] != out[i].Pointer() {
			t.Errorf("column slice %d reallocated", i)
		}
	}
}
