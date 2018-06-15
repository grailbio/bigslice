// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sortio

import (
	"context"
	"reflect"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/kernel"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/slicetype"
)

var (
	typeOfString = reflect.TypeOf("")
	typeOfInt    = reflect.TypeOf(0)
)

// FuzzFrame creates a fuzzed frame of length n, where columns
// have the provided types.
func fuzzFrame(fz *fuzz.Fuzzer, n int, types ...reflect.Type) frame.Frame {
	f := make(frame.Frame, len(types))
	for i := range f {
		f[i] = frame.Column(reflect.MakeSlice(reflect.SliceOf(types[i]), n, n))
		vp := reflect.New(types[i])
		for j := 0; j < n; j++ {
			fz.Fuzz(vp.Interface())
			f[i].Index(j).Set(vp.Elem())
		}
	}
	return f
}

type fuzzReader struct {
	Fuzz *fuzz.Fuzzer
	N    int
}

func (f *fuzzReader) Read(ctx context.Context, out frame.Frame) (int, error) {
	if f.N == 0 {
		return 0, sliceio.EOF
	}
	n := out.Len()
	if f.N < n {
		n = f.N
	}
	f.N -= n
	for i := range out {
		vp := reflect.New(out[i].Type().Elem())
		for j := 0; j < n; j++ {
			f.Fuzz.Fuzz(vp.Interface())
			out[i].Index(j).Set(vp.Elem())
		}
	}
	return n, nil
}

func TestSort(t *testing.T) {
	fz := fuzz.NewWithSeed(31415)

	f := fuzzFrame(fz, 1000, typeOfString, typeOfString, typeOfString)
	// Replace the third column with the concatenation of the two first
	// columns so we can verify that the full rows are swapped.
	for i := 0; i < f.Len(); i++ {
		f[2].Index(i).SetString(f[0].Index(i).String() + f[1].Index(i).String())
	}
	var sorter kernel.Sorter
	if !kernel.Lookup(typeOfString, &sorter) {
		t.Fatal("no sorter")
	}
	if sorter.IsSorted(f) {
		t.Fatal("unlikely")
	}
	sorter.Sort(f)
	if !sorter.IsSorted(f) {
		t.Fatal("frame did not sort")
	}
	// Make sure that the full rows are swapped.
	for i := 0; i < f.Len(); i++ {
		if got, want := f[2].Index(i).String(), f[0].Index(i).String()+f[1].Index(i).String(); got != want {
			t.Errorf("row %d: got %v, want %v", i, got, want)
		}
	}
}

func TestMergeReader(t *testing.T) {
	fz := fuzz.NewWithSeed(12345)
	const (
		N = 1000
		M = 100
	)

	var (
		sorter  kernel.Sorter
		frames  = make([]frame.Frame, M)
		readers = make([]sliceio.Reader, M)
	)
	if !kernel.Lookup(typeOfString, &sorter) {
		t.Fatal("no kernel")
	}
	for i := range frames {
		f := fuzzFrame(fz, N, typeOfString, typeOfString, typeOfString)
		// Replace the third column with the concatenation of the two first
		// columns so we can verify that the full rows are swapped.
		for i := 0; i < f.Len(); i++ {
			f[2].Index(i).SetString(f[0].Index(i).String() + f[1].Index(i).String())
		}
		sorter.Sort(f)
		frames[i] = f
		readers[i] = sliceio.FrameReader(f)
	}

	ctx := context.Background()
	m, err := NewMergeReader(ctx, frames[0], sorter, readers)
	if err != nil {
		t.Fatal(err)
	}

	out := frame.Make(frames[0], N*M)
	n, err := sliceio.ReadFull(ctx, m, out)
	if err != nil && err != sliceio.EOF {
		t.Fatal(err)
	}
	if got, want := n, N*M; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if !sorter.IsSorted(out) {
		t.Error("frame not sorted")
	}
	n, err = sliceio.ReadFull(ctx, m, out)
	if got, want := err, sliceio.EOF; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := n, 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSortReader(t *testing.T) {
	const N = 1 << 20
	var (
		fz  = fuzz.NewWithSeed(12345)
		r   = &fuzzReader{fz, N}
		ctx = context.Background()
		typ = slicetype.New(typeOfString, typeOfInt)
	)
	var sorter kernel.Sorter
	if !kernel.Lookup(typeOfString, &sorter) {
		t.Fatal("no sorter")
	}
	sorted, err := SortReader(ctx, sorter, 1<<19, typ, r)
	if err != nil {
		t.Fatal(err)
	}
	out := frame.Make(typ, N)
	n, err := sliceio.ReadFull(ctx, sorted, out)
	if err != nil && err != sliceio.EOF {
		t.Fatal(err)
	}
	if got, want := n, N; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if err == nil {
		n, err = sliceio.ReadFull(ctx, sorted, frame.Make(typ, 1))
		if got, want := err, sliceio.EOF; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := n, 0; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
	if !sorter.IsSorted(out) {
		t.Error("output not sorted")
	}
}
