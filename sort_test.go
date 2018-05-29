// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"context"
	"reflect"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/slicetype"
)

// FuzzFrame creates a fuzzed frame of length n, where columns
// have the provided types.
func fuzzFrame(fz *fuzz.Fuzzer, n int, types ...reflect.Type) frame.Frame {
	f := make(frame.Frame, len(types))
	for i := range f {
		f[i] = reflect.MakeSlice(reflect.SliceOf(types[i]), n, n)
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
		return 0, EOF
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
	sorter := makeSorter(typeOfString, 0)
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
		sorter  = makeSorter(typeOfString, 0)
		frames  = make([]frame.Frame, M)
		readers = make([]Reader, M)
	)
	for i := range frames {
		f := fuzzFrame(fz, N, typeOfString, typeOfString, typeOfString)
		// Replace the third column with the concatenation of the two first
		// columns so we can verify that the full rows are swapped.
		for i := 0; i < f.Len(); i++ {
			f[2].Index(i).SetString(f[0].Index(i).String() + f[1].Index(i).String())
		}
		sorter.Sort(f)
		frames[i] = f
		readers[i] = &frameReader{f}
	}

	ctx := context.Background()
	m, err := newMergeReader(ctx, frames[0], sorter, readers)
	if err != nil {
		t.Fatal(err)
	}

	out := frame.Make(frames[0], N*M)
	n, err := ReadFull(ctx, m, out)
	if err != nil && err != EOF {
		t.Fatal(err)
	}
	if got, want := n, N*M; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if !sorter.IsSorted(out) {
		t.Error("frame not sorted")
	}
	n, err = ReadFull(ctx, m, out)
	if got, want := err, EOF; got != want {
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
	sorter := makeSorter(typeOfString, 0)
	sorted, err := sortReader(ctx, sorter, 1<<19, typ, r)
	if err != nil {
		t.Fatal(err)
	}
	out := frame.Make(typ, N)
	n, err := ReadFull(ctx, sorted, out)
	if err != nil && err != EOF {
		t.Fatal(err)
	}
	if got, want := n, N; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if err == nil {
		n, err = ReadFull(ctx, sorted, frame.Make(typ, 1))
		if got, want := err, EOF; got != want {
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

type customType struct{ val int }

func TestSorterFunc(t *testing.T) {
	RegisterLessFunc(func(vals []customType) LessFunc {
		return func(i, j int) bool {
			return vals[i].val < vals[j].val
		}
	})
	f := frame.Columns(
		[]customType{{5}, {1}, {4}},
		[]string{"five", "one", "four"},
	)
	sorter := makeSorter(f.Out(0), 0)
	sorter.Sort(f)
	if !sorter.IsSorted(f) {
		t.Error("frame not sorted")
	}
	if got, want := f[0].Interface().([]customType), ([]customType{{1}, {4}, {5}}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := f[1].Interface().([]string), ([]string{"one", "four", "five"}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
