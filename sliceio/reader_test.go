// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sliceio

import (
	"context"
	"errors"
	"reflect"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/grailbio/bigslice/frame"
)

func TestFrameReader(t *testing.T) {
	const N = 1000
	var (
		fz  = fuzz.NewWithSeed(12345)
		f   = fuzzFrame(fz, N, typeOfString)
		r   = FrameReader(f)
		out = frame.Make(f, N, N)
		ctx = context.Background()
	)
	n, err := ReadFull(ctx, r, out)
	if err != nil && err != EOF {
		t.Fatal(err)
	}
	if got, want := n, N; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if err == nil {
		n, err := ReadFull(ctx, r, frame.Make(f, 1, 1))
		if got, want := err, EOF; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := n, 0; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	if !reflect.DeepEqual(f.Interface(0).([]string), out.Interface(0).([]string)) {
		t.Error("frames do not match")
	}
}

// TestMultiReaderClose verifies that (*multiReader).Close closes all of the
// comprising readers.
func TestMultiReaderClose(t *testing.T) {
	const NReaders = 10
	var (
		fz        = fuzz.NewWithSeed(12345)
		readers   = make([]ReadCloser, NReaders)
		numClosed int
	)
	for i := range readers {
		f := fuzzFrame(fz, 1000, typeOfString)
		closeFunc := func() error {
			numClosed++
			return nil
		}
		readers[i] = ReaderWithCloseFunc{FrameReader(f), closeFunc}
	}
	r := MultiReader(readers...)
	r.Close()
	if got, want := numClosed, NReaders; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

// TestMultiReaderClose verifies that (*multiReader).Close closes all of the
// comprising readers, even if not all readers have been exhausted.
func TestMultiReaderErrClose(t *testing.T) {
	const NReaders = 10
	var (
		fz        = fuzz.NewWithSeed(12345)
		readers   = make([]ReadCloser, NReaders)
		numClosed int
	)
	for i := range readers {
		closeFunc := func() error {
			numClosed++
			return nil
		}
		// One of the readers in the middle returns an error.
		if i == 3 {
			readers[i] = ReaderWithCloseFunc{ErrReader(errors.New("some error")), closeFunc}
			continue
		}
		f := fuzzFrame(fz, 1000, typeOfString)
		readers[i] = ReaderWithCloseFunc{FrameReader(f), closeFunc}
	}
	r := MultiReader(readers...)
	r.Close()
	// Make sure all readers are closed, despite error in the middle.
	if got, want := numClosed, NReaders; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

// FuzzFrame creates a fuzzed frame of length n, where columns
// have the provided types.
func fuzzFrame(fz *fuzz.Fuzzer, n int, types ...reflect.Type) frame.Frame {
	cols := make([]interface{}, len(types))
	for i := range cols {
		v := reflect.MakeSlice(reflect.SliceOf(types[i]), n, n)
		vp := reflect.New(types[i])
		for j := 0; j < n; j++ {
			fz.Fuzz(vp.Interface())
			v.Index(j).Set(vp.Elem())
		}
		cols[i] = v.Interface()
	}
	return frame.Slices(cols...)
}
