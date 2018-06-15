// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sliceio

import (
	"context"
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
		out = frame.Make(f, N)
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
		n, err := ReadFull(ctx, r, frame.Make(f, 1))
		if got, want := err, EOF; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := n, 0; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	if !reflect.DeepEqual(f[0].Interface().([]string), out[0].Interface().([]string)) {
		t.Error("frames do not match")
	}
}

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
