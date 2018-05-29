// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"bytes"
	"context"
	"math/rand"
	"reflect"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/slicetype"
)

func TestDecodingReader(t *testing.T) {
	const N = 10000
	fz := fuzz.New()
	fz.NilChance(0)
	fz.NumElements(N, N)
	var (
		col1 []string
		col2 []int
	)
	fz.Fuzz(&col1)
	fz.Fuzz(&col2)
	var buf sliceBuffer
	for i := 0; i < len(col1); {
		// Pick random batch size.
		n := int(rand.Int31n(int32(len(col1) - i + 1)))
		c1, c2 := col1[i:i+n], col2[i:i+n]
		if err := buf.WriteColumns(reflect.ValueOf(c1), reflect.ValueOf(c2)); err != nil {
			t.Fatal(err)
		}
		i += n
	}

	r := newDecodingReader(&buf)
	var (
		col1x []string
		col2x []int
	)
	if err := ReadAll(context.Background(), r, &col1x, &col2x); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(col1, col1x) {
		t.Error("col1 mismatch")
	}
	if !reflect.DeepEqual(col2, col2x) {
		t.Error("col2 mismatch")
	}
}

func TestEmptyDecodingReader(t *testing.T) {
	r := newDecodingReader(bytes.NewReader(nil))
	f := frame.Make(slicetype.New(typeOfString, typeOfInt), 100)
	n, err := r.Read(context.Background(), f)
	if got, want := n, 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := err, EOF; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	n, err = r.Read(context.Background(), f)
	if got, want := n, 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := err, EOF; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
