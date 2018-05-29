// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/slicetype"
)

func deepEqual(f, g frame.Frame) bool {
	if f.NumOut() != g.NumOut() {
		return false
	}
	for i := 0; i < f.NumOut(); i++ {
		if f.Out(i) != g.Out(i) {
			return false
		}
		if !reflect.DeepEqual(f[i].Interface(), g[i].Interface()) {
			return false
		}
	}
	return true
}

func TestCombiningFrame(t *testing.T) {
	typ := slicetype.New(typeOfString, typeOfInt)
	f := makeCombiningFrame(typ, reflect.ValueOf(func(n, m int) int { return n + m }))
	if f == nil {
		t.Fatal("nil frame")
	}
	f.Combine(frame.Columns(
		[]string{"a", "b", "a", "a", "a"},
		[]int{1, 2, 10, 20, 30},
	))
	f.Combine(frame.Columns(
		[]string{"x", "a", "a"},
		[]int{100, 0, 0},
	))
	if got, want := f.Len(), 3; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := f.Frame[0].Interface().([]string), ([]string{"a", "b", "x"}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := f.Frame[1].Interface().([]int), ([]int{61, 2, 100}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestCombiningCompact(t *testing.T) {
	typ := slicetype.New(typeOfString, typeOfInt)
	f := makeCombiningFrame(typ, reflect.ValueOf(func(n, m int) int { return n + m }))
	f.Combine(frame.Columns(
		[]string{"b", "a", "a", "a", "b", "a", "c", "d"},
		[]int{0, 1, 1, 1, 2, 1, 0, 1},
	))
	if got, want := f.Len(), 4; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	rest := f.Compact(2)
	if got, want := f.Len(), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := rest.Len(), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := f, frame.Columns([]string{"b", "a"}, []int{2, 4}); !deepEqual(got.Frame, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := rest, frame.Columns([]string{"c", "d"}, []int{0, 1}); !deepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestCombiner(t *testing.T) {
	const N = 100
	typ := slicetype.New(typeOfString, typeOfInt)
	// Set a small target value to ensure spilling.
	c, err := newCombiner(typ, "test", reflect.ValueOf(func(n, m int) int { return n + m }), 2)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	f := frame.Columns(
		[]string{"a", "a", "b", "c", "d"},
		[]int{0, 1, 2, 3, 4},
	)
	for i := 0; i < N; i++ {
		if err := c.Combine(ctx, f); err != nil {
			t.Fatal(err)
		}
	}
	var b bytes.Buffer
	n, err := c.WriteTo(ctx, NewEncoder(&b))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := n, int64(4); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	r := newDecodingReader(&b)
	g := frame.Make(f, int(n))
	m, err := ReadFull(ctx, r, g)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := m, 4; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := g, frame.Columns([]string{"a", "b", "c", "d"}, []int{N, 2 * N, 3 * N, 4 * N}); !deepEqual(got, want) {
		t.Errorf("got %v, want %v", got.TabString(), want.TabString())
	}
}
