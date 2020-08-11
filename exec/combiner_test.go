// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"bytes"
	"context"
	"reflect"
	"sort"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/slicefunc"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/slicetype"
)

var typeOfInt = reflect.TypeOf(0)

func deepEqual(f, g frame.Frame) bool {
	if f.NumOut() != g.NumOut() {
		return false
	}
	for i := 0; i < f.NumOut(); i++ {
		if f.Out(i) != g.Out(i) {
			return false
		}
		if !reflect.DeepEqual(f.Interface(i), g.Interface(i)) {
			return false
		}
	}
	return true
}

func TestCombiningFrame(t *testing.T) {
	typ := slicetype.New(typeOfString, typeOfInt)
	fn, ok := slicefunc.Of(func(n, m int) int { return n + m })
	if !ok {
		t.Fatal("unexpected bad func")
	}
	f := makeCombiningFrame(typ, fn, 2, 1)
	if f == nil {
		t.Fatal("nil frame")
	}
	f.Combine(frame.Slices(
		[]string{"a", "b", "a", "a", "a"},
		[]int{1, 2, 10, 20, 30},
	))
	f.Combine(frame.Slices(
		[]string{"x", "a", "a"},
		[]int{100, 0, 0},
	))
	if got, want := f.Len(), 3; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	g := f.Compact()
	sort.Sort(g)
	if got, want := g.Interface(0).([]string), ([]string{"a", "b", "x"}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := g.Interface(1).([]int), ([]int{61, 2, 100}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestCombiningFrameManyKeys(t *testing.T) {
	const N = 100000
	typ := slicetype.New(typeOfString, typeOfInt)
	fn, ok := slicefunc.Of(func(n, m int) int { return n + m })
	if !ok {
		t.Fatal("unexpected bad func")
	}
	f := makeCombiningFrame(typ, fn, 2, 1)
	if f == nil {
		t.Fatal("nil frame")
	}
	fz := fuzz.New()
	fz.NilChance(0)
	total := make(map[string]int)
	for i := 0; i < N; i++ {
		var elems map[string]int
		fz.Fuzz(&elems)
		// add some common ones too
		elems["a"] = 123
		elems["b"] = 333
		var (
			ks []string
			vs []int
		)
		for k, v := range elems {
			ks = append(ks, k)
			vs = append(vs, v)
			total[k] += v
		}
		f.Combine(frame.Slices(ks, vs))

	}
	c := f.Compact()
	sort.Sort(c)
	keys := make([]string, 0, len(total))
	for k := range total {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if got, want := c.Len(), len(total); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i, key := range keys {
		if got, want := c.Index(0, i).String(), key; got != want {
			t.Fatalf("index %d: got %v, want %v", i, got, want)
		}
		if got, want := c.Index(1, i).Int(), int64(total[key]); got != want {
			t.Errorf("index %d: got %v, want %v", i, got, want)
		}
	}
}

func TestCombiner(t *testing.T) {
	const N = 100
	typ := slicetype.New(typeOfString, typeOfInt)
	fn, ok := slicefunc.Of(func(n, m int) int { return n + m })
	if !ok {
		t.Fatal("unexpected bad func")
	}
	// Set a small target value to ensure spilling.
	c, err := newCombiner(typ, "test", fn, 2)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	f := frame.Slices(
		[]string{"a", "a", "b", "c", "d"},
		[]int{0, 1, 2, 3, 4},
	)
	for i := 0; i < N; i++ {
		if err = c.Combine(ctx, f); err != nil {
			t.Fatal(err)
		}
	}
	var b bytes.Buffer
	n, err := c.WriteTo(ctx, sliceio.NewEncodingWriter(&b))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := n, int64(4); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	r := sliceio.NewDecodingReader(&b)
	g := frame.Make(f, int(n), int(n))
	m, err := sliceio.ReadFull(ctx, r, g)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := m, 4; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := g, frame.Slices([]string{"a", "b", "c", "d"}, []int{N, 2 * N, 3 * N, 4 * N}); !deepEqual(got, want) {
		t.Errorf("got %v, want %v", got.TabString(), want.TabString())
	}
}
