// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"reflect"
	"testing"

	"github.com/grailbio/bigslice/slicetype"
)

func TestCombiner(t *testing.T) {
	typ := slicetype.New(typeOfString, typeOfInt)
	f := makeCombiningFrame(typ, reflect.ValueOf(func(n, m int) int { return n + m }))
	if f == nil {
		t.Fatal("nil frame")
	}
	f.Combine(Columns(
		[]string{"a", "b", "a", "a", "a"},
		[]int{1, 2, 10, 20, 30},
	))
	f.Combine(Columns(
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
