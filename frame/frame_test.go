// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package frame

import (
	"reflect"
	"testing"

	"github.com/grailbio/bigslice/slicetype"
)

var (
	typeOfString = reflect.TypeOf("")
	typeOfInt    = reflect.TypeOf(0)
)

func TestFrame(t *testing.T) {
	f := Make(slicetype.New(typeOfString, typeOfInt), 100)
	if got, want := len(f), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := f.Len(), 100; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := f.Cap(), f.Len(); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSwapper(t *testing.T) {
	f := Columns([]int{1, 2, 3, 4}, []string{"one", "two", "three", "four"})
	g := Make(f, f.Len())
	Copy(g, f)
	swap := f.Swapper()
	swap(0, 0)
	if got, want := f, g; !Equal(got, want) {
		t.Errorf("got %v, want %v", got.TabString(), want.TabString())
	}
	swap(0, 1)
	if got, want := f, Columns([]int{2, 1, 3, 4}, []string{"two", "one", "three", "four"}); !Equal(got, want) {
		t.Errorf("got %v, want %v", got.TabString(), want.TabString())
	}
}
