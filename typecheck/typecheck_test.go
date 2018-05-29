// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package typecheck

import (
	"reflect"
	"testing"

	"github.com/grailbio/bigslice/slicetype"
)

var (
	typeOfString = reflect.TypeOf("")
	typeOfInt    = reflect.TypeOf(0)
)

func TestEqual(t *testing.T) {
	for _, c := range []struct{ t1, t2 slicetype.Type }{
		{slicetype.New(typeOfString), slicetype.New()},
		{slicetype.New(typeOfInt, typeOfString), slicetype.New(typeOfString, typeOfInt)},
		{slicetype.New(typeOfInt), slicetype.New(typeOfInt, typeOfInt)},
		{slicetype.New(typeOfString), slicetype.New(typeOfInt)},
	} {
		if Equal(c.t1, c.t2) {
			t.Errorf("types %s and %s are equal", slicetype.String(c.t1), slicetype.String(c.t2))
		}
	}
	for _, typ := range []slicetype.Type{
		slicetype.New(typeOfString),
		slicetype.New(typeOfInt, typeOfString),
		slicetype.New(),
	} {
		if !Equal(typ, typ) {
			t.Errorf("type %s not equal to itself", slicetype.String(typ))
		}
	}
}

func TestSlices(t *testing.T) {
	typ, ok := Slices([]int{1, 2, 3}, []string{"a", "b", "c"})
	if !ok {
		t.Fatal("!ok")
	}
	if got, want := typ.NumOut(), 2; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := typ.Out(0), typeOfInt; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := typ.Out(1), typeOfString; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	_, ok = Slices([]int{1, 2, 3}, "ok")
	if ok {
		t.Error("ok")
	}
}

func TestDevectorize(t *testing.T) {
	tvec := slicetype.New(reflect.SliceOf(typeOfString), reflect.SliceOf(typeOfInt))
	tunvec, ok := Devectorize(tvec)
	if !ok {
		t.Fatal("!ok")
	}
	if got, want := tunvec.NumOut(), 2; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := tunvec.Out(0), typeOfString; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := tunvec.Out(1), typeOfInt; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	tvec = slicetype.New(typeOfString)
	_, ok = Devectorize(tvec)
	if ok {
		t.Error("ok")
	}
}
