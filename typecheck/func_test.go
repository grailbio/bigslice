// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package typecheck

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/grailbio/bigslice/slicefunc"
	"github.com/grailbio/bigslice/slicetype"
)

type testStringer struct{}

func (testStringer) String() string {
	return "testStringer"
}

var typeOfTestStringer = reflect.TypeOf(testStringer{})

// TestCanApply verifies basic handling of function and argument types.
func TestCanApply(t *testing.T) {
	fn, ok := slicefunc.Of(func(int, string) string { return "" })
	if !ok {
		t.Fatal("not a func")
	}
	for _, c := range []struct {
		args     []reflect.Type
		canApply bool
	}{
		{[]reflect.Type{}, false},
		{[]reflect.Type{typeOfInt}, false},
		{[]reflect.Type{typeOfString}, false},
		{[]reflect.Type{typeOfInt, typeOfString}, true},
		{[]reflect.Type{typeOfInt, typeOfString, typeOfString}, false},
		{[]reflect.Type{typeOfInt, typeOfString, typeOfBool}, false},
	} {
		argTyp := slicetype.New(c.args...)
		if got, want := CanApply(fn, argTyp), c.canApply; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

// TestCanApplyVariadic verifies the ability to apply a variadic function is
// properly evaluated.
func TestCanApplyVariadic(t *testing.T) {
	fn, ok := slicefunc.Of(func(int, ...string) string { return "" })
	if !ok {
		t.Fatal("not a func")
	}
	for _, c := range []struct {
		args     []reflect.Type
		canApply bool
	}{
		{[]reflect.Type{}, false},
		{[]reflect.Type{typeOfInt}, true},
		{[]reflect.Type{typeOfString}, false},
		{[]reflect.Type{typeOfInt, typeOfString}, true},
		{[]reflect.Type{typeOfInt, typeOfString, typeOfString}, true},
		{[]reflect.Type{typeOfInt, typeOfString, typeOfBool}, false},
	} {
		argTyp := slicetype.New(c.args...)
		if got, want := CanApply(fn, argTyp), c.canApply; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

// TestCanApplyInterface verifies that types that implement interfaces can be
// passed as their interface types.
func TestCanApplyInterface(t *testing.T) {
	fn, ok := slicefunc.Of(func(int, fmt.Stringer) string { return "" })
	if !ok {
		t.Fatal("not a func")
	}
	for _, c := range []struct {
		args     []reflect.Type
		canApply bool
	}{
		{[]reflect.Type{}, false},
		{[]reflect.Type{typeOfInt}, false},
		{[]reflect.Type{typeOfString}, false},
		{[]reflect.Type{typeOfInt, typeOfString}, false},
		{[]reflect.Type{typeOfInt, typeOfTestStringer}, true},
	} {
		argTyp := slicetype.New(c.args...)
		if got, want := CanApply(fn, argTyp), c.canApply; got != want {
			t.Logf("fn.In: %v, arg: %v", fn.In, argTyp)
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
