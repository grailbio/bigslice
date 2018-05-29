// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package typecheck contains a number of typechecking and inference
// utilities for bigslice operators.
package typecheck

import (
	"reflect"

	"github.com/grailbio/bigslice/slicetype"
)

// Equal tells whether the the expected and actual slicetypes are equal.
func Equal(expect, actual slicetype.Type) bool {
	if got, want := actual.NumOut(), expect.NumOut(); got != want {
		return false
	}
	for i := 0; i < expect.NumOut(); i++ {
		if got, want := actual.Out(i), expect.Out(i); got != want {
			return false
		}
	}
	return true
}

// Slices returns a slicetype of the provided column values. If the passed
// values are not valid column values, Slices returns false.
func Slices(columns ...interface{}) (slicetype.Type, bool) {
	types := make([]reflect.Type, len(columns))
	for i, col := range columns {
		t := reflect.TypeOf(col)
		if t.Kind() != reflect.Slice {
			return nil, false
		}
		types[i] = t.Elem()
	}
	return slicetype.New(types...), true
}

// Devectorize returns a devectorized version of the provided
// slicetype: Each of the type's columns is expected to be a slice;
// the returned type unwraps the slice from each column. If the
// provided type is not a valid vectorized slice type, false is
// returned.
func Devectorize(typ slicetype.Type) (slicetype.Type, bool) {
	elems := make([]reflect.Type, typ.NumOut())
	for i := 0; i < typ.NumOut(); i++ {
		t := typ.Out(i)
		if t.Kind() != reflect.Slice {
			return nil, false
		}
		elems[i] = t.Elem()
	}
	return slicetype.New(elems...), true
}
