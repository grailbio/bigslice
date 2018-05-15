// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package slicetype implements data types and utilities to describe
// Bigslice types: Slices, Frames, and Tasks all carry
// slicetype.Types.
package slicetype

import "reflect"

// A Type is the type of a set of columns.
type Type interface {
	// NumOut returns the number of columns.
	NumOut() int
	// Out returns the data type of the ith column.
	Out(i int) reflect.Type
}

type typeSlice []reflect.Type

// New returns a new Type using the provided column types.
func New(types ...reflect.Type) Type {
	return typeSlice(types)
}

func (t typeSlice) NumOut() int            { return len(t) }
func (t typeSlice) Out(i int) reflect.Type { return t[i] }

// Assignable reports whether column type in can be
// assigned to out.
func Assignable(in, out Type) bool {
	if in.NumOut() != out.NumOut() {
		return false
	}
	for i := 0; i < in.NumOut(); i++ {
		if !in.Out(i).AssignableTo(out.Out(i)) {
			return false
		}
	}
	return true
}

// Columns returns a slice of column types from the provided type.
func Columns(typ Type) []reflect.Type {
	if slice, ok := typ.(typeSlice); ok {
		return slice
	}
	out := make([]reflect.Type, typ.NumOut())
	for i := range out {
		out[i] = typ.Out(i)
	}
	return out
}
