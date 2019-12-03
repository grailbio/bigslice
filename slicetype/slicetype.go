// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package slicetype implements data types and utilities to describe
// Bigslice types: Slices, Frames, and Tasks all carry
// slicetype.Types.
package slicetype

import (
	"fmt"
	"reflect"
	"strings"
)

// A Type is the type of a set of columns.
type Type interface {
	// NumOut returns the number of columns.
	NumOut() int
	// Out returns the data type of the ith column.
	Out(i int) reflect.Type
	// Prefix returns the number of columns in the type
	// which are considered the type's prefix. A type's
	// prefix is the set of columns which are considered
	// the type's key columns for operations like reduce.
	Prefix() int
}

type typeSlice []reflect.Type

// New returns a new Type using the provided column types.
func New(types ...reflect.Type) Type {
	return typeSlice(types)
}

func (t typeSlice) NumOut() int            { return len(t) }
func (t typeSlice) Out(i int) reflect.Type { return t[i] }
func (t typeSlice) Prefix() int            { return 1 }

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

func Concat(types ...Type) Type {
	var t typeSlice
	for _, typ := range types {
		t = append(t, Columns(typ)...)
	}
	return t
}

func String(typ Type) string {
	elems := make([]string, typ.NumOut())
	for i := range elems {
		elems[i] = typ.Out(i).String()
	}
	return fmt.Sprintf("slice[%d]%s", typ.Prefix(), strings.Join(elems, ","))
}

type appendType struct {
	t1, t2 Type
}

func (a appendType) NumOut() int {
	return a.t1.NumOut() + a.t2.NumOut()
}

func (a appendType) Out(i int) reflect.Type {
	if i < a.t1.NumOut() {
		return a.t1.Out(i)
	}
	return a.t2.Out(i - a.t1.NumOut())
}

func (a appendType) Prefix() int { return a.t1.Prefix() }

func Append(t1, t2 Type) Type {
	return appendType{t1, t2}
}

type sliceType struct {
	t    Type
	i, j int
}

func (s sliceType) NumOut() int {
	return s.j - s.i
}

func (s sliceType) Out(i int) reflect.Type {
	if i >= s.NumOut() {
		panic("invalid index")
	}
	return s.t.Out(s.i + i)
}

// BUG(marius): prefixes are lost when slicing a type.
func (s sliceType) Prefix() int {
	// TODO(marius): figure out how to properly compute
	// prefixes for appended types and sliced types.
	// This is currently only used in places which do not
	// accept prefixes anyway.
	return 1
}

func Slice(t Type, i, j int) Type {
	if i < 0 || i > t.NumOut() || j < i || j > t.NumOut() {
		panic("slice: invalid argument")
	}
	return sliceType{t, i, j}
}

// Signature returns a Go function signature for a function that takes the
// provided arguments and returns the provided values.
func Signature(arg, ret Type) string {
	args := make([]string, arg.NumOut())
	for i := range args {
		args[i] = arg.Out(i).String()
	}
	rets := make([]string, ret.NumOut())
	for i := range rets {
		rets[i] = ret.Out(i).String()
	}
	var b strings.Builder
	b.WriteString("func(")
	b.WriteString(strings.Join(args, ", "))
	b.WriteString(")")
	switch len(rets) {
	case 0:
	case 1:
		b.WriteString(" ")
		b.WriteString(rets[0])
	default:
		b.WriteString(" (")
		b.WriteString(strings.Join(rets, ", "))
		b.WriteString(")")
	}
	return b.String()
}
