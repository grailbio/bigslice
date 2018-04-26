// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import "reflect"

// A Type is the type of a set of columns.
type Type interface {
	// NumOut returns the number of columns.
	NumOut() int
	// Out returns the data type of the ith column.
	Out(i int) reflect.Type
}

type typeSlice []reflect.Type

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

// A Frame is a list of column vectors of equal lengths (i.e., it's
// rectangular). Each column is represented as a reflect.Value that
// encapsulates a slice of values representing the column vector.
// Frames provide a set of methods that operate over the underlying
// column vectors in a uniform fashion.
type Frame []reflect.Value

// MakeFrame creates a new Frame of the given type, length, and
// capacity. If the capacity argument is omitted, a frame with
// capacity equal to the provided length is returned.
func MakeFrame(types Type, frameLen int, frameCap ...int) Frame {
	var cap int
	switch len(frameCap) {
	case 0:
		cap = frameLen
	case 1:
		cap = frameCap[0]
	default:
		panic("invalid lencap")
	}
	f := make(Frame, types.NumOut())
	for i := range f {
		f[i] = reflect.MakeSlice(reflect.SliceOf(types.Out(i)), frameLen, cap)
	}
	return f
}

// CopyFrame copies the frame src to dst. The number of copied rows
// are returned. CopyFrame panics if src is not assignable to dst.
func CopyFrame(dst, src Frame) int {
	var n int
	for i := range dst {
		n = reflect.Copy(dst[i], src[i])
	}
	return n
}

// Slice returns a frame with rows i to j, analagous to Go's native
// slice operation.
func (f Frame) Slice(i, j int) Frame {
	g := make(Frame, len(f))
	copy(g, f)
	for k := range g {
		g[k] = f[k].Slice(i, j)
	}
	return g
}

// Len returns the frame's length.
func (f Frame) Len() int {
	if len(f) == 0 {
		return 0
	}
	return f[0].Len()
}

// Cap returns the frame's capacity.
func (f Frame) Cap() int {
	if len(f) == 0 {
		return 0
	}
	return f[0].Cap()
}

// NumOut implements Type.
func (f Frame) NumOut() int {
	return len(f)
}

// Out implements Type.
func (f Frame) Out(i int) reflect.Type {
	return f[i].Type().Elem()
}

// Realloc returns a frame with the provided length, returning f if
// it has enough capacity. Note that in the case that Realloc has to
// allocate a new frame, it does not copy the contents of the frame
// f. Realloc can be called on a zero-valued Frame.
func (f Frame) Realloc(typ Type, len int) Frame {
	if len <= f.Cap() {
		return f.Slice(0, len)
	}
	return MakeFrame(typ, len)
}

// CopyIndex copies row i into the provided slice of column values.
func (f Frame) CopyIndex(row []reflect.Value, i int) {
	for j := range row {
		row[j] = f[j].Index(i)
	}
}

// SetIndex sets row i of the frame from the provided column values.
func (f Frame) SetIndex(row []reflect.Value, i int) {
	for j, v := range row {
		f[j].Index(i).Set(v)
	}
}
