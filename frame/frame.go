// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package frame contains definitions and utilities for bigslice frames.
// Frames are lists of column vectors that represent fixed buffers of
// data as it is processed by bigslice.
package frame

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strings"
	"text/tabwriter"
	"unsafe"

	"github.com/grailbio/bigslice/slicetype"
	"github.com/grailbio/bigslice/typecheck"
)

// Column represents a single column of values in a frame. Columns
// are always Go slices, but are represented here as a reflect.Value
// to support type polymorphism. We use the type Column instead of
// reflect.Value to distinguish between the various uses of
// reflect.Value in Bigslice's code base.
type Column reflect.Value

// ColumnOf returns a new column created from the given interface,
// which must be a slice.
func ColumnOf(x interface{}) Column {
	return Column(reflect.ValueOf(x))
}

// AppendColumn appends the provided value to the column and returns
// (possibly the same) column.
func AppendColumn(col Column, val reflect.Value) Column {
	return Column(reflect.Append(reflect.Value(col), val))
}

// Index returns the value at index i of the column c.
func (c Column) Index(i int) reflect.Value { return reflect.Value(c).Index(i) }

// Type returns the type of the column. The returned type is always a
// slice.
func (c Column) Type() reflect.Type { return reflect.Value(c).Type() }

// ElemType returns the element type of the column.
func (c Column) ElemType() reflect.Type { return c.Type().Elem() }

// Value returns the reflect.Value that represents this column.
func (c Column) Value() reflect.Value { return reflect.Value(c) }

// Slice slices the column.
func (c Column) Slice(i, j int) Column { return Column(reflect.Value(c).Slice(i, j)) }

// Len returns the column's length.
func (c Column) Len() int { return reflect.Value(c).Len() }

// Cap returns the column's capacity.
func (c Column) Cap() int { return reflect.Value(c).Cap() }

// Interface returns the column value as an empty interface.
func (c Column) Interface() interface{} { return reflect.Value(c).Interface() }

// A Frame is a list of column vectors of equal lengths (i.e., it's
// rectangular). Each column is represented as a reflect.Value that
// encapsulates a slice of values representing the column vector.
// Frames provide a set of methods that operate over the underlying
// column vectors in a uniform fashion.
type Frame []Column

// MakeFrame creates a new Frame of the given type, length, and
// capacity. If the capacity argument is omitted, a frame with
// capacity equal to the provided length is returned.
func Make(types slicetype.Type, frameLen int, frameCap ...int) Frame {
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
		f[i] = Column(reflect.MakeSlice(reflect.SliceOf(types.Out(i)), frameLen, cap))
	}
	return f
}

// Cast casts a lists of columns into a Frame.
func Cast(cols []reflect.Value) Frame {
	return *(*Frame)(unsafe.Pointer(&cols))
}

// AppendFrame appends the rows in the frame g to the rows in frame
// f, returning the appended frame. Its semantics matches that of
// Go's builtin append: the returned frame may share underlying
// storage with frame f.
func Append(f, g Frame) Frame {
	if f == nil {
		f = make(Frame, len(g))
		for i := range f {
			f[i] = Column(reflect.Zero(g[i].Type()))
		}
	}
	for i := range f {
		f[i] = Column(reflect.AppendSlice(f[i].Value(), g[i].Value()))
	}
	return f
}

// CopyFrame copies the frame src to dst. The number of copied rows
// are returned. CopyFrame panics if src is not assignable to dst.
func Copy(dst, src Frame) int {
	var n int
	for i := range dst {
		n = reflect.Copy(dst[i].Value(), src[i].Value())
	}
	return n
}

// Columns constructs a frame from a list of slices. Each slice is a
// column of the frame. Columns panics if any argument is not a slice
// or if the column lengths do not match.
func Columns(cols ...interface{}) Frame {
	f := make(Frame, len(cols))
	n := -1
	for i, col := range cols {
		val := reflect.ValueOf(col)
		if val.Kind() != reflect.Slice {
			typecheck.Panicf(1, "expected slice, got %v", val.Type())
		}
		if n < 0 {
			n = val.Len()
		} else if val.Len() != n {
			typecheck.Panicf(1,
				"inconsistent column lengths: "+
					"column %d has length %d, previous columns have length %d",
				i, val.Len(), n,
			)
		}
		f[i] = Column(val)
	}
	return f
}

// Slice returns a frame with rows i to j, analagous to Go's native
// slice operation.
func (f Frame) Slice(i, j int) Frame {
	if f == nil {
		return nil
	}
	if i == 0 && j == f.Len() {
		return f
	}
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
	return f[i].ElemType()
}

// Realloc returns a frame with the provided length, returning f if
// it has enough capacity. Note that in the case that Realloc has to
// allocate a new frame, it does not copy the contents of the frame
// f. Realloc can be called on a zero-valued Frame.
func (f Frame) Realloc(typ slicetype.Type, len int) Frame {
	if len <= f.Cap() {
		return f.Slice(0, len)
	}
	return Make(typ, len)
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

// String returns a descriptive string of the frame.
func (f Frame) String() string {
	types := make([]string, len(f))
	for i := range f {
		types[i] = f[i].ElemType().String()
	}
	return fmt.Sprintf("frame[%d]%s", f.Len(), strings.Join(types, ","))
}

// WriteTab writes the frame in tabular format to the provided io.Writer.
func (f Frame) WriteTab(w io.Writer) {
	var tw tabwriter.Writer
	tw.Init(w, 4, 4, 1, ' ', 0)
	types := make([]string, len(f))
	for i := range f {
		types[i] = f[i].ElemType().String()
	}
	fmt.Fprintln(&tw, strings.Join(types, "\t"))
	var (
		row    = make([]reflect.Value, len(f))
		values = make([]string, len(f))
	)
	for i := 0; i < f.Len(); i++ {
		f.CopyIndex(row, i)
		for j := range row {
			values[j] = fmt.Sprint(row[j])
		}
		fmt.Fprintln(&tw, strings.Join(values, "\t"))
	}
	tw.Flush()
}

// TabString returns a string representing the frame in tabular format.
func (f Frame) TabString() string {
	var b bytes.Buffer
	f.WriteTab(&b)
	return b.String()
}

// Swapper returns a function that can be used to swap two rows in
// the frame.
func (f Frame) Swapper() func(i, j int) {
	swappers := make([]func(i, j int), len(f))
	for i := range f {
		swappers[i] = reflect.Swapper(f[i].Interface())
	}
	return func(i, j int) {
		for _, swap := range swappers {
			swap(i, j)
		}
	}
}

// Swap swaps the rows i and j in frame f. Frequent users of swapping
// should create a reusable swapper through Frame.Swapper instead of
// calling Swap repeatedly.
func (f Frame) Swap(i, j int) {
	f.Swapper()(i, j)
}

// Clear zeros out the frame.
func (f Frame) Clear() {
	// TODO(marius): here we can safely use unsafe to zero out entire
	// vectors at a time.
	n := f.Len()
	for i := range f {
		zero := reflect.Zero(f.Out(i))
		for j := 0; j < n; j++ {
			f[i].Index(j).Set(zero)
		}
	}
}

// ColumnValues returns the frame's columns as a slice of reflect.Values.
func (f Frame) ColumnValues() []reflect.Value {
	return *(*[]reflect.Value)(unsafe.Pointer(&f))
}

// Equal tells whether f1 and f2 are (deeply) equal.
func Equal(f1, f2 Frame) bool {
	if len(f1) != len(f2) {
		return false
	}
	for i := range f1 {
		if !reflect.DeepEqual(f1[i].Interface(), f2[i].Interface()) {
			return false
		}
	}
	return true
}
