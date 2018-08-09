// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package frame implements a typed, columnar data structure that
// represents data vectors throughout Bigslice.
//
// The package contains the definition of Frame as well as a set of
// index-based operators that amortize runtime safety overhead.
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
)

// DataType represents the type of data held in a frame's column.
type dataType struct {
	// Type is the reflect representation of the column type.
	reflect.Type
	// Ptr holds a pointer to the type's runtime type representation.
	// This is used to pass to (unsafe) runtime methods for copying and
	// clearing.
	ptr unsafe.Pointer
	// Pointers tells whether the type contains any pointers. It is used
	// to perform memory manipulation without write barriers when
	// possible.
	pointers bool
	// Size is the size of each element. It is reflect.Type.Size() memoized.
	size uintptr
}

// NewDataType constructs a dataType from a reflect.Type.
func newDataType(t reflect.Type) dataType {
	var typ dataType
	typ.Type = t
	typ.ptr = unsafe.Pointer(reflect.ValueOf(&t).Elem().InterfaceData()[1])
	typ.pointers = pointers(t)
	typ.size = typ.Size()
	return typ
}

// Data represents a single data column of a frame.
type data struct {
	// Typ is the column's data type.
	typ dataType
	// Ptr is the base address of the column data.
	ptr unsafe.Pointer
	// Val is a slice-typed reflection value that represents the whole
	// data slice.
	val reflect.Value
	// Ops is a set of operators on the column's data.
	ops Ops
}

// NewData constructs a new data from the provided reflect.Value.
func newData(v reflect.Value) data {
	var data data
	data.ptr = unsafe.Pointer(v.Pointer())
	data.val = v
	data.typ = newDataType(v.Type().Elem())
	data.ops = makeSliceOps(data.typ.Type, v)
	data.ops.swap = reflect.Swapper(v.Interface())
	return data
}

// A Frame is a collection of 0 or more typed, equal-length columns
// that form a logical table. Each column is represented by a Go
// slice. Frames can be sliced efficiently, and the package provides
// computed operators that can perform efficient index-based
// operations on the Frame.
type Frame struct {
	data []data
	// Off, len, and cap are the offset, length, and capacity
	// of all columns of the frame. Since frames store raw
	// base pointers, the offset represents the 0 index of
	// this frame. Len and cap are relative to the offset.
	off, len, cap int
}

// Empty is the empty frame.
var Empty = Frame{data: make([]data, 0)}

// Make returns a new frame with the provided type, length, and
// capacity.
func Make(types slicetype.Type, len, cap int) Frame {
	if len < 0 || len > cap {
		panic("frame.Make: invalid len, cap")
	}
	f := Frame{
		data: make([]data, types.NumOut()),
		len:  len,
		cap:  cap,
	}
	for i := range f.data {
		v := reflect.MakeSlice(reflect.SliceOf(types.Out(i)), cap, cap)
		f.data[i] = newData(v)
	}
	return f
}

// Slices returns a new Frame constructed from a set of Go slices,
// each representing a column. The slices must have the same length,
// or Slices panics.
func Slices(cols ...interface{}) Frame {
	if len(cols) == 0 {
		return Empty
	}
	f := Frame{data: make([]data, len(cols))}
	for i := range cols {
		v := reflect.ValueOf(cols[i])
		if v.Kind() != reflect.Slice {
			panic("frame.From: non-slice argument " + v.Kind().String())
		}
		if n := v.Len(); i == 0 {
			f.len = n
			f.cap = v.Cap()
		} else if n != f.len {
			panic("frame.Slices: columns of unequal length")
		} else if cap := v.Cap(); cap < f.cap {
			f.cap = cap
		}
		f.data[i] = newData(v)
	}
	return f
}

// Values returns a new Frame constructed from a set of
// reflect.Values, each representing a column. The slices must have
// the same length, or Values panics.
func Values(cols []reflect.Value) Frame {
	if len(cols) == 0 {
		return Empty
	}
	f := Frame{data: make([]data, len(cols))}
	for i, v := range cols {
		if v.Kind() != reflect.Slice {
			panic("frame.Values: non-slice argument")
		}
		if n := v.Len(); i == 0 {
			f.len = n
			f.cap = v.Cap()
		} else if n != f.len {
			panic("frame.Values: columns of unequal length")
		} else if cap := v.Cap(); cap < f.cap {
			f.cap = cap
		}
		f.data[i] = newData(v)
	}
	return f
}

// Copy copies the contents of src until either dst has been filled
// or src exhausted. It returns the number of elements copied.
func Copy(dst, src Frame) (n int) {
	if !Compatible(dst, src) {
		panic("frame.Copy: incompatible frames dst=" + dst.String() + " src=" + src.String())
	}
	if dst.Len() == 0 || src.Len() == 0 {
		return 0
	}
	// Fast path for single element copy.
	if dst.Len() == 1 && src.Len() == 1 {
		for i := range dst.data {
			typ := dst.data[i].typ
			assign(typ,
				add(dst.data[i].ptr, uintptr(dst.off)*typ.size),
				add(src.data[i].ptr, uintptr(src.off)*typ.size))
		}
		return 1
	}
	for i := range dst.data {
		typ := dst.data[i].typ
		dh := sliceHeader{
			Data: add(dst.data[i].ptr, uintptr(dst.off)*typ.size),
			Len:  dst.len,
			Cap:  dst.cap,
		}
		sh := sliceHeader{
			Data: add(src.data[i].ptr, uintptr(src.off)*typ.size),
			Len:  src.len,
			Cap:  src.cap,
		}
		n = typedslicecopy(typ.ptr, dh, sh)
	}
	return
}

// AppendFrame appends src to dst, growing src if needed.
func AppendFrame(dst, src Frame) Frame {
	var i0, i1 int
	if !dst.IsValid() {
		dst = Make(src, src.len, src.len)
		i1 = src.len
	} else {
		dst, i0, i1 = dst.grow(src.len)
	}
	Copy(dst.Slice(i0, i1), src)
	return dst
}

// Compatible reports whether frames f and g are assignment
// compatible: that is, they have the same number of columns and the
// same column types.
func Compatible(f, g Frame) bool {
	if len(f.data) != len(g.data) {
		return false
	}
	for i := range f.data {
		if f.data[i].typ.Type != g.data[i].typ.Type {
			return false
		}
	}
	return true
}

// IsValid tells whether this frame is valid. A zero-valued frame
// is not valid.
func (f Frame) IsValid() bool { return f.data != nil }

// NumOut implements slicetype.Type
func (f Frame) NumOut() int { return len(f.data) }

// Out implements slicetype.Type.
func (f Frame) Out(i int) reflect.Type { return f.data[i].typ.Type }

// Slice returns the frame f[i:j]. It panics if indices are out of bounds.
func (f Frame) Slice(i, j int) Frame {
	if i < 0 || j < i || j > f.cap {
		panic(fmt.Sprintf("frame.Slice: slice index %d:%d out of bounds for slice %s", i, j, f))
	}
	return Frame{
		f.data,
		f.off + i,
		j - i,
		f.cap - i,
	}
}

// Grow returns a Frame with at least n extra capacity. The returned
// frame will have length f.Len()+n.
func (f Frame) Grow(n int) Frame {
	f, _, _ = f.grow(n)
	return f
}

// Ensure Slice(0, n), growing the frame as needed.
func (f Frame) Ensure(n int) Frame {
	if f.len == n {
		return f
	}
	if n <= f.cap {
		return f.Slice(0, n)
	}
	return f.Grow(n - f.len)
}

// Len returns the Frame's length.
func (f Frame) Len() int { return f.len }

// Cap returns the Frame's capacity.
func (f Frame) Cap() int { return f.cap }

// SliceHeader returns the slice header for column i. As with other uses
// of SliceHeader, the user must ensure that a reference to the frame is
// maintained so that the underlying slice is not garbage collected while
// (unsafely) using the slice header.
func (f Frame) SliceHeader(i int) reflect.SliceHeader {
	return reflect.SliceHeader{
		Data: uintptr(f.data[i].ptr) + uintptr(f.off)*f.data[i].typ.size,
		Len:  f.len,
		Cap:  f.cap,
	}
}

// Value returns the ith column as a reflect.Value.
func (f Frame) Value(i int) reflect.Value {
	if f.off == 0 && f.len == f.cap {
		return f.data[i].val
	}
	return f.data[i].val.Slice(f.off, f.off+f.len)
}

// Values returns the frame's columns as reflect.Values.
func (f Frame) Values() []reflect.Value {
	vs := make([]reflect.Value, f.NumOut())
	for i := range vs {
		vs[i] = f.Value(i)
	}
	return vs
}

// Interface returns the i'th column as an empty interface.
func (f Frame) Interface(i int) interface{} {
	return f.Value(i).Interface()
}

// Interfaces returns the frame's columns as empty interfaces.
func (f Frame) Interfaces() []interface{} {
	ifaces := make([]interface{}, f.NumOut())
	for i := range ifaces {
		ifaces[i] = f.Interface(i)
	}
	return ifaces
}

// Index returns the i'th row of col'th column as a reflect.Value.
func (f Frame) Index(col, i int) reflect.Value {
	return f.data[col].val.Index(f.off + i)
}

// UnsafeIndexAddr returns the address of the i'th row of the col'th
// column. This can be used by advanced clients that import the
// unsafe package. Clients are responsible for managing reference
// lifetimes so that the underlying objects will not be garbage
// collected while an address returned from this method may still be
// used.
func (f Frame) UnsafeIndexAddr(col, i int) uintptr {
	// In practice, this is safe to do: Go pads structures to be aligned,
	// but this does not seem to be guaranteed by the spec.
	return uintptr(f.data[col].ptr) + uintptr(f.off+i)*f.data[col].typ.size
}

// Swap swaps rows i and j in frame f.
func (f Frame) Swap(i, j int) {
	for k := range f.data {
		f.data[k].ops.swap(i-f.off, j-f.off)
	}
}

// Zero zeros the memory of column i
func (f Frame) Zero(i int) {
	c := f.data[i]
	zero(c.typ, add(c.ptr, uintptr(f.off)*c.typ.size), f.len)
}

// ZeroAll zeros the memory all columnns.
func (f Frame) ZeroAll() {
	for i := range f.data {
		f.Zero(i)
	}
}

// Less reports whether the row with index i should sort before the
// element with index j. Less operates on the frame's first column,
// and is available only if the operation is defined for the column's
// type. See RegisterOps for more details.
//
// TODO(marius): this method presents an unnecessary indirection;
// provide a way to get at a sort.Interface directly.
func (f Frame) Less(i, j int) bool {
	return f.data[0].ops.Less(i+f.off, j+f.off)
}

// Hash returns a 32-bit hash of the first column of frame f.
func (f Frame) Hash(i int) uint32 {
	return f.data[0].ops.HashWithSeed(i+f.off, 0)
}

// HashWithSeed returns a 32-bit seeded hash of the first column of
// frame f.
func (f Frame) HashWithSeed(i int, seed uint32) uint32 {
	return f.data[0].ops.HashWithSeed(i+f.off, seed)
}

// String returns a descriptive string of the frame.
func (f Frame) String() string {
	types := make([]string, f.NumOut())
	for i := range types {
		types[i] = f.Out(i).String()
	}
	return fmt.Sprintf("frame[%d,%d]%s", f.Len(), f.Cap(), strings.Join(types, ","))
}

// WriteTab writes the frame in tabular format to the provided io.Writer.
func (f Frame) WriteTab(w io.Writer) {
	var tw tabwriter.Writer
	tw.Init(w, 4, 4, 1, ' ', 0)
	types := make([]string, f.NumOut())
	for i := range types {
		types[i] = f.Out(i).String()
	}
	fmt.Fprintln(&tw, strings.Join(types, "\t"))
	values := make([]string, f.NumOut())
	for i := 0; i < f.Len(); i++ {
		for j := range values {
			values[j] = fmt.Sprint(f.Index(j, i))
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

func (f Frame) grow(need int) (Frame, int, int) {
	i0 := f.Len()
	i1 := i0 + need
	if i1 < i0 {
		panic("frame.grow: overflow")
	}
	m := f.cap
	if i1 <= m {
		return f.Slice(0, i1), i0, i1
	}
	// Same algorithm as the Go runtime:
	// TODO(marius): consider revisiting this for Bigslice.
	if m == 0 {
		m = need
	} else {
		for m < i1 {
			if i0 < 1024 {
				m += m
			} else {
				m += m / 4
			}
		}
	}
	g := Make(f, i1, m)
	Copy(g, f)
	return g, i0, i1
}
