// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"text/tabwriter"

	"github.com/grailbio/bigslice/slicetype"
)

// A Frame is a list of column vectors of equal lengths (i.e., it's
// rectangular). Each column is represented as a reflect.Value that
// encapsulates a slice of values representing the column vector.
// Frames provide a set of methods that operate over the underlying
// column vectors in a uniform fashion.
type Frame []reflect.Value

// MakeFrame creates a new Frame of the given type, length, and
// capacity. If the capacity argument is omitted, a frame with
// capacity equal to the provided length is returned.
func MakeFrame(types slicetype.Type, frameLen int, frameCap ...int) Frame {
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

// AppendFrame appends the rows in the frame g to the rows in frame
// f, returning the appended frame. Its semantics matches that of
// Go's builtin append: the returned frame may share underlying
// storage with frame f.
func AppendFrame(f, g Frame) Frame {
	if f == nil {
		f = make(Frame, len(g))
		for i := range f {
			f[i] = reflect.Zero(g[i].Type())
		}
	}
	for i := range f {
		f[i] = reflect.AppendSlice(f[i], g[i])
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

// Columns constructs a frame from a list of slices. Each slice is a
// column of the frame. Columns panics if any argument is not a slice
// or if the column lengths do not match.
func Columns(cols ...interface{}) Frame {
	f := make(Frame, len(cols))
	n := -1
	for i, col := range cols {
		val := reflect.ValueOf(col)
		if val.Kind() != reflect.Slice {
			typePanicf(1, "expected slice, got %v", val.Type())
		}
		if n < 0 {
			n = val.Len()
		} else if val.Len() != n {
			typePanicf(1,
				"inconsistent column lengths: "+
					"column %d has length %d, previous columns have length %d",
				i, val.Len(), n,
			)
		}
		f[i] = val
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
	return f[i].Type().Elem()
}

// Realloc returns a frame with the provided length, returning f if
// it has enough capacity. Note that in the case that Realloc has to
// allocate a new frame, it does not copy the contents of the frame
// f. Realloc can be called on a zero-valued Frame.
func (f Frame) Realloc(typ slicetype.Type, len int) Frame {
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

// String returns a descriptive string of the frame.
func (f Frame) String() string {
	types := make([]string, len(f))
	for i := range f {
		types[i] = f[i].Type().Elem().String()
	}
	return fmt.Sprintf("frame[%d]%s", f.Len(), strings.Join(types, ","))
}

// WriteTab writes the frame in tabular format to the provided io.Writer.
func (f Frame) WriteTab(w io.Writer) {
	var tw tabwriter.Writer
	tw.Init(w, 4, 4, 1, ' ', 0)
	types := make([]string, len(f))
	for i := range f {
		types[i] = f[i].Type().Elem().String()
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

// FrameReader implements a Reader for a single Frame.
type frameReader struct {
	Frame
}

func (f *frameReader) Read(ctx context.Context, out Frame) (int, error) {
	n := out.Len()
	max := f.Frame.Len()
	if max < n {
		n = max
	}
	CopyFrame(out, f.Frame)
	f.Frame = f.Frame.Slice(n, max)
	if f.Frame.Len() == 0 {
		return n, EOF
	}
	return n, nil
}
