// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package sliceio provides utilities for managing I/O for Bigslice
// operations.
package sliceio

import (
	"context"
	"io"
	"reflect"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/slicetype"
)

// DefaultChunksize is the default size used for I/O vectors within the
// sliceio package.
const defaultChunksize = 1024

// EOF is the error returned by Reader.Read when no more data is
// available. EOF is intended as a sentinel error: it signals a
// graceful end of output. If output terminates unexpectedly, a
// different error should be returned.
var EOF = errors.New("EOF")

// A Reader represents a stateful stream of records. Each call to
// Read reads the next set of available records.
type Reader interface {
	// Read reads a vector of records from the underlying Slice. Each
	// passed-in column should be a value containing a slice of column
	// values. The number of columns should match the number of columns
	// in the slice; their types should match the corresponding column
	// types of the slice. Each column should have the same slice
	// length.
	//
	// Read returns the total number of records read, or an error. When
	// no more records are available, Read returns EOF. Read may return
	// EOF when n > 0. In this case, n records were read, but no more
	// are available.
	//
	// Read should not be called concurrently.
	Read(ctx context.Context, frame frame.Frame) (int, error)
}

type multiReader struct {
	q   []Reader
	err error
}

// MultiReader returns a Reader that's the logical concatenation of
// the provided input readers. Once every underlying Reader has
// returned EOF, Read will return EOF, too. Non-EOF errors are
// returned immediately.
func MultiReader(readers ...Reader) Reader {
	return &multiReader{q: readers}
}

func (m *multiReader) Read(ctx context.Context, out frame.Frame) (n int, err error) {
	if m.err != nil {
		return 0, m.err
	}
	for len(m.q) > 0 {
		n, err := m.q[0].Read(ctx, out)
		switch {
		case err == EOF:
			err = nil
			m.q = m.q[1:]
		case err != nil:
			m.err = err
			return n, err
		case n > 0:
			return n, err
		}
	}
	return 0, EOF
}

// FrameReader implements a Reader for a single Frame.
type frameReader struct {
	frame.Frame
}

// FrameReader returns a Reader that reads the provided
// Frame to completion.
func FrameReader(frame frame.Frame) Reader {
	return &frameReader{frame}
}

func (f *frameReader) Read(ctx context.Context, out frame.Frame) (int, error) {
	n := out.Len()
	max := f.Frame.Len()
	if max < n {
		n = max
	}
	frame.Copy(out, f.Frame)
	f.Frame = f.Frame.Slice(n, max)
	if f.Frame.Len() == 0 {
		return n, EOF
	}
	return n, nil
}

// ReadAll copies all elements from reader r into the provided column
// pointers. ReadAll is not tuned for performance and is intended for
// testing purposes.
func ReadAll(ctx context.Context, r Reader, columns ...interface{}) error {
	columnsv := make([]reflect.Value, len(columns))
	types := make([]reflect.Type, len(columns))
	for i := range columns {
		columnsv[i] = reflect.ValueOf(columns[i])
		if columnsv[i].Type().Kind() != reflect.Ptr {
			return errors.E(errors.Invalid, "attempted to read into non-pointer")
		}
		types[i] = reflect.TypeOf(columns[i]).Elem().Elem()
	}
	buf := frame.Make(slicetype.New(types...), defaultChunksize, defaultChunksize)
	for {
		n, err := r.Read(ctx, buf)
		if err != nil && err != EOF {
			return err
		}
		buf = buf.Slice(0, n)
		for i := range columnsv {
			columnsv[i].Elem().Set(reflect.AppendSlice(columnsv[i].Elem(), buf.Value(i)))
		}
		if err == EOF {
			break
		}
		buf = buf.Slice(0, buf.Cap())
	}
	return nil
}

// ReadFull reads the full length of the frame. ReadFull reads short
// frames only on EOF.
func ReadFull(ctx context.Context, r Reader, f frame.Frame) (n int, err error) {
	len := f.Len()
	for n < len {
		m, err := r.Read(ctx, f.Slice(n, len))
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// An errReader is a reader that only returns errors.
type errReader struct{ Err error }

// ErrReader returns a reader that returns the provided error
// on every call to read. ErrReader panics if err is nil.
func ErrReader(err error) Reader {
	if err == nil {
		panic("nil error")
	}
	return &errReader{err}
}

func (e errReader) Read(ctx context.Context, f frame.Frame) (int, error) {
	return 0, e.Err
}

// A ClosingReader closes the provided io.Closer when Read returns
// any error.
type ClosingReader struct {
	Reader
	io.Closer
}

// Read implements sliceio.Reader.
func (c *ClosingReader) Read(ctx context.Context, out frame.Frame) (int, error) {
	n, err := c.Reader.Read(ctx, out)
	if err != nil && c.Closer != nil {
		c.Closer.Close()
		c.Closer = nil
	}
	return n, err
}

// EmptyReader returns an EOF.
type EmptyReader struct{}

func (EmptyReader) Read(ctx context.Context, f frame.Frame) (int, error) {
	return 0, EOF
}
