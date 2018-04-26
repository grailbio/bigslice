// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"context"
	"encoding/gob"
	"io"
	"reflect"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/bigslice/stats"
)

// A Reader represents a stateful stream of computed records from a
// slice. Each call to Read reads the next set of available records.
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
	Read(ctx context.Context, frame Frame) (int, error)
}

type errorReader struct{ error }

func (e errorReader) Read(ctx context.Context, f Frame) (int, error) {
	return 0, e.error
}

type closingReader struct {
	Reader
	io.Closer
}

func (c *closingReader) Read(ctx context.Context, out Frame) (int, error) {
	n, err := c.Reader.Read(ctx, out)
	if err != nil {
		c.Closer.Close()
	}
	return n, err
}

type emptyReader struct{}

func (emptyReader) Read(ctx context.Context, f Frame) (int, error) {
	return 0, EOF
}

func newDecodingReader(r io.Reader) *decodingReader {
	return &decodingReader{dec: gob.NewDecoder(r)}
}

// DecodingReader provides a Reader on top of a gob stream
// encoded with batches of rows stored in column-major order.
type decodingReader struct {
	dec      *gob.Decoder
	off, len int
	buf      []reflect.Value // points to slices
	err      error
}

func (d *decodingReader) Read(ctx context.Context, f Frame) (n int, err error) {
	if d.err != nil {
		return 0, d.err
	}
	if d.off == d.len {
		if d.buf == nil {
			d.buf = make([]reflect.Value, len(f))
			for i := range d.buf {
				d.buf[i] = reflect.New(reflect.SliceOf(f.Out(i)))
			}
		}
		// Read the next batch.
		for i := range f {
			if d.err = d.dec.DecodeValue(d.buf[i]); d.err != nil {
				if d.err == io.EOF {
					d.err = EOF
				}
				return 0, d.err
			}
		}
		d.off = 0
		d.len = d.buf[0].Elem().Len()
	}
	if d.len > 0 {
		for i := range f {
			n = reflect.Copy(f[i], d.buf[i].Elem().Slice(d.off, d.len))
		}
		d.off += n
	}
	return n, nil
}

type statsReader struct {
	reader  Reader
	numRead *stats.Int
}

func (s *statsReader) Read(ctx context.Context, f Frame) (n int, err error) {
	n, err = s.reader.Read(ctx, f)
	s.numRead.Add(int64(n))
	return
}

// ReadAll copies all elements from reader r into the provided column
// pointers. ReadAll is not tuned for performance and is intended for
// testing purposes.
func ReadAll(ctx context.Context, r Reader, columns ...interface{}) error {
	columnsv := make([]reflect.Value, len(columns))
	for i := range columns {
		columnsv[i] = reflect.ValueOf(columns[i])
		if columnsv[i].Type().Kind() != reflect.Ptr {
			return errors.E(errors.Invalid, "attempted to read into non-pointer")
		}
	}
	buf := make(Frame, len(columns))
	for i := range columns {
		typ := columnsv[i].Type().Elem()
		buf[i] = reflect.MakeSlice(reflect.SliceOf(typ.Elem()), defaultChunksize, defaultChunksize)
	}
	for {
		n, err := r.Read(ctx, buf)
		if err != nil && err != EOF {
			return err
		}
		buf = buf.Slice(0, n)
		for i := range columnsv {
			columnsv[i].Elem().Set(reflect.AppendSlice(columnsv[i].Elem(), buf[i]))
		}
		if err == EOF {
			break
		}
		buf = buf.Slice(0, buf.Cap())
	}
	return nil
}
