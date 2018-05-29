// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"bytes"
	"context"
	"encoding/gob"
	"reflect"

	"github.com/grailbio/bigslice/frame"
)

// TaskBuffer is an in-memory buffer of task output. It has the
// ability to handle multiple partitions, and stores vectors of
// records for efficiency.
//
// TaskBuffer layout is: partition, slices, frames.
type taskBuffer [][]frame.Frame

// Slice returns column vectors for the provided partition and global
// offset. The returned offset indicates the position of the global
// offset into the returned vectors. A returned offset of -1
// indicates EOF. Slice is designed to perform zero-copy reads
// from a taskBuffer.
//
// TODO(marius): Slicing is currently inefficient as it requires a
// linear walk through the stored vectors. We should aggregate
// lengths so that we can perform a binary search. Alternatively, we
// can return a cookie from Slice that enables efficient resumption.
func (b taskBuffer) Slice(partition, off int) (frame.Frame, int) {
	beg, end := partition, partition+1
	// Find the offset.
	var n int
	for i := beg; i < end; i++ {
		for _, f := range b[i] {
			l := f.Len()
			if n+l > off {
				return f, off - n
			}
			n += l
		}
	}
	return nil, -1
}

type taskBufferReader struct {
	q       taskBuffer
	i, j, k int
}

func (r *taskBufferReader) Read(ctx context.Context, out frame.Frame) (int, error) {
loop:
	for {
		switch {
		case len(r.q) == r.i:
			return 0, EOF
		case len(r.q[r.i]) == r.j:
			r.i++
			r.j, r.k = 0, 0
		case r.q[r.i][r.j][0].Len() == r.k:
			r.j++
			r.k = 0
		default:
			break loop
		}
	}
	buf := r.q[r.i][r.j]
	n := out.Len()
	if m := buf.Len() - r.k; m < n {
		n = m
	}
	l := r.k + n
	frame.Copy(out, r.q[r.i][r.j].Slice(r.k, l))
	r.k = l
	return n, nil
}

// Reader returns a Reader for a partition of the taskBuffer.
func (b taskBuffer) Reader(partition int) Reader {
	if len(b) == 0 {
		return emptyReader{}
	}
	return &taskBufferReader{q: b[partition : partition+1]}
}

// SliceBuffer buffers serialized slice output in memory. The data are stored
// as a gob-stream where records are as batches in column-major form.
type sliceBuffer struct {
	bytes.Buffer

	enc *gob.Encoder
	dec *gob.Decoder
}

// WriteColumns serializes a batch of records into the buffer.
func (s *sliceBuffer) WriteColumns(columns ...reflect.Value) error {
	if s.enc == nil {
		s.enc = gob.NewEncoder(&s.Buffer)
	}
	for i := range columns {
		if err := s.enc.EncodeValue(columns[i]); err != nil {
			return err
		}
	}
	return nil
}

// ReadColumns deserializes a batch of records into the provided column
// pointers. This interface is provided for testing.
func (s *sliceBuffer) ReadColumns(columnptrs ...reflect.Value) error {
	if s.dec == nil {
		s.dec = gob.NewDecoder(&s.Buffer)
	}
	for i := range columnptrs {
		if err := s.dec.DecodeValue(columnptrs[i]); err != nil {
			return err
		}
	}
	return nil
}
