// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sortio

import (
	"container/heap"
	"context"
	"reflect"

	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/slicetype"
	"github.com/grailbio/bigslice/typecheck"
)

const defaultChunksize = 1024

type reader struct {
	typ      slicetype.Type
	name     string
	combiner reflect.Value
	readers  []sliceio.Reader
	err      error

	heap  *FrameBufferHeap
	frame frame.Frame
}

// Reduce returns a Reader that merges and reduces a set of
// sorted (and possibly combined) readers. Reduce panics if
// the provided type is not reducable.
func Reduce(typ slicetype.Type, name string, readers []sliceio.Reader, combiner reflect.Value) sliceio.Reader {
	if typ.NumOut()-typ.Prefix() != 1 {
		typecheck.Panicf(1, "cannot reduce type %s", slicetype.String(typ))
	}
	return &reader{
		typ:      typ,
		name:     name,
		readers:  readers,
		combiner: combiner,
	}
}

func (r *reader) Read(ctx context.Context, out frame.Frame) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.heap == nil {
		n := len(r.readers) * defaultChunksize
		r.frame = frame.Make(r.typ, n, n)
		r.heap = new(FrameBufferHeap)
		r.heap.LessFunc = func(i, j int) bool {
			return r.frame.Less(r.heap.Buffers[i].Pos(), r.heap.Buffers[j].Pos())
		}
		r.heap.Buffers = make([]*FrameBuffer, 0, len(r.readers))
		for i := range r.readers {
			off := i * defaultChunksize
			buf := &FrameBuffer{
				Frame:  r.frame.Slice(off, off+defaultChunksize),
				Reader: r.readers[i],
				Off:    off,
			}
			switch err := buf.Fill(ctx); {
			case err == sliceio.EOF:
				// No data. Skip.
			case err != nil:
				r.err = err
				return 0, r.err
			default:
				r.heap.Buffers = append(r.heap.Buffers, buf)
			}
		}
		heap.Init(r.heap)
	}
	var (
		n   int
		max = out.Len()
	)
	for n < max && len(r.heap.Buffers) > 0 {
		// Gather all the buffers that have the same key. Each parent
		// reader has at most one entry for a given key, since they have
		// already been combined.
		var combine []*FrameBuffer
		for len(combine) == 0 || len(r.heap.Buffers) > 0 &&
			!r.frame.Less(combine[0].Pos(), r.heap.Buffers[0].Pos()) {
			buf := heap.Pop(r.heap).(*FrameBuffer)
			combine = append(combine, buf)
		}
		// TODO(marius): pass a vector of values to be combined, if it is supported
		// by the combiner.
		vcol := out.NumOut() - 1
		var combined reflect.Value
		for i, buf := range combine {
			val := buf.Frame.Index(vcol, buf.Index)
			if i == 0 {
				combined = val
			} else {
				combined = r.combiner.Call([]reflect.Value{combined, val})[0]
			}
		}
		// Emit the output before overwriting the frame. Note that key and val are
		// references to slice elements.

		// Copy key columns first, and then set the combined value.
		frame.Copy(out.Slice(n, n+1), combine[0].Frame.Slice(combine[0].Index, combine[0].Index+1))
		out.Index(vcol, n).Set(combined)

		for _, buf := range combine {
			buf.Index++
			if buf.Index == buf.Len {
				if err := buf.Fill(ctx); err != nil && err != sliceio.EOF {
					r.err = err
					return n, err
				} else if err == nil {
					heap.Push(r.heap, buf)
				} // Otherwise it's EOF and we drop it from the heap.
			} else {
				heap.Push(r.heap, buf)
			}
		}
		n++
	}
	var err error
	if len(r.heap.Buffers) == 0 {
		err = sliceio.EOF
	}
	return n, err
}
