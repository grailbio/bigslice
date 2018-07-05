// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sortio

import (
	"container/heap"
	"context"
	"reflect"

	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/kernel"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/slicetype"
)

const defaultChunksize = 1024

type reader struct {
	typ      slicetype.Type
	name     string
	combiner reflect.Value
	readers  []sliceio.Reader
	err      error

	sorter kernel.Sorter
	heap   *FrameBufferHeap
}

// Reduce returns a Reader that merges and reduces a set of
// sorted (and possibly combined) readers. For each
func Reduce(typ slicetype.Type, name string, readers []sliceio.Reader, combiner reflect.Value) sliceio.Reader {
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
		if !kernel.Lookup(r.typ.Out(0), &r.sorter) {
			panic("bigslice.Reducer: invalid reduce type")
		}
		r.heap = new(FrameBufferHeap)
		r.heap.Sorter = r.sorter
		r.heap.Buffers = make([]*FrameBuffer, 0, len(r.readers))
		for i := range r.readers {
			buf := &FrameBuffer{
				Frame:  frame.Make(r.typ, defaultChunksize),
				Reader: r.readers[i],
				Index:  i,
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
			!r.sorter.Less(combine[0].Frame, combine[0].Off, r.heap.Buffers[0].Frame, r.heap.Buffers[0].Off) {
			buf := heap.Pop(r.heap).(*FrameBuffer)
			combine = append(combine, buf)
		}
		var key, val reflect.Value
		for i, buf := range combine {
			if i == 0 {
				key = buf.Frame[0].Index(buf.Off)
				val = buf.Frame[1].Index(buf.Off)
			} else {
				val = r.combiner.Call([]reflect.Value{val, buf.Frame[1].Index(buf.Off)})[0]
			}
		}
		// Emit the output before overwriting the frame. Note that key and val are
		// references to slice elements.
		out[0].Index(n).Set(key)
		out[1].Index(n).Set(val)
		for _, buf := range combine {
			buf.Off++
			if buf.Off == buf.Len {
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
