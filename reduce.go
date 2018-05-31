// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"container/heap"
	"context"
	"expvar"
	"reflect"

	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/kernel"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/slicetype"
	"github.com/grailbio/bigslice/typecheck"
)

var (
	combinerKeys         = expvar.NewInt("combinerkeys")
	combinerRecords      = expvar.NewInt("combinerrecords")
	combinerTotalRecords = expvar.NewInt("combinertotalrecords")
)

// Reduce returns a slice that reduces elements pairwise. Reduce
// operations must be commutative and associative. Schematically:
//
//	Reduce(Slice<k, v>, func(v1, v2 v) v) Slice<k, v>
//
// The provided reducer function is invoked to aggregate values of
// type v. Reduce can perform map-side "combining", so that data are
// reduced to their aggregated value aggressively. This can often
// speed up computations significantly.
//
// TODO(marius): Reduce currently maintains the working set of keys
// in memory, and is thus appropriate only where the working set can
// fit in memory. For situations where this is not the case, Cogroup
// should be used instead (at an overhead). Reduce should spill to disk
// when necessary.
//
// TODO(marius): consider pushing combiners into task dependency
// definitions so that we can combine-read all partitions on one machine
// simultaneously.
func Reduce(slice Slice, reduce interface{}) Slice {
	arg, ret, ok := typecheck.Func(reduce)
	if !ok {
		typecheck.Panicf(1, "reduce: invalid reduce function %T", reduce)
	}
	if slice.NumOut() != 2 {
		typecheck.Panic(1, "reduce: input slice must have exactly two columns")
	}
	if arg.NumOut() != 2 || arg.Out(0) != slice.Out(1) || arg.Out(1) != slice.Out(1) || ret.NumOut() != 1 || ret.Out(0) != slice.Out(1) {
		typecheck.Panicf(1, "reduce: invalid reduce function %T, expected func(%s, %s) %s", reduce, slice.Out(1), slice.Out(1), slice.Out(1))
	}
	if !canMakeCombiningFrame(slice) {
		typecheck.Panicf(1, "cannot combine values for keys of type %s", slice.Out(0))
	}
	var hasher kernel.Hasher
	if !kernel.Lookup(slice.Out(0), &hasher) {
		typecheck.Panicf(1, "key type %s is not partitionable", slice.Out(0))
	}
	return &reduceSlice{slice, reflect.ValueOf(reduce), hasher}
}

// ReduceSlice implements "post shuffle" combining merge sort.
type reduceSlice struct {
	Slice
	combiner reflect.Value
	hasher   kernel.Hasher
}

func (r *reduceSlice) Hasher() kernel.Hasher    { return r.hasher }
func (r *reduceSlice) Op() string               { return "reduce" }
func (*reduceSlice) NumDep() int                { return 1 }
func (r *reduceSlice) Dep(i int) Dep            { return Dep{r.Slice, true, true} }
func (r *reduceSlice) Combiner() *reflect.Value { return &r.combiner }

type reduceReader struct {
	typ      slicetype.Type
	combiner reflect.Value
	readers  []sliceio.Reader
	err      error

	sorter kernel.Sorter
	heap   *frameBufferHeap
}

func (r *reduceReader) Read(ctx context.Context, out frame.Frame) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.heap == nil {
		if !kernel.Lookup(r.typ.Out(0), &r.sorter) {
			panic("bigslice.Reducer: invalid reduce type")
		}
		r.heap = new(frameBufferHeap)
		r.heap.Sorter = r.sorter
		r.heap.Buffers = make([]*frameBuffer, 0, len(r.readers))
		for i := range r.readers {
			buf := &frameBuffer{
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
		var combine []*frameBuffer
		for len(combine) == 0 || len(r.heap.Buffers) > 0 &&
			!r.sorter.Less(combine[0].Frame, combine[0].Off, r.heap.Buffers[0].Frame, r.heap.Buffers[0].Off) {
			buf := heap.Pop(r.heap).(*frameBuffer)
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
		out[0].Index(n).Set(key)
		out[1].Index(n).Set(val)
		n++
	}
	var err error
	if len(r.heap.Buffers) == 0 {
		err = sliceio.EOF
	}
	return n, err
}

func (r *reduceSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader {
	if len(deps) == 1 {
		return deps[0]
	}
	return &reduceReader{typ: r, combiner: r.combiner, readers: deps}
}
