// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"container/heap"
	"context"
	"expvar"
	"log"
	"reflect"

	"github.com/grailbio/base/data"
	"github.com/grailbio/bigslice/slicetype"
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
	if slice.NumOut() != 2 {
		typePanicf(1, "Reduce expects two columns, got %v", slice.NumOut())
	}
	reduceval := reflect.ValueOf(reduce)
	reducetyp := reduceval.Type()
	if reducetyp.Kind() != reflect.Func {
		typePanicf(1, "reduce must be of type func(x, y %s) %s, not %s", slice.Out(1), slice.Out(1), reducetyp)
	}
	if reducetyp.NumIn() != 2 || reducetyp.In(0) != reducetyp.In(1) || reducetyp.In(1) != slice.Out(1) {
		typePanicf(1, "reduce must be of type func(x, y %s) %s, not %s", slice.Out(1), slice.Out(1), reducetyp)
	}
	if reducetyp.NumOut() != 1 || reducetyp.Out(0) != slice.Out(1) {
		typePanicf(1, "reduce must be of type func(x, y %s) %s, not %s", slice.Out(1), slice.Out(1), reducetyp)
	}
	if !canMakeCombiningFrame(slice) {
		typePanicf(1, "cannot combine values for keys of type %s", slice.Out(0))
	}
	// Reduce outputs a pair of slices: one to perform "map side"
	// combining; the second to perform post-shuffle reduces. The first
	// combines values by maintaining an indexed in-memory Frame. The
	// results from this operation are sorted and sent to the reducers,
	// which performs a combining merge-sort.
	slice = &combineSlice{slice, reduceval}
	hasher := makeFrameHasher(slice.Out(0), 0)
	if hasher == nil {
		typePanicf(1, "key type %s is not partitionable", slice.Out(0))
	}
	slice = &reduceSlice{slice, reduceval, hasher}
	return slice
}

// CombineSlice implements "map side" combining.
type combineSlice struct {
	Slice
	combiner reflect.Value
}

func (c *combineSlice) Op() string    { return "combine" }
func (c *combineSlice) NumDep() int   { return 1 }
func (c *combineSlice) Dep(i int) Dep { return Dep{c.Slice, false, false} }

type combineReader struct {
	op       *combineSlice
	combined Reader
	reader   Reader
	spiller  spiller
	err      error
}

func (c *combineReader) compute(ctx context.Context) (Reader, error) {
	spiller, err := newSpiller()
	if err != nil {
		return nil, err
	}
	var (
		comb   = makeCombiningFrame(c.op, c.op.combiner)
		sorter = makeSorter(c.op.Out(0), 0)
		total  int
	)
	rec := Memory.Add(String(c.op))
	defer rec.Cancel()
	in := MakeFrame(c.op, defaultChunksize)
	for {
		reclaim, err := rec.ShouldReclaim(ctx)
		if err != nil {
			return nil, err
		}
		if reclaim {
			log.Printf("%s: spilling %d rows disk", String(c.op), comb.Frame.Len())
			// We've been asked to reclaim memory,
			// so we spill our in-memory frame to disk.
			sorter.Sort(comb.Frame)
			n, err := spiller.Spill(comb.Frame)
			if err == nil {
				combinerKeys.Add(-int64(comb.Frame.Len()))
				combinerRecords.Add(-int64(total))
				total = 0
				comb = makeCombiningFrame(c.op, c.op.combiner)
				log.Printf("%s: spilled %s to disk", String(c.op), data.Size(n))
			} else {
				log.Printf("%s: failed to spill to disk: %v", String(c.op), err)
			}
			rec.Done()
		}
		n, err := c.reader.Read(ctx, in)
		if err != nil && err != EOF {
			return nil, err
		}
		combinerRecords.Add(int64(n))
		combinerTotalRecords.Add(int64(n))
		total += n
		nkeys := comb.Frame.Len()
		comb.Combine(in.Slice(0, n))
		combinerKeys.Add(int64(comb.Frame.Len() - nkeys))
		if err == EOF {
			break
		}
	}
	rec.Cancel()
	rec = nil

	// Now return the actual results, merging any spilled frames.

	combinerKeys.Add(-int64(comb.Frame.Len()))
	combinerRecords.Add(-int64(total))
	frame := comb.Frame
	comb = nil
	sorter.Sort(frame)
	reader := &frameReader{frame}
	readers, err := spiller.Readers()
	if err != nil {
		return nil, err
	}
	if len(readers) == 0 {
		return reader, nil
	}
	return &reduceReader{
		typ:      c.op,
		combiner: c.op.combiner,
		readers:  append(readers, reader),
	}, nil
}

func (c *combineReader) Read(ctx context.Context, out Frame) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	if c.combined == nil {
		c.combined, c.err = c.compute(ctx)
		if c.err != nil {
			return 0, c.err
		}
	}
	var n int
	n, c.err = c.combined.Read(ctx, out)
	return n, c.err
}

func (c *combineSlice) Reader(shard int, deps []Reader) Reader {
	return &combineReader{op: c, reader: deps[0]}
}

// ReduceSlice implements "post shuffle" combining merge sort.
type reduceSlice struct {
	Slice
	combiner reflect.Value
	hasher   FrameHasher
}

func (r *reduceSlice) Hasher() FrameHasher { return r.hasher }
func (r *reduceSlice) Op() string          { return "reduce" }
func (*reduceSlice) NumDep() int           { return 1 }
func (r *reduceSlice) Dep(i int) Dep       { return Dep{r.Slice, true, true} }

type reduceReader struct {
	typ      slicetype.Type
	combiner reflect.Value
	readers  []Reader
	err      error

	sorter Sorter
	heap   *frameBufferHeap
}

func (r *reduceReader) Read(ctx context.Context, out Frame) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.heap == nil {
		r.sorter = makeSorter(r.typ.Out(0), 0)
		r.heap = new(frameBufferHeap)
		r.heap.Sorter = r.sorter
		r.heap.Buffers = make([]*frameBuffer, 0, len(r.readers))
		for i := range r.readers {
			buf := &frameBuffer{
				Frame:  MakeFrame(r.typ, defaultChunksize),
				Reader: r.readers[i],
				Index:  i,
			}
			switch err := buf.Fill(ctx); {
			case err == EOF:
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
				if err := buf.Fill(ctx); err != nil && err != EOF {
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
		err = EOF
	}
	return n, err
}

func (r *reduceSlice) Reader(shard int, deps []Reader) Reader {
	return &reduceReader{typ: r, combiner: r.combiner, readers: deps}
}
