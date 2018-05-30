// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"container/heap"
	"context"
	"reflect"

	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/kernel"
	"github.com/grailbio/bigslice/typecheck"
)

type cogroupSlice struct {
	slices   []Slice
	out      []reflect.Type
	numShard int
	hasher   kernel.Hasher
	sorter   kernel.Sorter
}

// Cogroup returns a slice that, for each key in any slice, contains
// the group of values for that key, in each slice. Schematically:
//
//	Cogroup(Slice<tk, t11, ..., t1n>, Slice<tk, t21, ..., t2n>, ..., Slice<tk, tm2, ..., tmn>)
//		Slice<tk, []t11, ..., []t1n, []t21, ..., []tmn>
//
// It thus implemnets a form of generalized JOIN and GROUP.
//
// Cogroup uses the first column of each slice as its key; keys must be
// partitionable.
//
// TODO(marius): don't require spilling to disk when the input data
// set is small enough.
//
// TODO(marius): consider providing a version that returns scanners
// in the returned slice, so that we can stream through. This would
// require some changes downstream, however, so that buffering and
// encoding functionality also know how to read scanner values.
func Cogroup(slices ...Slice) Slice {
	if len(slices) == 0 {
		typecheck.Panic(1, "cogroup: expected at least one slice")
	}
	var keyType reflect.Type
	for i, slice := range slices {
		if slice.NumOut() == 0 {
			typecheck.Panicf(1, "cogroup: slice %d has no columns", i)
		}
		if i == 0 {
			keyType = slice.Out(0)
		} else if got, want := slice.Out(0), keyType; got != want {
			typecheck.Panicf(1, "cogroup: key column type mismatch: expected %s but got %s", want, got)
		}
	}

	// We need a hasher for partitioning and a sorter for sorting
	// each partition of each slice.
	var hasher kernel.Hasher
	if !kernel.Lookup(keyType, &hasher) {
		typecheck.Panicf(1, "cogroup: key type %s cannot be joined", keyType)
	}
	var sorter kernel.Sorter
	if !kernel.Lookup(keyType, &sorter) {
		typecheck.Panicf(1, "cogroup: no kernel.Sorter for key type %s", keyType)
	}
	out := []reflect.Type{keyType}
	for _, slice := range slices {
		for i := 1; i < slice.NumOut(); i++ {
			out = append(out, reflect.SliceOf(slice.Out(i)))
		}
	}

	// Pick the max of the number of parent shards, so that the input
	// will be partitioned as widely as the user desires.
	var numShard int
	for _, slice := range slices {
		if slice.NumShard() > numShard {
			numShard = slice.NumShard()
		}
	}

	return &cogroupSlice{
		numShard: numShard,
		slices:   slices,
		out:      out,
		sorter:   sorter,
		hasher:   hasher,
	}
}

func (c *cogroupSlice) NumShard() int        { return c.numShard }
func (c *cogroupSlice) ShardType() ShardType { return HashShard }

func (c *cogroupSlice) NumOut() int            { return len(c.out) }
func (c *cogroupSlice) Out(i int) reflect.Type { return c.out[i] }
func (c *cogroupSlice) Hasher() kernel.Hasher  { return c.hasher }
func (c *cogroupSlice) Op() string             { return "cogroup" }
func (c *cogroupSlice) NumDep() int            { return len(c.slices) }
func (c *cogroupSlice) Dep(i int) Dep          { return Dep{c.slices[i], true, false} }
func (*cogroupSlice) Combiner() *reflect.Value { return nil }

type cogroupReader struct {
	err    error
	op     *cogroupSlice
	hasher kernel.Hasher
	sorter kernel.Sorter

	readers []Reader

	heap *frameBufferHeap
}

func (c *cogroupReader) Read(ctx context.Context, out frame.Frame) (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	if c.heap == nil {
		c.heap = new(frameBufferHeap)
		c.heap.Sorter = c.sorter
		c.heap.Buffers = make([]*frameBuffer, 0, len(c.readers))
		// Sort each partition one-by-one. Since tasks are scheduled
		// to map onto a single CPU, we attain parallelism through sharding
		// at a higher level.
		for i := range c.readers {
			// Do the actual sort. Aim for ~500 MB spill files.
			// TODO(marius): make spill sizes configurable, or dependent
			// on the environment: for example, we could pass down a memory
			// allotment to each task from the scheduler.
			var sorted Reader
			sorted, c.err = sortReader(ctx, c.sorter, 1<<29, c.op.Dep(i), c.readers[i])
			if c.err != nil {
				// TODO(marius): in case this fails, we may leave open file
				// descriptors. We should make sure we close readers that
				// implement Discard.
				return 0, c.err
			}
			buf := &frameBuffer{
				Frame:  frame.Make(c.op.Dep(i), 1024),
				Reader: sorted,
				Index:  i,
			}
			switch err := buf.Fill(ctx); {
			case err == EOF:
				// No data. Skip.
			case err != nil:
				c.err = err
				return 0, err
			default:
				c.heap.Buffers = append(c.heap.Buffers, buf)
			}
		}
	}
	// Now that we're sorted, perform a merge from each dependency.
	var (
		n   int
		max = out.Len()
	)
	// BUG: this is gnarly
	for n < max && len(c.heap.Buffers) > 0 {
		row := make([]frame.Frame, len(c.readers))
		var (
			key  reflect.Value
			last = -1
		)
		for last < 0 || len(c.heap.Buffers) > 0 && !c.sorter.Less(row[last], 0, c.heap.Buffers[0].Frame, c.heap.Buffers[0].Off) {
			buf := c.heap.Buffers[0]
			row[buf.Index] = frame.Append(row[buf.Index], buf.Slice(buf.Off, buf.Off+1))
			buf.Off++
			if last < 0 {
				key = row[buf.Index][0].Index(0)
			}
			last = buf.Index
			if buf.Off == buf.Len {
				if err := buf.Fill(ctx); err != nil && err != EOF {
					c.err = err
					return n, err
				} else if err == EOF {
					heap.Remove(c.heap, 0)
				} else {
					heap.Fix(c.heap, 0)
				}
			} else {
				heap.Fix(c.heap, 0)
			}
		}

		// Now that we've gathered all the row values for a given key,
		// push them into our output.
		var j int
		out[j].Index(n).Set(key)
		j++
		// Note that here we are assuming that the key column is always first;
		// elsewhere we don't really make this assumption, even though it is
		// enforced when constructing a cogroup.
		for i := range row {
			typ := c.op.Dep(i)
			if row[i].Len() == 0 {
				for k := 1; k < typ.NumOut(); k++ {
					out[j].Index(n).Set(reflect.Zero(c.op.out[j]))
					j++
				}
			} else {
				for k := 1; k < typ.NumOut(); k++ {
					out[j].Index(n).Set(row[i][k])
					j++
				}
			}
		}
		n++
	}
	if n == 0 {
		c.err = EOF
	}
	return n, c.err
}

func (c *cogroupSlice) Reader(shard int, deps []Reader) Reader {
	return &cogroupReader{
		op:      c,
		readers: deps,
		hasher:  c.hasher,
		sorter:  c.sorter,
	}
}
