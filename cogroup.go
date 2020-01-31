// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"container/heap"
	"context"
	"reflect"

	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/slicefunc"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/slicetype"
	"github.com/grailbio/bigslice/sortio"
	"github.com/grailbio/bigslice/typecheck"
)

type cogroupSlice struct {
	name     Name
	slices   []Slice
	out      []reflect.Type
	prefix   int
	numShard int
}

// Cogroup returns a slice that, for each key in any slice, contains
// the group of values for that key, in each slice. Schematically:
//
//	Cogroup(Slice<tk1, ..., tkp, t11, ..., t1n>, Slice<tk1, ..., tkp, t21, ..., t2n>, ..., Slice<tk1, ..., tkp, tm1, ..., tmn>)
//		Slice<tk1, ..., tkp, []t11, ..., []t1n, []t21, ..., []tmn>
//
// It thus implements a form of generalized JOIN and GROUP.
//
// Cogroup uses the prefix columns of each slice as its key; keys must be
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
	var keyTypes []reflect.Type
	for i, slice := range slices {
		if slice.NumOut() == 0 {
			typecheck.Panicf(1, "cogroup: slice %d has no columns", i)
		}
		if i == 0 {
			keyTypes = make([]reflect.Type, slice.Prefix())
			for j := range keyTypes {
				keyTypes[j] = slice.Out(j)
			}
		} else {
			if got, want := slice.Prefix(), len(keyTypes); got != want {
				typecheck.Panicf(1, "cogroup: prefix mismatch: expected %d but got %d", want, got)
			}
			for j := range keyTypes {
				if got, want := slice.Out(j), keyTypes[j]; got != want {
					typecheck.Panicf(1, "cogroup: key column type mismatch: expected %s but got %s", want, got)
				}
			}
		}
	}
	for i := range keyTypes {
		if !frame.CanHash(keyTypes[i]) {
			typecheck.Panicf(1, "cogroup: key column(%d) type %s cannot be hashed", i, keyTypes[i])
		}
		if !frame.CanCompare(keyTypes[i]) {
			typecheck.Panicf(1, "cogroup: key column(%d) type %s cannot be sorted", i, keyTypes[i])
		}
	}
	out := keyTypes
	for _, slice := range slices {
		for i := len(keyTypes); i < slice.NumOut(); i++ {
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
		name:     MakeName("cogroup"),
		numShard: numShard,
		slices:   slices,
		out:      out,
		prefix:   len(keyTypes),
	}
}

func (c *cogroupSlice) Name() Name             { return c.name }
func (c *cogroupSlice) NumShard() int          { return c.numShard }
func (c *cogroupSlice) ShardType() ShardType   { return HashShard }
func (c *cogroupSlice) NumOut() int            { return len(c.out) }
func (c *cogroupSlice) Out(i int) reflect.Type { return c.out[i] }
func (c *cogroupSlice) Prefix() int            { return c.prefix }
func (c *cogroupSlice) NumDep() int            { return len(c.slices) }
func (c *cogroupSlice) Dep(i int) Dep          { return Dep{c.slices[i], true, nil, false} }
func (*cogroupSlice) Combiner() slicefunc.Func { return slicefunc.Nil }

type cogroupReader struct {
	err error
	op  *cogroupSlice

	readers []sliceio.Reader

	heap *sortio.FrameBufferHeap
}

func (c *cogroupReader) Read(ctx context.Context, out frame.Frame) (int, error) {
	const (
		bufferSize = 128
		spillSize  = 1 << 25
	)
	if c.err != nil {
		return 0, c.err
	}
	if c.heap == nil {
		c.heap = new(sortio.FrameBufferHeap)
		c.heap.Buffers = make([]*sortio.FrameBuffer, 0, len(c.readers))
		// Maintain a compare buffer that's used to compare values across
		// the heterogeneously typed buffers.
		// TODO(marius): the extra copy and indirection here is unnecessary.
		lessBuf := frame.Make(slicetype.New(c.op.out[:c.op.prefix]...), 2, 2).Prefixed(c.op.prefix)
		c.heap.LessFunc = func(i, j int) bool {
			ib, jb := c.heap.Buffers[i], c.heap.Buffers[j]
			for i := 0; i < c.op.prefix; i++ {
				lessBuf.Index(i, 0).Set(ib.Frame.Index(i, ib.Index))
				lessBuf.Index(i, 1).Set(jb.Frame.Index(i, jb.Index))
			}
			return lessBuf.Less(0, 1)
		}

		// Sort each partition one-by-one. Since tasks are scheduled
		// to map onto a single CPU, we attain parallelism through sharding
		// at a higher level.
		for i := range c.readers {
			// Do the actual sort. Aim for ~30 MB spill files.
			// TODO(marius): make spill sizes configurable, or dependent
			// on the environment: for example, we could pass down a memory
			// allotment to each task from the scheduler.
			var sorted sliceio.Reader
			sorted, c.err = sortio.SortReader(ctx, spillSize, c.op.Dep(i), c.readers[i])
			if c.err != nil {
				// TODO(marius): in case this fails, we may leave open file
				// descriptors. We should make sure we close readers that
				// implement Discard.
				return 0, c.err
			}
			buf := &sortio.FrameBuffer{
				Frame:  frame.Make(c.op.Dep(i), bufferSize, bufferSize),
				Reader: sorted,
				Off:    i * bufferSize,
			}
			switch err := buf.Fill(ctx); {
			case err == sliceio.EOF:
				// No data. Skip.
			case err != nil:
				c.err = err
				return 0, err
			default:
				c.heap.Buffers = append(c.heap.Buffers, buf)
			}
		}
	}
	heap.Init(c.heap)

	// Now that we're sorted, perform a merge from each dependency.
	var (
		n       int
		max     = out.Len()
		lessBuf = frame.Make(slicetype.New(c.op.out[:c.op.prefix]...), 2, 2).Prefixed(c.op.prefix)
	)
	if max == 0 {
		panic("bigslice.Cogroup: max == 0")
	}
	// BUG: this is gnarly
	for n < max && len(c.heap.Buffers) > 0 {
		// First, gather all the records that have the same key.
		row := make([]frame.Frame, len(c.readers))
		var (
			key  = make([]reflect.Value, c.op.prefix)
			last = -1
		)
		// TODO(marius): the extra copy and indirection here is unnecessary.
		less := func() bool {
			buf := c.heap.Buffers[0]
			for i := 0; i < c.op.prefix; i++ {
				lessBuf.Index(i, 0).Set(row[last].Index(i, 0))
				lessBuf.Index(i, 1).Set(buf.Frame.Index(i, buf.Index))
			}
			return lessBuf.Less(0, 1)
		}

		for last < 0 || len(c.heap.Buffers) > 0 && !less() {
			// first key: need to pick the smallest one
			buf := c.heap.Buffers[0]
			idx := buf.Off / bufferSize
			row[idx] = frame.AppendFrame(row[idx], buf.Slice(buf.Index, buf.Index+1))
			buf.Index++
			if last < 0 {
				for i := 0; i < c.op.prefix; i++ {
					key[i] = row[idx].Index(i, 0)
				}
			}
			last = idx
			if buf.Index == buf.Len {
				if err := buf.Fill(ctx); err != nil && err != sliceio.EOF {
					c.err = err
					return n, err
				} else if err == sliceio.EOF {
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
		for i := range key {
			out.Index(j, n).Set(key[i])
			j++
		}
		// Note that here we are assuming that the key column is always first;
		// elsewhere we don't really make this assumption, even though it is
		// enforced when constructing a cogroup.
		for i := range row {
			typ := c.op.Dep(i)
			if row[i].Len() == 0 {
				for k := len(key); k < typ.NumOut(); k++ {
					out.Index(j, n).Set(reflect.Zero(c.op.out[j]))
					j++
				}
			} else {
				for k := len(key); k < typ.NumOut(); k++ {
					// TODO(marius): precompute type checks here.
					out.Index(j, n).Set(row[i].Value(k))
					j++
				}
			}
		}
		n++
	}
	if n == 0 {
		c.err = sliceio.EOF
	}
	return n, c.err
}

func (c *cogroupSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader {
	return &cogroupReader{
		op:      c,
		readers: deps,
	}
}
