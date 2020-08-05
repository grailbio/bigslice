// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"context"
	"fmt"
	"reflect"

	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/slicefunc"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/slicetype"
	"github.com/grailbio/bigslice/typecheck"
)

var (
	typeOfInt    = reflect.TypeOf(int(0))
	sliceTypeInt = slicetype.New(typeOfInt)
)

type reshuffleSlice struct {
	name        Name
	partitioner Partitioner
	Slice
}

// Reshuffle returns a slice that shuffles rows by prefix so that
// all rows with equal prefix values end up in the same shard.
// Rows are not sorted within a shard.
//
// The output slice has the same type as the input.
//
// TODO: Add ReshuffleSort, which also sorts keys within each shard.
func Reshuffle(slice Slice) Slice {
	if err := canMakeCombiningFrame(slice); err != nil {
		typecheck.Panic(1, err.Error())
	}
	return &reshuffleSlice{MakeName("reshuffle"), nil, slice}
}

// Repartition (re-)partitions the slice according to the provided function
// fn, which is invoked for each record in the slice to assign that record's
// shard. The function is supplied with the number of shards to partition
// over as well as the column values; the assigned shard is returned.
//
// Schematically:
//
//	Repartition(Slice<t1, t2, ..., tn> func(nshard int, v1 t1, ..., vn tn) int)  Slice<t1, t2, ..., tn>
func Repartition(slice Slice, partition interface{}) Slice {
	var (
		expectArg = slicetype.Append(sliceTypeInt, slice)
		expectRet = sliceTypeInt
	)
	fn, ok := slicefunc.Of(partition)
	if !ok {
		typecheck.Panicf(1, "repartition: not a function: %T", partition)
	}
	if !typecheck.Equal(fn.In, expectArg) || !typecheck.Equal(fn.Out, expectRet) {
		typecheck.Panicf(1, "repartition: expected %s, got %T", slicetype.Signature(expectArg, expectRet), partition)
	}
	part := func(ctx context.Context, frame frame.Frame, nshard int, shards []int) {
		args := make([]reflect.Value, slice.NumOut()+1)
		args[0] = reflect.ValueOf(nshard)
		for i := range shards {
			for j := 0; j < slice.NumOut(); j++ {
				args[j+1] = frame.Index(j, i)
			}
			result := fn.Call(ctx, args)
			shards[i] = int(result[0].Int())
		}
	}
	return &reshuffleSlice{MakeName("repartition"), part, slice}
}

func (r *reshuffleSlice) Name() Name             { return r.name }
func (*reshuffleSlice) NumDep() int              { return 1 }
func (r *reshuffleSlice) Dep(i int) Dep          { return Dep{r.Slice, true, r.partitioner, false} }
func (*reshuffleSlice) Combiner() slicefunc.Func { return slicefunc.Nil }

func (r *reshuffleSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader {
	if len(deps) != 1 {
		panic(fmt.Errorf("expected one dep, got %d", len(deps)))
	}
	return deps[0]
}
