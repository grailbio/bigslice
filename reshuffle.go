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

var typeOfInt = reflect.TypeOf(int(0))

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
	return &reshuffleSlice{makeName("reshuffle"), nil, slice}
}

// Repartition (re-)partitions the slice according to the provided
// function fn. Fn has the schematic type
//
//	func(nshard int, col1 type1, col2 type2, ..., colN typeN) int
//
// It is invoked for each record in the slice, and returns the
// shard assignment for that row.
func Repartition(slice Slice, fn interface{}) Slice {
	arg, ret, ok := typecheck.Func(fn)
	if !ok ||
		arg.NumOut() < 1 || arg.Out(0) != typeOfInt ||
		ret.NumOut() != 1 || ret.Out(0) != typeOfInt {
		typecheck.Panicf(1, "repartition: invalid partitioning function %T", fn)
	}
	// The first argument is the number of shards to partition over, so we
	// drop this.
	arg = slicetype.Slice(arg, 1, arg.NumOut())
	if !typecheck.Equal(slice, arg) {
		typecheck.Panicf(1, "repartition: function %T does not match input slice type %s", fn, slicetype.String(slice))
	}
	fval := slicefunc.Of(fn)
	part := func(frame frame.Frame, nshard int, shards []int) {
		args := make([]reflect.Value, slice.NumOut()+1)
		args[0] = reflect.ValueOf(nshard)
		for i := range shards {
			for j := 0; j < slice.NumOut(); j++ {
				args[j+1] = frame.Index(j, i)
			}
			result := fval.Call(context.TODO(), args)
			shards[i] = int(result[0].Int())
		}
	}
	return &reshuffleSlice{makeName("repartition"), part, slice}
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
