// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"fmt"

	"github.com/grailbio/bigslice/slicefunc"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/typecheck"
)

type reshardSlice struct {
	name   Name
	nshard int
	Slice
}

// Reshard returns a slice that is resharded to the given
// number of shards; this is done by re-shuffling to the
// provided number of shards.
func Reshard(slice Slice, nshard int) Slice {
	if err := canMakeCombiningFrame(slice); err != nil {
		typecheck.Panic(1, err.Error())
	}
	if slice.NumShard() == nshard {
		return slice
	}
	return &reshardSlice{MakeName("reshard"), nshard, slice}
}

func (r *reshardSlice) Name() Name             { return r.name }
func (*reshardSlice) NumDep() int              { return 1 }
func (r *reshardSlice) NumShard() int          { return r.nshard }
func (r *reshardSlice) Dep(i int) Dep          { return Dep{r.Slice, true, nil, false} }
func (*reshardSlice) Combiner() slicefunc.Func { return slicefunc.Nil }

func (r *reshardSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader {
	if len(deps) != 1 {
		panic(fmt.Errorf("expected one dep, got %d", len(deps)))
	}
	return deps[0]
}
