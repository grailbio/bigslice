// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"reflect"

	"github.com/grailbio/bigslice/slicetype"
)

// A CombiningFrame maintains a frame wherein values are continually
// combined by a user-supplied combiner. CombingFrames have two
// columns: the first column is the key by which values are combined;
// the second column is the combined value for that key.
//
// TODO(marius): CombiningFrame is currently in-memory only; it
// should spill to disk if it grows too large.
// TODO(marius): Instead of using an indexer (which must maintain
// redundant copies of keys), we could implement a hash table directly
// on top of a Frame.
type CombiningFrame struct {
	// Frame is the data frame that is being combined. Frame is
	// continually appended as new keys appear.
	Frame
	// Indexer is used to maintain an index on the Frame's key column.
	Indexer
	// Combiner is a function that combines values in the frame.
	// It should have the signature func(x, y t) t, where t is the type
	// of Frame[1].
	Combiner reflect.Value

	n       int
	indices []int
}

// CanMakeCombiningFrame tells whether the provided Frame type can be
// be made into a combining frame.
func canMakeCombiningFrame(typ slicetype.Type) bool {
	return typ.NumOut() == 2 && makeIndexer(typ.Out(0)) != nil
}

// MakeCombiningFrame creates and returns a new CombiningFrame
// with the provided type and combiner. MakeCombiningFrame panics
// if there is type disagreement.
func makeCombiningFrame(typ slicetype.Type, combiner reflect.Value) *CombiningFrame {
	if typ.NumOut() != 2 {
		typePanicf(1, "combining frame expects 2 columns, got %d", typ.NumOut())
	}
	f := new(CombiningFrame)
	f.Indexer = makeIndexer(typ.Out(0))
	if f.Indexer == nil {
		return nil
	}
	f.Frame = MakeFrame(typ, 0, defaultChunksize)
	f.Combiner = combiner
	return f
}

// Combine combines the provided frame into the the CombiningFrame:
// values in f are combined with existing values using the
// CombiningFrame's combiner. When no value exists for a key, the
// value is copied directly.
func (c *CombiningFrame) Combine(f Frame) {
	n := f.Len()
	if cap(c.indices) < n {
		c.indices = make([]int, n)
	}
	c.Index(f, c.indices[:n])
	for i := 0; i < n; i++ {
		ix := c.indices[i]
		if ix >= c.n {
			c.Frame = AppendFrame(c.Frame, f.Slice(i, i+1))
			c.n++
		} else {
			// TODO(marius): vectorize combiners too.
			rvs := c.Combiner.Call([]reflect.Value{c.Frame[1].Index(ix), f[1].Index(i)})
			c.Frame[1].Index(ix).Set(rvs[0])
		}
	}
}
