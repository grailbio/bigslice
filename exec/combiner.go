// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"context"
	"expvar"
	"fmt"
	"reflect"
	"sort"

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/log"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/slicefunc"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/slicetype"
	"github.com/grailbio/bigslice/sortio"
	"github.com/grailbio/bigslice/typecheck"
)

var (
	combinerKeys         = expvar.NewInt("combinerkeys")
	combinerRecords      = expvar.NewInt("combinerrecords")
	combinerTotalRecords = expvar.NewInt("combinertotalrecords")
	combineDiskSpills    = expvar.NewInt("combinediskspills")
)

var (
	combiningFrameInitSize    = defaultChunksize
	combiningFrameScratchSize = defaultChunksize
)

const (
	combiningFrameLoadFactor = 0.7

	// HashSeed is used when hashing keys in the hash table. This is to
	// prevent a previous partitioning step from reducing hash entropy.
	// In the extreme case, all entropy is removed and hash combine
	// operations become quadratic.
	hashSeed = 0x9acb0442

	// HashMaxCapacity is the largest possible combining hash table we
	// can maintain.
	hashMaxCapacity = 1 << 29
)

// TODO(marius): use ARC or something similarly adaptive when
// compacting and spilling combiner frames? It could make a big
// difference if keys have varying degrees of temporal locality.

// A combiningFrame maintains a frame wherein values are continually
// combined by a user-supplied combiner. CombingFrames have two
// columns: the first column is the key by which values are combined;
// the second column is the combined value for that key.
//
// CombiningFrame is a power-of-two sized hash table with quadratic
// probing (with c0=c1=1/2, which is guaranteed to explore every index
// in the hash table) implemented directly on top of a Frame.
type combiningFrame struct {
	// Combiner is a function that combines values in the frame.
	// It should have the signature func(x, y t) t, where t is the type
	// of Frame[1].
	Combiner slicefunc.Func

	typ slicetype.Type

	// vcol is the index of the column that stores the combined value.
	vcol int

	// Data is the data frame that is being combined. It stores both
	// the hash table and a scratch table.
	data frame.Frame

	// Scratch stores the scratch slice of data.
	scratch     frame.Frame
	scratchCall [2]reflect.Value

	// Threshold is the current a
	threshold int

	// Hits stores the hit count per index.
	hits []int

	// Len is the current data size of the hash table.
	len int
	// Cap is the size of the data portion of the data frame.
	cap int

	// Mask is the size mask to use for hashing.
	mask int
}

// MakeCombiningFrame creates and returns a new CombiningFrame with
// the provided type and combiner. MakeCombiningFrame panics if there
// is type disagreement. N and nscratch determine the initial frame
// size and scratch space size respective. The initial frame size
// must be a power of two.
func makeCombiningFrame(typ slicetype.Type, combiner slicefunc.Func, n, nscratch int) *combiningFrame {
	if res := typ.NumOut() - typ.Prefix(); res != 1 {
		typecheck.Panicf(1, "combining frame expects 1 residual column, got %d", res)
	}
	c := &combiningFrame{
		Combiner: combiner,
		typ:      typ,
		vcol:     typ.NumOut() - 1,
	}
	_, _, _ = c.make(n, nscratch)
	return c
}

func (c *combiningFrame) make(ndata, nscratch int) (data0, scratch0 frame.Frame, hits0 []int) {
	if ndata&(ndata-1) != 0 {
		panic("hash table size " + fmt.Sprint(ndata) + " not a power of two")
	}
	data0 = c.data
	scratch0 = c.scratch
	hits0 = c.hits
	c.data = frame.Make(c.typ, ndata+nscratch, ndata+nscratch)
	c.scratch = c.data.Slice(ndata, ndata+nscratch)
	c.hits = make([]int, ndata)
	c.threshold = int(combiningFrameLoadFactor * float64(ndata))
	c.mask = ndata - 1
	c.cap = ndata
	return
}

// Len returns the number of enetries in the combining frame.
func (c *combiningFrame) Len() int { return c.len }

// Cap returns the current capacity of the combining frame.
func (c *combiningFrame) Cap() int { return c.cap }

// Combine combines the provided frame into the the CombiningFrame:
// values in f are combined with existing values using the
// CombiningFrame's combiner. When no value exists for a key, the
// value is copied directly.
func (c *combiningFrame) Combine(f frame.Frame) {
	nchunk := (f.Len() + c.scratch.Len() - 1) / c.scratch.Len()
	for i := 0; i < nchunk; i++ {
		n := frame.Copy(c.scratch, f.Slice(c.scratch.Len()*i, f.Len()))
		c.combine(n)
	}
}

// Combine combines n items in the scratch space.
func (c *combiningFrame) combine(n int) {
	// TODO(marius): use cuckoo hashing
	// TODO(marius): propagate context
	ctx := context.Background()
	for i := 0; i < n; i++ {
		idx := int(c.scratch.HashWithSeed(i, hashSeed)) & c.mask
		for try := 1; ; try++ {
			if c.hits[idx] == 0 {
				c.hits[idx]++
				c.data.Swap(idx, c.cap+i)
				c.added()
				break
			} else if !c.data.Less(idx, c.cap+i) && !c.data.Less(c.cap+i, idx) {
				c.scratchCall[0] = c.data.Index(c.vcol, idx)
				c.scratchCall[1] = c.scratch.Index(c.vcol, i)
				rvs := c.Combiner.Call(ctx, c.scratchCall[:])
				c.data.Index(c.vcol, idx).Set(rvs[0])
				c.hits[idx]++
				break
			} else {
				// Probe quadratically.
				idx = (idx + try) & c.mask
			}
		}
	}
}

func (c *combiningFrame) added() {
	c.len += 1
	if c.len <= c.threshold {
		return
	}
	if c.cap == hashMaxCapacity {
		panic("hash table too large")
	}
	// Double the hash table size and rehash all the keys. Note that because
	// all of the keys are unique, we do not need to check for equality when
	// probing for a slot.
	n := c.cap * 2
	data0, scratch0, hits0 := c.make(n, c.scratch.Len())
	frame.Copy(c.scratch, scratch0)
	for i := range hits0 {
		if hits0[i] == 0 {
			continue
		}
		idx := int(data0.HashWithSeed(i, hashSeed)) & c.mask
		for try := 1; ; try++ {
			if c.hits[idx] == 0 {
				c.hits[idx] = hits0[i]
				frame.Copy(c.data.Slice(idx, idx+1), data0.Slice(i, i+1))
				break
			} else {
				idx = (idx + try) & c.mask
			}
		}
	}
}

// Compact returns a snapshot of all of the keys in the frame after
// compacting them into the beginning of the frame. After a call to
// Compact, the frame is considered empty; the returned Frame is
// valid only until the next call to Combine.
func (c *combiningFrame) Compact() frame.Frame {
	j := 0
	for i, n := range c.hits {
		if n == 0 {
			continue
		}
		c.data.Swap(i, j)
		c.hits[i] = 0
		j++
	}
	c.len = 0
	return c.data.Slice(0, j)
}

// A Combiner manages a CombiningFrame, spilling its contents to disk
// when it grows beyond a configured size threshold.
type combiner struct {
	slicetype.Type

	targetSize int
	comb       *combiningFrame
	combiner   slicefunc.Func
	spiller    sliceio.Spiller
	name       string
	total      int
	read       bool
}

// NewCombiner creates a new combiner with the given type, name,
// combiner, and target in-memory size (rows). Combiners can be
// safely accessed concurrently.
func newCombiner(typ slicetype.Type, name string, comb slicefunc.Func, targetSize int) (*combiner, error) {
	c := &combiner{
		Type:       typ,
		name:       name,
		combiner:   comb,
		targetSize: targetSize,
	}
	var err error
	c.spiller, err = sliceio.NewSpiller(name)
	if err != nil {
		return nil, err
	}
	c.comb = makeCombiningFrame(c, comb, *combiningFrameInitSize, *combiningFrameScratchSize)
	if !frame.CanCompare(typ.Out(0)) {
		typecheck.Panicf(1, "bigslice.newCombiner: cannot sort type %s", typ.Out(0))
	}
	return c, nil
}

func (c *combiner) spill(f frame.Frame) error {
	log.Debug.Printf("combiner %s: spilling %d rows disk", c.name, c.comb.Len())
	sort.Sort(f)
	n, err := c.spiller.Spill(f)
	if err == nil {
		combinerKeys.Add(-int64(f.Len()))
		combinerRecords.Add(-int64(c.total))
		c.total = 0
		log.Debug.Printf("combiner %s: spilled %s to disk", c.name, data.Size(n))
	} else {
		log.Error.Printf("combiner %s: failed to spill to disk: %v", c.name, err)
	}
	return err
}

// Combine combines the provided Frame into this combiner.
// If the number of in-memory keys is at or exceeds the target
// size threshold, the current frame is compacted and spilled to disk.
//
// TODO(marius): Combine blocks until the frame has been fully spilled
// to disk. We could copy the data and perform this spilling concurrently
// with writing.
func (c *combiner) Combine(ctx context.Context, f frame.Frame) error {
	n := f.Len()
	combinerRecords.Add(int64(n))
	combinerTotalRecords.Add(int64(n))
	c.total += n
	nkeys := c.comb.Len()
	c.comb.Combine(f)
	// TODO(marius): keep combining up to the next threshold; spill only if
	// we need to grow.  maybe Combine should return 'n', and then we invoke
	// 'grow' manually; or at least an option for this API.
	combinerKeys.Add(int64(c.comb.Len() - nkeys))
	if nkeys >= c.targetSize {
		// TODO(marius): we can copy the data and spill this concurrently
		spilled := c.comb.Compact()
		combineDiskSpills.Add(1)
		if err := c.spill(spilled); err != nil {
			return err
		}
	}
	return nil
}

// Discard discards this combiner's state. The combiner is invalid
// after a call to Discard.
func (c *combiner) Discard() error {
	return c.spiller.Cleanup()
}

// Reader returns a reader that streams the contents of this combiner.
// A call to Reader invalidates the combiner.
func (c *combiner) Reader() (sliceio.Reader, error) {
	defer c.spiller.Cleanup()
	readers, err := c.spiller.ClosingReaders()
	if err != nil {
		return nil, err
	}
	f := c.comb.Compact()
	sort.Sort(f)
	readers = append(readers, sliceio.FrameReader(f))
	return sortio.Reduce(c, c.name, readers, c.combiner), nil
}

// WriteTo writes the contents of this combiner to the provided
// encoder. A call to WriteTo invalidates the combiner. WriteTo
// merges content from the spilled combiner frames together with the
// current in-memory frame.
func (c *combiner) WriteTo(ctx context.Context, enc *sliceio.Encoder) (int64, error) {
	// TODO: this should be a generic encoder routine..
	reader, err := c.Reader()
	if err != nil {
		return 0, err
	}
	var total int64
	in := frame.Make(c, *defaultChunksize, *defaultChunksize)
	for {
		n, err := reader.Read(ctx, in)
		if err != nil && err != sliceio.EOF {
			return total, err
		}
		total += int64(n)
		if writeErr := enc.Write(ctx, in.Slice(0, n)); writeErr != nil {
			return total, writeErr
		}
		if err == sliceio.EOF {
			break
		}
	}
	return total, nil
}
