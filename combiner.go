// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"context"
	"expvar"
	"reflect"
	"sync"

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/log"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/kernel"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/slicetype"
	"github.com/grailbio/bigslice/typecheck"
)

// TODO(marius): use ARC or something similary adaptive when
// compacting and spilling combiner frames? It could make a big
// difference if keys have varying degrees of temporal locality.

var combineDiskSpills = expvar.NewInt("combinediskspills")

// A CombiningFrame maintains a frame wherein values are continually
// combined by a user-supplied combiner. CombingFrames have two
// columns: the first column is the key by which values are combined;
// the second column is the combined value for that key.
//
// TODO(marius): Instead of using an indexer (which must maintain
// redundant copies of keys), we could implement a hash table directly
// on top of a Frame.
type CombiningFrame struct {
	// Frame is the data frame that is being combined. Frame is
	// continually appended as new keys appear.
	frame.Frame
	// Combiner is a function that combines values in the frame.
	// It should have the signature func(x, y t) t, where t is the type
	// of Frame[1].
	Combiner reflect.Value

	// Swap swaps two elements of Frame.
	swap func(i, j int)

	// Index stores the index on the frame's Frame's key column.
	index   kernel.Index
	indexer kernel.Indexer

	// Hits stores the hit count per index.
	hits []int

	// Indices is the slice we pass into the indexer; it's stored here
	// for reuse.
	indices []int

	// N is the size of the combining frame.
	n int
}

// CanMakeCombiningFrame tells whether the provided Frame type can be
// be made into a combining frame.
func canMakeCombiningFrame(typ slicetype.Type) bool {
	return typ.NumOut() == 2 && kernel.Implements(typ.Out(0), kernel.IndexerInterface)
}

// MakeCombiningFrame creates and returns a new CombiningFrame
// with the provided type and combiner. MakeCombiningFrame panics
// if there is type disagreement.
func makeCombiningFrame(typ slicetype.Type, combiner reflect.Value) *CombiningFrame {
	if typ.NumOut() != 2 {
		typecheck.Panicf(1, "combining frame expects 2 columns, got %d", typ.NumOut())
	}
	f := new(CombiningFrame)
	if !kernel.Lookup(typ.Out(0), &f.indexer) {
		return nil
	}
	f.Frame = frame.Make(typ, 0, defaultChunksize)
	f.Combiner = combiner
	return f
}

// Combine combines the provided frame into the the CombiningFrame:
// values in f are combined with existing values using the
// CombiningFrame's combiner. When no value exists for a key, the
// value is copied directly.
func (c *CombiningFrame) Combine(f frame.Frame) {
	n := f.Len()
	if cap(c.indices) < n {
		c.indices = make([]int, n)
	}
	if c.index == nil {
		c.index = c.indexer.Index(f)
	}
	c.index.Index(f, c.indices[:n])
	for i := 0; i < n; i++ {
		ix := c.indices[i]
		if ix >= c.n {
			c.Frame = frame.Append(c.Frame, f.Slice(i, i+1))
			c.hits = append(c.hits, 0)
			c.n++
		} else {
			// TODO(marius): vectorize combiners too.
			rvs := c.Combiner.Call([]reflect.Value{c.Frame[1].Index(ix), f[1].Index(i)})
			c.Frame[1].Index(ix).Set(rvs[0])
			c.hits[ix]++
		}
	}
}

// Compact compacts this CombiningFrame to length n. The n most
// frequently accessed items are kept in the frame and reindexed. The
// returned Frame is the slice of the remainder of the Frame. It is
// only valid until the next call to Combine.
//
// Key hit counts are decayed by a factor of two; thus very frequent keys
// can retain memory residence for longer.
//
// TODO(marius): we could dynamically decide what the top N cutoff is.
func (c *CombiningFrame) Compact(n int) frame.Frame {
	if c.swap == nil {
		c.swap = c.Swapper()
	}
	if c.Len() < n {
		n = c.Len()
	}
	// Pack the top n hits into the frame.
	top := topn(c.hits, n)
	for i, j := range top {
		c.swap(i, j)
		c.hits[i] = c.hits[j] / 2
	}
	var g frame.Frame
	c.Frame, g = c.Slice(0, n), c.Slice(n, c.Len())
	c.index = c.indexer.Index(c.Frame)
	c.hits = c.hits[:n]
	c.n = n
	return g
}

// A Combiner manages a CombiningFrame, spilling its contents to disk
// when it grows beyond a configured size threshold.
type combiner struct {
	slicetype.Type

	mu sync.Mutex

	targetSize int
	comb       *CombiningFrame
	sorter     kernel.Sorter
	combiner   reflect.Value
	spiller    spiller
	name       string
	total      int
	read       bool
}

// NewCombiner creates a new combiner with the given type, name,
// combiner, and target in-memory size (rows). Combiners can be
// safely accessed concurrently.
func newCombiner(typ slicetype.Type, name string, comb reflect.Value, targetSize int) (*combiner, error) {
	c := &combiner{
		Type:       typ,
		name:       name,
		combiner:   comb,
		targetSize: targetSize,
	}
	var err error
	c.spiller, err = newSpiller()
	if err != nil {
		return nil, err
	}
	c.comb = makeCombiningFrame(c, comb)
	if !kernel.Lookup(typ.Out(0), &c.sorter) {
		typecheck.Panicf(1, "bigslice.newCombiner: no sorter kernel for type %s", typ.Out(0))
	}
	return c, nil
}

func (c *combiner) spill(f frame.Frame) error {
	log.Debug.Printf("combiner %s: spilling %d rows disk", c.name, c.comb.Frame.Len())
	c.sorter.Sort(f)
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
	c.mu.Lock()
	defer c.mu.Unlock()
	n := f.Len()
	combinerRecords.Add(int64(n))
	combinerTotalRecords.Add(int64(n))
	c.total += n
	nkeys := c.comb.Len()
	c.comb.Combine(f)
	combinerKeys.Add(int64(c.comb.Len() - nkeys))
	if nkeys >= c.targetSize {
		// TODO(marius): we can copy the data and spill this concurrently
		spilled := c.comb.Compact((c.targetSize + 9) / 10)
		combineDiskSpills.Add(1)
		if err := c.spill(spilled); err != nil {
			return err
		}
		spilled.Clear()
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
	readers, err := c.spiller.Readers()
	if err != nil {
		return nil, err
	}
	f := c.comb.Frame
	c.comb = nil
	c.sorter.Sort(f)
	readers = append(readers, sliceio.FrameReader(f))
	return &reduceReader{
		typ:      c,
		combiner: c.combiner,
		readers:  readers,
	}, nil
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
	in := frame.Make(c, defaultChunksize)
	for {
		n, err := reader.Read(ctx, in)
		if err != nil && err != sliceio.EOF {
			return total, err
		}
		total += int64(n)
		if err := enc.Encode(in.Slice(0, n)); err != nil {
			return total, err
		}
		if err == sliceio.EOF {
			break
		}
	}
	return total, nil
}
