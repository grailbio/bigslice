// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package sortio provides facilities for sorting slice outputs
// and merging and reducing sorted record streams.
package sortio

import (
	"container/heap"
	"context"
	"math"
	"sort"

	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/internal/defaultsize"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/slicetype"
)

var numCanaryRows = defaultsize.SortCanary

// SortReader sorts a Reader by its first column. SortReader may
// spill to disk, in which case it targets spill file sizes of
// spillTarget (in bytes). Because the encoded size of objects is not
// known in advance, sortReader uses a "canary" batch size of ~16k
// rows in order to estimate the size of future reads. The estimate
// is revisited on every subsequent fill and adjusted if it is
// violated by more than 5%.
func SortReader(ctx context.Context, spillTarget int, typ slicetype.Type, r sliceio.Reader) (sliceio.Reader, error) {
	spill, err := sliceio.NewSpiller("sorter")
	if err != nil {
		return nil, err
	}
	defer spill.Cleanup()
	f := frame.Make(typ, numCanaryRows, numCanaryRows)
	for {
		n, err := sliceio.ReadFull(ctx, r, f)
		if err != nil && err != sliceio.EOF {
			return nil, err
		}
		eof := err == sliceio.EOF
		g := f.Slice(0, n)
		sort.Sort(g)
		size, err := spill.Spill(g)
		if err != nil {
			return nil, err
		}
		if eof {
			break
		}
		bytesPerRow := size / n
		targetRows := spillTarget / bytesPerRow
		if targetRows < sliceio.SpillBatchSize {
			targetRows = sliceio.SpillBatchSize
		}
		// If we're within 5%, that's ok.
		if math.Abs(float64(f.Len()-targetRows)/float64(targetRows)) > 0.05 {
			f = f.Ensure(targetRows)
		}
	}
	readers, err := spill.Readers()
	if err != nil {
		return nil, err
	}
	return NewMergeReader(ctx, typ, readers)
}

// A FrameBuffer is a buffered frame. The frame is filled from
// a reader, and maintains a current index and length.
type FrameBuffer struct {
	// Frame is the buffer into which new data are read. The buffer is
	// always allocated externally and must be nonempty.
	frame.Frame
	// Reader is the slice reader from which the buffer is filled.
	sliceio.Reader
	// Index, Len is current index and length of the frame.
	Index, Len int

	// Off is the global offset of this framebuffer. It is added to
	// the index to compute the buffer's current position.
	Off int
}

// Pos returns this frame buffer's current position.
func (f FrameBuffer) Pos() int {
	return f.Off + f.Index
}

// Fill (re-) fills the FrameBuffer when it's empty. An error
// is returned if the underlying reader returns an error.
// EOF is returned if no more data are available.
func (f *FrameBuffer) Fill(ctx context.Context) error {
	if f.Index != f.Len {
		panic("FrameBuffer.Fill: fill on nonempty buffer")
	}
	var err error
	f.Len, err = f.Reader.Read(ctx, f.Frame)
	if err != nil && err != sliceio.EOF {
		return err
	}
	if err == sliceio.EOF && f.Len > 0 {
		err = nil
	}
	f.Index = 0
	if f.Len == 0 && err == nil {
		err = sliceio.EOF
	}
	return err
}

// FrameBufferHeap implements a heap of FrameBuffers,
// ordered by the provided sorter.
type FrameBufferHeap struct {
	Buffers []*FrameBuffer
	// Less compares the current index of buffers i and j.
	LessFunc func(i, j int) bool
}

func (f *FrameBufferHeap) Len() int { return len(f.Buffers) }
func (f *FrameBufferHeap) Less(i, j int) bool {
	return f.LessFunc(i, j)
}
func (f *FrameBufferHeap) Swap(i, j int) {
	f.Buffers[i], f.Buffers[j] = f.Buffers[j], f.Buffers[i]
}

// Push pushes a FrameBuffer onto the heap.
func (f *FrameBufferHeap) Push(x interface{}) {
	buf := x.(*FrameBuffer)
	f.Buffers = append(f.Buffers, buf)
}

// Pop removes the FrameBuffer with the smallest priority
// from the heap.
func (f *FrameBufferHeap) Pop() interface{} {
	n := len(f.Buffers)
	elem := f.Buffers[n-1]
	f.Buffers = f.Buffers[:n-1]
	return elem
}

// MergeReader merges multiple (sorted) readers into a
// single sorted reader.
type mergeReader struct {
	err  error
	heap *FrameBufferHeap
}

// NewMergeReader returns a new Reader that is sorted by its first
// column. The readers to be merged must already be sorted.
func NewMergeReader(ctx context.Context, typ slicetype.Type, readers []sliceio.Reader) (sliceio.Reader, error) {
	h := new(FrameBufferHeap)
	h.Buffers = make([]*FrameBuffer, 0, len(readers))
	n := len(readers) * sliceio.SpillBatchSize
	f := frame.Make(typ, n, n)
	h.LessFunc = func(i, j int) bool {
		return f.Less(h.Buffers[i].Pos(), h.Buffers[j].Pos())
	}
	for i := range readers {
		off := i * sliceio.SpillBatchSize
		fr := &FrameBuffer{
			Reader: readers[i],
			Frame:  f.Slice(off, off+sliceio.SpillBatchSize),
			Off:    off,
		}
		switch err := fr.Fill(ctx); {
		case err == sliceio.EOF:
			// No data. Skip.
		case err != nil:
			return nil, err
		default:
			h.Buffers = append(h.Buffers, fr)
		}
	}
	heap.Init(h)
	return &mergeReader{heap: h}, nil
}

// Read implements Reader.
func (m *mergeReader) Read(ctx context.Context, out frame.Frame) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	var (
		n   int
		max = out.Len()
	)
	for n < max && len(m.heap.Buffers) > 0 {
		idx := m.heap.Buffers[0].Index
		frame.Copy(out.Slice(n, n+1), m.heap.Buffers[0].Slice(idx, idx+1))
		n++
		m.heap.Buffers[0].Index++
		if m.heap.Buffers[0].Index == m.heap.Buffers[0].Len {
			if err := m.heap.Buffers[0].Fill(ctx); err != nil && err != sliceio.EOF {
				m.err = err
				return 0, err
			} else if err == sliceio.EOF {
				heap.Remove(m.heap, 0)
			} else {
				heap.Fix(m.heap, 0)
			}
		} else {
			heap.Fix(m.heap, 0)
		}
	}
	if n == 0 {
		m.err = sliceio.EOF
	}
	return n, m.err
}
