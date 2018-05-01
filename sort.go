// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"container/heap"
	"context"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
)

//go:generate go run gentype.go

// A Sorter implements a sort for a Frame schema.
type Sorter interface {
	// Sort sorts the provided frame.
	Sort(Frame)
	// Less returns true if row i of frame f is less than
	// row j of frame g.
	Less(f Frame, i int, g Frame, j int) bool
	// IsSorted tells whether the provided frame is sorted.
	IsSorted(Frame) bool
}

type frameSorter struct {
	Frame
	swappers []func(i, j int)
	less     func(i, j int) bool
}

func (s frameSorter) Less(i, j int) bool { return s.less(i, j) }

func (s frameSorter) Swap(i, j int) {
	for _, swap := range s.swappers {
		swap(i, j)
	}
}

// SortFrame sorts the provided frame using the provided
// comparison function.
func sortFrame(f Frame, less func(i, j int) bool) {
	var s frameSorter
	s.Frame = f
	s.swappers = make([]func(i, j int), len(f))
	for i := range f {
		s.swappers[i] = reflect.Swapper(f[i].Interface())
	}
	s.less = less
	sort.Sort(s)
}

// SortReader sorts a Reader using the provided Sorter. SortReader
// may spill to disk, in which case it targets spill file sizes of
// spillTarget (in bytes). Because the encoded size of objects is not
// known in advance, sortReader uses a "canary" batch size of ~16k
// rows in order to estimate the size of future reads. The estimate
// is revisited on every subsequent fill and adjusted if it is
// violated by more than 5%.
func sortReader(ctx context.Context, sorter Sorter, spillTarget int, typ Type, r Reader) (Reader, error) {
	spill, err := newSpiller()
	if err != nil {
		return nil, err
	}
	defer spill.Cleanup()
	f := MakeFrame(typ, 1<<14)
	for {
		n, err := ReadFull(ctx, r, f)
		if err != nil && err != EOF {
			return nil, err
		}
		eof := err == EOF
		g := f.Slice(0, n)
		sorter.Sort(g)
		size, err := spill.Spill(g)
		if err != nil {
			return nil, err
		}
		if eof {
			break
		}
		bytesPerRow := size / n
		targetRows := spillTarget / bytesPerRow
		if targetRows < spillBatchSize {
			targetRows = spillBatchSize
		}
		// If we're within 5%, that's ok.
		if math.Abs(float64(f.Len()-targetRows)/float64(targetRows)) > 0.05 {
			if targetRows <= f.Cap() {
				f = f.Slice(0, targetRows)
			} else {
				f = MakeFrame(typ, targetRows)
			}
		}
	}
	readers, err := spill.Readers()
	if err != nil {
		return nil, err
	}
	return newMergeReader(ctx, typ, sorter, readers)
}

// SpillBatchSize determines the amount of batching used in each
// spill file. A single read of a spill file produces this many rows.
// SpillBatchSize then trades off memory footprint for encoding size.
const spillBatchSize = 1024

// A spiller manages a set of spill files.
type spiller string

// NewSpiller creates and returns a new spiller backed by a
// temporary directory.
func newSpiller() (spiller, error) {
	dir, err := ioutil.TempDir("", "spiller")
	if err != nil {
		return "", err
	}
	return spiller(dir), nil
}

// Spill spills the provided frame to a new file in the spiller.
// Spill returns the file's encoded size, or an error. The frame
// is encoded in batches of spillBatchSize.
func (dir spiller) Spill(frame Frame) (int, error) {
	f, err := ioutil.TempFile(string(dir), "")
	if err != nil {
		return 0, err
	}
	// TODO(marius): buffer?
	enc := NewEncoder(f)
	for frame.Len() > 0 {
		n := spillBatchSize
		m := frame.Len()
		if m < n {
			n = m
		}
		if err := enc.Encode(frame.Slice(0, n)); err != nil {
			return 0, err
		}
		frame = frame.Slice(n, m)
	}
	size, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	return int(size), nil
}

// Readers returns a reader for each spiller file.
func (s spiller) Readers() ([]Reader, error) {
	dir, err := os.Open(string(s))
	if err != nil {
		return nil, err
	}
	infos, err := dir.Readdir(-1)
	if err != nil {
		return nil, err
	}
	readers := make([]Reader, len(infos))
	closers := make([]io.Closer, len(infos))
	for i := range infos {
		f, err := os.Open(filepath.Join(string(s), infos[i].Name()))
		if err != nil {
			for j := 0; j < i; j++ {
				closers[j].Close()
			}
			return nil, err
		}
		closers[i] = f
		readers[i] = &closingReader{newDecodingReader(f), f}
	}
	return readers, nil
}

// Cleanup removes the spiller's temporary files. It is safe to call
// Cleanup after Readers(), but before reading is done.
func (s spiller) Cleanup() error {
	return os.RemoveAll(string(s))
}

// A FrameBuffer is a buffered frame. The frame is filled from
// a reader, and maintains a current offset and length.
type frameBuffer struct {
	Frame
	Reader
	Off, Len int
	Index    int
}

// Fill (re-) filles the frameBuffer when it's empty. An error
// is returned if the underlying reader returns an error.
// EOF is returned if no more data are available.
func (f *frameBuffer) Fill(ctx context.Context) error {
	if f.Frame.Len() < f.Frame.Cap() {
		f.Frame = f.Frame.Slice(0, f.Frame.Cap())
	}
	var err error
	f.Len, err = f.Reader.Read(ctx, f.Frame)
	if err != nil && err != EOF {
		return err
	}
	if err == EOF && f.Len > 0 {
		err = nil
	}
	f.Off = 0
	return err
}

// FrameBufferHeap implements a heap of frameBuffers,
// ordered by the provided sorter.
type frameBufferHeap struct {
	Buffers []*frameBuffer
	Sorter  Sorter
}

func (f *frameBufferHeap) Len() int { return len(f.Buffers) }
func (f *frameBufferHeap) Less(i, j int) bool {
	return f.Sorter.Less(f.Buffers[i].Frame, f.Buffers[i].Off, f.Buffers[j].Frame, f.Buffers[j].Off)
}
func (f *frameBufferHeap) Swap(i, j int) {
	f.Buffers[i], f.Buffers[j] = f.Buffers[j], f.Buffers[i]
}

func (f *frameBufferHeap) Push(x interface{}) {
	f.Buffers = append(f.Buffers, x.(*frameBuffer))
}

func (f *frameBufferHeap) Pop() interface{} {
	n := len(f.Buffers)
	elem := f.Buffers[n-1]
	f.Buffers = f.Buffers[:n-1]
	return elem
}

// MergeReader merges multiple (sorted) readers into a
// single sorted reader.
type mergeReader struct {
	err  error
	heap *frameBufferHeap
}

// NewMergeReader returns a new mergeReader that is sorted
// according to the provided Sorter. The readers to be merged
// must already be sorted according to the same.
func newMergeReader(ctx context.Context, typ Type, sorter Sorter, readers []Reader) (*mergeReader, error) {
	h := new(frameBufferHeap)
	h.Sorter = sorter
	h.Buffers = make([]*frameBuffer, 0, len(readers))
	for i := range readers {
		fr := &frameBuffer{
			Reader: readers[i],
			Frame:  MakeFrame(typ, spillBatchSize),
		}
		switch err := fr.Fill(ctx); {
		case err == EOF:
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
func (m *mergeReader) Read(ctx context.Context, out Frame) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	var (
		row = make([]reflect.Value, len(out))
		n   int
		max = out.Len()
	)
	for n < max && len(m.heap.Buffers) > 0 {
		m.heap.Buffers[0].CopyIndex(row, m.heap.Buffers[0].Off)
		out.SetIndex(row, n)
		n++
		m.heap.Buffers[0].Off++
		if m.heap.Buffers[0].Off == m.heap.Buffers[0].Len {
			if err := m.heap.Buffers[0].Fill(ctx); err != nil && err != EOF {
				m.err = err
				return 0, err
			} else if err == EOF {
				heap.Remove(m.heap, 0)
			} else {
				heap.Fix(m.heap, 0)
			}
		} else {
			heap.Fix(m.heap, 0)
		}
	}
	if n == 0 {
		m.err = EOF
	}
	return n, m.err
}
