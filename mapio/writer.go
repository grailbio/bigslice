// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package mapio

import (
	"encoding/binary"
	"io"
)

const (
	maxBlockAddrSize = binary.MaxVarintLen64 + // offset
		binary.MaxVarintLen64 // len

	mapTrailerSize = maxBlockAddrSize + // meta block index (padded)
		maxBlockAddrSize + // index address (padded)
		8 // magic

	mapTrailerMagic = 0xa8b2374e8558bc76
)

type blockAddr struct {
	off uint64
	len uint64
}

func putBlockAddr(p []byte, b blockAddr) int {
	off := binary.PutUvarint(p, b.off)
	return off + binary.PutUvarint(p[off:], b.len)
}

func getBlockAddr(p []byte) (b blockAddr, n int) {
	var m int
	b.off, n = binary.Uvarint(p)
	b.len, m = binary.Uvarint(p[n:])
	n += m
	return
}

// A Writer appends key-value pairs to a map. Keys must be appended
// in lexicographic order.
type Writer struct {
	data, index blockBuffer
	w           io.Writer

	lastKey []byte

	blockSize int
	off       int
}

const (
	defaultBlockSize       = 1 << 12
	defaultRestartInterval = 16
)

// WriteOption represents a tunable writer parameter.
type WriteOption func(*Writer)

// BlockSize sets the writer's target block size to sz (in bytes).
// Note that keys and values cannot straddle blocks, so that if large
// data are added to a map, block sizes can grow large. The default
// target block size is 4KB.
func BlockSize(sz int) WriteOption {
	return func(w *Writer) {
		w.blockSize = sz
	}
}

// RestartInterval sets the writer's restart interval to
// provided value. The default restart interval is 16.
func RestartInterval(iv int) WriteOption {
	return func(w *Writer) {
		w.data.restartInterval = iv
		w.index.restartInterval = iv
	}
}

// NewWriter returns a new Writer that writes a map to the provided
// io.Writer. BlockSize specifies the target block size, while
// restartInterval determines the frequency of key restart points,
// which trades off lookup performance with size. See package docs
// for more details.
func NewWriter(w io.Writer, opts ...WriteOption) *Writer {
	wr := &Writer{
		w:         w,
		blockSize: defaultBlockSize,
	}
	wr.data.restartInterval = defaultRestartInterval
	wr.index.restartInterval = defaultRestartInterval
	for _, opt := range opts {
		opt(wr)
	}
	return wr
}

// Append appends an entry to the maps. Keys must be provided
// in lexicographic order.
func (w *Writer) Append(key, value []byte) error {
	w.data.Append(key, value)
	if w.lastKey == nil || cap(w.lastKey) < len(key) {
		w.lastKey = make([]byte, len(key))
	} else {
		w.lastKey = w.lastKey[:len(key)]
	}
	copy(w.lastKey, key)
	if w.data.Len() > w.blockSize {
		return w.Flush()
	}
	return nil
}

// Flush creates a new block with the current contents. It forces the
// creation of a new block, and overrides the Writer's block size
// parameter.
func (w *Writer) Flush() error {
	w.data.Finish()
	n, err := w.w.Write(w.data.Bytes())
	if err != nil {
		return err
	}
	w.data.Reset()
	off := w.off
	w.off += n

	// TODO(marius): we can get more clever about key compression here:
	// We need to guarantee that the lastKey <= indexKey < firstKey,
	// where firstKey is the first key in the next block. We can thus
	// construct a more minimal key to store in the index.
	b := make([]byte, maxBlockAddrSize)
	n = putBlockAddr(b, blockAddr{uint64(off), uint64(n)})
	w.index.Append(w.lastKey, b[:n])

	return nil
}

// Close flushes the last block of the writer and writes the map's
// trailer. After successful close, the map is ready to be opened.
func (w *Writer) Close() error {
	if err := w.Flush(); err != nil {
		return err
	}
	w.index.Finish()
	n, err := w.w.Write(w.index.Bytes())
	if err != nil {
		return err
	}
	w.index.Reset()
	indexAddr := blockAddr{uint64(w.off), uint64(n)}
	w.off += n

	trailer := make([]byte, mapTrailerSize)
	putBlockAddr(trailer, blockAddr{}) // address of meta block index. tbd.
	putBlockAddr(trailer[maxBlockAddrSize:], indexAddr)
	order.PutUint64(trailer[len(trailer)-8:], mapTrailerMagic)
	_, err = w.w.Write(trailer)
	return err
}
