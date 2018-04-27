// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package mapio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
)

const (
	maxBlockHeaderSize = binary.MaxVarintLen32 + // sharedSize
		binary.MaxVarintLen32 + // unsharedSize
		binary.MaxVarintLen32 // valueSize

	blockMinTrailerSize = 4 + // restart count
		1 + // block type
		4 // crc32 (IEEE) checksum of contents
)

var order = binary.LittleEndian

// A blockBuffer is a writable block buffer.
type blockBuffer struct {
	bytes.Buffer

	lastKey []byte

	restartInterval int
	restarts        []int
	restartCount    int
}

// Append appends the provided entry to the block. Must be called
// in lexicographic order of keys, or else Append panics.
func (b *blockBuffer) Append(key, value []byte) {
	if bytes.Compare(key, b.lastKey) < 0 {
		panic("keys added out of order")
	}
	var shared int
	if b.restartCount < b.restartInterval {
		n := len(b.lastKey)
		if len(key) < n {
			n = len(key)
		}
		for shared = 0; shared < n; shared++ {
			if key[shared] != b.lastKey[shared] {
				break
			}
		}
		b.restartCount++
	} else {
		b.restartCount = 0
		b.restarts = append(b.restarts, b.Len())
	}

	if b.lastKey == nil || cap(b.lastKey) < len(key) {
		b.lastKey = make([]byte, len(key))
	} else {
		b.lastKey = b.lastKey[:len(key)]
	}
	copy(b.lastKey[shared:], key[shared:])

	var hd [maxBlockHeaderSize]byte
	var pos int
	pos += binary.PutUvarint(hd[pos:], uint64(shared))
	pos += binary.PutUvarint(hd[pos:], uint64(len(key)-shared))
	pos += binary.PutUvarint(hd[pos:], uint64(len(value)))

	b.Write(hd[:pos])
	b.Write(key[shared:])
	b.Write(value)
}

// Finish completes the block by adding the block trailer.
func (b *blockBuffer) Finish() {
	b.Grow(4*(len(b.restarts)+1) + 1 + 4 + 4)
	var (
		pback [4]byte
		p     = pback[:]
	)
	if b.Buffer.Len() > 0 {
		// Add restart points. Zero is always a restart point (if block is nonempty).
		order.PutUint32(p, 0)
		b.Write(p)
		for _, off := range b.restarts {
			order.PutUint32(p, uint32(off))
			b.Write(p)
		}
		order.PutUint32(p, uint32(len(b.restarts)+1))
	} else {
		order.PutUint32(p, 0)
	}
	b.Write(p)
	b.WriteByte(0) // zero type. reserved.
	order.PutUint32(p, crc32.ChecksumIEEE(b.Bytes()))
	b.Write(p)
}

// Reset resets the contents of this block. After a call to reset,
// the blockBuffer instance may be used to write a new block.
func (b *blockBuffer) Reset() {
	b.lastKey = nil
	b.restarts = nil
	b.restartCount = 0
	b.Buffer.Reset()
}

// A block is an in-memory representation of a single block. Blocks
// maintain a current offset from which entries are scanned.
type block struct {
	p        []byte
	nrestart int
	restarts []byte

	key, value   []byte
	off, prevOff int
}

// Init initializes the block from the block contents stored at b.p.
// Init returns an error if the block is malformed or corrupted.
func (b *block) init() error {
	if len(b.p) < blockMinTrailerSize {
		return errors.New("invalid block: too small")
	}
	if got, want := crc32.ChecksumIEEE(b.p[:len(b.p)-4]), order.Uint32(b.p[len(b.p)-4:]); got != want {
		return fmt.Errorf("invalid checksum: expected %x, got %v", want, got)
	}
	off := len(b.p) - blockMinTrailerSize
	b.nrestart = int(order.Uint32(b.p[off:]))
	if b.nrestart*4 > off {
		return errors.New("corrupt block")
	}
	b.restarts = b.p[off-4*b.nrestart : off]
	if btype := b.p[off+4]; btype != 0 {
		return fmt.Errorf("invalid block type %d", btype)
	}
	b.p = b.p[:off-4*b.nrestart]
	b.key = nil
	b.value = nil
	b.off = 0
	b.prevOff = 0
	return nil
}

// Seek sets the block to the first position for which key <= b.Key().
func (b *block) Seek(key []byte) {
	restart := sort.Search(b.nrestart, func(i int) bool {
		b.off = int(order.Uint32(b.restarts[i*4:]))
		if !b.Scan() {
			panic("corrupt block")
		}
		return bytes.Compare(key, b.Key()) <= 0
	})
	if restart == 0 {
		// No more work needed. key <= the first key in the block.
		b.off = 0
		return
	}
	b.off = int(order.Uint32(b.restarts[(restart-1)*4:]))
	for b.Scan() {
		if bytes.Compare(key, b.Key()) <= 0 {
			b.unscan()
			break
		}
	}
}

// Scan reads the entry at the current position and then advanced the
// block's position to the next entry. Scan returns false when the
// position is at or beyond the end of the block.
func (b *block) Scan() bool {
	if b.off >= len(b.p) {
		return false
	}
	b.prevOff = b.off
	nshared, n := binary.Uvarint(b.p[b.off:])
	b.off += n
	nunshared, n := binary.Uvarint(b.p[b.off:])
	b.off += n
	nvalue, n := binary.Uvarint(b.p[b.off:])
	b.off += n
	b.key = append(b.key[:nshared], b.p[b.off:b.off+int(nunshared)]...)
	b.off += int(nunshared)
	b.value = b.p[b.off : b.off+int(nvalue)]
	b.off += int(nvalue)
	return true
}

func (b *block) unscan() {
	b.off = b.prevOff
}

// Key returns the key for the last scanned entry of the block.
func (b *block) Key() []byte {
	return b.key
}

// Value returns the value for the last scanned entry of the block.
func (b *block) Value() []byte {
	return b.value
}

func readBlock(p []byte) (*block, error) {
	b := &block{p: p}
	return b, b.init()
}
