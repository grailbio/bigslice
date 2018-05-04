// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"reflect"
)

var typeOfHasher = reflect.TypeOf((*Hasher)(nil)).Elem()

// Hasher is implemented by user types that wish to provide
// their own hashing implementation.
type Hasher interface {
	// Hash32 returns a 32-bit hash of a value.
	Hash32() uint32
}

// A FrameHasher hashes rows in a Frame.
type FrameHasher interface {
	// HashFrame hashes the provided frame, depositing the 32-bit hash values
	// into the provided slice. Hashers should hash only the first
	// len(sum) rows of frame.
	HashFrame(frame Frame, sum []uint32)
}

// MakeFrameHasher mints a new hasher for the provided key type at the
// provided column. It returns nil if no such hasher can be created.
func makeFrameHasher(typ reflect.Type, col int) FrameHasher {
	if typ.Implements(typeOfHasher) {
		return hashFrameHasher(col)
	}
	h := makeFrameHasherGen(typ, col)
	if h != nil {
		return h
	}
	// Otherwise, we can hash fixed-size structures.
	if binary.Size(reflect.Zero(typ).Interface()) < 0 {
		return nil
	}
	return binaryHasher(col)
}

// A partitioner uses a Hasher to partition a set of frame rows.
type partitioner struct {
	hasher FrameHasher
	width  int
	sum    []uint32
}

// NewPartitioner returns a partitioner that uses the provided
// Hasher and partition width.
func newPartitioner(h FrameHasher, width int) *partitioner {
	return &partitioner{hasher: h, width: width}
}

// Partition assigns rows of f into partitions. The first
// len(partition) rows of f are read.
func (p *partitioner) Partition(f Frame, partitions []int) {
	if len(partitions) > cap(p.sum) {
		p.sum = make([]uint32, len(partitions))
	}
	p.hasher.HashFrame(f, p.sum[:len(partitions)])
	for i := range partitions {
		partitions[i] = int(p.sum[i]) % p.width
	}
}

// HashFrameHasher implements a frameHasher for column types
// that implement Hasher.
type hashFrameHasher int

func (col hashFrameHasher) HashFrame(f Frame, sum []uint32) {
	// TODO(marius): consider supporting a vectorized version of
	// this so we don't have to do virtual calls and interface conversions
	// for each row.
	for i := range sum {
		sum[i] = f[col].Index(i).Interface().(Hasher).Hash32()
	}
}

// BinaryHasher is a fall-back Hasher that uses Go's encoding/binary
// encoding to provide a deterministic hash.
type binaryHasher int

func (col binaryHasher) HashFrame(f Frame, sum []uint32) {
	var (
		b bytes.Buffer
		h = fnv.New32a()
	)
	for i := range sum {
		if err := binary.Write(&b, binary.LittleEndian, f[col].Interface()); err != nil {
			panic(err)
		}
		h.Write(b.Bytes())
		sum[i] = h.Sum32()
		h.Reset()
		b.Reset()
	}
}

// Hash32 is the 32-bit integer hashing function from
// http://burtleburtle.net/bob/hash/integer.html. (Public domain.)
func hash32(x uint32) uint32 {
	x = (x + 0x7ed55d16) + (x << 12)
	x = (x ^ 0xc761c23c) ^ (x >> 19)
	x = (x + 0x165667b1) + (x << 5)
	x = (x + 0xd3a2646c) ^ (x << 9)
	x = (x + 0xfd7046c5) + (x << 3)
	x = (x ^ 0xb55a4f09) ^ (x >> 16)
	return x
}

// Hash64 uses hash32 to compute a 64-bit integer hash.
func hash64(x uint64) uint32 {
	lo := hash32(uint32(x))
	hi := hash32(uint32(x >> 32))
	return lo ^ hi
}
