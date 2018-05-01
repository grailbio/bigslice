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

// Hasher implements deterministic hashing of a Frame.
type Hasher interface {
	// Hash hashes the provided frame, depositing the 32-bit hash values
	// into the provided slice. Hashers should hash only the first
	// len(sum) rows of frame.
	Hash(frame Frame, sum []uint32)
}

// makeHasher mints a new hasher for the provided key type at the
// provided column. It returns nil if no such hasher can be created.
func makeHasher(typ reflect.Type, col int) Hasher {
	h := makeHasherGen(typ, col)
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
	hasher Hasher
	width  int
	sum    []uint32
}

// NewPartitioner returns a partitioner that uses the provided
// Hasher and partition width.
func newPartitioner(h Hasher, width int) *partitioner {
	return &partitioner{hasher: h, width: width}
}

// Partition assigns rows of f into partitions. The first
// len(partition) rows of f are read.
func (p *partitioner) Partition(f Frame, partitions []int) {
	if len(partitions) > cap(p.sum) {
		p.sum = make([]uint32, len(partitions))
	}
	p.hasher.Hash(f, p.sum[:len(partitions)])
	for i := range partitions {
		partitions[i] = int(p.sum[i]) % p.width
	}
}

// BinaryHasher is a fall-back Hasher that uses Go's encoding/binary
// encoding to provide a deterministic hash.
type binaryHasher int

func (col binaryHasher) Hash(f Frame, sum []uint32) {
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
