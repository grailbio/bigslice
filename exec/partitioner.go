// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/kernel"
)

// A partitioner uses a Hasher to partition a set of frame rows.
type partitioner struct {
	hasher kernel.Hasher
	width  int
	sum    []uint32
}

// NewPartitioner returns a partitioner that uses the provided
// Hasher and partition width.
func newPartitioner(h kernel.Hasher, width int) *partitioner {
	return &partitioner{hasher: h, width: width}
}

// Partition assigns rows of f into partitions. The first
// len(partition) rows of f are read.
func (p *partitioner) Partition(f frame.Frame, partitions []int) {
	if len(partitions) > cap(p.sum) {
		p.sum = make([]uint32, len(partitions))
	}
	p.hasher.HashFrame(f, p.sum[:len(partitions)])
	for i := range partitions {
		partitions[i] = int(p.sum[i]) % p.width
	}
}
