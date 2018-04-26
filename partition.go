// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"encoding/binary"
	"hash/fnv"
	"reflect"
)

// A Partitioner assigns partition numbers to a Slice entry.
type Partitioner interface {
	// Partition writes the set of partitions for the entries (presented
	// as columns). Partition should write n entries, and the number of
	// partitions is provided by the argument width.
	Partition(f Frame, partitions []int, n, width int)
}

// makePartitioner creates a partitioner for the provided type and
// column. Nil is returned if no partitioner is available for the
// type.
func makePartitioner(typ reflect.Type, col int) Partitioner {
	switch typ.Kind() {
	case reflect.String:
		return stringPartitioner(col)
	case reflect.Int:
		return intPartitioner(col)
	case reflect.Int64:
		return int64Partitioner(col)
	default:
		return nil
	}
}

// StringPartitioner partitions strings by the provided column.
type stringPartitioner int

func (col stringPartitioner) Partition(in Frame, partitions []int, n, width int) {
	h := fnv.New32a()
	keys := in[col].Interface().([]string)
	for i := 0; i < n; i++ {
		h.Reset()
		h.Write([]byte(keys[i]))
		partitions[i] = int(h.Sum32()) % width
	}
}

// IntPartitioner partitions ints by the provided column.
type intPartitioner int

func (col intPartitioner) Partition(in Frame, partitions []int, n, width int) {
	h := fnv.New32a()
	keys := in[col].Interface().([]int)
	var (
		array [8]byte
		b     = array[:]
	)
	for i := 0; i < n; i++ {
		h.Reset()
		binary.LittleEndian.PutUint64(b, uint64(keys[i]))
		h.Write(b)
		partitions[i] = int(h.Sum32()) % width
	}
}

// Int64Partitioner partitions int64s by the provided column.
type int64Partitioner int

func (col int64Partitioner) Partition(in Frame, partitions []int, n, width int) {
	h := fnv.New32a()
	keys := in[col].Interface().([]int64)
	var (
		array [8]byte
		b     = array[:]
	)
	for i := 0; i < n; i++ {
		h.Reset()
		binary.LittleEndian.PutUint64(b, uint64(keys[i]))
		h.Write(b)
		partitions[i] = int(h.Sum32()) % width
	}
}
