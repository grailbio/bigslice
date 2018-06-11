// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package kernel

import (
	"reflect"

	"github.com/grailbio/bigslice/frame"
)

//go:generate go run genimpl.go

// The following are provided as convenience types to look up
// standard kernels.
var (
	SorterInterface  = reflect.TypeOf((*Sorter)(nil)).Elem()
	HasherInterface  = reflect.TypeOf((*Hasher)(nil)).Elem()
	IndexerInterface = reflect.TypeOf((*Indexer)(nil)).Elem()
)

// A Sorter sorts frames and implements comparison operators.
type Sorter interface {
	// Sort sorts the provided frame by its 0th column.
	Sort(frame.Frame)
	// Less reports whether row i in frame x is less than row j in frame y.
	Less(x frame.Frame, i int, y frame.Frame, j int) bool
	// IsSorted reports whether the frame f is sorted.
	IsSorted(frame.Frame) bool
}

// A Hasher hashes rows in a frame.
type Hasher interface {
	// HashFrame hashes each row in the provided frame by
	// the value in the frame's 0th column.
	HashFrame(frame.Frame, []uint32)
}

// Index represents an index of a frame's 0th column.
type Index interface {
	// Index indexes the provided column and deposits its
	// results in the provided integer slice.
	Index(reflect.Value, []int)
}

// An Indexer creates an (updateable) index from a frame.
type Indexer interface {
	// Index indexes a column and returns it.
	Index(reflect.Value) Index
}
