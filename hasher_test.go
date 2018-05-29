// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"reflect"
	"testing"

	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/slicetype"
)

var typeOfCustomHash = reflect.TypeOf(customHash(0))

type customHash uint32

func (c customHash) Hash32() uint32 { return uint32(c) }

func TestHasher(t *testing.T) {
	f := frame.Make(slicetype.New(typeOfCustomHash), 10)
	c0 := f[0].Interface().([]customHash)
	for i := range c0 {
		c0[i] = customHash(i)
	}
	var (
		hasher = makeFrameHasher(typeOfCustomHash, 0)
		sum    = make([]uint32, f.Len())
	)
	hasher.HashFrame(f, sum)
	for i, v := range sum {
		if got, want := uint32(i), v; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
