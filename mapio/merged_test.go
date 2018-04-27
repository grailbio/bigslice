// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package mapio

import (
	"bytes"
	"testing"
)

type mergedSeeker struct{ Merged }

func (m mergedSeeker) Seek(key []byte) Scanner { return m.Merged.Seek(key) }

func TestMerged(t *testing.T) {
	const (
		N = 10000
		M = 10
	)
	var (
		entries = makeEntries(N)
		buffers = make([]bytes.Buffer, M)
		writers = make([]*Writer, M)
	)
	for i := range writers {
		writers[i] = NewWriter(&buffers[i], BlockSize(1024))
	}
	for i := range entries {
		writers[i%M].Append(entries[i].Key, entries[i].Value)
	}
	for i := range writers {
		if err := writers[i].Close(); err != nil {
			t.Fatal(i, err)
		}
	}
	merged := make(Merged, M)
	for i := range buffers {
		var err error
		merged[i], err = New(bytes.NewReader(buffers[i].Bytes()))
		if err != nil {
			t.Fatal(i, err)
		}
	}

	testSeeker(t, entries, mergedSeeker{merged})
}
