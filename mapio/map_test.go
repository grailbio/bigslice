// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package mapio

import (
	"bytes"
	"testing"
)

type mapSeeker struct{ *Map }

func (m mapSeeker) Seek(key []byte) Scanner { return m.Map.Seek(key) }

func TestMap(t *testing.T) {
	const N = 15000
	entries := makeEntries(N)

	var b bytes.Buffer
	w := NewWriter(&b, BlockSize(1024))
	for i := range entries {
		if err := w.Append(entries[i].Key, entries[i].Value); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	m, err := New(bytes.NewReader(b.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	testSeeker(t, entries, mapSeeker{m})
}

func TestEmptyMap(t *testing.T) {
	var b bytes.Buffer
	w := NewWriter(&b, BlockSize(1024))
	// Flush to get an extra (empty) block.
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	m, err := New(bytes.NewReader(b.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	scan := m.Seek(nil)
	if scan.Scan() {
		t.Error("expected EOF")
	}
	if err := scan.Err(); err != nil {
		t.Error(err)
	}
}
