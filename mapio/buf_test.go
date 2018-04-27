// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package mapio

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestBuf(t *testing.T) {
	const N = 1000
	entries := makeEntries(N)
	// Shuffle to make sure the buffer sorts properly.
	rand.Shuffle(len(entries), func(i, j int) {
		entries[i], entries[j] = entries[j], entries[i]
	})
	var buf Buf
	for _, e := range entries {
		buf.Append(e.Key, e.Value)
	}
	var b bytes.Buffer
	w := NewWriter(&b, BlockSize(1<<10), RestartInterval(10))
	if err := buf.WriteTo(w); err != nil {
		t.Fatal(err)
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
