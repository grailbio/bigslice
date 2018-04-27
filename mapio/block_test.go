// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package mapio

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"

	fuzz "github.com/google/gofuzz"
)

type entry struct{ Key, Value []byte }

func (e entry) Equal(f entry) bool {
	return bytes.Compare(e.Key, f.Key) == 0 &&
		bytes.Compare(e.Value, f.Value) == 0
}

func makeEntries(n int) []entry {
	fz := fuzz.New()
	//	fz.NumElements(1, 100)
	entries := make([]entry, n)
	// Fuzz manually so we can control the number of entries.
	for i := range entries {
		fz.Fuzz(&entries[i].Key)
		fz.Fuzz(&entries[i].Value)
	}
	sortEntries(entries)
	return entries
}

func sortEntries(entries []entry) {
	sort.Slice(entries, func(i, j int) bool {
		x := bytes.Compare(entries[i].Key, entries[j].Key)
		return x < 0 || x == 0 && bytes.Compare(entries[i].Value, entries[j].Value) < 0
	})
}

type seeker interface {
	Seek(key []byte) Scanner
}

func testSeeker(t *testing.T, entries []entry, seeker seeker) {
	t.Helper()

	s := seeker.Seek(nil)
	var scanned []entry
	for s.Scan() {
		var (
			key   = make([]byte, len(s.Key()))
			value = make([]byte, len(s.Value()))
		)
		copy(key, s.Key())
		copy(value, s.Value())
		scanned = append(scanned, entry{key, value})
	}
	if got, want := len(scanned), len(entries); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	isSorted := sort.SliceIsSorted(scanned, func(i, j int) bool {
		return bytes.Compare(scanned[i].Key, scanned[j].Key) < 0
	})
	if !isSorted {
		t.Error("scan returned non-sorted entries")
	}
	sortEntries(scanned)
	sortEntries(entries)
	for i := range scanned {
		if !entries[i].Equal(scanned[i]) {
			t.Errorf("scan: entry %d does not match", i)
		}
	}

	// Look up keys but in a random order.
	for n, i := range rand.Perm(len(entries)) {
		s := seeker.Seek(entries[i].Key)
		for s.Scan() {
			if bytes.Compare(entries[i].Key, s.Key()) != 0 {
				t.Errorf("%d: did not find key for %d", n, i)
			}

			// Since we may have multiple keys with the same value,
			// we have to scan until we see our expected value.
			if bytes.Compare(entries[i].Key, s.Key()) != 0 ||
				bytes.Compare(entries[i].Value, s.Value()) == 0 {
				break
			}
		}
		if !entries[i].Equal(entry{s.Key(), s.Value()}) {
			t.Errorf("%d: seek: entry %d does not match %d", n, i, bytes.Compare(entries[i].Key, s.Key()))
		}
	}

	lastKey := entries[len(entries)-1].Key
	bigKey := make([]byte, len(lastKey)+1)
	copy(bigKey, lastKey)
	s = seeker.Seek(bigKey)
	if s.Scan() {
		t.Error("scanned bigger key")
	}
}

type blockSeeker struct{ *block }

func (b *blockSeeker) Seek(key []byte) Scanner {
	b.block.Seek(key)
	return b
}

func (b *blockSeeker) Err() error { return nil }

func TestBlock(t *testing.T) {
	const N = 10000
	entries := makeEntries(N)

	buf := blockBuffer{restartInterval: 100}
	for i := range entries {
		buf.Append(entries[i].Key, entries[i].Value)
	}
	buf.Finish()

	block, err := readBlock(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	if got, want := block.nrestart, 100; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	testSeeker(t, entries, &blockSeeker{block})
}
