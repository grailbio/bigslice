// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tarslice_test

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"sort"
	"testing"

	"github.com/grailbio/base/must"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/archive/tarslice"
	"github.com/grailbio/bigslice/slicetest"
)

var readTar = bigslice.Func(func(N int) bigslice.Slice {
	var buf bytes.Buffer
	w := tar.NewWriter(&buf)

	rnd := rand.New(rand.NewSource(1))
	p := make([]byte, 256)
	for i := 0; i < N; i++ {
		n := rnd.Intn(256)
		must.Nil(w.WriteHeader(&tar.Header{
			Name: fmt.Sprintf("%03d", i),
			Size: int64(n),
		}))
		for j := 0; j < n; j++ {
			p[j] = byte(n)
		}
		_, err := w.Write(p[:n])
		must.Nil(err)
	}
	must.Nil(w.Close())
	return tarslice.Reader(10, func() (io.ReadCloser, error) { return ioutil.NopCloser(bytes.NewReader(buf.Bytes())), nil })
})

func TestReader(t *testing.T) {
	const N = 1000
	var entries []tarslice.Entry
	args := []interface{}{N}
	slicetest.RunAndScan(t, readTar, args, &entries)
	if got, want := len(entries), N; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	for i, entry := range entries {
		if got, want := entry.Name, fmt.Sprintf("%03d", i); got != want {
			t.Errorf("entry %d: got %v, want %v", i, got, want)
		}
		n := len(entry.Body)
		for _, b := range entry.Body {
			if got, want := b, byte(n); got != want {
				t.Errorf("got %v, want %v", got, want)
			}
		}
	}
}
