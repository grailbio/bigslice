// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sliceio

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/grailbio/bigslice/frame"
)

// SpillBatchSize determines the amount of batching used in each
// spill file. A single read of a spill file produces this many rows.
// SpillBatchSize then trades off memory footprint for encoding size.
const SpillBatchSize = defaultChunksize

// A Spiller manages a set of spill files.
type Spiller string

// NewSpiller creates and returns a new spiller backed by a
// temporary directory.
func NewSpiller(name string) (Spiller, error) {
	dir, err := ioutil.TempDir("", fmt.Sprintf("spiller-%s-", name))
	if err != nil {
		return "", err
	}
	return Spiller(dir), nil
}

// Spill spills the provided frame to a new file in the spiller.
// Spill returns the file's encoded size, or an error. The frame
// is encoded in batches of SpillBatchSize.
func (dir Spiller) Spill(frame frame.Frame) (int, error) {
	f, err := ioutil.TempFile(string(dir), "")
	if err != nil {
		return 0, err
	}
	// TODO(marius): buffer?
	enc := NewEncoder(f)
	for frame.Len() > 0 {
		n := SpillBatchSize
		m := frame.Len()
		if m < n {
			n = m
		}
		if err := enc.Encode(frame.Slice(0, n)); err != nil {
			return 0, err
		}
		frame = frame.Slice(n, m)
	}
	size, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	return int(size), nil
}

// Readers returns a reader for each spiller file.
func (dir Spiller) Readers() ([]Reader, error) {
	f, err := os.Open(string(dir))
	if err != nil {
		return nil, err
	}
	infos, err := f.Readdir(-1)
	if err != nil {
		return nil, err
	}
	readers := make([]Reader, len(infos))
	closers := make([]io.Closer, len(infos))
	for i := range infos {
		f, err := os.Open(filepath.Join(string(dir), infos[i].Name()))
		if err != nil {
			for j := 0; j < i; j++ {
				closers[j].Close()
			}
			return nil, err
		}
		closers[i] = f
		readers[i] = &ClosingReader{NewDecodingReader(f), f}
	}
	return readers, nil
}

// Cleanup removes the spiller's temporary files. It is safe to call
// Cleanup after Readers(), but before reading is done.
func (dir Spiller) Cleanup() error {
	return os.RemoveAll(string(dir))
}
