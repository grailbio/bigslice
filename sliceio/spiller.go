// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sliceio

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/grailbio/base/backgroundcontext"
	"github.com/grailbio/base/file"
	"github.com/grailbio/bigslice/frame"
)

// SpillBatchSize determines the amount of batching used in each
// spill file. A single read of a spill file produces this many rows.
// SpillBatchSize then trades off memory footprint for encoding size.
var SpillBatchSize = defaultChunksize

// A Spiller manages a set of spill files.
type Spiller string

// NewSpiller creates and returns a new spiller backed by a
// temporary directory. Spillers do not guarantee that the order
// of spillers returned matches the order of spills.
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
	// Generate a random path and divide it into a hierarchy
	// of paths so that any particular directory does not get
	// too big. We'll use 3 levels of hierarchy with a fanout
	// of 255.
	dirPath := string(dir)
	for i := 0; i < 3; i++ {
		dirPath = filepath.Join(dirPath, fmt.Sprintf("%02x", rand.Intn(255)))
	}
	_ = os.MkdirAll(dirPath, 0777)
	f, err := ioutil.TempFile(dirPath, "spill-")
	if err != nil {
		return 0, err
	}
	// TODO(marius): buffer?
	enc := NewEncodingWriter(f)
	for frame.Len() > 0 {
		n := SpillBatchSize
		m := frame.Len()
		if m < n {
			n = m
		}
		if err := enc.Write(frame.Slice(0, n)); err != nil {
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

// Readers returns a ReadCloser for each spiller file.
func (dir Spiller) Readers() ([]ReadCloser, error) {
	var paths []string
	// These are always on local paths, so background context is ok.
	list := file.List(backgroundcontext.Get(), string(dir), true)
	for list.Scan() {
		if list.IsDir() {
			continue
		}
		paths = append(paths, list.Path())
	}
	if err := list.Err(); err != nil {
		return nil, err
	}
	readers := make([]ReadCloser, len(paths))
	for i, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			for j := 0; j < i; j++ {
				readers[j].Close()
			}
			return nil, err
		}
		readers[i] = ReaderWithCloseFunc{NewDecodingReader(f), f.Close}
	}
	return readers, nil
}

// ClosingReaders returns a reader for each spiller file. The readers close the
// underlying file when Read returns a non-nil error (otherwise the underlying
// file resource will leak).
func (dir Spiller) ClosingReaders() ([]Reader, error) {
	readers, err := dir.Readers()
	if err != nil {
		return nil, err
	}
	cReaders := make([]Reader, len(readers))
	for i, r := range readers {
		cReaders[i] = NewClosingReader(r)
	}
	return cReaders, nil
}

// Cleanup removes the spiller's temporary files. It is safe to call
// Cleanup after Readers(), but before reading is done.
func (dir Spiller) Cleanup() error {
	return os.RemoveAll(string(dir))
}
