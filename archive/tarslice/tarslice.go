// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package tarslice implements bigslice operations for reading tar archives.
package tarslice

import (
	"archive/tar"
	"io"
	"io/ioutil"

	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/sliceio"
)

// Entry describes a single tar file entry, including its full contents.
type Entry struct {
	// Header is the full tar header.
	tar.Header
	// Body is the file contents.
	Body []byte
}

// Reader returns a slice of Entry records representing the tar
// archive of the io.ReadCloser returned by the archive func. Slices
// are sharded nshard ways, striped across entries. Note that the
// archive is read fully for each shard produced.
func Reader(nshard int, archive func() (io.ReadCloser, error)) bigslice.Slice {
	bigslice.Helper()
	type state struct {
		*tar.Reader
		io.Closer
	}
	return bigslice.ReaderFunc(nshard, func(shard int, state *state, entries []Entry) (n int, err error) {
		first := state.Reader == nil
		defer func() {
			if err != nil && state.Closer != nil {
				state.Close()
			}
		}()
		if first {
			rc, err := archive()
			if err != nil {
				return 0, err
			}
			state.Reader = tar.NewReader(rc)
			state.Closer = rc
			if err := skip(state.Reader, shard); err != nil {
				return 0, err
			}
		}
		for i := range entries {
			if !first || i > 0 {
				if err := skip(state.Reader, nshard-1); err != nil {
					return i, err
				}
			}
			head, err := state.Next()
			if err != nil {
				if err == io.EOF {
					err = sliceio.EOF
				}
				return i, err
			}
			entries[i].Header = *head
			entries[i].Body, err = ioutil.ReadAll(state)
			if err != nil {
				return i, err
			}
		}
		return len(entries), nil
	})
}

func skip(r *tar.Reader, n int) error {
	for i := 0; i < n; i++ {
		_, err := r.Next()
		if err == io.EOF {
			return sliceio.EOF
		}
	}
	return nil
}
