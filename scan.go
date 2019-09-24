// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"bufio"
	"io"
	"reflect"

	"github.com/grailbio/bigslice/sliceio"
)

var typeOfString = reflect.TypeOf("")

// ScanReader returns a slice of strings that are scanned from the
// provided reader. ScanReader shards the file by lines. Note that
// since ScanReader is unaware of the underlying data layout, it may
// be inefficient for highly parallel access: each shard must read
// the full file, skipping over data not belonging to the shard.
func ScanReader(nshard int, reader func() (io.ReadCloser, error)) Slice {
	Helper()
	type state struct {
		*bufio.Scanner
		io.Closer
	}
	return ReaderFunc(nshard, func(shard int, state *state, lines []string) (n int, err error) {
		defer func() {
			if err != nil && state.Closer != nil {
				state.Close()
			}
		}()
		first := state.Scanner == nil
		if first {
			rc, err := reader()
			if err != nil {
				return 0, err
			}
			state.Scanner = bufio.NewScanner(rc)
			state.Closer = rc
			if err := skip(state.Scanner, shard); err != nil {
				return 0, err
			}
		}

		for i := range lines {
			if !first || i != 0 {
				if err := skip(state.Scanner, nshard); err != nil {
					return i, err
				}
			}
			lines[i] = state.Text()
		}
		return len(lines), nil
	})
}

func skip(scan *bufio.Scanner, n int) error {
	for i := 0; i < n; i++ {
		if !scan.Scan() {
			if err := scan.Err(); err != nil {
				return err
			}
			return sliceio.EOF
		}
	}
	return nil
}
