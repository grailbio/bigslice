// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sliceio

import (
	"context"
	"reflect"

	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/slicetype"
	"github.com/grailbio/bigslice/typecheck"
)

// A Scanner provides a convenient interface for reading records
// (e.g. from a Slice or a shard of a Slice). Successive calls to
// Scan (or Scanv) returns the next record (batch of records).
// Scanning stops when no more data are available or if an error is
// encountered. Scan returns true while it's safe to continue
// scanning. When scanning is complete, the user should inspect the
// scanner's error to see if scanning stopped because of an EOF or
// because another error occurred.
//
// Callers should not mix calls to Scan and Scanv.
type Scanner struct {
	Reader Reader
	Type   slicetype.Type

	err      error
	started  bool
	in       frame.Frame
	beg, end int
}

// Scan the next record into the provided columns. Scanning fails if
// the columns do not match arity and type with the underlying data
// set. Scan returns true while no errors are encountered and there
// remains data to be scanned.
func (s *Scanner) Scan(ctx context.Context, out ...interface{}) bool {
	if s.err != nil {
		return false
	}
	if len(out) != s.Type.NumOut() {
		s.err = typecheck.Errorf(1, "wrong arity: expected %d columns, got %d", s.Type.NumOut(), len(out))
		return false
	}
	for i := range out {
		if got, want := reflect.TypeOf(out[i]), reflect.PtrTo(s.Type.Out(i)); got != want {
			s.err = typecheck.Errorf(1, "wrong type for argument %d: expected *%s, got %s", i, want, got)
			return false
		}
	}
	if !s.started {
		s.started = true
		s.in = frame.Make(s.Type, defaultChunksize, defaultChunksize)
		s.beg, s.end = 0, 0
	}
	// Read the next batch of input.
	for s.beg == s.end {
		if s.Reader == nil {
			s.err = EOF
			return false
		}
		n, err := s.Reader.Read(ctx, s.in)
		if err != nil && err != EOF {
			s.err = err
			return false
		}
		s.beg, s.end = 0, n
		if err == EOF {
			s.Reader = nil
		}
	}
	// TODO(marius): this can be made faster
	for i, col := range out {
		reflect.ValueOf(col).Elem().Set(s.in.Index(i, s.beg))
	}
	s.beg++
	return true
}

// Scanv scans a batch of elements into the provided column vectors.
// Each column should be a slice of the correct type. Scanv fails
// when the type or arity of the column vectors do not match the
// underlying dataset. The number of records scanned is returned
// together with a boolean indicating whether scanning should
// continue, as in Scan.
func (s *Scanner) Scanv(ctx context.Context, out ...interface{}) (int, bool) {
	// TODO(marius): vectorize this all the way down
	if s.err != nil {
		return 0, false
	}
	columnvs := make([]reflect.Value, len(out))
	for i := range out {
		columnvs[i] = reflect.ValueOf(out[i])
		if columnvs[i].Kind() != reflect.Slice {
			panic("passed in non-slice column")
		}
	}
	n := columnvs[0].Len()
	for i := 0; i < n; i++ {
		args := make([]interface{}, len(out))
		for j := range args {
			args[j] = columnvs[j].Index(i).Addr().Interface()
		}
		if !s.Scan(ctx, args...) {
			return i, false
		}
	}
	return n, true
}

// Err returns any error that occurred while scanning.
func (s *Scanner) Err() error {
	if s.err == EOF {
		return nil
	}
	return s.err
}
