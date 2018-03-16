// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"reflect"
	"testing"

	fuzz "github.com/google/gofuzz"
)

func TestTaskBuffer(t *testing.T) {
	var batches [][]string
	fz := fuzz.New()
	fz.NilChance(0)
	fz.Fuzz(&batches)
	b := make(taskBuffer, 1)
	for _, batch := range batches {
		col := reflect.ValueOf(batch)
		b[0] = append(b[0], []reflect.Value{col})
	}
	s := &Scanner{
		readers: []Reader{b.Reader(0)},
		out:     []reflect.Type{typeOfString},
	}
	var (
		i   int
		str string
		q   = batches
	)
	for s.Scan(&str) {
		for len(q) > 0 && len(q[0]) == 0 {
			q = q[1:]
		}
		if len(q) == 0 {
			t.Error("long read")
			break
		}
		if got, want := str, q[0][0]; got != want {
			t.Errorf("element %d: got %v, want %v", i, got, want)
		}
		q[0] = q[0][1:]
		i++
	}
	if err := s.Err(); err != nil {
		t.Error(err)
	}
}
