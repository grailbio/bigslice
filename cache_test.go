// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.
package bigslice_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/grailbio/base/file"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/slicetest"
	"github.com/grailbio/testutil"
)

func TestCache(t *testing.T) {
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	ctx := context.Background()

	const (
		N      = 10000
		Nshard = 10
	)
	input := make([]int, N)
	for i := range input {
		input[i] = i
	}
	makeSlice := func() bigslice.Slice {
		slice := bigslice.Const(Nshard, input)
		slice = bigslice.Map(slice, func(i int) int { return i * 2 })
		var err error
		slice, err = bigslice.Cache(ctx, slice, filepath.Join(dir, "cached"))
		if err != nil {
			t.Fatal(err)
		}
		return slice
	}

	slice := makeSlice()
	if got, want := len(ls1(t, dir)), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	scan1 := run(ctx, t, slice)["Local"]
	if got, want := len(ls1(t, dir)), Nshard; got != want {
		t.Errorf("got %v [%v], want %v", got, ls1(t, dir), want)
	}

	// Recompute the slice to pick up the cached results.
	slice = makeSlice()
	if isWriteThrough(slice) {
		t.Error("did not expect writethrough slice")
	}

	scan2 := run(ctx, t, slice)["Local"]
	if got, want := len(ls1(t, dir)), Nshard; got != want {
		t.Errorf("got %v [%v], want %v", got, ls1(t, dir), want)
	}

	var n int
	for {
		var v1, v2 int
		ok := scan1.Scan(ctx, &v1)
		if got, want := scan2.Scan(ctx, &v2), ok; got != want {
			t.Errorf("got %v, want %v", got, want)
			break
		}
		if !ok {
			break
		}
		if v1 != v2 {
			t.Errorf("%v != %v", v1, v2)
		}
		n++
	}
	if got, want := n, N; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if err := scan1.Err(); err != nil {
		t.Errorf("scan1: %v", err)
	}
	if err := scan2.Err(); err != nil {
		t.Errorf("scan2: %v", err)
	}
}

func TestCacheErr(t *testing.T) {
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	ctx := context.Background()

	makeSlice := func() bigslice.Slice {
		slice := bigslice.ReaderFunc(1, func(shard int, state *bool, ints []int) (n int, err error) {
			if *state {
				return 0, errors.New("random error")
			}
			for i := range ints {
				ints[i] = i
			}
			*state = true
			return len(ints), nil
		})
		var err error
		slice, err = bigslice.Cache(ctx, slice, file.Join(dir, "cached"))
		if err != nil {
			t.Fatal(err)
		}
		return slice
	}
	if slice := makeSlice(); !isWriteThrough(slice) {
		t.Errorf("expected writethrough slice, got %T", slice)
	}
	if err := slicetest.RunErr(makeSlice()); err == nil {
		t.Error("expected error")
	}
	if slice := makeSlice(); !isWriteThrough(slice) {
		t.Errorf("expected writethrough slice, got %T", slice)
	}
}

func ls1(t *testing.T, dir string) []string {
	t.Helper()
	d, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	infos, err := d.Readdir(-1)
	if err != nil {
		t.Fatal(err)
	}
	paths := make([]string, len(infos))
	for i := range paths {
		paths[i] = infos[i].Name()
	}
	return paths
}

func isWriteThrough(slice bigslice.Slice) bool {
	_, ok := slice.(interface{ IsWriteThrough() })
	return ok
}
