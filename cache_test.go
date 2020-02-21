// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.
package bigslice_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/exec"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/slicetest"
	"github.com/grailbio/testutil"
)

var cache = bigslice.Func(func(n, nShard int, dir string, computeAllowed bool) bigslice.Slice {
	input := make([]int, n)
	for i := range input {
		input[i] = i
	}
	slice := bigslice.Const(nShard, input)
	slice = bigslice.Map(slice, func(i int) int {
		if !computeAllowed {
			panic("compute not allowed")
		}
		return i * 2
	})
	var err error
	ctx := context.Background()
	slice, err = bigslice.Cache(ctx, slice, filepath.Join(dir, "cached"))
	if err != nil {
		panic(err)
	}
	return slice
})

func TestCache(t *testing.T) {
	runTestCache(t, cache)
}

var cacheDeps = bigslice.Func(func(n, nShard int, dir string, computeAllowed bool) bigslice.Slice {
	input := make([]int, n)
	for i := range input {
		input[i] = i
	}
	slice := bigslice.Const(nShard, input)
	// This shuffle causes a break in the pipeline, so the pipelined task
	// will have a dependency on the Const slice tasks. Caching should cause
	// compilation/execution to eliminate these dependencies safely.
	slice = bigslice.Reshuffle(slice)
	slice = bigslice.Map(slice, func(i int) int {
		if !computeAllowed {
			panic("compute not allowed")
		}
		return i * 2
	})
	var err error
	ctx := context.Background()
	slice, err = bigslice.Cache(ctx, slice, filepath.Join(dir, "cached"))
	if err != nil {
		panic(err)
	}
	return slice
})

// TestCacheDeps verifies that caching works when pipelined tasks have non-empty
// dependencies. When the cache is valid, we do not need to read from these
// dependencies. Verify that this does not break compilation or execution (e.g.
// empty dependencies given to tasks that expect non-empty dependencies).
func TestCacheDeps(t *testing.T) {
	exec.DoShuffleReaders = false
	defer func() {
		exec.DoShuffleReaders = true
	}()
	runTestCache(t, cacheDeps)
}

// runTestCache verifies that the caching in the slice returned by makeSlice
// behaves as expected. See usage in TestCache.
func runTestCache(t *testing.T, fn *bigslice.FuncValue) {
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	ctx := context.Background()

	const (
		N      = 10000
		Nshard = 10
	)
	if got, want := len(ls1(t, dir)), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	scan1 := slicetest.Run(t, fn, N, Nshard, dir, true)
	defer scan1.Close()
	if got, want := len(ls1(t, dir)), Nshard; got != want {
		t.Errorf("got %v [%v], want %v", got, ls1(t, dir), want)
	}

	// Recompute the slice to pick up the cached results.
	scan2 := slicetest.Run(t, fn, N, Nshard, dir, false)
	defer scan2.Close()
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

var cacheIncremental = bigslice.Func(func(N, Nshard int, dir string, rowsRan []bool) bigslice.Slice {
	input := make([]int, N)
	for i := range input {
		input[i] = i
	}
	slice := bigslice.Const(Nshard, input)
	slice = bigslice.Map(slice, func(i int) int {
		rowsRan[i] = true
		return i * 2
	})
	var err error
	ctx := context.Background()
	slice, err = bigslice.Cache(ctx, slice, filepath.Join(dir, "cached"))
	if err != nil {
		panic(err)
	}
	return slice
})

func TestCacheIncremental(t *testing.T) {
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()

	const (
		N      = 10000
		Nshard = 10
	)

	rowsRan := make([]bool, N)

	// Run and populate the cache.
	_ = runLocal(t, cacheIncremental, N, Nshard, dir, rowsRan)
	if got, want := len(ls1(t, dir)), Nshard; got != want {
		t.Errorf("got %v [%v], want %v", got, ls1(t, dir), want)
	}

	// Run and ensure there's no new computation.
	for i := range rowsRan {
		rowsRan[i] = false
	}
	_ = runLocal(t, cacheIncremental, N, Nshard, dir, rowsRan)
	if got, want := len(ls1(t, dir)), Nshard; got != want {
		t.Errorf("got %v [%v], want %v", got, ls1(t, dir), want)
	}
	for _, ran := range rowsRan {
		if ran {
			t.Error("want cache use")
		}
	}

	// Delete some cache entries and ensure there's recomputation.
	for i, f := range ls1(t, dir) {
		if i%2 == 0 {
			continue
		}
		if err := os.Remove(filepath.Join(dir, f)); err != nil {
			t.Error(err)
		}
	}
	for i := range rowsRan {
		rowsRan[i] = false
	}
	_ = runLocal(t, cacheIncremental, N, Nshard, dir, rowsRan)
	if got, want := len(ls1(t, dir)), Nshard; got != want {
		t.Errorf("got %v [%v], want %v", got, ls1(t, dir), want)
	}
	var nRans int
	for _, ran := range rowsRan {
		if ran {
			nRans++
		}
	}
	if nRans < Nshard {
		t.Error("want all recompution")
	}
}

var cachePartialIncremental = bigslice.Func(func(N, Nshard int, dir string, rowsRan []bool) bigslice.Slice {
	input := make([]int, N)
	for i := range input {
		input[i] = i
	}
	slice := bigslice.Const(Nshard, input)
	slice = bigslice.Map(slice, func(i int) int {
		rowsRan[i] = true
		return i * 2
	})
	var err error
	ctx := context.Background()
	slice, err = bigslice.CachePartial(ctx, slice, filepath.Join(dir, "cached"))
	if err != nil {
		panic(err)
	}
	return slice
})

func TestCachePartialIncremental(t *testing.T) {
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()

	const (
		N      = 10000
		Nshard = 10
	)

	rowsRan := make([]bool, N)

	// Run and populate the cache.
	_ = runLocal(t, cachePartialIncremental, N, Nshard, dir, rowsRan)
	if got, want := len(ls1(t, dir)), Nshard; got != want {
		t.Errorf("got %v [%v], want %v", got, ls1(t, dir), want)
	}

	// Run and ensure there's no new computation.
	for i := range rowsRan {
		rowsRan[i] = false
	}
	_ = runLocal(t, cachePartialIncremental, N, Nshard, dir, rowsRan)
	if got, want := len(ls1(t, dir)), Nshard; got != want {
		t.Errorf("got %v [%v], want %v", got, ls1(t, dir), want)
	}
	for _, ran := range rowsRan {
		if ran {
			t.Error("want cache use")
		}
	}

	// Delete some cache entries and ensure there's partial recomputation.
	for i, f := range ls1(t, dir) {
		if i%2 == 0 {
			continue
		}
		if err := os.Remove(filepath.Join(dir, f)); err != nil {
			t.Error(err)
		}
	}
	for i := range rowsRan {
		rowsRan[i] = false
	}
	_ = runLocal(t, cachePartialIncremental, N, Nshard, dir, rowsRan)
	if got, want := len(ls1(t, dir)), Nshard; got != want {
		t.Errorf("got %v [%v], want %v", got, ls1(t, dir), want)
	}
	var nRowsRan int
	for _, ran := range rowsRan {
		if ran {
			nRowsRan++
		}
	}
	if nRowsRan == 0 || nRowsRan >= N {
		t.Errorf("want partial recomputation, got %d of %d rows", nRowsRan, N)
	}
}

var cacheErr = bigslice.Func(func(dir string, errMsg string) bigslice.Slice {
	slice := bigslice.ReaderFunc(1, func(shard int, state *bool, ints []int) (n int, err error) {
		if *state {
			return 0, errors.New(errMsg)
		}
		for i := range ints {
			ints[i] = i
		}
		*state = true
		return len(ints), nil
	})
	ctx := context.Background()
	var err error
	slice, err = bigslice.Cache(ctx, slice, file.Join(dir, "cached"))
	if err != nil {
		panic(err)
	}
	return slice
})

func TestCacheErr(t *testing.T) {
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	err := slicetest.RunErr(cacheErr, dir, "first error")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "first error") {
		t.Error()
	}
	// Ensure computation is rerun after error.
	err = slicetest.RunErr(cacheErr, dir, "second error")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "second error") {
		t.Error()
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
	sort.Strings(paths)
	return paths
}

// runLocal runs f using the local executor, which runs all tasks within this
// process. We use it for tests that use shared memory in the slice
// computations.
func runLocal(t *testing.T, f *bigslice.FuncValue, args ...interface{}) *sliceio.Scanner {
	t.Helper()
	sess := exec.Start(exec.Local)
	res, err := sess.Run(context.Background(), f, args...)
	if err != nil {
		t.Fatalf("error running func: %v", err)
	}
	return res.Scanner()
}
