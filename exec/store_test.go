// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/testutil"
)

func testStore(t *testing.T, store Store) {
	t.Helper()
	fz := fuzz.New()
	fz.NumElements(1e3, 1e6)
	var data []byte
	fz.Fuzz(&data)
	ctx := context.Background()
	task := TaskName{Op: "test", Shard: 1, NumShard: 2}
	wc, err := store.Create(ctx, task, 0)
	if err != nil {
		t.Error(err)
		return
	}
	if _, err := io.Copy(wc, bytes.NewReader(data)); err != nil {
		t.Error(err)
		return
	}
	// Make sure the buffer isn't available until it's closed.
	_, err = store.Open(ctx, task, 0, 0)
	if err == nil {
		t.Error("store prematurely unavailable")
	} else if !errors.Is(errors.NotExist, err) {
		t.Errorf("unexpected error: %v", err)
	}
	if err := wc.Commit(ctx, 12345); err != nil {
		t.Error(err)
		return
	}
	info, err := store.Stat(ctx, task, 0)
	if err != nil {
		t.Error(err)
	} else {
		if got, want := info.Size, int64(len(data)); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := info.Records, int64(12345); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}

	rc, err := store.Open(ctx, task, 0, 0)
	if err != nil {
		t.Error(err)
		return
	}
	defer rc.Close()
	got, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Error(err)
		return
	}
	if !bytes.Equal(data, got) {
		t.Error("data do not match")
	}
	// No partition 1 was ever stored, so discarding is a no-op.
	err = store.Discard(ctx, task, 1)
	if err == nil {
		t.Error("expected error discarding non-existent (task, partition)")
	}
	// Make sure we can still Open successfully after the unrelated Discard.
	rc, err = store.Open(ctx, task, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()
	// Now discard, and try to Open. It should fail.
	err = store.Discard(ctx, task, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Open(ctx, task, 0, 0)
	if err == nil {
		t.Fatal("expected error opening discarded (task, partition)")
	}
}

func TestStoreImpls(t *testing.T) {
	testStore(t, newMemoryStore())
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	testStore(t, &fileStore{dir})
}
