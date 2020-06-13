// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sliceio

import (
	"context"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/grailbio/bigslice/frame"
)

func TestSpiller(t *testing.T) {
	const n = 100
	var (
		fz = fuzz.NewWithSeed(123)
		f1 = fuzzFrame(fz, n/2, typeOfString, typeOfInt)
	)
	spill, err := NewSpiller("test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = spill.Cleanup(); err != nil {
			t.Fatal(err)
		}
	}()
	if _, err = spill.Spill(f1); err != nil {
		t.Fatal(err)
	}
	if _, err := spill.Spill(f1); err != nil {
		t.Fatal(err)
	}

	f12 := frame.Make(f1, 2*n, 2*n)
	frame.Copy(f12, f1)
	frame.Copy(f12.Slice(n/2, n), f1)

	readers, err := spill.Readers()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(readers), 2; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	reader := MultiReader(readers...)
	defer reader.Close()
	m, err := ReadFull(context.Background(), reader, f12.Slice(n, 2*n))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := n, m; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	m, err = reader.Read(context.Background(), frame.Make(f12, 1, 1))
	if got, want := err, EOF; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := m, 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	for i := 0; i < n; i++ {
		if f12.Less(i, n+i) || f12.Less(n+i, i) {
			t.Fatalf("not equal: mismatch at %d", i)
		}
	}

}
