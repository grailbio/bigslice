// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reclaimer

import (
	"testing"
	"time"
)

func TestReclaimer(t *testing.T) {
	r := New()

	rec0 := r.Add("zero")
	time.Sleep(time.Millisecond)
	rec1 := r.Add("one")

	r0, r1 := r.Pop(), r.Pop()
	if r2 := r.Pop(); r2 != nil {
		t.Errorf("excess reclaimable %v", r2)
	}
	if got, want := r0, rec0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := r1, rec1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	r0.Cancel()
	r.Push(r0)
	if r2 := r.Pop(); r2 != nil {
		t.Errorf("unexpected reclaimable %v", r2)
	}
	r.Push(r1)
	if got, want := r.Pop(), r1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

}

func TestPause(t *testing.T) {
	r := New()

	rec0 := r.Add("zero")
	if got, want := rec0.State, Idle; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	r.Pause()
	if got, want := rec0.State, Pause; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	r.Resume()
	if got, want := rec0.State, Idle; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
