// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestTaskOnceConcurrency(t *testing.T) {
	const N = 10
	var (
		once        taskOnce
		start, done sync.WaitGroup
		count       uint32
	)
	start.Add(N)
	done.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			start.Done()
			start.Wait()
			err := once.Do(123, func() error {
				atomic.AddUint32(&count, 1)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			done.Done()
		}()
	}
	done.Wait()
	if got, want := count, uint32(1); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestTaskOnceError(t *testing.T) {
	var (
		once     taskOnce
		expected = errors.New("expected error")
	)
	err := once.Do(123, func() error { return expected })
	if got, want := err, expected; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	err = once.Do(123, func() error { panic("should not be called") })
	if got, want := err, expected; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	err = once.Do(124, func() error { return nil })
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
}
