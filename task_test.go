// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"context"
	"sync"
	"testing"
)

func TestTaskCond(t *testing.T) {
	var (
		task        Task
		start, done sync.WaitGroup
	)
	const N = 100
	start.Add(N)
	done.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			task.Lock()
			start.Done()
			if err := task.Wait(context.Background()); err != nil {
				t.Fatal(err)
			}
			task.Unlock()
			done.Done()
		}()
	}

	start.Wait()
	task.Lock()
	task.Broadcast()
	task.Unlock()
	done.Wait()
}

func TestTaskCondErr(t *testing.T) {
	var task Task
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	task.Lock()
	if got, want := task.Wait(ctx), context.Canceled; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
