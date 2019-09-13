// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"math/rand"
	"reflect"
	"sync"
	"testing"
)

// TestTaskSubscriber verifies that task subscribers receive all tasks whose
// state changes.
func TestTaskSubscriber(t *testing.T) {
	const (
		numTasks   = 100000
		numWriters = 8
	)
	var (
		sub   = NewTaskSubscriber()
		unsub = NewTaskSubscriber()
		tasks = make([]*Task, numTasks)
	)
	for i := range tasks {
		tasks[i] = &Task{}
		tasks[i].Subscribe(sub)
		// Throw in a subscriber that is immediately unsubscribed to make sure
		// it doesn't gum up the works.
		tasks[i].Subscribe(unsub)
		tasks[i].Unsubscribe(unsub)
	}
	var (
		mu      sync.Mutex
		want    = make(map[*Task]bool)
		writeWG sync.WaitGroup
	)
	for i := 0; i < numWriters; i++ {
		writeWG.Add(1)
		go func() {
			defer writeWG.Done()
			for j := 0; j < numTasks/numWriters/2; j++ {
				task := tasks[rand.Intn(len(tasks))]
				newState := TaskState(1 + rand.Intn(int(maxState)-1))
				task.Set(newState)
				mu.Lock()
				want[task] = true
				mu.Unlock()
			}
		}()
	}
	var (
		got    = make(map[*Task]bool)
		donec  = make(chan struct{})
		readWG sync.WaitGroup
	)
	readWG.Add(1)
	go func() {
		defer readWG.Done()
		for {
			select {
			case <-sub.Ready():
				for _, task := range sub.Tasks() {
					got[task] = true
				}
			case <-donec:
				// Drain.
				for {
					select {
					case <-sub.Ready():
						for _, task := range sub.Tasks() {
							got[task] = true
						}
					default:
						return
					}
				}
			}
		}
	}()
	writeWG.Wait()
	close(donec)
	readWG.Wait()
	if !reflect.DeepEqual(got, want) {
		t.Logf("len(got), len(want): %d, %d", len(got), len(want))
		t.Errorf("modified task was not seen by subscriber")
	}
	// The unsubscribed subscriber should see nothing.
	if got, want := len(unsub.Tasks()), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
