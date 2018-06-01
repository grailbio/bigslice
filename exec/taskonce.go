// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"sync"
	"sync/atomic"
)

// OnceTask manages a computation that must be run at most once.
// It's similar to sync.Once, except it also handles and returns errors.
type onceTask struct {
	mu   sync.Mutex
	done uint32
	err  error
}

// Do run the function do at most once. Successive invocations of Do
// guarantee exactly one invocation of the function do. Do returns
// the error of do's invocation.
func (o *onceTask) Do(do func() error) error {
	if atomic.LoadUint32(&o.done) == 1 {
		return o.err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if atomic.LoadUint32(&o.done) == 0 {
		o.err = do()
		atomic.StoreUint32(&o.done, 1)
	}
	return o.err
}

// TaskOnce coordinates actions that must happen exactly once.
type taskOnce sync.Map

// Perform the provided action named by a key. Do invokes the action
// exactly once for each key, and returns any errors produced by the
// provided action.
func (t *taskOnce) Do(key interface{}, do func() error) error {
	taskv, _ := (*sync.Map)(t).LoadOrStore(key, new(onceTask))
	task := taskv.(*onceTask)
	return task.Do(do)
}

// Forget forgets past computations associated with the provided key.
func (t *taskOnce) Forget(key interface{}) {
	(*sync.Map)(t).Delete(key)
}
