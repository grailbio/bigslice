// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ctxsync

import (
	"context"
	"sync"
)

// A Cond is a condition variable that implements
// a context-aware Wait.
type Cond struct {
	l     sync.Locker
	waitc chan struct{}
}

// NewCond returns a new ContextCond based on Locker l.
func NewCond(l sync.Locker) *Cond {
	return &Cond{l: l}
}

// Broadcast notifies waiters of a state change. Broadcast must only
// be called while the cond's lock is held.
func (c *Cond) Broadcast() {
	if c.waitc != nil {
		close(c.waitc)
		c.waitc = nil
	}
}

// Done returns a channel that is closed after the next broadcast of
// this Cond. Done mus tbe called with the Cond's lock held; the lock
// is released before Done returns.
func (c *Cond) Done() <-chan struct{} {
	if c.waitc == nil {
		c.waitc = make(chan struct{})
	}
	waitc := c.waitc
	c.l.Unlock()
	return waitc
}

// Wait returns after the next call to Broadcast, or if the context
// is complete. The context's lock must be held when calling Wait.
// An error returns with the context's error if the context completes
// while waiting.
func (c *Cond) Wait(ctx context.Context) error {
	waitc := c.Done()
	var err error
	select {
	case <-waitc:
	case <-ctx.Done():
		err = ctx.Err()
	}
	c.l.Lock()
	return err
}
