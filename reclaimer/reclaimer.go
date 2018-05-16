// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package reclaimer provides facilities for managing reclaimable resources.
package reclaimer

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/grailbio/bigslice/ctxsync"
)

// State describes the state of a reclaimable resource.
type State int

const (
	// Idle indicates that the resource is not currently performing
	// any reclamation activities.
	Idle State = iota
	// Pause indicates that the resource should temporarily pause
	// processing (but not perform reclamation).
	Pause
	// NeedsReclaim indicates that the reclaimable needs reclamation.
	NeedsReclaim
	// Reclaiming indicates that the reclaimable is currently reclaiming
	// resources.
	Reclaiming
	// Canceled means the reclaimable no longer exists.
	Canceled
)

// String returns a human-readable string of the state.
func (s State) String() string {
	switch s {
	case Idle:
		return "idle"
	case Pause:
		return "pause"
	case NeedsReclaim:
		return "needsreclaim"
	case Reclaiming:
		return "reclaiming"
	case Canceled:
		return "canceled"
	default:
		return "unknown"
	}
}

// A Reclaimable represents a reclaimable resource. Reclaimables are
// minted from (and attached to) a Reclaimer, which monitor resource
// usage and issues reclamation requests.
//
// Reclaimables are also locks and condition variables: these are used
// to coordinate state changes and observations.
type Reclaimable struct {
	sync.Mutex
	*ctxsync.Cond
	State State

	owner *Reclaimer

	last  time.Time
	index int
	name  string
}

func newReclaimable(name string) *Reclaimable {
	r := &Reclaimable{name: name}
	r.Cond = ctxsync.NewCond(&r.Mutex)
	return r
}

// Cancel indicates that this Reclaimable no longer controls any
// reclaimable resources. Cancel sets the Reclaimable's state to
// Canceled and removes it from its owner Reclaimer.
func (r *Reclaimable) Cancel() {
	r.Lock()
	defer r.Unlock()
	if r.State == Canceled {
		return
	}
	if r.owner != nil {
		r.owner.removeLocked(r)
	}
	r.State = Canceled
	r.Broadcast()
}

// ShouldReclaim should be called when a Reclaimable can reclaim resources.
// ShouldReclaim returns with a boolean indicating whether resources should
// be reclaimed. ShouldReclaim also performs a Yield, potentially halting
// processing until resource pressure subsides. If ShouldReclaim returns
// true, the reclaimable should reclaim resources and then call
// Done to indicate reclamation was performed.
func (r *Reclaimable) ShouldReclaim(ctx context.Context) (bool, error) {
	r.Lock()
	defer r.Unlock()
	for r.State == Pause {
		if err := r.Wait(ctx); err != nil {
			return false, err
		}
	}
	if r.State != NeedsReclaim {
		return false, nil
	}
	r.State = Reclaiming
	r.Broadcast()
	return true, nil
}

// Done is called to indicate that reclamation was completed.
func (r *Reclaimable) Done() {
	r.Lock()
	defer r.Unlock()
	if r.State != Reclaiming {
		return
	}
	if r.owner != nil && r.owner.reclaimAll {
		r.State = NeedsReclaim
	} else {
		r.State = Idle
	}
	r.Broadcast()
}

// Yield should be called by Reclaimables to yield resource
// allocation: Yield may sleep until resource pressure has subsided.
func (r *Reclaimable) Yield(ctx context.Context) error {
	r.Lock()
	defer r.Unlock()
	for r.State == Pause {
		if err := r.Wait(ctx); err != nil {
			return err
		}
	}
	return nil
}

// String returns a string with the Reclaimable's name and state.
func (r *Reclaimable) String() string {
	return fmt.Sprintf("%s: %s", r.name, r.State)
}

type reclaimables []*Reclaimable

func (q reclaimables) Len() int           { return len(q) }
func (q reclaimables) Less(i, j int) bool { return q[i].last.Before(q[j].last) }
func (q reclaimables) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index, q[j].index = i, j
}

func (q *reclaimables) Push(x interface{}) {
	r := x.(*Reclaimable)
	r.index = len(*q)
	*q = append(*q, r)
}

func (q *reclaimables) Pop() interface{} {
	old := *q
	n := len(old)
	x := old[n-1]
	*q = old[:n-1]
	x.index = -1
	return x
}

// A Reclaimer represents a reclaimable resource against which
// allocators can register. A monitor is responsible for initiating
// resource reclamation as needed.
type Reclaimer struct {
	mu         sync.Mutex
	cond       *ctxsync.Cond
	q          reclaimables
	paused     bool
	reclaimAll bool
}

// New returns a new Reclaimer.
func New() *Reclaimer {
	r := new(Reclaimer)
	r.cond = ctxsync.NewCond(&r.mu)
	return r
}

// ReclaimAll sets a boolean indicating whether this Reclaimer should
// proactively reclaim all future Reclaimables. This API is intended for
// testing.
func (r *Reclaimer) ReclaimAll(x bool) {
	r.mu.Lock()
	r.reclaimAll = x
	r.mu.Unlock()
}

// Wait returns when a new Reclaimable has been added to the
// reclaimer or when the Reclaimer has changed pause state, or else
// when there is a context error.
func (r *Reclaimer) Wait(ctx context.Context) error {
	r.mu.Lock()
	err := r.cond.Wait(ctx)
	r.mu.Unlock()
	return err
}

// Yield is called by resource users when they can pause processing
// in order to relieve resource pressure. Yield returns an error if the
// context completes while waiting.
func (r *Reclaimer) Yield(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for r.paused {
		if err := r.cond.Wait(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Pause instructs all Reclaimables to pause processing (i.e., Yield
// will block).
func (r *Reclaimer) Pause() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.paused = true
	for _, rec := range r.q {
		rec.Lock()
		if rec.State == Idle {
			rec.State = Pause
			rec.Broadcast()
		}
		rec.Unlock()
	}
	r.cond.Broadcast()
}

// Resume resumes resource allocation. Yielding goroutines are resumed.
func (r *Reclaimer) Resume() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.paused = false
	for _, rec := range r.q {
		rec.Lock()
		if rec.State == Pause {
			rec.State = Idle
			rec.Broadcast()
		}
		rec.Unlock()
	}
	r.cond.Broadcast()
}

// Add returns a new Reclaimable that's owned by this Reclaimer.
func (r *Reclaimer) Add(name string) *Reclaimable {
	rec := newReclaimable(name)
	if r.reclaimAll {
		rec.State = NeedsReclaim
	} else if r.paused {
		rec.State = Pause
	}
	r.Push(rec)
	return rec
}

// Push adds the provided Reclaimable to the Reclaimer.
func (r *Reclaimer) Push(rec *Reclaimable) bool {
	rec.Lock()
	defer rec.Unlock()
	if rec.State == Canceled || rec.owner != nil {
		return false
	}
	if r.reclaimAll && rec.State != Reclaiming {
		rec.State = NeedsReclaim
	} else if r.paused && rec.State == Idle {
		rec.State = Pause
	}
	rec.owner = r
	rec.last = time.Now()
	r.mu.Lock()
	heap.Push(&r.q, rec)
	r.cond.Broadcast()
	r.mu.Unlock()
	return true
}

// Pop removes a Reclaimable from the Reclaimer. Pop returns
// Reclaimables ordered by last reclamation time: thus, the returned
// Reclaimable is the one with oldest reclamation time.
func (r *Reclaimer) Pop() *Reclaimable {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.q) == 0 {
		return nil
	}
	rec := heap.Pop(&r.q).(*Reclaimable)
	rec.Lock()
	rec.owner = nil
	rec.Unlock()
	return rec
}

func (r *Reclaimer) removeLocked(rec *Reclaimable) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rec.index < 0 {
		return
	}
	heap.Remove(&r.q, rec.index)
}
