// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package stats provides collections of counters. Each counter
// belongs to a snapshottable collection, and these collections can be
// aggregated.
package stats

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Values is a snapshot of the values in a collection.
type Values map[string]int64

// Copy returns a copy of the values v.
func (v Values) Copy() Values {
	w := make(Values)
	for k, v := range v {
		w[k] = v
	}
	return w
}

// String returns an abbreviated string with the values in this
// snapshot sorted by key.
func (v Values) String() string {
	var keys []string
	for key := range v {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for i, key := range keys {
		keys[i] = fmt.Sprintf("%s:%d", key, v[key])
	}
	return strings.Join(keys, " ")
}

// A Map is a set of counters keyed by name.
type Map struct {
	mu     sync.Mutex
	values map[string]*Int
}

// NewMap returns a fresh Map.
func NewMap() *Map {
	return &Map{
		values: make(map[string]*Int),
	}
}

// Int returns the counter with the provided name. The counter is
// created if it does not already exist.
func (m *Map) Int(name string) *Int {
	m.mu.Lock()
	v := m.values[name]
	if v == nil {
		v = new(Int)
		m.values[name] = v
	}
	m.mu.Unlock()
	return v
}

// AddAll adds all counters in the map to the provided snapshot.
func (m *Map) AddAll(vals Values) {
	m.mu.Lock()
	for k, v := range m.values {
		vals[k] += v.Get()
	}
	m.mu.Unlock()
}

// An Int is a integer counter. Ints can be atomically
// incremented and set.
type Int struct {
	val int64
}

// Add increments v by delta.
func (v *Int) Add(delta int64) {
	if v == nil {
		return
	}
	atomic.AddInt64(&v.val, delta)
}

// Set sets the counter's value to val.
func (v *Int) Set(val int64) {
	if v == nil {
		return
	}
	atomic.StoreInt64(&v.val, val)
}

// Get returns the current value of a counter.
func (v *Int) Get() int64 {
	if v == nil {
		return 0
	}
	return atomic.LoadInt64(&v.val)
}
