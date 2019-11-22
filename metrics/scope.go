// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package metrics

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"sync/atomic"
	"unsafe"
)

// Scope is a collection of metric instances.
type Scope struct {
	// storage is an unsafe pointer to a *[]unsafe.Pointer. It is used to
	// implement scopes as a thread-safe, persistent data structure.
	storage unsafe.Pointer
}

// GobEncode implements a custom gob encoder for scopes.
func (s *Scope) GobEncode() ([]byte, error) {
	var b bytes.Buffer
	list := make([]interface{}, len(metrics))
	for i, m := range metrics {
		list[i] = s.load(m)
	}
	err := gob.NewEncoder(&b).Encode(list)
	return b.Bytes(), err
}

// GobDecode implements a custom gob decoder for scopes.
func (s *Scope) GobDecode(p []byte) error {
	var list []interface{}
	dec := gob.NewDecoder(bytes.NewReader(p))
	if err := dec.Decode(&list); err != nil {
		return err
	}
	if len(list) != len(metrics) {
		return fmt.Errorf("incompatible metric set: remote: %d, local: %d", len(list), len(metrics))
	}
	for i, v := range list {
		s.store(metrics[i], v)
	}
	return nil
}

// Merge merges instances from Scope u into Scope s.
func (s *Scope) Merge(u *Scope) {
	for _, m := range metrics {
		inst := u.load(m)
		if inst == nil {
			continue
		}
		m.merge(s.instance(m), inst)
	}
}

// Reset resets the scope s to u. It is reset to its initial (zero) state
// if u is nil.
func (s *Scope) Reset(u *Scope) {
	if u == nil {
		atomic.StorePointer(&s.storage, unsafe.Pointer((uintptr)(0)))
	} else {
		for _, m := range metrics {
			s.store(m, u.load(m))
		}
	}
}

// instance returns the instance associated with metrics m in the scope s. A new
// instance is created if none exists yet.
func (s *Scope) instance(m Metric) interface{} {
	if inst := s.load(m); inst != nil {
		return inst
	}
	var (
		list = s.list()
		inst = m.newInstance()
	)
	if inst == nil {
		panic("metric: metric returned nil instance")
	}
	for {
		ptr := atomic.LoadPointer(&list[m.metricID()])
		if ptr != nil {
			return *(*interface{})(ptr)
		}
		if atomic.CompareAndSwapPointer(&list[m.metricID()], ptr, unsafe.Pointer(&inst)) {
			return inst
		}
	}
}

// load loads the metric m from the Scope s, returning the value and whether it
// was found.
func (s *Scope) load(m Metric) interface{} {
	list := s.list()
	ptr := atomic.LoadPointer(&list[m.metricID()])
	if ptr == nil {
		return nil
	}
	return *(*interface{})(ptr)
}

// store stores the instance value v for metric m into this scope. If v is nil,
// the metrics is reset.
func (s *Scope) store(m Metric, v interface{}) {
	var (
		list = s.list()
		ptr  unsafe.Pointer
	)
	if v != nil {
		ptr = unsafe.Pointer(&v)
	}
	atomic.StorePointer(&list[m.metricID()], ptr)
}

// list returns the slice of instances in this scope. It is created
// if empty.
func (s *Scope) list() []unsafe.Pointer {
	for {
		ptr := atomic.LoadPointer(&s.storage)
		if ptr != nil {
			return *(*[]unsafe.Pointer)(ptr)
		}
		list := make([]unsafe.Pointer, len(metrics))
		if atomic.CompareAndSwapPointer(&s.storage, ptr, unsafe.Pointer(&list)) {
			return list
		}
	}
}

// contextKeyType is used to create unique context key for scopes,
// available only to code in this package.
type contextKeyType struct{}

// contextKey is the key used to attach scopes to contexts.
var contextKey contextKeyType

// ScopedContext returns a context with the provided scope attached.
// The scope may be retrieved by ContextScope.
func ScopedContext(ctx context.Context, scope *Scope) context.Context {
	return context.WithValue(ctx, contextKey, scope)
}

// ContextScope returns the scope attached to the provided context. ContextScope
// panics if the context does not have an attached scope.
func ContextScope(ctx context.Context) *Scope {
	s := ctx.Value(contextKey)
	if s == nil {
		panic("metrics: context does not provide metrics")
	}
	return s.(*Scope)
}
