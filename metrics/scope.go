// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package metrics

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync/atomic"
	"unsafe"
)

// Scope is a collection of metric instances.
type Scope struct {
	storage *[]interface{}
}

// GobEncode implements a custom gob encoder for scopes.
func (s *Scope) GobEncode() ([]byte, error) {
	var b bytes.Buffer
	list := s.list()
	if list == nil {
		list = new([]interface{})
	}
	err := gob.NewEncoder(&b).Encode(list)
	return b.Bytes(), err
}

// GobDecode implements a custom gob decoder for scopes.
func (s *Scope) GobDecode(p []byte) error {
	s.storage = new([]interface{})
	dec := gob.NewDecoder(bytes.NewReader(p))
	return dec.Decode(s.storage)
}

// Merge merges instances from Scope u into Scope s.
func (s *Scope) Merge(u *Scope) {
	ulist := u.list()
	if ulist == nil {
		return
	}
	for i, inst := range *ulist {
		if inst == nil {
			continue
		}
		m := metrics[i]
		m.merge(s.instance(m), inst)
	}
}

// Reset removes all recorded metric instances in this scope.
func (s *Scope) Reset() {
	atomic.StorePointer(s.pointer(), unsafe.Pointer((*[]interface{})(nil)))
}

// instance returns the instance associated with metrics m in the scope s. A new
// instance is created if none exists yet.
func (s *Scope) instance(m Metric) interface{} {
	if inst := s.load(m); inst != nil {
		return inst
	}
	for {
		ptr := atomic.LoadPointer(s.pointer())
		list := (*[]interface{})(ptr)
		if list == nil {
			list = new([]interface{})
		}
		for len(*list) <= m.metricID() {
			*list = append(*list, nil)
		}
		inst := m.newInstance()
		if inst == nil {
			panic("metric: metric returned nil instance")
		}
		(*list)[m.metricID()] = inst
		if ok := atomic.CompareAndSwapPointer(s.pointer(), ptr, unsafe.Pointer(list)); ok {
			return inst
		}
	}
}

// load loads the metric m from the Scope s, returning the value and whether it
// was found.
func (s *Scope) load(m Metric) interface{} {
	list := s.list()
	if list == nil || len(*list) <= m.metricID() {
		return nil
	}
	return (*list)[m.metricID()]
}

// list returns the slice of instances in this scope.
func (s *Scope) list() *[]interface{} {
	return (*[]interface{})(atomic.LoadPointer(s.pointer()))
}

// pointer returns an unsafe.Pointer to the instance list in this scope.
func (s *Scope) pointer() *unsafe.Pointer {
	return (*unsafe.Pointer)(unsafe.Pointer(&s.storage))
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
