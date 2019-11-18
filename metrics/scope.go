// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package metrics

import (
	"context"
	"sync"
)

// Scope is a collection of metric instances.
type Scope sync.Map

// Merge merges instances from Scope u into Scope s.
func (s *Scope) Merge(u *Scope) {
	for _, m := range all() {
		inst, ok := u.load(m)
		if !ok {
			continue
		}
		m.merge(s.instance(m), inst)
	}
}

// instance returns the instance associated with metrics m in the scope s. A new
// instance is created if none exists yet.
func (s *Scope) instance(m Metric) interface{} {
	inst, ok := s.load(m)
	if !ok {
		inst, _ = (*sync.Map)(s).LoadOrStore(m.metricID(), m.newInstance())
	}
	return inst
}

// load loads the metric m from the Scope s, returning the value and whether it
// was found.
func (s *Scope) load(m Metric) (interface{}, bool) {
	return (*sync.Map)(s).Load(m.metricID())
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
