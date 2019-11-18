// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package metrics

import "sync"

import "context"

type contextKeyType struct{}

var contextKey contextKeyType

type Scope sync.Map

func (s *Scope) instance(m Metric) interface{} {
	inst, ok := (*sync.Map)(s).Load(m.metricID())
	if !ok {
		inst, _ = (*sync.Map)(s).LoadOrStore(m.metricID(), m.newInstance())
	}
	return inst
}

func ScopedContext(ctx context.Context) (context.Context, *Scope) {
	s := new(Scope)
	return context.WithValue(ctx, contextKey, s), s
}

func contextScope(ctx context.Context) *Scope {
	s := ctx.Value(contextKey)
	if s == nil {
		panic("metrics: context does not provide metrics")
	}
	return s.(*Scope)
}
