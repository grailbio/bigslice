// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package metrics defines a set of primitives for expressing and managing
// metrics within Bigslice. A metric (such as a counter) is declared by a
// toplevel registration mechanism (e.g., NewCounter()). All metrics must be
// declared before a Bigslice session is started.
//
// Each metric defines a set of operations (e.g., incrementing a counter).
// Operations in turn must be provided with a Scope. A Scope is a collection of
// metrics instances that can be merged. Scopes are managed by the Bigslice
// runtime. For example, each Bigslice task is assigned a Scope. These scopes of
// all tasks comprising a Bigslice operation are merged before being presented
// to the user.
//
// Metrics cannot be declared concurrently.
package metrics

import "sync/atomic"

// metrics maps all registered metrics by id. We reserve index 0 to minimize
// the chances of zero-valued metrics instances begin used uninitialized.
var metrics = []Metric{nil}

// newMetric defines a new metric.
func newMetric(makeMetric func(id int) Metric) {
	metrics = append(metrics, makeMetric(len(metrics)))
}

// all returns all currently defined metrics.
func all() []Metric {
	return metrics[1:]
}

// Metric is the abstract type of a metrics. Each metric type must implement a
// set of generic operations; the metric-specific operations are provided by the
// metric types themselves.
//
// TODO(marius): eventually consider opening up this interface to allow users to
// provide their own metrics implementations.
type Metric interface {
	// metricID is the registered ID of the metric.
	metricID() int
	// newInstance creates a new instance of this metric.
	// Instances are managed by Scopes.
	newInstance() interface{}
	// merge merges the second metric instance into the first.
	merge(interface{}, interface{})
}

// Counter is a simple counter metric. Counters implement atomic addition on top
// of an int64.
type Counter struct {
	id int
}

// NewCounter creates, registers, and returns a new Counter metric.
func NewCounter() Counter {
	var c Counter
	newMetric(func(id int) Metric {
		c.id = id
		return c
	})
	return c
}

// Value retrieves the current value of this metric in the provided scope.
func (c Counter) Value(scope *Scope) int64 {
	return atomic.LoadInt64(scope.instance(c).(*int64))
}

// Incr increments this counter's value in the provided scope by n.
func (c Counter) Incr(scope *Scope, n int64) {
	atomic.AddInt64(scope.instance(c).(*int64), n)
}

// metricID implements Metric.
func (c Counter) metricID() int { return c.id }

// newInstance implements Metric.
func (c Counter) newInstance() interface{} {
	return new(int64)
}

// Merge implements Metric.
func (c Counter) merge(x, y interface{}) {
	atomic.AddInt64(x.(*int64), atomic.LoadInt64(y.(*int64)))
}
