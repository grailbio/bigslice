// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package metrics defines a set of primitives for declaring and
// managing metrics within Bigslice. Users declare metrics (such as a
// counter) using the registration mechanisms provided by this
// package (e.g., NewCounter). These return handles that are used for
// metric operations (e.g., incrementing a counter).
//
// Every operation on a metric is performed in a Scope. Scopes are
// provided by the Bigslice runtime and represent an operational
// scope in which the metric is aggregated. For example, Bigslice
// defines a Scope that is attached to each task scheduled by the
// system. Scopes are merged by the Bigslice runtime to provide
// aggregated metrics across larger operations (e.g., a single
// session.Run).
//
// User functions called by Bigslice are supplied a scope through the
// optional context.Context argument. The user must retrieve this
// Scope using the ContextScope func.
//
// Metrics cannot be declared concurrently.
package metrics

import (
	"encoding/gob"
	"sync/atomic"
)

// metrics maps all registered metrics by id. We reserve index 0 to minimize
// the chances of zero-valued metrics instances being used uninitialized.
var metrics = []Metric{zeroMetric{}}

// newMetric defines a new metric.
func newMetric(makeMetric func(id int) Metric) {
	metrics = append(metrics, makeMetric(len(metrics)))
}

// Metric is the abstract type of a metric. Each metric type must implement a
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

// Counter is a simple counter metric. Counters implement atomic
// addition and subtraction on top of an int64.
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
	return scope.instance(c).(*counterValue).load()
}

// Incr increments this counter's value in the provided scope by n.
func (c Counter) Incr(scope *Scope, n int64) {
	scope.instance(c).(*counterValue).incr(n)
}

// metricID implements Metric.
func (c Counter) metricID() int { return c.id }

// newInstance implements Metric.
func (c Counter) newInstance() interface{} {
	return new(counterValue)
}

// merge implements Metric.
func (c Counter) merge(x, y interface{}) {
	x.(*counterValue).merge(y.(*counterValue))
}

func init() {
	gob.Register(&counterValue{})
}

// counterValue holds a single counter value. This is abstracted as its own
// struct only to control how gob encodes these values. If given an
// interface{}-int64, gob will happily flatten the pointers, so that they are
// decoded as the wrong value (int64, not *int64). This is a workaround to avoid
// this fate.
type counterValue struct {
	Value int64
}

func (c *counterValue) incr(n int64) {
	atomic.AddInt64(&c.Value, n)
}

func (c *counterValue) load() int64 {
	return atomic.LoadInt64(&c.Value)
}

func (c *counterValue) merge(d *counterValue) {
	atomic.AddInt64(&c.Value, d.load())
}

// zeroMetric is used to occupy the 0th metric,
// in order to help catch zero initialization bugs.
type zeroMetric struct{}

func (zeroMetric) metricID() int                  { return 0 }
func (zeroMetric) newInstance() interface{}       { return nil }
func (zeroMetric) merge(interface{}, interface{}) {}
