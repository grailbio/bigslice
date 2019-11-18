// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package metrics

import (
	"sync"
	"sync/atomic"
)

var (
	mu sync.Mutex
	// metrics maps all registered metrics by id. We reserve index 0 to minimize
	// the chances of zero-valued metrics instances begin used uninitialized.
	metrics = []Metric{nil}
)

func newMetric(makeMetric func(id int) Metric) {
	mu.Lock()
	metrics = append(metrics, makeMetric(len(metrics)))
	mu.Unlock()
}

// TODO(marius): eventually consider opening up this interface to allow users to
// provide their own metrics implementations.
type Metric interface {
	metricID() int
	newInstance() interface{}

	merge(interface{}, interface{})
}

type Counter struct {
	id int
}

func NewCounter() Counter {
	var c Counter
	newMetric(func(id int) Metric {
		c.id = id
		return c
	})
	return c
}

func (c Counter) Value(scope *Scope) uint64 {
	return atomic.LoadUint64(scope.instance(c).(*uint64))
}

func (c Counter) Incr(scope *Scope, n int) {
	atomic.AddUint64(scope.instance(c).(*uint64), uint64(n))
}

func (c Counter) metricID() int { return c.id }
func (c Counter) newInstance() interface{} {
	return new(uint64)
}
func (c Counter) increment(instance interface{}, n int) {
	atomic.AddUint64(instance.(*uint64), uint64(n))
}
func (c Counter) merge(x, y interface{}) {
	atomic.AddUint64(x.(*uint64), atomic.LoadUint64(y.(*uint64)))
}
