// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package kernel provides a mechanism to register and look up
// bigslice operation kernels. Each kernel is associated with a type
// and may provide a set of operations, usually exposed through
// interfaces. Package kernel also includes a set of standard kernel
// interfaces and implementations for these for Go's primitive types.
package kernel

import (
	"reflect"
	"sync"
)

var (
	mu      sync.RWMutex
	kernels = map[reflect.Type][]reflect.Value{}
)

// Register associated the provided kernel with the provided type.
// Interfaces implemented by ops are defined for the given type.
func Register(typ reflect.Type, ops interface{}) {
	mu.Lock()
	defer mu.Unlock()
	kernels[typ] = append(kernels[typ], reflect.ValueOf(ops))
}

// Lookup retrieves a kernel implementing the operations in the
// interface pointed to by the provided pointer. If no such kernel
// exists, Lookup returns false. If a non-pointer is passed, Lookup
// panics.
func Lookup(typ reflect.Type, ptr interface{}) bool {
	mu.RLock()
	defer mu.RUnlock()
	v := reflect.ValueOf(ptr)
	if v.Kind() != reflect.Ptr {
		panic("kernel.Lookup: passed non-pointer")
	}
	iface := v.Type().Elem()
	if iface.Kind() != reflect.Interface {
		panic("kernel.Lookup: non-interface pointer")
	}
	for _, k := range kernels[typ] {
		if k.Type().Implements(iface) {
			v.Elem().Set(k)
			return true
		}
	}
	return false
}

// Implements reports whether the provided interface is
// implemented for the provided type.
func Implements(typ, iface reflect.Type) bool {
	mu.RLock()
	defer mu.RUnlock()
	if iface.Kind() != reflect.Interface {
		panic("kernel.Implements: non-interface type")
	}
	for _, k := range kernels[typ] {
		if k.Type().Implements(iface) {
			return true
		}
	}
	return false
}
