// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package zero provides facilities for efficiently zeroing Go values.
package zero

import (
	"reflect"
	"sync"
	"unsafe"
)

var cache sync.Map // map[reflect.Type]func(ptr uintptr, n int)

// Slice zeroes the elements 0 <= i < v.Len() of the provided slice.
// Slice panics if the value is not a slice. f
func Slice(v interface{}) {
	SliceValue(reflect.ValueOf(v))
}

// SliceValue zeroes the elements 0 <= i < v.Len() of the provided slice
// value. Slice panics if the value is not a slice. f
func SliceValue(v reflect.Value) {
	if v.Kind() != reflect.Slice {
		panic("zero.Slice: called on non-slice value")
	}
	Unsafe(v.Type().Elem(), unsafe.Pointer(v.Pointer()), v.Len())
}

// Unsafe zeroes n elements starting at the address ptr. Elements
// must be of type t.
func Unsafe(t reflect.Type, ptr unsafe.Pointer, n int) {
	zi, ok := cache.Load(t)
	if !ok {
		zi, _ = cache.LoadOrStore(t, slice(t))
	}
	z := zi.(func(ptr unsafe.Pointer, n int))
	z(ptr, n)
}

func slice(elem reflect.Type) func(ptr unsafe.Pointer, n int) {
	switch kind := elem.Kind(); {
	case isValueType(elem):
		return sliceValue(elem)
	case kind == reflect.String:
		return func(ptr unsafe.Pointer, n int) {
			var strs []string
			strsHdr := (*reflect.SliceHeader)(unsafe.Pointer(&strs))
			strsHdr.Data = uintptr(ptr)
			strsHdr.Len = n
			strsHdr.Cap = n
			for i := range strs {
				strs[i] = ""
			}
		}
	case kind == reflect.Slice:
		return func(ptr unsafe.Pointer, n int) {
			var slices []reflect.SliceHeader
			slicesHdr := (*reflect.SliceHeader)(unsafe.Pointer(&slices))
			slicesHdr.Data = uintptr(ptr)
			slicesHdr.Len = n
			slicesHdr.Cap = n
			for i := range slices {
				slices[i].Data = uintptr(0)
				slices[i].Len = 0
				slices[i].Cap = 0
			}
		}
	case kind == reflect.Ptr:
		return func(ptr unsafe.Pointer, n int) {
			var ps []unsafe.Pointer
			psHdr := (*reflect.SliceHeader)(unsafe.Pointer(&ps))
			psHdr.Data = uintptr(ptr)
			psHdr.Len = n
			psHdr.Cap = n
			for i := range ps {
				ps[i] = nil
			}
		}
	default:
		// Slow case: use reflection API.
		zero := reflect.Zero(elem)
		return func(ptr unsafe.Pointer, n int) {
			v := reflect.NewAt(reflect.ArrayOf(n, elem), ptr).Elem()
			for i := 0; i < n; i++ {
				v.Index(i).Set(zero)
			}
		}
	}
}

func sliceValue(elem reflect.Type) func(ptr unsafe.Pointer, n int) {
	switch size := elem.Size(); size {
	case 8:
		return func(ptr unsafe.Pointer, n int) {
			var vs []int64
			vsHdr := (*reflect.SliceHeader)(unsafe.Pointer(&vs))
			vsHdr.Data = uintptr(ptr)
			vsHdr.Len = n
			vsHdr.Cap = n
			for i := range vs {
				vs[i] = 0
			}
		}
	case 4:
		return func(ptr unsafe.Pointer, n int) {
			var vs []int32
			vsHdr := (*reflect.SliceHeader)(unsafe.Pointer(&vs))
			vsHdr.Data = uintptr(ptr)
			vsHdr.Len = n
			vsHdr.Cap = n
			for i := range vs {
				vs[i] = 0
			}
		}
	case 2:
		return func(ptr unsafe.Pointer, n int) {
			var vs []int16
			vsHdr := (*reflect.SliceHeader)(unsafe.Pointer(&vs))
			vsHdr.Data = uintptr(ptr)
			vsHdr.Len = n
			vsHdr.Cap = n
			for i := range vs {
				vs[i] = 0
			}
		}
	case 1:
		return func(ptr unsafe.Pointer, n int) {
			var vs []int8
			vsHdr := (*reflect.SliceHeader)(unsafe.Pointer(&vs))
			vsHdr.Data = uintptr(ptr)
			vsHdr.Len = n
			vsHdr.Cap = n
			for i := range vs {
				vs[i] = 0
			}
		}
	default:
		// Slow case: reinterpret to []byte, and set that. Note that the
		// compiler should be able to optimize this too. In this case
		// it's always a value type, so this is always safe to do.
		return func(ptr unsafe.Pointer, n int) {
			var b []byte
			bHdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
			bHdr.Data = uintptr(ptr)
			bHdr.Len = int(size) * n
			bHdr.Cap = bHdr.Len
			for i := range b {
				b[i] = 0
			}
		}
	}
}

func isValueType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Uintptr, reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return true
	case reflect.Array:
		return isValueType(t.Elem())
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if !isValueType(t.Field(i).Type) {
				return false
			}
		}
		return true
	}
	return false
}
