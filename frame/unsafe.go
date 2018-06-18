// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package frame

import (
	"reflect"
	"unsafe"
)

const ptrSize = 4 << (^uintptr(0) >> 63)

// Pointers reports whether type t contains any pointers. It is used
// to decide whether write barriers need to be applied in memory
// operations.
func pointers(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return false
	case reflect.Array:
		return pointers(t.Elem())
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if pointers(t.Field(i).Type) {
				return true
			}
		}
		return false
	default:
		return true
	}
}

// sliceHeader is a safe version of SliceHeader used within this package.
type sliceHeader struct {
	Data unsafe.Pointer
	Len  int
	Cap  int
}

// Assign copies src to dst where the data are the provided type.
func assign(typ dataType, dst, src unsafe.Pointer) {
	if typ.pointers {
		if typ.size == ptrSize {
			*(*unsafe.Pointer)(dst) = *(*unsafe.Pointer)(src)
			return
		}
	} else {
		switch typ.size {
		case 8:
			*(*int64)(dst) = *(*int64)(src)
			return
		case 4:
			*(*int32)(dst) = *(*int32)(src)
			return
		case 2:
			*(*int16)(dst) = *(*int16)(src)
			return
		case 1:
			*(*int8)(dst) = *(*int8)(src)
			return
		}
		// TODO(marius): see if using copy() is cheaper than typedmemove
		// in these cases.
	}
	typedmemmove(typ.ptr, dst, src)
}

func zero(typ dataType, p unsafe.Pointer, n int) {
	switch {
	case !typ.pointers:
		// Represent as a byte slice and zero it. Loops that
		// zero memory are optimized by the Go compiler.
		h := sliceHeader{
			Data: p,
			Len:  n * int(typ.size),
			Cap:  n * int(typ.size),
		}
		b := *(*[]byte)(unsafe.Pointer(&h))
		for i := range b {
			b[i] = 0
		}
	case typ.size == ptrSize:
		h := sliceHeader{
			Data: p,
			Len:  n,
			Cap:  n,
		}
		ps := *(*[]unsafe.Pointer)(unsafe.Pointer(&h))
		for i := range ps {
			ps[i] = nil
		}
	case typ.Type.Kind() == reflect.String: // a common type
		h := sliceHeader{
			Data: p,
			Len:  n,
			Cap:  n,
		}
		s := *(*[]string)(unsafe.Pointer(&h))
		for i := range s {
			s[i] = ""
		}
	default: // slow path
		zero := reflect.Zero(typ.Type)
		for i := 0; i < n; i++ {
			reflect.NewAt(typ, p).Elem().Set(zero)
			p = add(p, typ.size)
		}
	}
}

func add(base unsafe.Pointer, off uintptr) unsafe.Pointer {
	return unsafe.Pointer(uintptr(base) + off)
}

//go:linkname typedslicecopy reflect.typedslicecopy
func typedslicecopy(elemType unsafe.Pointer, dst, src sliceHeader) int

//go:linkname typedmemmove reflect.typedmemmove
func typedmemmove(t unsafe.Pointer, dst, src unsafe.Pointer)
