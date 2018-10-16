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

func add(base unsafe.Pointer, off uintptr) unsafe.Pointer {
	return unsafe.Pointer(uintptr(base) + off)
}

//go:linkname typedslicecopy reflect.typedslicecopy
func typedslicecopy(elemType unsafe.Pointer, dst, src sliceHeader) int

//go:linkname typedmemmove reflect.typedmemmove
func typedmemmove(t unsafe.Pointer, dst, src unsafe.Pointer)
