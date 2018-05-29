// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.

package bigslice

import (
	"reflect"

	"github.com/grailbio/bigslice/frame"
)

func makeIndexer(typ reflect.Type) Indexer {
	switch typ.Kind() {
	case reflect.String:
		ix := make(stringIndexer)
		return &ix
	case reflect.Uint:
		ix := make(uintIndexer)
		return &ix
	case reflect.Uint8:
		ix := make(uint8Indexer)
		return &ix
	case reflect.Uint16:
		ix := make(uint16Indexer)
		return &ix
	case reflect.Uint32:
		ix := make(uint32Indexer)
		return &ix
	case reflect.Uint64:
		ix := make(uint64Indexer)
		return &ix
	case reflect.Int:
		ix := make(intIndexer)
		return &ix
	case reflect.Int8:
		ix := make(int8Indexer)
		return &ix
	case reflect.Int16:
		ix := make(int16Indexer)
		return &ix
	case reflect.Int32:
		ix := make(int32Indexer)
		return &ix
	case reflect.Int64:
		ix := make(int64Indexer)
		return &ix
	case reflect.Float32:
		ix := make(float32Indexer)
		return &ix
	case reflect.Float64:
		ix := make(float64Indexer)
		return &ix
	case reflect.Uintptr:
		ix := make(uintptrIndexer)
		return &ix
	}
	return nil
}

type stringIndexer map[string]int

func (x stringIndexer) Index(f frame.Frame, indices []int) {
	vec := f[0].Interface().([]string)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}
func (x *stringIndexer) Reindex(f frame.Frame) {
	*x = make(stringIndexer)
	vec := f[0].Interface().([]string)
	for i := range vec {
		(*x)[vec[i]] = i
	}
}

type uintIndexer map[uint]int

func (x uintIndexer) Index(f frame.Frame, indices []int) {
	vec := f[0].Interface().([]uint)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}
func (x *uintIndexer) Reindex(f frame.Frame) {
	*x = make(uintIndexer)
	vec := f[0].Interface().([]uint)
	for i := range vec {
		(*x)[vec[i]] = i
	}
}

type uint8Indexer map[uint8]int

func (x uint8Indexer) Index(f frame.Frame, indices []int) {
	vec := f[0].Interface().([]uint8)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}
func (x *uint8Indexer) Reindex(f frame.Frame) {
	*x = make(uint8Indexer)
	vec := f[0].Interface().([]uint8)
	for i := range vec {
		(*x)[vec[i]] = i
	}
}

type uint16Indexer map[uint16]int

func (x uint16Indexer) Index(f frame.Frame, indices []int) {
	vec := f[0].Interface().([]uint16)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}
func (x *uint16Indexer) Reindex(f frame.Frame) {
	*x = make(uint16Indexer)
	vec := f[0].Interface().([]uint16)
	for i := range vec {
		(*x)[vec[i]] = i
	}
}

type uint32Indexer map[uint32]int

func (x uint32Indexer) Index(f frame.Frame, indices []int) {
	vec := f[0].Interface().([]uint32)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}
func (x *uint32Indexer) Reindex(f frame.Frame) {
	*x = make(uint32Indexer)
	vec := f[0].Interface().([]uint32)
	for i := range vec {
		(*x)[vec[i]] = i
	}
}

type uint64Indexer map[uint64]int

func (x uint64Indexer) Index(f frame.Frame, indices []int) {
	vec := f[0].Interface().([]uint64)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}
func (x *uint64Indexer) Reindex(f frame.Frame) {
	*x = make(uint64Indexer)
	vec := f[0].Interface().([]uint64)
	for i := range vec {
		(*x)[vec[i]] = i
	}
}

type intIndexer map[int]int

func (x intIndexer) Index(f frame.Frame, indices []int) {
	vec := f[0].Interface().([]int)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}
func (x *intIndexer) Reindex(f frame.Frame) {
	*x = make(intIndexer)
	vec := f[0].Interface().([]int)
	for i := range vec {
		(*x)[vec[i]] = i
	}
}

type int8Indexer map[int8]int

func (x int8Indexer) Index(f frame.Frame, indices []int) {
	vec := f[0].Interface().([]int8)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}
func (x *int8Indexer) Reindex(f frame.Frame) {
	*x = make(int8Indexer)
	vec := f[0].Interface().([]int8)
	for i := range vec {
		(*x)[vec[i]] = i
	}
}

type int16Indexer map[int16]int

func (x int16Indexer) Index(f frame.Frame, indices []int) {
	vec := f[0].Interface().([]int16)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}
func (x *int16Indexer) Reindex(f frame.Frame) {
	*x = make(int16Indexer)
	vec := f[0].Interface().([]int16)
	for i := range vec {
		(*x)[vec[i]] = i
	}
}

type int32Indexer map[int32]int

func (x int32Indexer) Index(f frame.Frame, indices []int) {
	vec := f[0].Interface().([]int32)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}
func (x *int32Indexer) Reindex(f frame.Frame) {
	*x = make(int32Indexer)
	vec := f[0].Interface().([]int32)
	for i := range vec {
		(*x)[vec[i]] = i
	}
}

type int64Indexer map[int64]int

func (x int64Indexer) Index(f frame.Frame, indices []int) {
	vec := f[0].Interface().([]int64)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}
func (x *int64Indexer) Reindex(f frame.Frame) {
	*x = make(int64Indexer)
	vec := f[0].Interface().([]int64)
	for i := range vec {
		(*x)[vec[i]] = i
	}
}

type float32Indexer map[float32]int

func (x float32Indexer) Index(f frame.Frame, indices []int) {
	vec := f[0].Interface().([]float32)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}
func (x *float32Indexer) Reindex(f frame.Frame) {
	*x = make(float32Indexer)
	vec := f[0].Interface().([]float32)
	for i := range vec {
		(*x)[vec[i]] = i
	}
}

type float64Indexer map[float64]int

func (x float64Indexer) Index(f frame.Frame, indices []int) {
	vec := f[0].Interface().([]float64)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}
func (x *float64Indexer) Reindex(f frame.Frame) {
	*x = make(float64Indexer)
	vec := f[0].Interface().([]float64)
	for i := range vec {
		(*x)[vec[i]] = i
	}
}

type uintptrIndexer map[uintptr]int

func (x uintptrIndexer) Index(f frame.Frame, indices []int) {
	vec := f[0].Interface().([]uintptr)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}
func (x *uintptrIndexer) Reindex(f frame.Frame) {
	*x = make(uintptrIndexer)
	vec := f[0].Interface().([]uintptr)
	for i := range vec {
		(*x)[vec[i]] = i
	}
}
