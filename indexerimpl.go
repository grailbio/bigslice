// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.

package bigslice

import "reflect"

func makeIndexer(typ reflect.Type) Indexer {
	switch typ.Kind() {
	case reflect.String:
		return make(stringIndexer)
	case reflect.Uint:
		return make(uintIndexer)
	case reflect.Uint8:
		return make(uint8Indexer)
	case reflect.Uint16:
		return make(uint16Indexer)
	case reflect.Uint32:
		return make(uint32Indexer)
	case reflect.Uint64:
		return make(uint64Indexer)
	case reflect.Int:
		return make(intIndexer)
	case reflect.Int8:
		return make(int8Indexer)
	case reflect.Int16:
		return make(int16Indexer)
	case reflect.Int32:
		return make(int32Indexer)
	case reflect.Int64:
		return make(int64Indexer)
	case reflect.Float32:
		return make(float32Indexer)
	case reflect.Float64:
		return make(float64Indexer)
	case reflect.Uintptr:
		return make(uintptrIndexer)
	}
	return nil
}

type stringIndexer map[string]int

func (x stringIndexer) Index(f Frame, indices []int) {
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

type uintIndexer map[uint]int

func (x uintIndexer) Index(f Frame, indices []int) {
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

type uint8Indexer map[uint8]int

func (x uint8Indexer) Index(f Frame, indices []int) {
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

type uint16Indexer map[uint16]int

func (x uint16Indexer) Index(f Frame, indices []int) {
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

type uint32Indexer map[uint32]int

func (x uint32Indexer) Index(f Frame, indices []int) {
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

type uint64Indexer map[uint64]int

func (x uint64Indexer) Index(f Frame, indices []int) {
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

type intIndexer map[int]int

func (x intIndexer) Index(f Frame, indices []int) {
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

type int8Indexer map[int8]int

func (x int8Indexer) Index(f Frame, indices []int) {
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

type int16Indexer map[int16]int

func (x int16Indexer) Index(f Frame, indices []int) {
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

type int32Indexer map[int32]int

func (x int32Indexer) Index(f Frame, indices []int) {
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

type int64Indexer map[int64]int

func (x int64Indexer) Index(f Frame, indices []int) {
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

type float32Indexer map[float32]int

func (x float32Indexer) Index(f Frame, indices []int) {
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

type float64Indexer map[float64]int

func (x float64Indexer) Index(f Frame, indices []int) {
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

type uintptrIndexer map[uintptr]int

func (x uintptrIndexer) Index(f Frame, indices []int) {
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
