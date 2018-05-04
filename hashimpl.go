// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.

package bigslice

import (
	"hash/fnv"
	"math"
	"reflect"
)

func makeFrameHasherGen(typ reflect.Type, col int) FrameHasher {
	switch typ.Kind() {
	case reflect.String:
		return stringHasher(col)
	case reflect.Uint:
		return uintHasher(col)
	case reflect.Uint8:
		return uint8Hasher(col)
	case reflect.Uint16:
		return uint16Hasher(col)
	case reflect.Uint32:
		return uint32Hasher(col)
	case reflect.Uint64:
		return uint64Hasher(col)
	case reflect.Int:
		return intHasher(col)
	case reflect.Int8:
		return int8Hasher(col)
	case reflect.Int16:
		return int16Hasher(col)
	case reflect.Int32:
		return int32Hasher(col)
	case reflect.Int64:
		return int64Hasher(col)
	case reflect.Float32:
		return float32Hasher(col)
	case reflect.Float64:
		return float64Hasher(col)
	case reflect.Uintptr:
		return uintptrHasher(col)
	}
	return nil
}

type stringHasher int

func (col stringHasher) HashFrame(f Frame, sum []uint32) {
	vec := f[col].Interface().([]string)
	h := fnv.New32a()
	for i := range sum {
		h.Write([]byte(vec[i]))
		sum[i] = h.Sum32()
		h.Reset()
	}
}

type uintHasher int

func (col uintHasher) HashFrame(f Frame, sum []uint32) {
	vec := f[col].Interface().([]uint)
	for i := range sum {
		sum[i] = hash64(uint64(vec[i]))
	}
}

type uint8Hasher int

func (col uint8Hasher) HashFrame(f Frame, sum []uint32) {
	vec := f[col].Interface().([]uint8)
	for i := range sum {
		sum[i] = hash32(uint32(vec[i]))
	}
}

type uint16Hasher int

func (col uint16Hasher) HashFrame(f Frame, sum []uint32) {
	vec := f[col].Interface().([]uint16)
	for i := range sum {
		sum[i] = hash32(uint32(vec[i]))
	}
}

type uint32Hasher int

func (col uint32Hasher) HashFrame(f Frame, sum []uint32) {
	vec := f[col].Interface().([]uint32)
	for i := range sum {
		sum[i] = hash32(uint32(vec[i]))
	}
}

type uint64Hasher int

func (col uint64Hasher) HashFrame(f Frame, sum []uint32) {
	vec := f[col].Interface().([]uint64)
	for i := range sum {
		sum[i] = hash64(uint64(vec[i]))
	}
}

type intHasher int

func (col intHasher) HashFrame(f Frame, sum []uint32) {
	vec := f[col].Interface().([]int)
	for i := range sum {
		sum[i] = hash64(uint64(vec[i]))
	}
}

type int8Hasher int

func (col int8Hasher) HashFrame(f Frame, sum []uint32) {
	vec := f[col].Interface().([]int8)
	for i := range sum {
		sum[i] = hash32(uint32(vec[i]))
	}
}

type int16Hasher int

func (col int16Hasher) HashFrame(f Frame, sum []uint32) {
	vec := f[col].Interface().([]int16)
	for i := range sum {
		sum[i] = hash32(uint32(vec[i]))
	}
}

type int32Hasher int

func (col int32Hasher) HashFrame(f Frame, sum []uint32) {
	vec := f[col].Interface().([]int32)
	for i := range sum {
		sum[i] = hash32(uint32(vec[i]))
	}
}

type int64Hasher int

func (col int64Hasher) HashFrame(f Frame, sum []uint32) {
	vec := f[col].Interface().([]int64)
	for i := range sum {
		sum[i] = hash64(uint64(vec[i]))
	}
}

type float32Hasher int

func (col float32Hasher) HashFrame(f Frame, sum []uint32) {
	vec := f[col].Interface().([]float32)
	for i := range sum {
		sum[i] = hash32(math.Float32bits(vec[i]))
	}
}

type float64Hasher int

func (col float64Hasher) HashFrame(f Frame, sum []uint32) {
	vec := f[col].Interface().([]float64)
	for i := range sum {
		sum[i] = hash64(math.Float64bits(vec[i]))
	}
}

type uintptrHasher int

func (col uintptrHasher) HashFrame(f Frame, sum []uint32) {
	vec := f[col].Interface().([]uintptr)
	for i := range sum {
		sum[i] = hash64(uint64(vec[i]))
	}
}
