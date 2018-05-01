// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.

package bigslice

import (
	"reflect"
	"sort"
)

func makeSorter(typ reflect.Type, col int) Sorter {
	switch typ.Kind() {
	case reflect.String:
		return stringSorter(col)
	case reflect.Uint:
		return uintSorter(col)
	case reflect.Uint8:
		return uint8Sorter(col)
	case reflect.Uint16:
		return uint16Sorter(col)
	case reflect.Uint32:
		return uint32Sorter(col)
	case reflect.Uint64:
		return uint64Sorter(col)
	case reflect.Int:
		return intSorter(col)
	case reflect.Int8:
		return int8Sorter(col)
	case reflect.Int16:
		return int16Sorter(col)
	case reflect.Int32:
		return int32Sorter(col)
	case reflect.Int64:
		return int64Sorter(col)
	case reflect.Float32:
		return float32Sorter(col)
	case reflect.Float64:
		return float64Sorter(col)
	case reflect.Uintptr:
		return uintptrSorter(col)
	}
	return nil
}

type stringSorter int

func (col stringSorter) Sort(f Frame) {
	v := f[col].Interface().([]string)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (col stringSorter) Less(x Frame, i int, y Frame, j int) bool {
	return x[col].Index(i).String() < y[col].Index(j).String()
}

func (col stringSorter) IsSorted(f Frame) bool {
	v := f[col].Interface().([]string)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

type uintSorter int

func (col uintSorter) Sort(f Frame) {
	v := f[col].Interface().([]uint)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (col uintSorter) Less(x Frame, i int, y Frame, j int) bool {
	return x[col].Index(i).Uint() < y[col].Index(j).Uint()
}

func (col uintSorter) IsSorted(f Frame) bool {
	v := f[col].Interface().([]uint)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

type uint8Sorter int

func (col uint8Sorter) Sort(f Frame) {
	v := f[col].Interface().([]uint8)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (col uint8Sorter) Less(x Frame, i int, y Frame, j int) bool {
	return x[col].Index(i).Uint() < y[col].Index(j).Uint()
}

func (col uint8Sorter) IsSorted(f Frame) bool {
	v := f[col].Interface().([]uint8)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

type uint16Sorter int

func (col uint16Sorter) Sort(f Frame) {
	v := f[col].Interface().([]uint16)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (col uint16Sorter) Less(x Frame, i int, y Frame, j int) bool {
	return x[col].Index(i).Uint() < y[col].Index(j).Uint()
}

func (col uint16Sorter) IsSorted(f Frame) bool {
	v := f[col].Interface().([]uint16)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

type uint32Sorter int

func (col uint32Sorter) Sort(f Frame) {
	v := f[col].Interface().([]uint32)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (col uint32Sorter) Less(x Frame, i int, y Frame, j int) bool {
	return x[col].Index(i).Uint() < y[col].Index(j).Uint()
}

func (col uint32Sorter) IsSorted(f Frame) bool {
	v := f[col].Interface().([]uint32)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

type uint64Sorter int

func (col uint64Sorter) Sort(f Frame) {
	v := f[col].Interface().([]uint64)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (col uint64Sorter) Less(x Frame, i int, y Frame, j int) bool {
	return x[col].Index(i).Uint() < y[col].Index(j).Uint()
}

func (col uint64Sorter) IsSorted(f Frame) bool {
	v := f[col].Interface().([]uint64)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

type intSorter int

func (col intSorter) Sort(f Frame) {
	v := f[col].Interface().([]int)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (col intSorter) Less(x Frame, i int, y Frame, j int) bool {
	return x[col].Index(i).Int() < y[col].Index(j).Int()
}

func (col intSorter) IsSorted(f Frame) bool {
	v := f[col].Interface().([]int)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

type int8Sorter int

func (col int8Sorter) Sort(f Frame) {
	v := f[col].Interface().([]int8)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (col int8Sorter) Less(x Frame, i int, y Frame, j int) bool {
	return x[col].Index(i).Int() < y[col].Index(j).Int()
}

func (col int8Sorter) IsSorted(f Frame) bool {
	v := f[col].Interface().([]int8)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

type int16Sorter int

func (col int16Sorter) Sort(f Frame) {
	v := f[col].Interface().([]int16)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (col int16Sorter) Less(x Frame, i int, y Frame, j int) bool {
	return x[col].Index(i).Int() < y[col].Index(j).Int()
}

func (col int16Sorter) IsSorted(f Frame) bool {
	v := f[col].Interface().([]int16)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

type int32Sorter int

func (col int32Sorter) Sort(f Frame) {
	v := f[col].Interface().([]int32)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (col int32Sorter) Less(x Frame, i int, y Frame, j int) bool {
	return x[col].Index(i).Int() < y[col].Index(j).Int()
}

func (col int32Sorter) IsSorted(f Frame) bool {
	v := f[col].Interface().([]int32)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

type int64Sorter int

func (col int64Sorter) Sort(f Frame) {
	v := f[col].Interface().([]int64)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (col int64Sorter) Less(x Frame, i int, y Frame, j int) bool {
	return x[col].Index(i).Int() < y[col].Index(j).Int()
}

func (col int64Sorter) IsSorted(f Frame) bool {
	v := f[col].Interface().([]int64)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

type float32Sorter int

func (col float32Sorter) Sort(f Frame) {
	v := f[col].Interface().([]float32)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (col float32Sorter) Less(x Frame, i int, y Frame, j int) bool {
	return x[col].Index(i).Float() < y[col].Index(j).Float()
}

func (col float32Sorter) IsSorted(f Frame) bool {
	v := f[col].Interface().([]float32)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

type float64Sorter int

func (col float64Sorter) Sort(f Frame) {
	v := f[col].Interface().([]float64)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (col float64Sorter) Less(x Frame, i int, y Frame, j int) bool {
	return x[col].Index(i).Float() < y[col].Index(j).Float()
}

func (col float64Sorter) IsSorted(f Frame) bool {
	v := f[col].Interface().([]float64)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

type uintptrSorter int

func (col uintptrSorter) Sort(f Frame) {
	v := f[col].Interface().([]uintptr)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (col uintptrSorter) Less(x Frame, i int, y Frame, j int) bool {
	return x[col].Index(i).Uint() < y[col].Index(j).Uint()
}

func (col uintptrSorter) IsSorted(f Frame) bool {
	v := f[col].Interface().([]uintptr)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}
