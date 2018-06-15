// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.

package kernel

import (
	"hash/fnv"
	"math"
	"reflect"
	"sort"

	"github.com/grailbio/bigslice/frame"
)

func init() {
	Register(reflect.TypeOf((*string)(nil)).Elem(), stringKernel{})
	Register(reflect.TypeOf((*uint)(nil)).Elem(), uintKernel{})
	Register(reflect.TypeOf((*uint8)(nil)).Elem(), uint8Kernel{})
	Register(reflect.TypeOf((*uint16)(nil)).Elem(), uint16Kernel{})
	Register(reflect.TypeOf((*uint32)(nil)).Elem(), uint32Kernel{})
	Register(reflect.TypeOf((*uint64)(nil)).Elem(), uint64Kernel{})
	Register(reflect.TypeOf((*int)(nil)).Elem(), intKernel{})
	Register(reflect.TypeOf((*int8)(nil)).Elem(), int8Kernel{})
	Register(reflect.TypeOf((*int16)(nil)).Elem(), int16Kernel{})
	Register(reflect.TypeOf((*int32)(nil)).Elem(), int32Kernel{})
	Register(reflect.TypeOf((*int64)(nil)).Elem(), int64Kernel{})
	Register(reflect.TypeOf((*float32)(nil)).Elem(), float32Kernel{})
	Register(reflect.TypeOf((*float64)(nil)).Elem(), float64Kernel{})
	Register(reflect.TypeOf((*uintptr)(nil)).Elem(), uintptrKernel{})
}

type stringKernel struct{}

var (
	_ Sorter  = stringKernel{}
	_ Hasher  = stringKernel{}
	_ Indexer = stringKernel{}
)

func (stringKernel) Sort(f frame.Frame) {
	v := f[0].Interface().([]string)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (stringKernel) Less(x frame.Frame, i int, y frame.Frame, j int) bool {
	return x[0].Index(i).String() < y[0].Index(j).String()
}

func (stringKernel) IsSorted(f frame.Frame) bool {
	v := f[0].Interface().([]string)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

func (stringKernel) HashFrame(f frame.Frame, sum []uint32) {
	vec := f[0].Interface().([]string)
	h := fnv.New32a()
	for i := range sum {
		h.Write([]byte(vec[i]))
		sum[i] = h.Sum32()
		h.Reset()
	}
}

type stringIndex map[string]int

func (x stringIndex) Index(col frame.Column, indices []int) {
	vec := col.Interface().([]string)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}

func (stringKernel) Index(col frame.Column) Index {
	vec := col.Interface().([]string)
	x := make(stringIndex, len(vec))
	for _, key := range vec {
		if _, ok := x[key]; ok {
			continue
		}
		x[key] = len(x)
	}
	return x
}

type uintKernel struct{}

var (
	_ Sorter  = uintKernel{}
	_ Hasher  = uintKernel{}
	_ Indexer = uintKernel{}
)

func (uintKernel) Sort(f frame.Frame) {
	v := f[0].Interface().([]uint)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (uintKernel) Less(x frame.Frame, i int, y frame.Frame, j int) bool {
	return x[0].Index(i).Uint() < y[0].Index(j).Uint()
}

func (uintKernel) IsSorted(f frame.Frame) bool {
	v := f[0].Interface().([]uint)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

func (uintKernel) HashFrame(f frame.Frame, sum []uint32) {
	vec := f[0].Interface().([]uint)
	for i := range sum {
		sum[i] = hash64(uint64(vec[i]))
	}
}

type uintIndex map[uint]int

func (x uintIndex) Index(col frame.Column, indices []int) {
	vec := col.Interface().([]uint)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}

func (uintKernel) Index(col frame.Column) Index {
	vec := col.Interface().([]uint)
	x := make(uintIndex, len(vec))
	for _, key := range vec {
		if _, ok := x[key]; ok {
			continue
		}
		x[key] = len(x)
	}
	return x
}

type uint8Kernel struct{}

var (
	_ Sorter  = uint8Kernel{}
	_ Hasher  = uint8Kernel{}
	_ Indexer = uint8Kernel{}
)

func (uint8Kernel) Sort(f frame.Frame) {
	v := f[0].Interface().([]uint8)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (uint8Kernel) Less(x frame.Frame, i int, y frame.Frame, j int) bool {
	return x[0].Index(i).Uint() < y[0].Index(j).Uint()
}

func (uint8Kernel) IsSorted(f frame.Frame) bool {
	v := f[0].Interface().([]uint8)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

func (uint8Kernel) HashFrame(f frame.Frame, sum []uint32) {
	vec := f[0].Interface().([]uint8)
	for i := range sum {
		sum[i] = hash32(uint32(vec[i]))
	}
}

type uint8Index map[uint8]int

func (x uint8Index) Index(col frame.Column, indices []int) {
	vec := col.Interface().([]uint8)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}

func (uint8Kernel) Index(col frame.Column) Index {
	vec := col.Interface().([]uint8)
	x := make(uint8Index, len(vec))
	for _, key := range vec {
		if _, ok := x[key]; ok {
			continue
		}
		x[key] = len(x)
	}
	return x
}

type uint16Kernel struct{}

var (
	_ Sorter  = uint16Kernel{}
	_ Hasher  = uint16Kernel{}
	_ Indexer = uint16Kernel{}
)

func (uint16Kernel) Sort(f frame.Frame) {
	v := f[0].Interface().([]uint16)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (uint16Kernel) Less(x frame.Frame, i int, y frame.Frame, j int) bool {
	return x[0].Index(i).Uint() < y[0].Index(j).Uint()
}

func (uint16Kernel) IsSorted(f frame.Frame) bool {
	v := f[0].Interface().([]uint16)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

func (uint16Kernel) HashFrame(f frame.Frame, sum []uint32) {
	vec := f[0].Interface().([]uint16)
	for i := range sum {
		sum[i] = hash32(uint32(vec[i]))
	}
}

type uint16Index map[uint16]int

func (x uint16Index) Index(col frame.Column, indices []int) {
	vec := col.Interface().([]uint16)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}

func (uint16Kernel) Index(col frame.Column) Index {
	vec := col.Interface().([]uint16)
	x := make(uint16Index, len(vec))
	for _, key := range vec {
		if _, ok := x[key]; ok {
			continue
		}
		x[key] = len(x)
	}
	return x
}

type uint32Kernel struct{}

var (
	_ Sorter  = uint32Kernel{}
	_ Hasher  = uint32Kernel{}
	_ Indexer = uint32Kernel{}
)

func (uint32Kernel) Sort(f frame.Frame) {
	v := f[0].Interface().([]uint32)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (uint32Kernel) Less(x frame.Frame, i int, y frame.Frame, j int) bool {
	return x[0].Index(i).Uint() < y[0].Index(j).Uint()
}

func (uint32Kernel) IsSorted(f frame.Frame) bool {
	v := f[0].Interface().([]uint32)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

func (uint32Kernel) HashFrame(f frame.Frame, sum []uint32) {
	vec := f[0].Interface().([]uint32)
	for i := range sum {
		sum[i] = hash32(uint32(vec[i]))
	}
}

type uint32Index map[uint32]int

func (x uint32Index) Index(col frame.Column, indices []int) {
	vec := col.Interface().([]uint32)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}

func (uint32Kernel) Index(col frame.Column) Index {
	vec := col.Interface().([]uint32)
	x := make(uint32Index, len(vec))
	for _, key := range vec {
		if _, ok := x[key]; ok {
			continue
		}
		x[key] = len(x)
	}
	return x
}

type uint64Kernel struct{}

var (
	_ Sorter  = uint64Kernel{}
	_ Hasher  = uint64Kernel{}
	_ Indexer = uint64Kernel{}
)

func (uint64Kernel) Sort(f frame.Frame) {
	v := f[0].Interface().([]uint64)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (uint64Kernel) Less(x frame.Frame, i int, y frame.Frame, j int) bool {
	return x[0].Index(i).Uint() < y[0].Index(j).Uint()
}

func (uint64Kernel) IsSorted(f frame.Frame) bool {
	v := f[0].Interface().([]uint64)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

func (uint64Kernel) HashFrame(f frame.Frame, sum []uint32) {
	vec := f[0].Interface().([]uint64)
	for i := range sum {
		sum[i] = hash64(uint64(vec[i]))
	}
}

type uint64Index map[uint64]int

func (x uint64Index) Index(col frame.Column, indices []int) {
	vec := col.Interface().([]uint64)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}

func (uint64Kernel) Index(col frame.Column) Index {
	vec := col.Interface().([]uint64)
	x := make(uint64Index, len(vec))
	for _, key := range vec {
		if _, ok := x[key]; ok {
			continue
		}
		x[key] = len(x)
	}
	return x
}

type intKernel struct{}

var (
	_ Sorter  = intKernel{}
	_ Hasher  = intKernel{}
	_ Indexer = intKernel{}
)

func (intKernel) Sort(f frame.Frame) {
	v := f[0].Interface().([]int)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (intKernel) Less(x frame.Frame, i int, y frame.Frame, j int) bool {
	return x[0].Index(i).Int() < y[0].Index(j).Int()
}

func (intKernel) IsSorted(f frame.Frame) bool {
	v := f[0].Interface().([]int)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

func (intKernel) HashFrame(f frame.Frame, sum []uint32) {
	vec := f[0].Interface().([]int)
	for i := range sum {
		sum[i] = hash64(uint64(vec[i]))
	}
}

type intIndex map[int]int

func (x intIndex) Index(col frame.Column, indices []int) {
	vec := col.Interface().([]int)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}

func (intKernel) Index(col frame.Column) Index {
	vec := col.Interface().([]int)
	x := make(intIndex, len(vec))
	for _, key := range vec {
		if _, ok := x[key]; ok {
			continue
		}
		x[key] = len(x)
	}
	return x
}

type int8Kernel struct{}

var (
	_ Sorter  = int8Kernel{}
	_ Hasher  = int8Kernel{}
	_ Indexer = int8Kernel{}
)

func (int8Kernel) Sort(f frame.Frame) {
	v := f[0].Interface().([]int8)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (int8Kernel) Less(x frame.Frame, i int, y frame.Frame, j int) bool {
	return x[0].Index(i).Int() < y[0].Index(j).Int()
}

func (int8Kernel) IsSorted(f frame.Frame) bool {
	v := f[0].Interface().([]int8)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

func (int8Kernel) HashFrame(f frame.Frame, sum []uint32) {
	vec := f[0].Interface().([]int8)
	for i := range sum {
		sum[i] = hash32(uint32(vec[i]))
	}
}

type int8Index map[int8]int

func (x int8Index) Index(col frame.Column, indices []int) {
	vec := col.Interface().([]int8)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}

func (int8Kernel) Index(col frame.Column) Index {
	vec := col.Interface().([]int8)
	x := make(int8Index, len(vec))
	for _, key := range vec {
		if _, ok := x[key]; ok {
			continue
		}
		x[key] = len(x)
	}
	return x
}

type int16Kernel struct{}

var (
	_ Sorter  = int16Kernel{}
	_ Hasher  = int16Kernel{}
	_ Indexer = int16Kernel{}
)

func (int16Kernel) Sort(f frame.Frame) {
	v := f[0].Interface().([]int16)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (int16Kernel) Less(x frame.Frame, i int, y frame.Frame, j int) bool {
	return x[0].Index(i).Int() < y[0].Index(j).Int()
}

func (int16Kernel) IsSorted(f frame.Frame) bool {
	v := f[0].Interface().([]int16)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

func (int16Kernel) HashFrame(f frame.Frame, sum []uint32) {
	vec := f[0].Interface().([]int16)
	for i := range sum {
		sum[i] = hash32(uint32(vec[i]))
	}
}

type int16Index map[int16]int

func (x int16Index) Index(col frame.Column, indices []int) {
	vec := col.Interface().([]int16)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}

func (int16Kernel) Index(col frame.Column) Index {
	vec := col.Interface().([]int16)
	x := make(int16Index, len(vec))
	for _, key := range vec {
		if _, ok := x[key]; ok {
			continue
		}
		x[key] = len(x)
	}
	return x
}

type int32Kernel struct{}

var (
	_ Sorter  = int32Kernel{}
	_ Hasher  = int32Kernel{}
	_ Indexer = int32Kernel{}
)

func (int32Kernel) Sort(f frame.Frame) {
	v := f[0].Interface().([]int32)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (int32Kernel) Less(x frame.Frame, i int, y frame.Frame, j int) bool {
	return x[0].Index(i).Int() < y[0].Index(j).Int()
}

func (int32Kernel) IsSorted(f frame.Frame) bool {
	v := f[0].Interface().([]int32)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

func (int32Kernel) HashFrame(f frame.Frame, sum []uint32) {
	vec := f[0].Interface().([]int32)
	for i := range sum {
		sum[i] = hash32(uint32(vec[i]))
	}
}

type int32Index map[int32]int

func (x int32Index) Index(col frame.Column, indices []int) {
	vec := col.Interface().([]int32)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}

func (int32Kernel) Index(col frame.Column) Index {
	vec := col.Interface().([]int32)
	x := make(int32Index, len(vec))
	for _, key := range vec {
		if _, ok := x[key]; ok {
			continue
		}
		x[key] = len(x)
	}
	return x
}

type int64Kernel struct{}

var (
	_ Sorter  = int64Kernel{}
	_ Hasher  = int64Kernel{}
	_ Indexer = int64Kernel{}
)

func (int64Kernel) Sort(f frame.Frame) {
	v := f[0].Interface().([]int64)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (int64Kernel) Less(x frame.Frame, i int, y frame.Frame, j int) bool {
	return x[0].Index(i).Int() < y[0].Index(j).Int()
}

func (int64Kernel) IsSorted(f frame.Frame) bool {
	v := f[0].Interface().([]int64)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

func (int64Kernel) HashFrame(f frame.Frame, sum []uint32) {
	vec := f[0].Interface().([]int64)
	for i := range sum {
		sum[i] = hash64(uint64(vec[i]))
	}
}

type int64Index map[int64]int

func (x int64Index) Index(col frame.Column, indices []int) {
	vec := col.Interface().([]int64)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}

func (int64Kernel) Index(col frame.Column) Index {
	vec := col.Interface().([]int64)
	x := make(int64Index, len(vec))
	for _, key := range vec {
		if _, ok := x[key]; ok {
			continue
		}
		x[key] = len(x)
	}
	return x
}

type float32Kernel struct{}

var (
	_ Sorter  = float32Kernel{}
	_ Hasher  = float32Kernel{}
	_ Indexer = float32Kernel{}
)

func (float32Kernel) Sort(f frame.Frame) {
	v := f[0].Interface().([]float32)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (float32Kernel) Less(x frame.Frame, i int, y frame.Frame, j int) bool {
	return x[0].Index(i).Float() < y[0].Index(j).Float()
}

func (float32Kernel) IsSorted(f frame.Frame) bool {
	v := f[0].Interface().([]float32)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

func (float32Kernel) HashFrame(f frame.Frame, sum []uint32) {
	vec := f[0].Interface().([]float32)
	for i := range sum {
		sum[i] = hash32(math.Float32bits(vec[i]))
	}
}

type float32Index map[float32]int

func (x float32Index) Index(col frame.Column, indices []int) {
	vec := col.Interface().([]float32)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}

func (float32Kernel) Index(col frame.Column) Index {
	vec := col.Interface().([]float32)
	x := make(float32Index, len(vec))
	for _, key := range vec {
		if _, ok := x[key]; ok {
			continue
		}
		x[key] = len(x)
	}
	return x
}

type float64Kernel struct{}

var (
	_ Sorter  = float64Kernel{}
	_ Hasher  = float64Kernel{}
	_ Indexer = float64Kernel{}
)

func (float64Kernel) Sort(f frame.Frame) {
	v := f[0].Interface().([]float64)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (float64Kernel) Less(x frame.Frame, i int, y frame.Frame, j int) bool {
	return x[0].Index(i).Float() < y[0].Index(j).Float()
}

func (float64Kernel) IsSorted(f frame.Frame) bool {
	v := f[0].Interface().([]float64)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

func (float64Kernel) HashFrame(f frame.Frame, sum []uint32) {
	vec := f[0].Interface().([]float64)
	for i := range sum {
		sum[i] = hash64(math.Float64bits(vec[i]))
	}
}

type float64Index map[float64]int

func (x float64Index) Index(col frame.Column, indices []int) {
	vec := col.Interface().([]float64)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}

func (float64Kernel) Index(col frame.Column) Index {
	vec := col.Interface().([]float64)
	x := make(float64Index, len(vec))
	for _, key := range vec {
		if _, ok := x[key]; ok {
			continue
		}
		x[key] = len(x)
	}
	return x
}

type uintptrKernel struct{}

var (
	_ Sorter  = uintptrKernel{}
	_ Hasher  = uintptrKernel{}
	_ Indexer = uintptrKernel{}
)

func (uintptrKernel) Sort(f frame.Frame) {
	v := f[0].Interface().([]uintptr)
	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })
}

func (uintptrKernel) Less(x frame.Frame, i int, y frame.Frame, j int) bool {
	return x[0].Index(i).Uint() < y[0].Index(j).Uint()
}

func (uintptrKernel) IsSorted(f frame.Frame) bool {
	v := f[0].Interface().([]uintptr)
	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })
}

func (uintptrKernel) HashFrame(f frame.Frame, sum []uint32) {
	vec := f[0].Interface().([]uintptr)
	for i := range sum {
		sum[i] = hash64(uint64(vec[i]))
	}
}

type uintptrIndex map[uintptr]int

func (x uintptrIndex) Index(col frame.Column, indices []int) {
	vec := col.Interface().([]uintptr)
	for i := range indices {
		ix, ok := x[vec[i]]
		if !ok {
			ix = len(x)
			x[vec[i]] = ix
		}
		indices[i] = ix
	}
}

func (uintptrKernel) Index(col frame.Column) Index {
	vec := col.Interface().([]uintptr)
	x := make(uintptrIndex, len(vec))
	for _, key := range vec {
		if _, ok := x[key]; ok {
			continue
		}
		x[key] = len(x)
	}
	return x
}

type frameSorter struct {
	frame.Frame
	swap func(i, j int)
	less func(i, j int) bool
}

func (s frameSorter) Less(i, j int) bool { return s.less(i, j) }
func (s frameSorter) Swap(i, j int)      { s.swap(i, j) }

// SortFrame sorts the provided frame using the provided
// comparison function.
func sortFrame(f frame.Frame, less func(i, j int) bool) {
	sort.Sort(frameSorter{
		Frame: f,
		swap:  f.Swapper(),
		less:  less,
	})
}

// Hash32 is the 32-bit integer hashing function from
// http://burtleburtle.net/bob/hash/integer.html. (Public domain.)
func hash32(x uint32) uint32 {
	x = (x + 0x7ed55d16) + (x << 12)
	x = (x ^ 0xc761c23c) ^ (x >> 19)
	x = (x + 0x165667b1) + (x << 5)
	x = (x + 0xd3a2646c) ^ (x << 9)
	x = (x + 0xfd7046c5) + (x << 3)
	x = (x ^ 0xb55a4f09) ^ (x >> 16)
	return x
}

// Hash64 uses hash32 to compute a 64-bit integer hash.
func hash64(x uint64) uint32 {
	lo := hash32(uint32(x))
	hi := hash32(uint32(x >> 32))
	return lo ^ hi
}
