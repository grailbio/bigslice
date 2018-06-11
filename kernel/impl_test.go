// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.

package kernel

import (
	"reflect"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/slicetype"
)

var (
	typeOfString  = reflect.TypeOf((*string)(nil)).Elem()
	typeOfUint    = reflect.TypeOf((*uint)(nil)).Elem()
	typeOfUint8   = reflect.TypeOf((*uint8)(nil)).Elem()
	typeOfUint16  = reflect.TypeOf((*uint16)(nil)).Elem()
	typeOfUint32  = reflect.TypeOf((*uint32)(nil)).Elem()
	typeOfUint64  = reflect.TypeOf((*uint64)(nil)).Elem()
	typeOfInt     = reflect.TypeOf((*int)(nil)).Elem()
	typeOfInt8    = reflect.TypeOf((*int8)(nil)).Elem()
	typeOfInt16   = reflect.TypeOf((*int16)(nil)).Elem()
	typeOfInt32   = reflect.TypeOf((*int32)(nil)).Elem()
	typeOfInt64   = reflect.TypeOf((*int64)(nil)).Elem()
	typeOfFloat32 = reflect.TypeOf((*float32)(nil)).Elem()
	typeOfFloat64 = reflect.TypeOf((*float64)(nil)).Elem()
	typeOfUintptr = reflect.TypeOf((*uintptr)(nil)).Elem()
)

func TestStringImplements(t *testing.T) {
	interfaces := []reflect.Type{
		SorterInterface,
		HasherInterface,
		IndexerInterface,
	}
	for _, iface := range interfaces {
		if !Implements(typeOfString, iface) {
			t.Errorf("interface %s not implemented", iface)
		}
	}
}

func TestStringSort(t *testing.T) {
	const N = 1000
	var sorter Sorter
	if !Lookup(typeOfString, &sorter) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfString), N)
	fuzzFrame(f)
	sorter.Sort(f)
	if !sorter.IsSorted(f) {
		t.Error("not sorted")
	}
	for i := 1; i < N; i++ {
		if sorter.Less(f, i, f, i-1) {
			t.Errorf("sorter.Less: %d", i)
		}
	}
}

func TestStringHasher(t *testing.T) {
	var hasher Hasher
	if !Lookup(typeOfString, &hasher) {
		t.Fatal("no implementation")
	}
}

func TestStringIndex(t *testing.T) {
	const N = 1000
	var indexer Indexer
	if !Lookup(typeOfString, &indexer) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfString), N)
	fuzzFrame(f)
	index := indexer.Index(f[0])
	ix := make([]int, f.Len())
	index.Index(f[0], ix)
	for _, i := range ix {
		if i >= f.Len() {
			t.Errorf("invalid index %v", i)
		}
	}
}

func TestUintImplements(t *testing.T) {
	interfaces := []reflect.Type{
		SorterInterface,
		HasherInterface,
		IndexerInterface,
	}
	for _, iface := range interfaces {
		if !Implements(typeOfUint, iface) {
			t.Errorf("interface %s not implemented", iface)
		}
	}
}

func TestUintSort(t *testing.T) {
	const N = 1000
	var sorter Sorter
	if !Lookup(typeOfUint, &sorter) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfUint), N)
	fuzzFrame(f)
	sorter.Sort(f)
	if !sorter.IsSorted(f) {
		t.Error("not sorted")
	}
	for i := 1; i < N; i++ {
		if sorter.Less(f, i, f, i-1) {
			t.Errorf("sorter.Less: %d", i)
		}
	}
}

func TestUintHasher(t *testing.T) {
	var hasher Hasher
	if !Lookup(typeOfUint, &hasher) {
		t.Fatal("no implementation")
	}
}

func TestUintIndex(t *testing.T) {
	const N = 1000
	var indexer Indexer
	if !Lookup(typeOfUint, &indexer) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfUint), N)
	fuzzFrame(f)
	index := indexer.Index(f[0])
	ix := make([]int, f.Len())
	index.Index(f[0], ix)
	for _, i := range ix {
		if i >= f.Len() {
			t.Errorf("invalid index %v", i)
		}
	}
}

func TestUint8Implements(t *testing.T) {
	interfaces := []reflect.Type{
		SorterInterface,
		HasherInterface,
		IndexerInterface,
	}
	for _, iface := range interfaces {
		if !Implements(typeOfUint8, iface) {
			t.Errorf("interface %s not implemented", iface)
		}
	}
}

func TestUint8Sort(t *testing.T) {
	const N = 1000
	var sorter Sorter
	if !Lookup(typeOfUint8, &sorter) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfUint8), N)
	fuzzFrame(f)
	sorter.Sort(f)
	if !sorter.IsSorted(f) {
		t.Error("not sorted")
	}
	for i := 1; i < N; i++ {
		if sorter.Less(f, i, f, i-1) {
			t.Errorf("sorter.Less: %d", i)
		}
	}
}

func TestUint8Hasher(t *testing.T) {
	var hasher Hasher
	if !Lookup(typeOfUint8, &hasher) {
		t.Fatal("no implementation")
	}
}

func TestUint8Index(t *testing.T) {
	const N = 1000
	var indexer Indexer
	if !Lookup(typeOfUint8, &indexer) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfUint8), N)
	fuzzFrame(f)
	index := indexer.Index(f[0])
	ix := make([]int, f.Len())
	index.Index(f[0], ix)
	for _, i := range ix {
		if i >= f.Len() {
			t.Errorf("invalid index %v", i)
		}
	}
}

func TestUint16Implements(t *testing.T) {
	interfaces := []reflect.Type{
		SorterInterface,
		HasherInterface,
		IndexerInterface,
	}
	for _, iface := range interfaces {
		if !Implements(typeOfUint16, iface) {
			t.Errorf("interface %s not implemented", iface)
		}
	}
}

func TestUint16Sort(t *testing.T) {
	const N = 1000
	var sorter Sorter
	if !Lookup(typeOfUint16, &sorter) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfUint16), N)
	fuzzFrame(f)
	sorter.Sort(f)
	if !sorter.IsSorted(f) {
		t.Error("not sorted")
	}
	for i := 1; i < N; i++ {
		if sorter.Less(f, i, f, i-1) {
			t.Errorf("sorter.Less: %d", i)
		}
	}
}

func TestUint16Hasher(t *testing.T) {
	var hasher Hasher
	if !Lookup(typeOfUint16, &hasher) {
		t.Fatal("no implementation")
	}
}

func TestUint16Index(t *testing.T) {
	const N = 1000
	var indexer Indexer
	if !Lookup(typeOfUint16, &indexer) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfUint16), N)
	fuzzFrame(f)
	index := indexer.Index(f[0])
	ix := make([]int, f.Len())
	index.Index(f[0], ix)
	for _, i := range ix {
		if i >= f.Len() {
			t.Errorf("invalid index %v", i)
		}
	}
}

func TestUint32Implements(t *testing.T) {
	interfaces := []reflect.Type{
		SorterInterface,
		HasherInterface,
		IndexerInterface,
	}
	for _, iface := range interfaces {
		if !Implements(typeOfUint32, iface) {
			t.Errorf("interface %s not implemented", iface)
		}
	}
}

func TestUint32Sort(t *testing.T) {
	const N = 1000
	var sorter Sorter
	if !Lookup(typeOfUint32, &sorter) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfUint32), N)
	fuzzFrame(f)
	sorter.Sort(f)
	if !sorter.IsSorted(f) {
		t.Error("not sorted")
	}
	for i := 1; i < N; i++ {
		if sorter.Less(f, i, f, i-1) {
			t.Errorf("sorter.Less: %d", i)
		}
	}
}

func TestUint32Hasher(t *testing.T) {
	var hasher Hasher
	if !Lookup(typeOfUint32, &hasher) {
		t.Fatal("no implementation")
	}
}

func TestUint32Index(t *testing.T) {
	const N = 1000
	var indexer Indexer
	if !Lookup(typeOfUint32, &indexer) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfUint32), N)
	fuzzFrame(f)
	index := indexer.Index(f[0])
	ix := make([]int, f.Len())
	index.Index(f[0], ix)
	for _, i := range ix {
		if i >= f.Len() {
			t.Errorf("invalid index %v", i)
		}
	}
}

func TestUint64Implements(t *testing.T) {
	interfaces := []reflect.Type{
		SorterInterface,
		HasherInterface,
		IndexerInterface,
	}
	for _, iface := range interfaces {
		if !Implements(typeOfUint64, iface) {
			t.Errorf("interface %s not implemented", iface)
		}
	}
}

func TestUint64Sort(t *testing.T) {
	const N = 1000
	var sorter Sorter
	if !Lookup(typeOfUint64, &sorter) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfUint64), N)
	fuzzFrame(f)
	sorter.Sort(f)
	if !sorter.IsSorted(f) {
		t.Error("not sorted")
	}
	for i := 1; i < N; i++ {
		if sorter.Less(f, i, f, i-1) {
			t.Errorf("sorter.Less: %d", i)
		}
	}
}

func TestUint64Hasher(t *testing.T) {
	var hasher Hasher
	if !Lookup(typeOfUint64, &hasher) {
		t.Fatal("no implementation")
	}
}

func TestUint64Index(t *testing.T) {
	const N = 1000
	var indexer Indexer
	if !Lookup(typeOfUint64, &indexer) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfUint64), N)
	fuzzFrame(f)
	index := indexer.Index(f[0])
	ix := make([]int, f.Len())
	index.Index(f[0], ix)
	for _, i := range ix {
		if i >= f.Len() {
			t.Errorf("invalid index %v", i)
		}
	}
}

func TestIntImplements(t *testing.T) {
	interfaces := []reflect.Type{
		SorterInterface,
		HasherInterface,
		IndexerInterface,
	}
	for _, iface := range interfaces {
		if !Implements(typeOfInt, iface) {
			t.Errorf("interface %s not implemented", iface)
		}
	}
}

func TestIntSort(t *testing.T) {
	const N = 1000
	var sorter Sorter
	if !Lookup(typeOfInt, &sorter) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfInt), N)
	fuzzFrame(f)
	sorter.Sort(f)
	if !sorter.IsSorted(f) {
		t.Error("not sorted")
	}
	for i := 1; i < N; i++ {
		if sorter.Less(f, i, f, i-1) {
			t.Errorf("sorter.Less: %d", i)
		}
	}
}

func TestIntHasher(t *testing.T) {
	var hasher Hasher
	if !Lookup(typeOfInt, &hasher) {
		t.Fatal("no implementation")
	}
}

func TestIntIndex(t *testing.T) {
	const N = 1000
	var indexer Indexer
	if !Lookup(typeOfInt, &indexer) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfInt), N)
	fuzzFrame(f)
	index := indexer.Index(f[0])
	ix := make([]int, f.Len())
	index.Index(f[0], ix)
	for _, i := range ix {
		if i >= f.Len() {
			t.Errorf("invalid index %v", i)
		}
	}
}

func TestInt8Implements(t *testing.T) {
	interfaces := []reflect.Type{
		SorterInterface,
		HasherInterface,
		IndexerInterface,
	}
	for _, iface := range interfaces {
		if !Implements(typeOfInt8, iface) {
			t.Errorf("interface %s not implemented", iface)
		}
	}
}

func TestInt8Sort(t *testing.T) {
	const N = 1000
	var sorter Sorter
	if !Lookup(typeOfInt8, &sorter) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfInt8), N)
	fuzzFrame(f)
	sorter.Sort(f)
	if !sorter.IsSorted(f) {
		t.Error("not sorted")
	}
	for i := 1; i < N; i++ {
		if sorter.Less(f, i, f, i-1) {
			t.Errorf("sorter.Less: %d", i)
		}
	}
}

func TestInt8Hasher(t *testing.T) {
	var hasher Hasher
	if !Lookup(typeOfInt8, &hasher) {
		t.Fatal("no implementation")
	}
}

func TestInt8Index(t *testing.T) {
	const N = 1000
	var indexer Indexer
	if !Lookup(typeOfInt8, &indexer) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfInt8), N)
	fuzzFrame(f)
	index := indexer.Index(f[0])
	ix := make([]int, f.Len())
	index.Index(f[0], ix)
	for _, i := range ix {
		if i >= f.Len() {
			t.Errorf("invalid index %v", i)
		}
	}
}

func TestInt16Implements(t *testing.T) {
	interfaces := []reflect.Type{
		SorterInterface,
		HasherInterface,
		IndexerInterface,
	}
	for _, iface := range interfaces {
		if !Implements(typeOfInt16, iface) {
			t.Errorf("interface %s not implemented", iface)
		}
	}
}

func TestInt16Sort(t *testing.T) {
	const N = 1000
	var sorter Sorter
	if !Lookup(typeOfInt16, &sorter) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfInt16), N)
	fuzzFrame(f)
	sorter.Sort(f)
	if !sorter.IsSorted(f) {
		t.Error("not sorted")
	}
	for i := 1; i < N; i++ {
		if sorter.Less(f, i, f, i-1) {
			t.Errorf("sorter.Less: %d", i)
		}
	}
}

func TestInt16Hasher(t *testing.T) {
	var hasher Hasher
	if !Lookup(typeOfInt16, &hasher) {
		t.Fatal("no implementation")
	}
}

func TestInt16Index(t *testing.T) {
	const N = 1000
	var indexer Indexer
	if !Lookup(typeOfInt16, &indexer) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfInt16), N)
	fuzzFrame(f)
	index := indexer.Index(f[0])
	ix := make([]int, f.Len())
	index.Index(f[0], ix)
	for _, i := range ix {
		if i >= f.Len() {
			t.Errorf("invalid index %v", i)
		}
	}
}

func TestInt32Implements(t *testing.T) {
	interfaces := []reflect.Type{
		SorterInterface,
		HasherInterface,
		IndexerInterface,
	}
	for _, iface := range interfaces {
		if !Implements(typeOfInt32, iface) {
			t.Errorf("interface %s not implemented", iface)
		}
	}
}

func TestInt32Sort(t *testing.T) {
	const N = 1000
	var sorter Sorter
	if !Lookup(typeOfInt32, &sorter) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfInt32), N)
	fuzzFrame(f)
	sorter.Sort(f)
	if !sorter.IsSorted(f) {
		t.Error("not sorted")
	}
	for i := 1; i < N; i++ {
		if sorter.Less(f, i, f, i-1) {
			t.Errorf("sorter.Less: %d", i)
		}
	}
}

func TestInt32Hasher(t *testing.T) {
	var hasher Hasher
	if !Lookup(typeOfInt32, &hasher) {
		t.Fatal("no implementation")
	}
}

func TestInt32Index(t *testing.T) {
	const N = 1000
	var indexer Indexer
	if !Lookup(typeOfInt32, &indexer) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfInt32), N)
	fuzzFrame(f)
	index := indexer.Index(f[0])
	ix := make([]int, f.Len())
	index.Index(f[0], ix)
	for _, i := range ix {
		if i >= f.Len() {
			t.Errorf("invalid index %v", i)
		}
	}
}

func TestInt64Implements(t *testing.T) {
	interfaces := []reflect.Type{
		SorterInterface,
		HasherInterface,
		IndexerInterface,
	}
	for _, iface := range interfaces {
		if !Implements(typeOfInt64, iface) {
			t.Errorf("interface %s not implemented", iface)
		}
	}
}

func TestInt64Sort(t *testing.T) {
	const N = 1000
	var sorter Sorter
	if !Lookup(typeOfInt64, &sorter) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfInt64), N)
	fuzzFrame(f)
	sorter.Sort(f)
	if !sorter.IsSorted(f) {
		t.Error("not sorted")
	}
	for i := 1; i < N; i++ {
		if sorter.Less(f, i, f, i-1) {
			t.Errorf("sorter.Less: %d", i)
		}
	}
}

func TestInt64Hasher(t *testing.T) {
	var hasher Hasher
	if !Lookup(typeOfInt64, &hasher) {
		t.Fatal("no implementation")
	}
}

func TestInt64Index(t *testing.T) {
	const N = 1000
	var indexer Indexer
	if !Lookup(typeOfInt64, &indexer) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfInt64), N)
	fuzzFrame(f)
	index := indexer.Index(f[0])
	ix := make([]int, f.Len())
	index.Index(f[0], ix)
	for _, i := range ix {
		if i >= f.Len() {
			t.Errorf("invalid index %v", i)
		}
	}
}

func TestFloat32Implements(t *testing.T) {
	interfaces := []reflect.Type{
		SorterInterface,
		HasherInterface,
		IndexerInterface,
	}
	for _, iface := range interfaces {
		if !Implements(typeOfFloat32, iface) {
			t.Errorf("interface %s not implemented", iface)
		}
	}
}

func TestFloat32Sort(t *testing.T) {
	const N = 1000
	var sorter Sorter
	if !Lookup(typeOfFloat32, &sorter) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfFloat32), N)
	fuzzFrame(f)
	sorter.Sort(f)
	if !sorter.IsSorted(f) {
		t.Error("not sorted")
	}
	for i := 1; i < N; i++ {
		if sorter.Less(f, i, f, i-1) {
			t.Errorf("sorter.Less: %d", i)
		}
	}
}

func TestFloat32Hasher(t *testing.T) {
	var hasher Hasher
	if !Lookup(typeOfFloat32, &hasher) {
		t.Fatal("no implementation")
	}
}

func TestFloat32Index(t *testing.T) {
	const N = 1000
	var indexer Indexer
	if !Lookup(typeOfFloat32, &indexer) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfFloat32), N)
	fuzzFrame(f)
	index := indexer.Index(f[0])
	ix := make([]int, f.Len())
	index.Index(f[0], ix)
	for _, i := range ix {
		if i >= f.Len() {
			t.Errorf("invalid index %v", i)
		}
	}
}

func TestFloat64Implements(t *testing.T) {
	interfaces := []reflect.Type{
		SorterInterface,
		HasherInterface,
		IndexerInterface,
	}
	for _, iface := range interfaces {
		if !Implements(typeOfFloat64, iface) {
			t.Errorf("interface %s not implemented", iface)
		}
	}
}

func TestFloat64Sort(t *testing.T) {
	const N = 1000
	var sorter Sorter
	if !Lookup(typeOfFloat64, &sorter) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfFloat64), N)
	fuzzFrame(f)
	sorter.Sort(f)
	if !sorter.IsSorted(f) {
		t.Error("not sorted")
	}
	for i := 1; i < N; i++ {
		if sorter.Less(f, i, f, i-1) {
			t.Errorf("sorter.Less: %d", i)
		}
	}
}

func TestFloat64Hasher(t *testing.T) {
	var hasher Hasher
	if !Lookup(typeOfFloat64, &hasher) {
		t.Fatal("no implementation")
	}
}

func TestFloat64Index(t *testing.T) {
	const N = 1000
	var indexer Indexer
	if !Lookup(typeOfFloat64, &indexer) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfFloat64), N)
	fuzzFrame(f)
	index := indexer.Index(f[0])
	ix := make([]int, f.Len())
	index.Index(f[0], ix)
	for _, i := range ix {
		if i >= f.Len() {
			t.Errorf("invalid index %v", i)
		}
	}
}

func TestUintptrImplements(t *testing.T) {
	interfaces := []reflect.Type{
		SorterInterface,
		HasherInterface,
		IndexerInterface,
	}
	for _, iface := range interfaces {
		if !Implements(typeOfUintptr, iface) {
			t.Errorf("interface %s not implemented", iface)
		}
	}
}

func TestUintptrSort(t *testing.T) {
	const N = 1000
	var sorter Sorter
	if !Lookup(typeOfUintptr, &sorter) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfUintptr), N)
	fuzzFrame(f)
	sorter.Sort(f)
	if !sorter.IsSorted(f) {
		t.Error("not sorted")
	}
	for i := 1; i < N; i++ {
		if sorter.Less(f, i, f, i-1) {
			t.Errorf("sorter.Less: %d", i)
		}
	}
}

func TestUintptrHasher(t *testing.T) {
	var hasher Hasher
	if !Lookup(typeOfUintptr, &hasher) {
		t.Fatal("no implementation")
	}
}

func TestUintptrIndex(t *testing.T) {
	const N = 1000
	var indexer Indexer
	if !Lookup(typeOfUintptr, &indexer) {
		t.Fatal("no implementation")
	}
	f := frame.Make(slicetype.New(typeOfUintptr), N)
	fuzzFrame(f)
	index := indexer.Index(f[0])
	ix := make([]int, f.Len())
	index.Index(f[0], ix)
	for _, i := range ix {
		if i >= f.Len() {
			t.Errorf("invalid index %v", i)
		}
	}
}

func fuzzFrame(f frame.Frame) {
	fz := fuzz.New()
	fz.NilChance(0)
	for _, col := range f {
		for i := 0; i < f.Len(); i++ {
			fz.Fuzz(col.Index(i).Addr().Interface())
		}
	}
}
