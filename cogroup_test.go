// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/slicetest"
	"github.com/grailbio/bigslice/slicetype"
)

// sortedCogroup returns a Cogroup slice that sorts resulting groups with
// ordered elements.
func sortedCogroup(slices ...bigslice.Slice) bigslice.Slice {
	slice := bigslice.Cogroup(slices...)
	return bigslice.Map(slice, makeSortSlices(slice))
}

// makeSortSlices returns a map function (to be used with bigslice.Map) that sorts
// any slices with ordered elements.
func makeSortSlices(typ slicetype.Type) interface{} {
	in := make([]reflect.Type, typ.NumOut())
	for i := 0; i < typ.NumOut(); i++ {
		in[i] = typ.Out(i)
	}
	fTyp := reflect.FuncOf(in, in, false)
	f := reflect.MakeFunc(fTyp, func(args []reflect.Value) []reflect.Value {
		for _, arg := range args {
			if arg.Kind() != reflect.Slice {
				// Ignore anything that isn't a slice.
				continue
			}
			switch arg.Type().Elem().Kind() {
			case reflect.String:
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			case reflect.Float32, reflect.Float64:
			default:
				// Ignore anything that isn't ordered.
				continue
			}
			sort.Slice(arg.Interface(), func(i, j int) bool {
				ai := arg.Index(i)
				aj := arg.Index(j)
				switch arg.Type().Elem().Kind() {
				case reflect.String:
					return ai.String() < aj.String()
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					return ai.Int() < aj.Int()
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					return ai.Uint() < aj.Uint()
				case reflect.Float32, reflect.Float64:
					return ai.Float() < aj.Float()
				default:
					panic("unreachable")
				}
			})
		}
		return args
	})
	return f.Interface()
}

func TestCogroup(t *testing.T) {
	data1 := []interface{}{
		[]string{"z", "b", "d", "d"},
		[]int{1, 2, 3, 4},
	}
	data2 := []interface{}{
		[]string{"x", "y", "z", "d"},
		[]string{"one", "two", "three", "four"},
	}
	sharding := [][]int{{1, 1}, {1, 4}, {2, 1}, {4, 4}}
	for _, shard := range sharding {
		slice1 := bigslice.Const(shard[0], data1...)
		slice2 := bigslice.Const(shard[1], data2...)

		assertEqual(t, sortedCogroup(slice1, slice2), true,
			[]string{"b", "d", "x", "y", "z"},
			[][]int{{2}, {3, 4}, nil, nil, {1}},
			[][]string{nil, {"four"}, {"one"}, {"two"}, {"three"}},
		)
		assertEqual(t, sortedCogroup(slice2, slice1), true,
			[]string{"b", "d", "x", "y", "z"},
			[][]string{nil, {"four"}, {"one"}, {"two"}, {"three"}},
			[][]int{{2}, {3, 4}, nil, nil, {1}},
		)
		// Should work equally well for one slice.
		assertEqual(t, sortedCogroup(slice1), true,
			[]string{"b", "d", "z"},
			[][]int{{2}, {3, 4}, {1}},
		)
		if testing.Short() {
			break
		}
	}
}

func TestCogroupPrefixed(t *testing.T) {
	data1 := []interface{}{
		[]string{"z", "a", "a", "b", "d"},
		[]int{0, 0, 0, 2, 3},
		[]string{"foo", "bar", "baz", "qux", "quux"},
	}
	data2 := []interface{}{
		[]string{"d", "a", "a", "b", "c", "d", "b"},
		[]int{3, 0, 1, 1, 2, 3, 1},
		[]int{0, 1, 2, 3, 4, 5, 6},
	}
	sharding := [][]int{{1, 1}, {1, 4}, {2, 1}, {4, 4}}
	for _, shard := range sharding {
		slice1 := bigslice.Const(shard[0], data1...)
		slice1 = bigslice.Prefixed(slice1, 2)
		slice2 := bigslice.Const(shard[1], data2...)
		slice2 = bigslice.Prefixed(slice2, 2)

		assertEqual(t, sortedCogroup(slice1, slice2), true,
			[]string{"a", "a", "b", "b", "c", "d", "z"},
			[]int{0, 1, 1, 2, 2, 3, 0},
			[][]string{{"bar", "baz"}, nil, nil, {"qux"}, nil, {"quux"}, {"foo"}},
			[][]int{{1}, {2}, {3, 6}, nil, {4}, {0, 5}, nil},
		)
		assertEqual(t, sortedCogroup(slice2, slice1), true,
			[]string{"a", "a", "b", "b", "c", "d", "z"},
			[]int{0, 1, 1, 2, 2, 3, 0},
			[][]int{{1}, {2}, {3, 6}, nil, {4}, {0, 5}, nil},
			[][]string{{"bar", "baz"}, nil, nil, {"qux"}, nil, {"quux"}, {"foo"}},
		)
		// Should work equally well for one slice.
		assertEqual(t, sortedCogroup(slice1), true,
			[]string{"a", "b", "d", "z"},
			[]int{0, 2, 3, 0},
			[][]string{{"bar", "baz"}, {"qux"}, {"quux"}, {"foo"}},
		)
		if testing.Short() {
			break
		}
	}
}

func ExampleCogroup() {
	slice0 := bigslice.Const(2,
		[]int{0, 1, 2, 3, 0, 1},
		[]string{"zero", "one", "two", "three", "cero", "uno"},
	)
	slice1 := bigslice.Const(2,
		[]int{0, 1, 2, 3, 4, 5, 6},
		[]int{0, 1, 4, 9, 16, 25, 36},
	)
	slice := bigslice.Cogroup(slice0, slice1)
	slicetest.Print(slice)
	// Output:
	// 0 [cero zero] [0]
	// 1 [one uno] [1]
	// 2 [two] [4]
	// 3 [three] [9]
	// 4 [] [16]
	// 5 [] [25]
	// 6 [] [36]
}
