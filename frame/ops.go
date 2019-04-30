// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package frame

import (
	"bytes"
	"fmt"
	"reflect"
	"runtime"
	"sync"

	"github.com/spaolacci/murmur3"
)

//go:generate go run genops.go

var (
	mu        sync.Mutex
	makeOps   = map[reflect.Type]reflect.Value{}
	locations = map[reflect.Type]string{}
	typeOfOps = reflect.TypeOf((*Ops)(nil)).Elem()
)

// Ops represents a set of operations on a single frame instance. Ops
// are instantiated from implementations registered with RegisterOps
// and are managed by the frame instance.
//
// Note that not all types may support all operations.
type Ops struct {
	// Less compares two indices of a slice.
	Less func(i, j int) bool
	// HashWithSeed computes a 32-bit hash, given a seed, of an index
	// of a slice.
	HashWithSeed func(i int, seed uint32) uint32

	// Encode encodes a slice of the underlying vector. Encode is
	// optional, and overrides the default encoding (gob) used when
	// serializing and deserializing frames. Encode and decode must
	// always be specified together.
	Encode func(enc Encoder, i, j int) error

	// Decode decodes a slice encoded by Encode(i, j).
	Decode func(dec Decoder, i, j int) error

	// Swap swaps two elements in a slice. It is implemented generically
	// and cannot be overridden by a user implementation.
	swap func(i, j int)
}

// RegisterOps registers an ops implementation. The provided argument
// make should be a function of the form
//
//	func(slice []t) Ops
//
// returning operations for a t-typed slice. RegisterOps panics if
// the argument does not have the required shape or if operations
// have already been registered for type t.
func RegisterOps(make interface{}) {
	typ := reflect.TypeOf(make)
	check := func(ok bool) {
		if !ok {
			panic("frame.RegisterOps: bad type " + typ.String() + "; expected func([]t) frame.Ops")
		}
	}
	check(typ.Kind() == reflect.Func)
	check(typ.NumIn() == 1 && typ.In(0).Kind() == reflect.Slice)
	check(typ.NumOut() == 1 && typ.Out(0) == typeOfOps)
	elem := typ.In(0).Elem()
	mu.Lock()
	defer mu.Unlock()
	if _, ok := makeOps[elem]; ok {
		location, ok := locations[elem]
		if !ok {
			location = "<unknown>"
		}
		panic("frame.RegisterOps: ops already registered for type " + elem.String() + " at " + location)
	}
	makeOps[elem] = reflect.ValueOf(make)
	if _, file, line, ok := runtime.Caller(1); ok {
		locations[elem] = fmt.Sprintf("%s:%d", file, line)
	}
}

func makeSliceOps(typ reflect.Type, slice reflect.Value) Ops {
	make, ok := makeOps[typ]
	if !ok {
		return Ops{}
	}
	ops := make.Call([]reflect.Value{slice})[0].Interface().(Ops)
	if (ops.Encode != nil) != (ops.Decode != nil) {
		panic("encode and decode not defined together")
	}
	return ops
}

// CanCompare returns whether values of the provided type are comparable.
func CanCompare(typ reflect.Type) bool {
	return makeSliceOps(typ, reflect.MakeSlice(reflect.SliceOf(typ), 0, 0)).Less != nil
}

// CanHash returns whether values of the provided type can be hashed.
func CanHash(typ reflect.Type) bool {
	return makeSliceOps(typ, reflect.MakeSlice(reflect.SliceOf(typ), 0, 0)).HashWithSeed != nil
}

func init() {
	RegisterOps(func(slice [][]byte) Ops {
		return Ops{
			Less:         func(i, j int) bool { return bytes.Compare(slice[i], slice[j]) < 0 },
			HashWithSeed: func(i int, seed uint32) uint32 { return murmur3.Sum32WithSeed(slice[i], seed) },
		}
	})
	RegisterOps(func(slice []bool) Ops {
		return Ops{
			Less: func(i, j int) bool { return !slice[i] && slice[j] },
			HashWithSeed: func(i int, seed uint32) uint32 {
				if slice[i] {
					return seed + 1
				}
				return seed
			},
		}
	})
}
