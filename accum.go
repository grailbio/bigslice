// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"reflect"

	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/sliceio"
)

// An Accumulator represents a stateful accumulation of values of
// a certain type. Accumulators maintain their state in memory.
//
// Accumulators should be read only after accumulation is complete.
type Accumulator interface {
	// Accumulate the provided columns of length n.
	Accumulate(in frame.Frame, n int)
	// Read a batch of accumulated values into keys and values. These
	// are slices of the key type and accumulator type respectively.
	Read(keys, values reflect.Value) (int, error)
}

func canMakeAccumulatorForKey(keyType reflect.Type) bool {
	switch keyType.Kind() {
	case reflect.String, reflect.Int, reflect.Int64:
		return true
	default:
		return false
	}
}

func makeAccumulator(keyType, accType reflect.Type, fn reflect.Value) Accumulator {
	switch keyType.Kind() {
	case reflect.String:
		return &stringAccumulator{
			accType: accType,
			fn:      fn,
			state:   make(map[string]reflect.Value),
		}
	case reflect.Int:
		return &intAccumulator{
			accType: accType,
			fn:      fn,
			state:   make(map[int]reflect.Value),
		}
	case reflect.Int64:
		return &int64Accumulator{
			accType: accType,
			fn:      fn,
			state:   make(map[int64]reflect.Value),
		}
	default:
		return nil
	}
}

// StringAccumulator accumulates values by string keys.
type stringAccumulator struct {
	accType reflect.Type
	fn      reflect.Value
	state   map[string]reflect.Value
}

func (s *stringAccumulator) Accumulate(in frame.Frame, n int) {
	keys := in.Interface(0).([]string)
	args := make([]reflect.Value, in.NumOut())
	for i := 0; i < n; i++ {
		key := keys[i]
		val, ok := s.state[key]
		if !ok {
			val = reflect.Zero(s.accType)
		}
		args[0] = val
		for j := 1; j < in.NumOut(); j++ {
			args[j] = in.Index(j, i)
		}
		s.state[key] = s.fn.Call(args)[0]
	}
}

func (s *stringAccumulator) Read(keys, values reflect.Value) (n int, err error) {
	max := keys.Len()
	for key, val := range s.state {
		if n >= max {
			break
		}
		keys.Index(n).Set(reflect.ValueOf(key))
		values.Index(n).Set(val)
		delete(s.state, key)
		n++
	}
	if len(s.state) == 0 {
		return n, sliceio.EOF
	}
	return n, nil
}

// IntAccumulator accumulates values by integer keys.
type intAccumulator struct {
	accType reflect.Type
	fn      reflect.Value
	state   map[int]reflect.Value
}

func (s *intAccumulator) Accumulate(in frame.Frame, n int) {
	keys := in.Interface(0).([]int)
	args := make([]reflect.Value, in.NumOut())
	for i := 0; i < n; i++ {
		key := keys[i]
		val, ok := s.state[key]
		if !ok {
			val = reflect.Zero(s.accType)
		}
		args[0] = val
		for j := 1; j < in.NumOut(); j++ {
			args[j] = in.Index(j, i)
		}
		s.state[key] = s.fn.Call(args)[0]
	}
}

func (s *intAccumulator) Read(keys, values reflect.Value) (n int, err error) {
	max := keys.Len()
	for key, val := range s.state {
		if n >= max {
			break
		}
		keys.Index(n).Set(reflect.ValueOf(key))
		values.Index(n).Set(val)
		delete(s.state, key)
		n++
	}
	if len(s.state) == 0 {
		return n, sliceio.EOF
	}
	return n, nil
}

// Int64Accumulator accumulates values by integer keys.
type int64Accumulator struct {
	accType reflect.Type
	fn      reflect.Value
	state   map[int64]reflect.Value
}

func (s *int64Accumulator) Accumulate(in frame.Frame, n int) {
	keys := in.Interface(0).([]int64)
	args := make([]reflect.Value, in.NumOut())
	for i := 0; i < n; i++ {
		key := keys[i]
		val, ok := s.state[key]
		if !ok {
			val = reflect.Zero(s.accType)
		}
		args[0] = val
		for j := 1; j < in.NumOut(); j++ {
			args[j] = in.Index(j, i)
		}
		s.state[key] = s.fn.Call(args)[0]
	}
}

func (s *int64Accumulator) Read(keys, values reflect.Value) (n int, err error) {
	max := keys.Len()
	for key, val := range s.state {
		if n >= max {
			break
		}
		keys.Index(n).Set(reflect.ValueOf(key))
		values.Index(n).Set(val)
		delete(s.state, key)
		n++
	}
	if len(s.state) == 0 {
		return n, sliceio.EOF
	}
	return n, nil
}
