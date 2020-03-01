// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"reflect"
	"testing"
)

type testStruct0 struct{ field0 int }
type testStruct1 struct{ field1 int }

type testInterface interface{ FuncTestMethod() }
type testInterfaceImpl struct{}

func (s *testInterfaceImpl) FuncTestMethod() {}

var fnTestNilFuncArgs = Func(
	func(i int, s string, ss []string, m map[int]int,
		ts0 testStruct0, pts1 *testStruct1, ti testInterface) Slice {

		return Const(1, []int{})
	})

// TestNilFuncArgs verifies that Func invocation handles untyped nil arguments
// properly.
func TestNilFuncArgs(t *testing.T) {
	ts0 := testStruct0{field0: 0}
	pts1 := &testStruct1{field1: 0}
	ptii := &testInterfaceImpl{}
	for _, c := range []struct {
		name string
		args []interface{}
		ok   bool
	}{
		{
			name: "all non-nil",
			args: []interface{}{
				0, "", []string{}, map[int]int{0: 0},
				ts0, pts1, ptii,
			},
			ok: true,
		},
		{
			name: "nil for types that can be nil",
			args: []interface{}{
				0, "", nil, nil,
				ts0, nil, nil,
			},
			ok: true,
		},
		{
			name: "nil for int",
			args: []interface{}{
				nil, "", []string{}, map[int]int{0: 0},
				ts0, pts1, ptii,
			},
			ok: false,
		},
		{
			name: "nil for string",
			args: []interface{}{
				0, nil, []string{}, map[int]int{0: 0},
				ts0, pts1, ptii,
			},
			ok: false,
		},
		{
			name: "nil for struct",
			args: []interface{}{
				0, "", []string{}, map[int]int{0: 0},
				nil, pts1, ptii,
			},
			ok: false,
		},
		{
			name: "too few args",
			args: []interface{}{},
			ok:   false,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			checkPanic := func() {
				r := recover()
				if c.ok {
					if r != nil {
						t.Errorf("expected no panic, got %v", r)
					}
				} else {
					if r == nil {
						t.Errorf("expected panic")
					}
				}
			}
			func() {
				defer checkPanic()
				fnTestNilFuncArgs.Invocation("", c.args...)
			}()
			func() {
				defer checkPanic()
				fnTestNilFuncArgs.Apply(c.args...)
			}()
		})
	}
}

func TestFuncLocationsDiff(t *testing.T) {
	for _, c := range []struct {
		lhs  []string
		rhs  []string
		diff []string
	}{
		{nil, nil, nil},
		{[]string{"a"}, []string{"a"}, nil},
		{
			[]string{},
			[]string{"a"},
			[]string{"+ a"},
		},
		{
			[]string{"a", "b"},
			[]string{"a"},
			[]string{"a", "- b"},
		},
		{
			[]string{"a", "b"},
			[]string{"b"},
			[]string{"- a", "b"},
		},
		{
			[]string{"a"},
			[]string{"a", "b"},
			[]string{"a", "+ b"},
		},
		{
			[]string{"a", "c"},
			[]string{"a", "b", "c", "d"},
			[]string{"a", "+ b", "c", "+ d"},
		},
		{
			[]string{"a", "b", "d"},
			[]string{"a", "c", "d"},
			[]string{"a", "- b", "+ c", "d"},
		},
		{
			[]string{"a", "b", "c"},
			[]string{"a", "c", "d", "e"},
			[]string{"a", "- b", "c", "+ d", "+ e"},
		},
	} {
		if got, want := FuncLocationsDiff(c.lhs, c.rhs), c.diff; !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
