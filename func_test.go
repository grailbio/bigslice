// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"reflect"
	"testing"
	"unsafe"
)

type testStruct0 struct{ field0 int }
// testStruct1 exists to avoid the problem of registering the same struct twice
// with gob. As a convenience, bigslice.Func registers its argument types.
// However, if you pass the same struct as a value and a pointer, we attempt to
// register the same type twice with different names, e.g.
// "github.com/grailbio/bigslice.testStruct0" and "*bigslice.testStruct0". This
// causes a panic in gob. Instead, we just use a different type altogether for
// our pointer-to-struct argument.
type testStruct1 struct{ field1 int }

// Disable unused checking for testInterface, as we're just using it to make
// sure that our func typechecking works properly, and we don't need to
// call it to do so.
// nolint:unused
type testInterface interface{ FuncTestMethod() }
type testInterfaceImpl struct{}

func (s *testInterfaceImpl) FuncTestMethod() {}

var fnTestNilFuncArgs = Func(
	func(_ int, _ string, _ []string, _ map[int]int,
		_ testStruct0, _ *testStruct1, _ unsafe.Pointer,
		_ testInterface) Slice {

		return Const(1, []int{})
	})

// TestNilFuncArgs verifies that Func invocation handles untyped nil arguments
// properly.
func TestNilFuncArgs(t *testing.T) {
	ts0 := testStruct0{field0: 0}
	pts1 := &testStruct1{field1: 0}
	upts1 := unsafe.Pointer(pts1)
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
				ts0, pts1, upts1, ptii,
			},
			ok: true,
		},
		{
			name: "nil for types that can be nil",
			args: []interface{}{
				0, "", nil, nil,
				ts0, nil, nil, nil,
			},
			ok: true,
		},
		{
			name: "nil for int",
			args: []interface{}{
				nil, "", []string{}, map[int]int{0: 0},
				ts0, pts1, upts1, ptii,
			},
			ok: false,
		},
		{
			name: "nil for string",
			args: []interface{}{
				0, nil, []string{}, map[int]int{0: 0},
				ts0, pts1, upts1, ptii,
			},
			ok: false,
		},
		{
			name: "nil for struct",
			args: []interface{}{
				0, "", []string{}, map[int]int{0: 0},
				nil, pts1, upts1, ptii,
			},
			ok: false,
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
