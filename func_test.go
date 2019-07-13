// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"reflect"
	"testing"
)

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
