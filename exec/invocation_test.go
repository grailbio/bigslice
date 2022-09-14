// Copyright 2022 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/grailbio/bigslice"
	"github.com/grailbio/testutil/assert"
	"github.com/grailbio/testutil/expect"
)

type testInterface interface{ IsTestInterface() }
type testInterfaceImpl struct {
	Dummy int
}

func (s *testInterfaceImpl) IsTestInterface() {}

func init() {
	// When we declare a Func argument to be an interface (testInterface), we
	// still need to register the implementation, per normal gob usage
	// expectation.
	gob.Register(&testInterfaceImpl{})
}

// unregistered is a gob-encodable type for testing invocation argument
// encoding/decoding.  Note that it is not registered with gob.Register, so it
// would normally not be allowed to be encoded as an interface value.
type unregistered struct {
	Dummy int
}

var fnTestInvocationEncoding = bigslice.Func(
	// See TestInvocationGob for the cases exercised by each argument type.
	func(
		int,
		unregistered,
		*unregistered,
		testInterfaceImpl,
		testInterface,
		interface{},
		*Result,
		bigslice.Slice,
	) bigslice.Slice {
		return bigslice.Const(1, []int{})
	})

// TestInvocationGob verifies that we can round-trip through gob.  We implement
// custom encoding/decoding so that we can serialize arbitary Func arguments as
// interface values without registering the types with gob, as we capture
// the argument types ourselves (for typechecking).
func TestInvocationGob(t *testing.T) {
	inv := makeExecInvocation(
		fnTestInvocationEncoding.Invocation(
			"test",
			[]interface{}{
				2,                             // primitive
				unregistered{Dummy: 3},        // struct not registered with gob
				&unregistered{Dummy: 5},       // pointer to struct not registered with gob
				testInterfaceImpl{Dummy: 7},   // struct registered with gob
				&testInterfaceImpl{Dummy: 11}, // concrete as interface
				&testInterfaceImpl{Dummy: 13}, // concrete as empty interface
				&Result{},                     // *Result as concrete
				&Result{},                     // *Result as interface (Slice)
			}...,
		),
	)
	// Simulate replacement of *Result arguments with invocation references, as
	// we do when we send invocations to workers.
	inv.Args[6] = invocationRef{Index: 17}
	inv.Args[7] = invocationRef{Index: 19}
	var (
		b   bytes.Buffer
		enc = gob.NewEncoder(&b)
		dec = gob.NewDecoder(&b)
		got execInvocation
	)
	assert.NoError(t, enc.Encode(inv))
	assert.NoError(t, dec.Decode(&got))
	expect.EQ(t, got, inv)
}
