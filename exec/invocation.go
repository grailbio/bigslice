// Copyright 2022 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"

	"github.com/grailbio/bigslice"
)

// execInvocation embeds bigslice.Invocation and is augmented with fields used
// for execution.
type execInvocation struct {
	bigslice.Invocation
	// Env is the compilation environment.  This is saved so that workers
	// compile slices with the same environment as the driver.
	Env CompileEnv
}

// invocationRef is a reference to an invocation that can be serialized and
// used across process boundaries.  We convert *Result arguments to
// invocationRefs.  Receivers then convert them back to *Result values in their
// address space.
type invocationRef struct{ Index uint64 }

func makeExecInvocation(inv bigslice.Invocation) execInvocation {
	return execInvocation{
		Invocation: inv,
		Env:        makeCompileEnv(),
	}
}

// GobEncode implements gob.GobEncoder.  The implementation handles the
// encoding of the arbitrary interface{} argument types without registration of
// those types using gob.Register, as we record the argument types in the call
// to bigslice.Func.
func (inv execInvocation) GobEncode() ([]byte, error) {
	var (
		b   bytes.Buffer
		enc = gob.NewEncoder(&b)
	)
	for _, field := range inv.directEncodedFields() {
		if err := enc.Encode(field.ptr); err != nil {
			return nil, fmt.Errorf("encoding %s: %v", field.name, err)
		}
	}
	fv := bigslice.FuncByIndex(inv.Func)
	for i, arg := range inv.Args {
		typ := fv.In(i)
		if typ.Kind() == reflect.Interface {
			// Pass the address of arg so Encode sees (and hence sends) a value
			// of interface type.  If arg is of a concrete type and we passed
			// it directly, it would see the concrete type instead.  See the
			// blog post "The Laws of Reflection" for background:
			// https://go.dev/blog/laws-of-reflection.
			if err := enc.Encode(&arg); err != nil {
				return nil, fmt.Errorf("encoding arg %d of type %v: %v", i, typ, err)
			}
			continue
		}
		if err := enc.Encode(arg); err != nil {
			return nil, fmt.Errorf("encoding arg %d of type %v: %v", i, typ, err)
		}
	}
	return b.Bytes(), nil
}

var (
	typResultPtr      = reflect.TypeOf((*Result)(nil))
	typEmptyInterface = reflect.TypeOf((*interface{})(nil)).Elem()
	typInvocationRef  = reflect.TypeOf(invocationRef{})
)

// GobDecode implements gob.GobDecoder.  The implementation handles the
// decoding of the arbitrary interface{} argument types without registration of
// those types using gob.Register, as we record the argument types in the call
// to bigslice.Func.
func (inv *execInvocation) GobDecode(p []byte) error {
	var (
		b   = bytes.NewBuffer(p)
		dec = gob.NewDecoder(b)
	)
	for _, field := range inv.directEncodedFields() {
		if err := dec.Decode(field.ptr); err != nil {
			return fmt.Errorf("decoding %s: %v", field.name, err)
		}
	}
	fv := bigslice.FuncByIndex(inv.Func)
	inv.Args = make([]interface{}, fv.NumIn())
	for i := range inv.Args {
		typ := fv.In(i)
		var v reflect.Value
		switch {
		case typ == typResultPtr:
			// *Result arguments are replaced with invocationRefs for transit
			// across process boundaries.  See (*bigmachine.Executor).compile.
			v = reflect.New(typInvocationRef)
		case typ.Kind() == reflect.Interface:
			// A *Result can also be passed as an interface that invocationRef
			// does not implement, like Slice, so we decode into an empty
			// interface.  The invocationRef is replaced back with a *Result
			// (in the new address space) elsewhere.  Arguments are typechecked
			// later, so we can safely decode into an empty interface in all
			// cases here.
			v = reflect.New(typEmptyInterface)
		default:
			v = reflect.New(typ)
		}
		if err := dec.DecodeValue(v); err != nil {
			return fmt.Errorf("decoding arg %d of type %v: %v", i, typ, err)
		}
		inv.Args[i] = v.Elem().Interface()
	}
	return nil
}

// field is a field (of an *execInvocation) that we can gob-encode/decode
// directly.
type field struct {
	// name is the name of the field.  We use it in error messages.
	name string
	// ptr is the address of the field.  We use the address so that we can
	// decode into it.
	ptr interface{}
}

// directEncodedFields returns the fields that we can gob-encode/decode
// directly in our custom encoding/decoding.
func (inv *execInvocation) directEncodedFields() []field {
	return []field{
		{"Index", &inv.Index},
		{"Func", &inv.Func},
		{"Exclusive", &inv.Exclusive},
		{"Location", &inv.Location},
		{"Env", &inv.Env},
	}
}
