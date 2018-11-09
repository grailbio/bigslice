// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package frame

import "sync/atomic"

// Key represents a key that can be used to store session specific
// state.
type Key uint64

// Session is a key-value store that is used by frame codecs to
// store session-specific state.
type Session interface {
	// State retrieves the session state for the provided key
	// into the pointer state. If state is not a pointer, then
	// State will panic. State returns true the first time the key
	// is encountered in the session. This is typically used by
	// user code to initialize the state.
	//
	// If the state value is a pointer, then the first call to State
	// sets the state pointer (a pointer to a pointer) to a newly
	// allocated value. Otherwise it is set to a zero value of the
	// value type.
	State(key Key, state interface{}) bool
}

// An Encoder manages transmission of data over a connection or file.
type Encoder interface {
	Session
	// Encode encodes the provided value into the
	// encoder's stream using Gob.
	Encode(v interface{}) error
}

type Decoder interface {
	Session
	// Decode decodes a value from the underlying stream using
	// Gob.
	Decode(v interface{}) error
}

var key uint64

// FreshKey returns a unique key that can be used to store
// state in sessions.
func FreshKey() Key {
	return Key(atomic.AddUint64(&key, 1))
}
