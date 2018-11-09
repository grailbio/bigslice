// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package frame

// Session is a key-value store that is used by frame codecs to
// store session-specific state.
type Session interface {
	// Store associates the provided key with value. Store
	// previous values associated with the key.
	Store(key, value interface{})
	// Load returns the value associated with key, as well
	// as a boolean indicating whether a nonempty entry
	// was found.
	Load(key interface{}) (value interface{}, ok bool)
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
