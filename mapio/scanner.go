// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package mapio

// Scanner is an ordered iterator over map entries.
type Scanner interface {
	// Scan scans the next entry, returning true on success, after which
	// time the entry is available to inspect using the Key and Value
	// methods.
	Scan() bool
	// Err returns the last error encountered while scanning, if any.
	Err() error
	// Key returns the key of the last scanned entry.
	Key() []byte
	// Value returns the value of the last scanned entry.
	Value() []byte
}
