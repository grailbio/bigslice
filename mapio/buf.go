// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package mapio

import (
	"bytes"
	"sort"
)

// A Buf is an unordered write buffer for maps. It holds entries
// in memory; these are then sorted and written to a map.
type Buf struct {
	keys, values       [][]byte
	keySize, valueSize int
}

// Append append the given entry to the buffer.
func (b *Buf) Append(key, value []byte) {
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	b.keys = append(b.keys, keyCopy)
	b.values = append(b.values, valueCopy)
	b.keySize += len(keyCopy)
	b.valueSize += len(valueCopy)
}

// Size returns the number size of this buffer in bytes.
func (b *Buf) Size() int { return b.keySize + b.valueSize }

// Len implements sort.Interface
func (b *Buf) Len() int { return len(b.keys) }

// Less implements sort.Interface
func (b *Buf) Less(i, j int) bool { return bytes.Compare(b.keys[i], b.keys[j]) < 0 }

// Swap implements sort.Interface
func (b *Buf) Swap(i, j int) {
	b.keys[i], b.keys[j] = b.keys[j], b.keys[i]
	b.values[i], b.values[j] = b.values[j], b.values[i]
}

// WriteTo sorts and then writes all of the entries in this buffer to
// the provided writer.
func (b *Buf) WriteTo(w *Writer) error {
	sort.Sort(b)
	for i := range b.keys {
		if err := w.Append(b.keys[i], b.values[i]); err != nil {
			return err
		}
	}
	return nil
}
