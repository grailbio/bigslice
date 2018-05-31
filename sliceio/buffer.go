// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sliceio

import (
	"bytes"
	"encoding/gob"
	"reflect"
)

// Buffer buffers serialized slice output in memory. The data are stored
// as a gob-stream where records are as batches in column-major form.
type Buffer struct {
	bytes.Buffer

	enc *gob.Encoder
	dec *gob.Decoder
}

// WriteColumns serializes a batch of records into the buffer.
func (s *Buffer) WriteColumns(columns ...reflect.Value) error {
	if s.enc == nil {
		s.enc = gob.NewEncoder(&s.Buffer)
	}
	for i := range columns {
		if err := s.enc.EncodeValue(columns[i]); err != nil {
			return err
		}
	}
	return nil
}

// ReadColumns deserializes a batch of records into the provided column
// pointers. This interface is provided for testing.
func (s *Buffer) ReadColumns(columnptrs ...reflect.Value) error {
	if s.dec == nil {
		s.dec = gob.NewDecoder(&s.Buffer)
	}
	for i := range columnptrs {
		if err := s.dec.DecodeValue(columnptrs[i]); err != nil {
			return err
		}
	}
	return nil
}
