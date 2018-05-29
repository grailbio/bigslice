// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"encoding/gob"
	"io"
	"reflect"

	"github.com/grailbio/bigslice/frame"
)

// An Encoder manages transmission of slices through an underlying
// io.Writer. The stream of slice values represented by batches of
// rows stored in column-major order. Streams can be read by a
// Decoder.
type Encoder struct {
	enc *gob.Encoder
}

// NewEncoder returns a a new Encoder that streams slices into the
// provided writer.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{gob.NewEncoder(w)}
}

// Encode encodes a batch of rows and writes the encoded output into
// the encoder's writer.
func (e *Encoder) Encode(f frame.Frame) error {
	for i := range f {
		if err := e.enc.EncodeValue(f[i]); err != nil {
			return err
		}
	}
	return nil
}

// A Decoder manages the receipt of slices, as encoded by an Encoder,
// through an underlying io.Reader.
type Decoder struct {
	dec *gob.Decoder
}

// NewDecoder returns a new Decoder that reads an encoded input
// stream from the provided io.Reader.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{gob.NewDecoder(r)}
}

// Decode decodes a batch of columns from the encoded stream.
// The provided values should be pointers to slices. The slices are
// reallocated if they do not have enough room for the next batch
// of columns.
func (d *Decoder) Decode(columnptrs ...reflect.Value) error {
	for i := range columnptrs {
		if err := d.dec.DecodeValue(columnptrs[i]); err != nil {
			return err
		}
	}
	return nil
}
