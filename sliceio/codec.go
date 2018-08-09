// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sliceio

import (
	"context"
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
	for i := 0; i < f.NumOut(); i++ {
		if err := e.enc.EncodeValue(f.Value(i)); err != nil {
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

// DecodingReader provides a Reader on top of a gob stream
// encoded with batches of rows stored in column-major order.
type decodingReader struct {
	dec      *gob.Decoder
	off, len int
	buf      []reflect.Value // points to slices
	err      error
}

// NewDecodingReader returns a new Reader that decodes values from
// the provided stream. Since values are streamed in vectors, decoding
// reader must buffer values until they are read by the consumer.
func NewDecodingReader(r io.Reader) Reader {
	return &decodingReader{dec: gob.NewDecoder(r)}
}

func (d *decodingReader) Read(ctx context.Context, f frame.Frame) (n int, err error) {
	if d.err != nil {
		return 0, d.err
	}
	for d.off == d.len {
		if d.buf == nil {
			d.buf = make([]reflect.Value, f.NumOut())
			for i := range d.buf {
				if f.Out(i).Kind() != reflect.Slice {
					d.buf[i] = reflect.New(reflect.SliceOf(f.Out(i)))
				}
			}
		}
		// Read the next batch.
		for i := 0; i < f.NumOut(); i++ {
			// Reset slice columns that contain slices since these may
			// be reused by Gob across decodes.
			if d.off+n == d.len {
				if t := f.Out(i); t.Kind() == reflect.Slice {
					d.buf[i] = reflect.New(reflect.SliceOf(t))
				}
			}
			if d.err = d.dec.DecodeValue(d.buf[i]); d.err != nil {
				if d.err == io.EOF {
					d.err = EOF
				}
				return 0, d.err
			}
		}
		d.off = 0
		d.len = d.buf[0].Elem().Len()
	}
	if d.len > 0 {
		for i := 0; i < f.NumOut(); i++ {
			n = reflect.Copy(f.Value(i), d.buf[i].Elem().Slice(d.off, d.len))
		}
		d.off += n
	}
	return n, nil
}
