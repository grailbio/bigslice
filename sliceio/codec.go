// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sliceio

import (
	"context"
	"encoding/gob"
	"io"
	"reflect"
	"unsafe"

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
	zeroers  []func(ptr uintptr, len int)
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
			d.zeroers = make([]func(uintptr, int), len(d.buf))
			for i := range d.buf {
				elem := f.Out(i)
				d.buf[i] = reflect.New(reflect.SliceOf(elem))
				if canZeroSliceOf(elem) {
					d.zeroers[i] = zero(elem)
				}
			}
		} else {
			for i, v := range d.buf {
				if zero := d.zeroers[i]; zero != nil {
					v = reflect.Indirect(v)
					zero(v.Pointer(), v.Len())
				} else {
					// We have not choice but to discard the whole slice
					// and start anew.
					d.buf[i] = reflect.New(reflect.SliceOf(f.Out(i)))
				}
			}

		}
		// Read the next batch.
		for i := 0; i < f.NumOut(); i++ {
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

func slice(ptr uintptr, len int) unsafe.Pointer {
	var h reflect.SliceHeader
	h.Data = ptr
	h.Len = len
	h.Cap = len
	return unsafe.Pointer(&h)
}

func zero(t reflect.Type) func(ptr uintptr, len int) {
	switch t.Kind() {
	case reflect.Ptr:
		return func(ptr uintptr, len int) {
			ps := *(*[]unsafe.Pointer)(slice(ptr, len))
			for i := range ps {
				ps[i] = unsafe.Pointer(uintptr(0))
			}
		}
	case reflect.Slice:
		return func(ptr uintptr, len int) {
			ps := *(*[]reflect.SliceHeader)(slice(ptr, len))
			for i := range ps {
				*(*unsafe.Pointer)(unsafe.Pointer(&ps[i].Data)) = unsafe.Pointer(uintptr(0))
				ps[i].Len = 0
				ps[i].Cap = 0
			}
		}
	case reflect.String:
		return func(ptr uintptr, len int) {
			strs := *(*[]string)(slice(ptr, len))
			for i := range strs {
				strs[i] = ""
			}
		}
	}
	// Value types:
	switch t.Size() {
	case 8:
		return func(ptr uintptr, len int) {
			vs := *(*[]int64)(slice(ptr, len))
			for i := range vs {
				vs[i] = 0
			}
		}
	case 4:
		return func(ptr uintptr, len int) {
			vs := *(*[]int32)(slice(ptr, len))
			for i := range vs {
				vs[i] = 0
			}
		}
	case 2:
		return func(ptr uintptr, len int) {
			vs := *(*[]int16)(slice(ptr, len))
			for i := range vs {
				vs[i] = 0
			}
		}
	case 1:
		return func(ptr uintptr, len int) {
			vs := *(*[]int8)(slice(ptr, len))
			for i := range vs {
				vs[i] = 0
			}
		}
	}

	// Slow case: reinterpret to []byte, and set that. Note that the
	// compiler should be able to optimize this too. In this case
	// it's always a value type, so this is always safe to do.
	size := t.Size()
	return func(ptr uintptr, len int) {
		var h reflect.SliceHeader
		h.Data = ptr
		h.Len = int(size) * len
		h.Cap = h.Len
		b := *(*[]byte)(unsafe.Pointer(&h))
		for i := range b {
			b[i] = 0
		}
	}
}

func canZeroSliceOf(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Ptr, reflect.String, reflect.Slice:
		return true
	default:
		return isValueType(t)
	}
}

func isValueType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Uintptr, reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return true
	case reflect.Array:
		return isValueType(t.Elem())
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if !isValueType(t.Field(i).Type) {
				return false
			}
		}
		return true
	}
	return false
}
