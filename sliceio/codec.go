// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sliceio

import (
	"bufio"
	"context"
	"encoding/gob"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"reflect"
	"strings"
	"unsafe"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/bigslice/frame"
)

type session map[frame.Key]reflect.Value

func (s session) State(key frame.Key, state interface{}) (fresh bool) {
	v, ok := s[key]
	if !ok {
		typ := reflect.TypeOf(state).Elem()
		if typ.Kind() == reflect.Ptr {
			v = reflect.New(typ.Elem())
		} else {
			v = reflect.Zero(typ)
		}
		s[key] = v
	}
	reflect.Indirect(reflect.ValueOf(state)).Set(v)
	return !ok
}

type gobEncoder struct {
	*gob.Encoder
	session
}

func newGobEncoder(w io.Writer) *gobEncoder {
	return &gobEncoder{
		Encoder: gob.NewEncoder(w),
		session: make(session),
	}
}

type gobDecoder struct {
	*gob.Decoder
	session
}

func newGobDecoder(r io.Reader) *gobDecoder {
	return &gobDecoder{
		Decoder: gob.NewDecoder(r),
		session: make(session),
	}
}

// An Encoder manages transmission of slices through an underlying
// io.Writer. The stream of slice values represented by batches of
// rows stored in column-major order. Streams can be read by a
// Decoder.
type Encoder struct {
	enc *gobEncoder
	crc hash.Hash32
}

// NewEncoder returns a a new Encoder that streams slices into the
// provided writer.
func NewEncoder(w io.Writer) *Encoder {
	crc := crc32.NewIEEE()
	return &Encoder{
		enc: newGobEncoder(io.MultiWriter(w, crc)),
		crc: crc,
	}
}

func (e *Encoder) Write(f frame.Frame) error {
	return e.Encode(f)
}

// Encode encodes a batch of rows and writes the encoded output into
// the encoder's writer.
func (e *Encoder) Encode(f frame.Frame) error {
	e.crc.Reset()
	if err := e.enc.Encode(f.Len()); err != nil {
		return err
	}
	for col := 0; col < f.NumOut(); col++ {
		codec := f.HasCodec(col)
		if err := e.enc.Encode(codec); err != nil {
			return err
		}
		var err error
		if codec {
			err = f.Encode(col, e.enc)
		} else {
			err = e.enc.EncodeValue(f.Value(col))
		}
		if err != nil {
			// Here we're encoding a user-defined type. We pessimistically
			// attribute any errors that appear to come from gob as being
			// related to the inability to encode this user-defined type.
			if strings.HasPrefix(err.Error(), "gob: ") {
				err = errors.E(errors.Fatal, err)
			}
			return err
		}
	}
	return e.enc.Encode(e.crc.Sum32())
}

// DecodingReader provides a Reader on top of a gob stream
// encoded with batches of rows stored in column-major order.
type decodingReader struct {
	dec     *gobDecoder
	crc     hash.Hash32
	scratch frame.Frame
	buf     frame.Frame
	err     error
}

// NewDecodingReader returns a new Reader that decodes values from
// the provided stream. Since values are streamed in vectors, decoding
// reader must buffer values until they are read by the consumer.
func NewDecodingReader(r io.Reader) Reader {
	// We need to compute checksums by inspecting the underlying
	// bytestream, however, gob uses whether the reader implements
	// io.ByteReader as a proxy for whether the passed reader is
	// buffered. io.TeeReader does not implement io.ByteReader, and thus
	// gob.Decoder will insert a buffered reader leaving us without
	// means of synchronizing stream positions, required for
	// checksumming. Instead we fake an implementation of io.ByteReader,
	// and take over the responsibility of ensuring that IO is buffered.
	crc := crc32.NewIEEE()
	if _, ok := r.(io.ByteReader); !ok {
		r = bufio.NewReader(r)
	}
	r = io.TeeReader(r, crc)
	return &decodingReader{dec: newGobDecoder(readerByteReader{Reader: r}), crc: crc}
}

func (d *decodingReader) Read(ctx context.Context, f frame.Frame) (n int, err error) {
	if d.err != nil {
		return 0, d.err
	}
	for d.buf.Len() == 0 {
		d.crc.Reset()
		if d.err = d.dec.Decode(&n); d.err != nil {
			if d.err == io.EOF {
				d.err = EOF
			}
			return 0, d.err
		}
		// In most cases, we should be able to decode directly into the
		// provided frame without any buffering.
		if n <= f.Len() {
			if d.err = d.decode(f.Slice(0, n)); d.err != nil {
				return 0, d.err
			}
			return n, nil
		}
		// Otherwise we have to buffer the decoded frame.
		if d.scratch.IsZero() {
			d.scratch = frame.Make(f, n, n)
		} else {
			d.scratch = d.scratch.Ensure(n)
		}
		d.buf = d.scratch
		if d.err = d.decode(d.buf); d.err != nil {
			return 0, d.err
		}
	}
	n = frame.Copy(f, d.buf)
	d.buf = d.buf.Slice(n, d.buf.Len())
	return n, nil
}

// Decode a batch of column vectors into the provided frame.
// The frame is preallocated and is guaranteed to have enough
// space to decode all of the values.
func (d *decodingReader) decode(f frame.Frame) error {
	// Always zero memory before decoding with Gob, as it will reuse
	// existing memory. This can be dangerous; especially when
	// that involves user code.
	f.Zero()
	for col := 0; col < f.NumOut(); col++ {
		var codec bool
		if err := d.dec.Decode(&codec); err != nil {
			return err
		}
		if codec && !f.HasCodec(col) {
			return errors.New("column encoded with custom codec but no codec available on receipt")
		}
		if codec {
			if err := f.Decode(col, d.dec); err != nil {
				return err
			}
			continue
		}
		// Arrange for gob to decode directly into the frame's underlying
		// slice. We have to do some gymnastics to produce a pointer to
		// this value (which we'll anyway discard) so that gob can do its
		// job.
		sh := f.SliceHeader(col)
		var p []unsafe.Pointer
		ptr := unsafe.Pointer(&p)
		*(*reflect.SliceHeader)(ptr) = sh
		v := reflect.NewAt(reflect.SliceOf(f.Out(col)), ptr)
		err := d.dec.DecodeValue(v)
		if err != nil {
			if err == io.EOF {
				return EOF
			}
			return err
		}
		// This is guaranteed by gob, but it seems worthy of some defensive programming here.
		// It's also an extra check against the correctness of the codec.
		if (*(*reflect.SliceHeader)(ptr)).Data != sh.Data {
			panic("gob reallocated a slice")
		}
	}
	sum := d.crc.Sum32()
	var decoded uint32
	if err := d.dec.Decode(&decoded); err != nil {
		return err
	}
	if sum != decoded {
		return errors.E(errors.Integrity, fmt.Errorf("computed checksum %x but expected checksum %x", sum, decoded))
	}
	return nil
}

// readerByteReader is used to provide an (invalid) implementation of
// io.ByteReader to gob.Encoder. See comment in NewDecodingReader
// for details.
type readerByteReader struct {
	io.Reader
	io.ByteReader
}
