// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"context"
	"fmt"
	"reflect"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/sliceio"
)

type fileSlice struct {
	sliceOp
	Slice
	prefix string
}

func (f *fileSlice) Op() (string, string, int) { return f.sliceOp.Op() }
func (f *fileSlice) Combiner() *reflect.Value  { return nil }
func (f *fileSlice) NumDep() int               { return 0 }
func (f *fileSlice) Dep(i int) Dep             { panic("no deps") }

type fileReader struct {
	sliceio.Reader
	file file.File
	path string
}

func (f *fileReader) Read(ctx context.Context, frame frame.Frame) (int, error) {
	if f.file == nil {
		var err error
		f.file, err = file.Open(ctx, f.path)
		if err != nil {
			return 0, err
		}
		f.Reader = sliceio.NewDecodingReader(f.file.Reader(context.Background()))
	}
	n, err := f.Reader.Read(ctx, frame)
	if err != nil {
		if err := f.file.Close(ctx); err != nil {
			log.Error.Printf("%s: close: %v", f.file.Name(), err)
		}
	}
	return n, err
}

func (f *fileSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader {
	return &fileReader{path: shardPath(f.prefix, shard, f.NumShard())}
}

type writethroughSlice struct {
	sliceOp
	Slice
	prefix string
}

func (w *writethroughSlice) Op() (string, string, int) { return w.sliceOp.Op() }

type writethroughReader struct {
	sliceio.Reader
	path string
	file file.File
	enc  *sliceio.Encoder
}

func (r *writethroughReader) Read(ctx context.Context, frame frame.Frame) (int, error) {
	if r.file == nil {
		var err error
		r.file, err = file.Create(ctx, r.path)
		if err != nil {
			return 0, err
		}
		// Ideally we'd use the underlying context for each op here,
		// but the way encoder is set up, we can't (understandably)
		// pass a new writer for each encode.
		r.enc = sliceio.NewEncoder(r.file.Writer(context.Background()))
	}
	n, err := r.Reader.Read(ctx, frame)
	if err == nil || err == sliceio.EOF {
		if err := r.enc.Encode(frame.Slice(0, n)); err != nil {
			return n, err
		}
		if err == sliceio.EOF {
			if err := r.file.Close(ctx); err != nil {
				return n, err
			}
		}
	} else {
		r.file.Discard(context.Background())
	}
	return n, err
}

// IsWriteThrough is used for testing.
func (w *writethroughSlice) IsWriteThrough() {}

func (w *writethroughSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader {
	return &writethroughReader{
		Reader: w.Slice.Reader(shard, deps),
		path:   shardPath(w.prefix, shard, w.NumShard()),
	}
}

// Cache caches the output of a slice to the given file prefix.
// Cached data are stored as "prefix-nnnn-of-mmmm" for shards nnnn of
// mmmm. When the slice is computed, each shard is encoded and
// written to a separate file with this prefix. If all shards exist,
// then Cache shortcuts computation and instead reads directly from
// the previously computed output. The user must guarantee cache
// consistency: if the cache is could be invalid (e.g., because of
// code changes), the user is responsible for removing existing
// cached files, or picking a different prefix that correctly
// represents the operation to be cached.
//
// Cache uses GRAIL's file library, so prefix may refer to URLs to a
// distributed object store such as S3.
func Cache(ctx context.Context, slice Slice, prefix string) (Slice, error) {
	m := slice.NumShard()
	_, err := file.Stat(ctx, shardPath(prefix, 0, m))
	if err == nil {
		// Make sure the remaining shards are also there.
		err = traverse.Each(m-1, func(i int) error {
			_, err := file.Stat(ctx, shardPath(prefix, i+1, m))
			return err
		})
	}
	if err == nil {
		return &fileSlice{
			sliceOp: makeSliceOp(fmt.Sprintf("file(%s)", prefix)),
			Slice:   slice,
			prefix:  prefix,
		}, nil
	}
	if !errors.Is(errors.NotExist, err) {
		return nil, err
	}
	return &writethroughSlice{makeSliceOp("writethrough"), slice, prefix}, nil
}

func shardPath(prefix string, n, m int) string {
	return fmt.Sprintf("%s-%04d-of-%04d", prefix, n, m)
}
