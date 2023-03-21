package slicecache

import (
	"context"
	"io"

	"github.com/grailbio/base/backgroundcontext"
	"github.com/grailbio/base/compress/zstd"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/sliceio"
)

type fileReader struct {
	sliceio.Reader
	file file.File
	zr   io.ReadCloser
	path string
}

func (f *fileReader) Read(ctx context.Context, frame frame.Frame) (int, error) {
	if f.file == nil {
		var err error
		f.file, err = file.Open(ctx, f.path)
		if err != nil {
			return 0, err
		}
		f.zr, err = zstd.NewReader(f.file.Reader(backgroundcontext.Get()))
		if err != nil {
			return 0, err
		}
		f.Reader = sliceio.NewDecodingReader(f.zr)
	}
	n, err := f.Reader.Read(ctx, frame)
	if err != nil {
		if closeErr := f.zr.Close(); closeErr != nil {
			log.Error.Printf("%s: close zstd reader: %v", f.file.Name(), closeErr)
		}
		if closeErr := f.file.Close(ctx); closeErr != nil {
			log.Error.Printf("%s: close file: %v", f.file.Name(), closeErr)
		}
	}
	return n, err
}

func newFileReader(path string) sliceio.Reader {
	return &fileReader{path: path}
}

type writethroughReader struct {
	sliceio.Reader
	path string
	file file.File
	zw   io.WriteCloser
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
		r.zw, err = zstd.NewWriter(r.file.Writer(backgroundcontext.Get()))
		if err != nil {
			return 0, err
		}
		r.enc = sliceio.NewEncodingWriter(r.zw)
	}
	n, err := r.Reader.Read(ctx, frame)
	if err == nil || err == sliceio.EOF {
		if writeErr := r.enc.Write(ctx, frame.Slice(0, n)); writeErr != nil {
			return n, writeErr
		}
		if err == sliceio.EOF {
			closeErr := r.zw.Close()
			errors.CleanUpCtx(ctx, r.file.Close, &closeErr)
			if closeErr != nil {
				return n, closeErr
			}
		}
	} else {
		r.file.Discard(backgroundcontext.Get())
	}
	return n, err
}

func newWritethroughReader(reader sliceio.Reader, path string) sliceio.Reader {
	return &writethroughReader{Reader: reader, path: path}
}
