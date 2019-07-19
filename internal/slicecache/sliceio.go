package slicecache

import (
	"context"

	"github.com/grailbio/base/backgroundcontext"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/bigslice/frame"
	"github.com/grailbio/bigslice/sliceio"
)

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
		f.Reader = sliceio.NewDecodingReader(f.file.Reader(backgroundcontext.Get()))
	}
	n, err := f.Reader.Read(ctx, frame)
	if err != nil {
		if err := f.file.Close(ctx); err != nil {
			log.Error.Printf("%s: close: %v", f.file.Name(), err)
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
		r.enc = sliceio.NewEncoder(r.file.Writer(backgroundcontext.Get()))
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
		r.file.Discard(backgroundcontext.Get())
	}
	return n, err
}

func newWritethroughReader(reader sliceio.Reader, path string) sliceio.Reader {
	return &writethroughReader{Reader: reader, path: path}
}
