// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"context"

	"github.com/grailbio/bigslice/frame"
)

// FrameReader implements a Reader for a single Frame.
type frameReader struct {
	frame.Frame
}

func (f *frameReader) Read(ctx context.Context, out frame.Frame) (int, error) {
	n := out.Len()
	max := f.Frame.Len()
	if max < n {
		n = max
	}
	frame.Copy(out, f.Frame)
	f.Frame = f.Frame.Slice(n, max)
	if f.Frame.Len() == 0 {
		return n, EOF
	}
	return n, nil
}
