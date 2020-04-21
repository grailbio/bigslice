// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sliceio

import (
	"context"

	"github.com/grailbio/bigslice/frame"
)

// Writer can write a frame to an underlying data stream.
type Writer interface {
	// Write writes f to an underlying data stream. It returns a non-nil error
	// if there is a problem writing, and f may have been partially written.
	Write(ctx context.Context, f frame.Frame) error
}
