package sliceio

import (
	"github.com/grailbio/bigslice/frame"
)

// Writer can write a frame to an underlying data stream.
type Writer interface {
	// Write writes f to an underlying data stream. It returns a non-nil error
	// if there is a problem writing, and f may have been partially written.
	Write(f frame.Frame) error
}
