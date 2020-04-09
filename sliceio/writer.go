package sliceio

import (
	"github.com/grailbio/bigslice/frame"
)

type Writer interface {
	Write(f frame.Frame) error
}
