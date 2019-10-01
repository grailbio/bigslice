package defaultsize

import "flag"

var (
	// Chunk is the default chunk size (number of rows), configured by flag.
	// It's part of a temporary solution; see go/bigslice-chunk-thread.
	Chunk int
	// SortCanary is the default sort canary size (number of rows), configured by flag.
	// It's part of a temporary solution; see go/bigslice-chunk-thread.
	SortCanary int
)

func init() {
	flag.IntVar(&Chunk, "bigslice-internal-default-chunk-rows", 128,
		"Default vector size to use internally. Temporary; see go/bigslice-chunk-thread")
	flag.IntVar(&SortCanary, "bigslice-internal-default-sort-canary-rows", 1<<8,
		"Default sort canary size to use internally. Temporary; see go/bigslice-chunk-thread")
}
