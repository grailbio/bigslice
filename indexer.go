// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import "github.com/grailbio/bigslice/frame"

// Indexer maintains an index of the first (key) column of a Frame.
type Indexer interface {
	// Index indexes the provided frame, depositing the index
	// for each row into the provided slice of indices.
	Index(frame.Frame, []int)
	Reindex(frame.Frame)
}
