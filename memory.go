// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import "github.com/grailbio/bigslice/reclaimer"

// Memory is the memory reclaimer used for each bigslice process.
//
// TODO(marius): thread this into an execution context passed to tasks
// instead.
var Memory = reclaimer.New()
