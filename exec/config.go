// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"github.com/grailbio/base/config"
	"github.com/grailbio/bigmachine"
)

func init() {
	config.Register("bigslice", func(inst *config.Instance) {
		sess := newSession()
		inst.IntVar(&sess.p, "parallelism", 1024, "allowable parallelism for the job")
		var system bigmachine.System
		inst.InstanceVar(&system, "system", "", "the bigmachine system used for job execution")
		inst.FloatVar(&sess.maxLoad, "max-load", DefaultMaxLoad, "per-machine maximum load")
		inst.Doc = "bigslice configures the bigslice runtime"
		inst.New = func() (interface{}, error) {
			if system != nil {
				sess.executor = newBigmachineExecutor(system)
			} else {
				sess.executor = newLocalExecutor()
			}
			sess.start()
			return sess, nil
		}
	})
}
