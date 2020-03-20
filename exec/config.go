// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package exec

import (
	"github.com/grailbio/base/config"
	// Make eventer/cloudwatch instance available.
	_ "github.com/grailbio/base/eventlog/cloudwatch"
	"github.com/grailbio/base/status"
	"github.com/grailbio/bigmachine"
)

func init() {
	config.Register("bigslice", func(constr *config.Constructor) {
		sess := newSession()
		constr.IntVar(&sess.p, "parallelism", 1024, "allowable parallelism for the job")
		var system bigmachine.System
		constr.InstanceVar(&system, "system", "", "the bigmachine system used for job execution")
		constr.InstanceVar(&sess.eventer, "eventer", "", "the eventer used to log bigslice events")
		constr.FloatVar(&sess.maxLoad, "max-load", DefaultMaxLoad, "per-machine maximum load")
		constr.Doc = "bigslice configures the bigslice runtime"
		constr.New = func() (interface{}, error) {
			if system != nil {
				sess.executor = newBigmachineExecutor(system)
			} else {
				sess.executor = newLocalExecutor()
			}
			sess.status = new(status.Status)
			// Ensure bigmachine's group is displayed first.
			_ = sess.status.Group(BigmachineStatusGroup)
			_ = sess.status.Groups()

			sess.start()
			return sess, nil
		}
	})
}
