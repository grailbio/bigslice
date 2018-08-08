// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package slicecmd provides utilities for implementing
// bigslice-based command line tools. The main entry point,
// slicecmd.Main, configures bigslice according to a common set of
// flags, and then invokes the user's driver code.
//
// A slicecmd tool follows this form:
//
//	func main() {
//		var (
//			applicationFlag1 = flag.Int(...)
//			applicationFlag2 = ...
//		)
//		// Register systems. This can be done in package initialization as well.
//		slicecmd.RegisterSystem("ec2", &ec2system.System{
//			InstanceType: "m4.16xlarge",
//		})
//		slicecmd.RegisterSystem("ec2test", &ec2system.System{
//			InstanceType: "t2.nano",
//		})
//		slicecmd.Main(func(sess *bigslice.Session, args []string) error) {
//			ctx := context.Background()
//			if err := sess.Run(ctx, MyComputation); err != nil {
//				return err
//			}
//			// Do something else...
//			return nil
//		}
//	}
package slicecmd

import (
	"flag"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync" // Pprof is included to be exposed on the local diagnostic web server.

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/status"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigslice/exec"
)

var (
	mu      sync.Mutex
	systems = map[string]bigmachine.System{}
)

// RegisterSystem registers a bigmachine system for use in this
// slicecmd. The named registration is recalled via the -system
// flag.
func RegisterSystem(name string, system bigmachine.System) {
	mu.Lock()
	defer mu.Unlock()
	if systems[name] != nil {
		log.Panicf("system %s is already registered", name)
	}
	systems[name] = system
}

// Main is the entry point for a slicecmd. Main does not return; it
// should be called after other initialization is performed. Main
// parses (global) flags, and configures bigslice accordingly. Main
// then invokes the provided func with a bigslice session which can
// be used to run bigslice computations. Main also passes the
// unparsed arguments.
//
// Main starts a diagnostic web server (default address :3333), using
// http.DefaultServeMux, which includes pprof handlers as well as
// bigmachine's aggregated pprof handlers.
//
// Main terminates the program after the user func returns. If it
// returns with an error, it is reported and the process exits with
// code 1, otherwise it exits successfully.
//
// TODO(marius): abstract this into a struct so that it can be more
// easily be used with command line utilities like Vanadium's.
func Main(main func(sess *exec.Session, args []string) error) {
	var (
		// TODO(marius): consider letting the system flag itself take parameters, e.g.,
		// 	-system ec2,r3.8xlarge
		system = flag.String("system", "", "the bigmachine system on which to run, defaults to local")
		addr   = flag.String("addr", ":3333", "address of local diagnostic web server")
		// TODO(marius): this should eventually be maximum parallelism, once the underlying
		// executors are dynamic.
		p       = flag.Int("p", 0, "target parallelism; inferred if 0")
		maxLoad = flag.Float64("maxload", exec.DefaultMaxLoad, "maximum machine load")
	)
	log.AddFlags()
	flag.Parse()
	var options []exec.Option
	switch *system {
	case "":
		// Use in-process evaluation instead of bigmachine out-of-process
		// by default. The latter is mostly useful for debugging bigmachine
		// issues.
		options = append(options, exec.Local)
	case "local":
		options = append(options, exec.Bigmachine(bigmachine.Local))
	default:
		impl := systems[*system]
		if impl == nil {
			log.Fatalf("system %s not found", *system)
		}
		options = append(options, exec.Bigmachine(impl))
		// TODO(marius): get rid of this requirement once the bigmachine executor is dynamic.
		if *p == 0 {
			log.Fatalf("target parallelism (-p) must be specified for system %s", *system)
		}
	}
	if *p > 0 {
		options = append(options, exec.Parallelism(*p))
	}
	options = append(options, exec.MaxLoad(*maxLoad))
	top := new(status.Status)
	options = append(options, exec.Status(top))
	sess := exec.Start(options...)
	sess.HandleDebug(http.DefaultServeMux)
	http.Handle("/debug/status", status.Handler(top))
	go func() {
		log.Printf("http.ListenAndServe %s: %v", *addr, http.ListenAndServe(*addr, nil))
	}()
	err := main(sess, flag.Args())
	if err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}
