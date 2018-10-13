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
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sort"
	"strings"
	"sync" // Pprof is included to be exposed on the local diagnostic web server.

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/status"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigslice/exec"
	"github.com/grailbio/bigslice/sliceflags"
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

// Main is a convenient entry point for a slicecmd. Main does not return;
// it should be called after other initialization is performed. Main
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
// Integration with other command line processing is best achieved using
// the sliceflags package and Init and DisplayStatus functions.
func Main(main func(sess *exec.Session, args []string) error) {
	var fl sliceflags.Flags
	sliceflags.RegisterFlags(flag.CommandLine, &fl, "")
	log.AddFlags()
	flag.Parse()
	sess, err := Init(fl)
	if err != nil {
		log.Fatal(err)
	}
	if err := main(sess, flag.Args()); err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}

// Init initializes bigslice according to the supplied flags.
func Init(bf sliceflags.Flags) (*exec.Session, error) {
	if bf.SystemHelp {
		providers, profiles := sliceflags.ProvidersAndProfiles()
		sort.Strings(providers)
		wr := bf.Output()
		str := []string{}
		fmt.Fprintf(wr, "%s\n\n", sliceflags.SystemHelpLong)
		fmt.Fprintf(wr, "The available providers are: %v\n",
			strings.Join(providers, ", "))
		for k, v := range profiles {
			str = append(str, fmt.Sprintf("%v is shorthand for: %v\n", k, v))
		}
		sort.Strings(str)
		for _, s := range str {
			wr.Write([]byte(s))
		}
		os.Exit(0)
	}
	options, err := bf.ExecOptions()
	if err != nil {
		return nil, err
	}
	sess := exec.Start(options...)
	DisplayStatus(bf, sess)
	return sess, nil
}

// DisplayStatus arranges for the bigslice execution status to be
// displayed on the console and/or a web page depending on the flags
// specified on the command line. The web page is hosted /debug/status
// and http.DefaultServeMux.
func DisplayStatus(bf sliceflags.Flags, sess *exec.Session) {
	if bf.ConsoleStatus {
		var console status.Reporter
		go console.Go(os.Stdout, sess.Status())
	}
	if len(bf.HTTPAddress.Address) > 0 {
		sess.HandleDebug(http.DefaultServeMux)
		http.Handle("/debug/status", status.Handler(sess.Status()))
		go func() {
			log.Printf("HTTP Status at: %v\n", bf.HTTPAddress)
			err := http.ListenAndServe(bf.HTTPAddress.Address, nil)
			if err != nil {
				log.Error.Printf("Failed to start HTTP at: %v: %v\n", bf.HTTPAddress, err)
			}
		}()
	}
}
