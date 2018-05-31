// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Urls is a bigslice demo program that uses the GDELT public data
// set aggregate counts by domain names mentioned in news event
// reports.
package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	_ "net/http/pprof"
	"net/url"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/file/s3file"
	"github.com/grailbio/base/log"
	"github.com/grailbio/bigmachine/ec2system"
	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/slicecmd"
	"github.com/grailbio/bigslice/sliceio"
)

func init() {
	file.RegisterImplementation("s3", s3file.NewImplementation(
		s3file.NewDefaultProvider(session.Options{})))
	s3file.SetBucketRegion("gdelt-open-data", "us-east-1")
}

var domainCounts = bigslice.Func(func(files []string, prefix string) bigslice.Slice {
	ctx := context.Background()
	type state struct {
		reader *csv.Reader
		file   file.File
	}
	slice := bigslice.ReaderFunc(len(files), func(shard int, state *state, urls []string) (n int, err error) {
		if state.file == nil {
			log.Printf("reading file %s", files[shard])
			state.file, err = file.Open(ctx, files[shard])
			if err != nil {
				return
			}
			state.reader = csv.NewReader(state.file.Reader(ctx))
			state.reader.Comma = '	'
		}
		for i := range urls {
			fields, err := state.reader.Read()
			if err == io.EOF {
				return i, sliceio.EOF
			}
			if err != nil {
				return i, err
			}
			urls[i] = fields[60]
		}
		return len(urls), nil
	})
	// Extract the domain.
	slice = bigslice.Map(slice, func(rawurl string) (domain string, count int) {
		u, err := url.Parse(rawurl)
		if err != nil {
			domain = "<unknown>"
			count = 1
			return
		}
		domain = u.Host
		count = 1
		return
	})
	slice = bigslice.Fold(slice, func(a, e int) int { return a + e })
	slice = bigslice.Scan(slice, func(shard int, scan *sliceio.Scanner) error {
		f, err := file.Create(ctx, fmt.Sprintf("%s-%03d-of-%03d", prefix, shard, len(files)))
		if err != nil {
			return err
		}
		w := bufio.NewWriter(f.Writer(ctx))
		var (
			domain string
			count  int
		)
		for scan.Scan(context.Background(), &domain, &count) {
			fmt.Fprintf(w, "%s\t%d\n", domain, count)
		}
		w.Flush()
		f.Close(ctx)
		return scan.Err()
	})
	return slice
})

func main() {
	var (
		n   = flag.Int("n", 1000, "number of shards to process")
		out = flag.String("out", "", "output path")
	)
	slicecmd.RegisterSystem("ec2", &ec2system.System{
		InstanceType: "r3.8xlarge",
	})
	slicecmd.Main(func(sess *bigslice.Session, args []string) error {
		if *out == "" {
			return errors.New("missing flag -out")
		}
		ctx := context.Background()
		var paths []string
		url := "s3://gdelt-open-data/v2/events"
		lst := file.List(ctx, url)
		for lst.Scan() {
			if strings.HasSuffix(lst.Path(), ".csv") {
				paths = append(paths, lst.Path())
			}
		}
		if err := lst.Err(); err != nil {
			log.Fatal(err)
		}
		sort.Strings(paths)
		if len(paths) > *n {
			paths = paths[:*n]
		}
		log.Printf("computing %d paths", len(paths))
		if err := sess.Run(ctx, domainCounts, paths, *out); err != nil {
			return err
		}
		return nil
	})
}
