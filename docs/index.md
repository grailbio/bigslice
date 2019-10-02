---
title: Bigslice
layout: default
---

<a href="https://github.com/grailbio/bigslice/" class="github-corner" aria-label="View source on GitHub"><svg width="80" height="80" viewBox="0 0 250 250" style="fill:#800080; color:#fff; position: absolute; top: 0; border: 0; right: 0;" aria-hidden="true"><path d="M0,0 L115,115 L130,115 L142,142 L250,250 L250,0 Z"></path><path d="M128.3,109.0 C113.8,99.7 119.0,89.6 119.0,89.6 C122.0,82.7 120.5,78.6 120.5,78.6 C119.2,72.0 123.4,76.3 123.4,76.3 C127.3,80.9 125.5,87.3 125.5,87.3 C122.9,97.6 130.6,101.9 134.4,103.2" fill="currentColor" style="transform-origin: 130px 106px;" class="octo-arm"></path><path d="M115.0,115.0 C114.9,115.1 118.7,116.5 119.8,115.4 L133.7,101.6 C136.9,99.2 139.9,98.4 142.2,98.6 C133.8,88.0 127.5,74.4 143.8,58.0 C148.5,53.4 154.0,51.2 159.7,51.0 C160.3,49.4 163.2,43.6 171.4,40.1 C171.4,40.1 176.1,42.5 178.8,56.2 C183.1,58.6 187.2,61.8 190.9,65.4 C194.5,69.0 197.7,73.2 200.1,77.6 C213.8,80.2 216.3,84.9 216.3,84.9 C212.7,93.1 206.9,96.0 205.4,96.6 C205.1,102.4 203.0,107.8 198.3,112.5 C181.9,128.9 168.3,122.5 157.7,114.1 C157.9,116.9 156.7,120.9 152.7,124.9 L141.0,136.5 C139.8,137.7 141.6,141.9 141.8,141.8 Z" fill="currentColor" class="octo-body"></path></svg></a><style>.github-corner:hover .octo-arm{animation:octocat-wave 560ms ease-in-out}@keyframes octocat-wave{0%,100%{transform:rotate(0)}20%,60%{transform:rotate(-25deg)}40%,80%{transform:rotate(10deg)}}@media (max-width:500px){.github-corner:hover .octo-arm{animation:none}.github-corner .octo-arm{animation:octocat-wave 560ms ease-in-out}}</style>

# Bigslice

<img src="bigslice.png" alt="Bigslice gopher" height="200"/>

Bigslice is a system for
<i>fast</i>, large-scale,
serverless data processing
using [Go](https://golang.org).

Bigslice provides an API that lets users express their
computation with a handful of familiar data transformation
primitives such as
<span class="small">map</span>,
<span class="small">filter</span>,
<span class="small">reduce</span>, and
<span class="small">join</span>.
When the program is run,
Bigslice creates an ad hoc cluster on a cloud computing provider
and transparently distributes the computation among the nodes
in the cluster.

Bigslice is similar to data processing systems like
[Apache Spark](https://spark.apache.org/)
and [FlumeJava](https://ai.google/research/pubs/pub35650),
but with different aims:

* *Bigslice is built for Go.* Bigslice is used as an ordinary Go package,
  users use their existing Go code, and Bigslice binaries are compiled
  like ordinary Go binaries.
* *Bigslice is serverless.* Requiring nothing more than cloud credentials,
  Bigslice will have you processing large datasets in no time, without the use
  of any other external infrastructure.
* *Bigslice is simple and transparent.* Bigslice programs are regular
  Go programs, providing users with a familiar environment and tools.
  A Bigslice program can be run on a single node like any other program,
  but it is also capable of transparently distributing itself across an
  ad hoc cluster, managed entirely by the program itself.

<div class="links">
<a href="https://github.com/grailbio/bigslice">GitHub project</a> &middot;
<a href="https://godoc.org/github.com/grailbio/bigslice">API documentation</a> &middot;
<a href="https://github.com/grailbio/bigslice/issues">issue tracker</a> &middot;
<a href="implementation.html">about the implementation</a> &middot;
<a href="https://godoc.org/github.com/grailbio/bigmachine">bigmachine</a>
</div>

# Getting started

To get a sense of what writing and running Bigslice programs is like,
we'll implement a simple word counter,
computing the frequencies of words used in Shakespeare's combined works.
Of course, it would be silly to use Bigslice for a corpus this small,
but it serves to illustrate the various features of Bigslice, and,
because the data are small, we enjoy a very quick feedback loop.

First, we'll install the bigslice command.
This command is not strictly needed to use Bigslice,
but it helps to make common tasks and setup easy and simple.
The bigslice command helps us to build and run Bigslice programs, and,
as we'll see later,
also perform the necessary setup for your cloud provider.

```
GO111MODULE=on go get github.com/grailbio/bigslice/cmd/bigslice@latest
```

Now, we write a Go file that implements our word count.
Don't worry too much about the details for now;
we'll go over this later.

```
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"

	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/sliceconfig"
)

var wordCount = bigslice.Func(func(url string) bigslice.Slice {
	slice := bigslice.ScanReader(8, func() (io.ReadCloser, error) {
		resp, err := http.Get(url)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("get %v: %v", url, resp.Status)
		}
		return resp.Body, nil
	})
	slice = bigslice.Flatmap(slice, strings.Fields)
	slice = bigslice.Map(slice, func(token string) (string, int) {
		return token, 1
	})
	slice = bigslice.Reduce(slice, func(a, e int) int {
		return a + e
	})
	return slice
})


const shakespeare = "https://ocw.mit.edu/ans7870/6"+
	"/6.006/s08/lecturenotes/files/t8.shakespeare.txt"


func main() {
	sess, shutdown := sliceconfig.Parse()
	defer shutdown()

	ctx := context.Background()
	tokens, err := sess.Run(ctx, wordCount, shakespeare)
	if err != nil {
		log.Fatal(err)
	}
	scan := tokens.Scan(ctx)
	type counted struct {
		token string
		count int
	}
	var (
		token  string
		count  int
		counts []counted
	)
	for scan.Scan(ctx, &token, &count) {
		counts = append(counts, counted{token, count})
	}
	if err := scan.Err(); err != nil {
		log.Fatal(err)
	}

	sort.Slice(counts, func(i, j int) bool {
		return counts[i].count > counts[j].count
	})
	if len(counts) > 10 {
		counts = counts[:10]
	}
	for _, count := range counts {
		fmt.Println(count.token, count.count)
	}
}
```

Now that we have our computation,
we can run it with the bigslice tool.
In order to test it out,
we'll run it in local mode.


```
$ GO111MODULE=on bigslice run shake.go -local
the 23242
I 19540
and 18297
to 15623
of 15544
a 12532
my 10824
in 9576
you 9081
is 7851
$
```

Let's run the same thing on EC2.
First we run bigslice setup-ec2
to configure the required EC2 security group,
and then run shake.go without the -local flag:

```
$ bigslice setup-ec2
bigslice: no existing bigslice security group found; creating new
bigslice: found default VPC vpc-2c860354
bigslice: authorizing ingress traffic for security group sg-0d4f69daa025633f9
bigslice: tagging security group sg-0d4f69daa025633f9
bigslice: created security group sg-0d4f69daa025633f9
bigslice: set up new security group sg-0d4f69daa025633f9
bigslice: wrote configuration to /Users/marius/.bigslice/config
$ GO111MODULE=on bigslice run shake.go
2019/09/26 07:43:33 http: serve :3333
2019/09/26 07:43:33 slicemachine: 0 machines (0 procs); 1 machines pending (3 procs)
2019/09/26 07:43:33 slicemachine: 0 machines (0 procs); 2 machines pending (6 procs)
2019/09/26 07:43:33 slicemachine: 0 machines (0 procs); 3 machines pending (9 procs)
the 23242
I 19540
and 18297
to 15623
of 15544
a 12532
my 10824
in 9576
you 9081
is 7851
$
```

Bigslice launched an ad hoc cluster of 3 nodes
in order to perform the computation;
as soon as the job finished,
the cluster tears itself down automatically.

While a job is running,
Bigslice exports its status via built-in http server.
For example,
in this case we can use curl to inspect
the current status of the job using
[curl](https://curl.haxx.se/).

```
$ curl :3333/debug/status
bigmachine:
  :  waiting for machine to boot  33s
  :  waiting for machine to boot  19s
  :  waiting for machine to boot  19s
run /Users/marius/shake.go:41 [1] slices: count: 4
  reader@/Users/marius/shake.go:17:   tasks idle/running/done: 8/0/0  1m6s
  flatmap@/Users/marius/shake.go:27:  tasks idle/running/done: 8/0/0  1m6s
  map@/Users/marius/shake.go:28:      tasks idle/running/done: 8/0/0  1m6s
  reduce@/Users/marius/shake.go:29:   tasks idle/running/done: 8/0/0  1m6s
run /Users/marius/shake.go:41 [1] tasks: tasks: runnable: 8
  inv1_reader_flatmap_map@8:0(1):  waiting for a machine  1m6s
  inv1_reader_flatmap_map@8:1(1):  waiting for a machine  1m6s
  inv1_reader_flatmap_map@8:2(1):  waiting for a machine  1m6s
  inv1_reader_flatmap_map@8:3(1):  waiting for a machine  1m6s
  inv1_reader_flatmap_map@8:4(1):  waiting for a machine  1m6s
  inv1_reader_flatmap_map@8:5(1):  waiting for a machine  1m6s
  inv1_reader_flatmap_map@8:6(1):  waiting for a machine  1m6s
  inv1_reader_flatmap_map@8:7(1):  waiting for a machine  1m6s
```

The first clause tells us there are 3 machines
(in this case, EC2 instances)
waiting to boot.
The second clause shows the status tasks associated with
the slice operations at the given source lines, above.
In this case, every task is idle
because there are not yet any machines on which to run them.
The final clause shows the physical tasks
that require scheduling by Bigslice.

A little later,
we query the status again.

```
$ curl :3333/debug/status
bigmachine:
  :                  waiting for machine to boot                                                 36s
  https://ec2-.../:  mem 117.0MiB/15.2GiB disk 62.4MiB/7.6GiB load 0.4/0.1/0.0 counters tasks:4  22s
  https://ec2-.../:  mem 120.8MiB/15.2GiB disk 62.4MiB/7.6GiB load 0.2/0.1/0.0 counters tasks:4  22s
run /Users/marius/shake.go:41 [1] slices: count: 4
  reader@/Users/marius/shake.go:17:   tasks idle/running/done: 0/8/0  1m8s
  flatmap@/Users/marius/shake.go:27:  tasks idle/running/done: 0/8/0  1m8s
  map@/Users/marius/shake.go:28:      tasks idle/running/done: 0/8/0  1m8s
  reduce@/Users/marius/shake.go:29:   tasks idle/running/done: 8/0/0  1m8s
run /Users/marius/shake.go:41 [1] tasks: tasks: runnable: 8
  inv1_reader_flatmap_map@8:0(1):  https://ec2-18-236-204-88.../  1m8s
  inv1_reader_flatmap_map@8:1(1):  https://ec2-34-221-236-36.../  1m8s
  inv1_reader_flatmap_map@8:2(1):  https://ec2-18-236-204-88.../  1m8s
  inv1_reader_flatmap_map@8:3(1):  https://ec2-18-236-204-88.../  1m8s
  inv1_reader_flatmap_map@8:4(1):  https://ec2-34-221-236-36.../  1m8s
  inv1_reader_flatmap_map@8:5(1):  https://ec2-18-236-204-88.../  1m8s
  inv1_reader_flatmap_map@8:6(1):  https://ec2-34-221-236-36.../  1m8s
  inv1_reader_flatmap_map@8:7(1):  https://ec2-34-221-236-36.../  1m8s
```

This time,
we see that the computation is in progress.
Two out of 3 machines in the cluster have booted;
the first clause shows the resource utilization of these machines.
Next, we see that all but the reduce steps are currently running.
This is because <span class="small">reduce</span> requires
a shuffle step, and so depends upon the completion
of its antecedent tasks.
Finally,
the last clause shows the individual tasks and their runtimes.

Note that there is not a one-to-one correspondence
between the high-level slice operations in the second clause
with the tasks in the third.
This is for two reasons.
First, Bigslice *pipelines* operations when it can.
The tasks names give a hint at this:
the currently running tasks each correspond to
a pipeline of reader, flatmap, and map.
Second, the underlying data are split into individual *shards*,
each task handling a subset of the data.
This is how Bigslice parallelizes computation.

Let's walk through the code.

```
var wordCount = bigslice.Func(func(url string) bigslice.Slice {
	slice := bigslice.ScanReader(8, func() (io.ReadCloser, error) {  // (1)
		resp, err := http.Get(url)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("get %v: %v", url, resp.Status)
		}
		return resp.Body, nil
	})
	slice = bigslice.Flatmap(slice, strings.Fields)                  // (2)
	slice = bigslice.Map(slice, func(token string) (string, int) {   // (3)
		return token, 1
	})
	slice = bigslice.Reduce(slice, func(a, e int) int {              // (4)
		return a + e
	})
	return slice
})
```

Every Bigslice operation must be implemented by a `bigslice.Func`.
A `bigslice.Func` is a way to wrap an ordinary Go func value so that
it can be invoked by Bigslice. `bigslice.Func`s must return values of
type `bigslice.Slice`, which describe the actual operation to be done.
This may seem like an indirect way of doing things,
but it provides two big advantages:
First, by using `bigslice.Func`,
Bigslice can name and run Go code remotely without
performing on-demand compilation or shipping a whole toolchain.
Second, by expressing data processing tasks in terms of
`bigslice.Slice` values,
Bigslice is free to partition, distribute, and retry bits of
the operations in ways not specified by the user.

In our example,
we define a wordcount operation as a function of a URL.
The first operation is a [ScanReader](https://godoc.org/github.com/grailbio/bigslice#ScanReader) (1),
which takes an io.Reader and returns a `bigslice.Slice`
that represents the scanned lines from that io.Reader.
The type of this `bigslice.Slice` value is schematically `bigslice.Slice<string>`.
While we do not have generics in Go,
a `bigslice.Slice` can nevertheless represent a container of any underlying type;
Bigslice performs runtime type checking to make sure that incompatible
`bigslice.Slice` operators are not combined together.

We then take the output and tokenize it with
[Flatmap](https://godoc.org/github.com/grailbio/bigslice#ScanReader) (2),
which takes each input string (line) and outputs a list of strings (for each token).
The resulting `bigslice.Slice` represents
all the tokens in the corpus.
Note that since `strings.Fields` already has the correct signature,
we did not need to wrap it with our own `func`.

Next,
we map each token found in the corpus to two columns:
the first column is the token itself,
and the second column is the integer value 1,
representing the count of that token.
`bigslice.Slice` values may contain multiple columns of values;
they are analagous to tuples in other programming languages.
The type of the returned `bigslice.Slice` is schematically
`bigslice.Slice<string, int>`.

Finally,
we apply `bigslice.Reduce` (4) to the slice of token counts.
The reduce operation aggregates the values for each unique
value of the first column (the "key").
In this case, we just want to add them together
in order to produce the final count for each unique token.

That's the end of our `bigslice.Func`.
Let's look at our `main` function.

```
func main() {
	sess, shutdown := sliceconfig.Parse()                // (1)
	defer shutdown()

	ctx := context.Background()
	tokens, err := sess.Run(ctx, wordCount, shakespeare) // (2)
	if err != nil {
		log.Fatal(err)
	}
	scan := tokens.Scan(ctx)                             // (3)
	type counted struct {
		token string
		count int
	}
	var (
		token  string
		count  int
		counts []counted
	)
	for scan.Scan(ctx, &token, &count) {                 // (4)
		counts = append(counts, counted{token, count})
	}
	if err := scan.Err(); err != nil {                   // (5)
		log.Fatal(err)
	}

	sort.Slice(counts, func(i, j int) bool {
		return counts[i].count > counts[j].count
	})
	if len(counts) > 10 {
		counts = counts[:10]
	}
	for _, count := range counts {
		fmt.Println(count.token, count.count)
	}
}
```

First, notice that our program is an ordinary Go program,
with an ordinary entry point.
While Bigslice offers low-level APIs to set up a Bigslice session,
the [sliceconfig](https://godoc.org/github.com/grailbio/bigslice/sliceconfig)
package offers a convenient way to set up such
a session by reading the configuration in `$HOME/.bigslice/config`,
which in our case was written by `bigslice setup-ec2`.
`sliceconfig.Parse` reads the Bigslice configuration,
parses command line flags,
and then sets up a session accordingly.
The Bigslice session is required in order to invoke `bigslice.Func`s.

That is exactly what we do next (2):
we invoke the `wordCount` `bigslice.Func` with the
Shakespeare corpus URL as an argument.
The returned value represents the results of the computation.
We can scan the result to extract the rows,
each of which consists of two columns:
the token and
the number of times that token occured in the corpus.
Scanning in Bigslice follows the general pattern for scanning in Go:
First, we extract a scanner (3)
which has a `Scan` (4) that returns a boolean indicating whether to continue scanning
(and also populates the value for each column in the scanned row),
while the `Err` method (5) returns any error that occured while scanning.'

# Some more details to keep you going

In the examples above,
we used the [command bigslice](https://godoc.org/github.com/grailbio/bigslice/cmd/bigslice)
to build and run Bigslice jobs.
This is needed only to build "fat" binaries that include binaries
for both the host operating system and architecture
as well as linux/amd64,
which is used by the cluster nodes.
If your host operating system is already linux/amd64,
then `bigslice build` and `bigslice run`
are equivalent to `go build` and `go run`;

Bigslice
uses package [github.com/grailbio/base/config](https://godoc.org/github.com/grailbio/base/config)
to maintain its configuration at `$HOME/.bigslice/config`.
You can either edit this file directly,
or override individual parameters
at runtime with the `-set` flag,
for example,
to use [m5.24xlarge](https://aws.amazon.com/ec2/instance-types/m5/)
instances in Bigslice cluster:

```
bigslice run shake.go -set bigmachine/ec2cluster.instance=m5.24xlarge
```

Bigslice uses [bigmachine](https://github.com/grailbio/bigmachine)
to manage clusters of cloud compute instances.
See its documentation for further details.


<footer>
The Bigslice gopher design was inspired by
<a href="http://reneefrench.blogspot.com/">Renee French</a>.
</footer>
