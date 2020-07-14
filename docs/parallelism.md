---
title: Bigslice - parallelism
layout: default
---

# Parallelism in Bigslice

## Tasks
Bigslice computations are specified as data transformations operating on
"slices" of data. When a computation is evaluated with `(*exec.Session).Run`, it
is compiled into an acyclic directed graph. Each node is a *task* that computes
a portion of the whole computation. For example, a single task may perform a
`Map` transformation on a particular shard. Each edge is a dependency between
tasks.

A task is the unit of parallelism. Any task whose dependencies are satisfied can
be scheduled to run.

## Procs
Bigslice represents a computational resource that can be used to evaluate tasks
as a *proc*. This generally corresponds to a single CPU core. For example, if we are 
using EC2, each core of each instance provides a single proc.

By default, each task occupies a single proc for its evaluation.

## Controlling parallelism
The set of procs made available and used by a computation is a function of
configuration parameters and the computation itself.

### Proc supply
`bigslice parallelism` specifies the number of procs that Bigslice will try to
make available, e.g. by launching new EC2 instances. Bigslice will only make
procs available as needed by the computation. For example, if a computation only
ever needs to compute `5` tasks in parallel, Bigslice will only make `5` procs
available even if `bigslice parallelism` is set `>5`.

If we're using an EC2 system, e.g. `bigmachine/ec2system`, procs are made
available by launching EC2 instances. Bigslice provides parameters to control
the type and number of instances to launch and use.

- `bigmachine/ec2system instance` specifies the EC2 instance type. Each vCPU
  provides a proc, e.g. `m4.xlarge` will provide 4 procs per instance.
- `bigslice max-load` specifies the maximum number of vCPUs to use per instance
  as a proportion in `[0.0, 1.0]`. For example, if we are using `m4.xlarge`
  instances with `max-load` of `0.6`, each instance will only provide `2` procs,
  `floor(4*0.6)`. Tune this parameter if your computation is constrained by
  factors other than CPU.
  
Let's consider a complete example. Suppose we are performing a mapping
computation with 1000 shards, i.e. there are 1000 tasks that we could run in
parallel given no other constraints. We also have the following configuration:

```
param bigmachine/ec2system instance = "m4.xlarge"
param bigslice parallelism = 16
param bigslice max-load = 0.9
```

When Bigslice evaluates our computation, it will see a demand for 1000 procs.
However, `parallelism` will cap this at `16`. Bigslice knows that each instance
provides 3 procs, `floor((4 vCPUs per instance) * 0.9)`. Bigslice will launch 6
instances, resulting in 18 available procs. Notice that this is more than the
`16` we specified; Bigslice will launch (and fully utilize) the minimum number
of machines necessary to provide the requested procs/parallelism.

### Proc demand
By default, each task occupies a single proc for its evaluation.
Pragmas[^pragma] can be specified on slice operations to customize this
behavior.

#### `bigslice.Procs`
`bigslice.Procs(n int)` specifies the number of procs that each task compiled
from the slice will occupy. (`bigslice.Procs(1)` is a no-op, as that's the
default.)

For example,
```go
slice = bigslice.Map(slice, bigslice.Procs(6))
```

This mapping slice will be compiled into `S` tasks, where `S` is the number of
shards of the input slice. When Bigslice evaluates one of these tasks, it will
occupy `6` procs.

The number of procs a task requires is clamped to the number of procs a single
instance provides. A single task cannot be divided across multiple instances.

There are (at least) two use cases for `bigslice.Procs`.

1. Your computation has internal parallelism, e.g. your function passed to
   `bigslice.Map` uses multiple threads to perform the mapping of a single
   element. In general, it's preferable to allow Bigslice to manage parallelism,
   but this isn't always convenient.
2. Your computation is constrained on resources other than CPU. This is similar
   to the usage of `bigslice.max-load` but specified at the slice level instead
   of the whole-computation level.

#### `bigslice.Exclusive`
`bigslice.Exclusive` specifies that each task compiled from a slice should
occupy an entire instance, regardless of the type of instance. (It is
practically equivalent to
`bigslice.Procs(nThatIsAtLeastNumberOfProcsPerInstance)`.)

Use `bigslice.Exclusive` if your tasks will consume the entire resources of a
machine, e.g. fully occupy a GPU.

[^pragma]: A pragma is a directive used to specify some intention that may
    modify Bigslice evaluation. They are passed as optional arguments to slice
    operations. Pragmas do not affect the results of a computation but may
    change how machines are allocated, tasks are distributed, results are
    materialized, etc.
