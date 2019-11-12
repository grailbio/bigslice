---
title: Bigslice - implementation
layout: default
---

# About the Bigslice implementation
{:.no_toc}

In this document,
we'll attempt to describe some of the high-level
implementation details of Bigslice.
The goal of this is to help the user understand
the internals of Bigslice,
and to help implementors of new slice operations.

* ToC
{:toc}

# What is a `bigslice.Slice`?

A [`bigslice.Slice`](https://godoc.org/github.com/grailbio/bigslice#Slice)
represents a collection of rows of data. `bigslice.Slice` values
are typed, and contain one or more columns of data.
By convention,
we write the type schematically using a Java-style generics syntax.
For example,
the type `Slice<string, int>` describes a `bigslice.Slice`
with two columns:
the first is string-typed;
the second is integer-typed.

Bigslice slices are *sharded*:
their underlying dataset is split into a number of underlying partitions.
`bigslice.Slice` is an interface,
and the user may implement custom `bigslice.Slice`s.

```
type Slice interface {
	slicetype.Type

	// Name returns a unique (composite) name for this Slice that also has
	// useful context for diagnostic or status display.
	Name() Name

	// NumShard returns the number of shards in this Slice.
	NumShard() int
	// ShardType returns the sharding type of this Slice.
	ShardType() ShardType

	// NumDep returns the number of dependencies of this Slice.
	NumDep() int
	// Dep returns the i'th dependency for this Slice.
	Dep(i int) Dep

	// Combiner is an optional function that is used to combine multiple
	// values with the same key from the slice's output. No combination
	// is performed if nil.
	Combiner() *reflect.Value

	// Reader returns a Reader for a shard of this Slice. The reader
	// itself computes the shard's values on demand. The caller must
	// provide Readers for all of this shard's dependencies, constructed
	// according to the dependency type (see Dep).
	Reader(shard int, deps []sliceio.Reader) sliceio.Reader
}
```

A `bigslice.Slice` may declare dependencies on other slices.
At runtime,
these dependencies are materialized by the Bigslice pipeline
and provided as input to func `Reader`.

The kernel of a slice operation is `Reader`:
it is invoked at runtime to produce the actual rows
computed by the slice operation.
The Bigslice runtime provides materialized
[readers](https://godoc.org/github.com/grailbio/bigslice/sliceio#Reader)
for each of the slice's dependencies;
the returned reader is the output of the operation.

`sliceio.Reader` is analagous to `io.Reader`,
but operating on a
[`frame.Frame`](https://godoc.org/github.com/grailbio/bigslice/frame#Frame),
which is typed according to the slice.
The `sliceio.Reader` implementation is responsible for
filling the provided frame with up to `frame.Len()` rows of output.

```
type Reader interface {
	// Read reads a vector of records from the underlying Slice. Each
	// passed-in column should be a value containing a slice of column
	// values. The number of columns should match the number of columns
	// in the slice; their types should match the corresponding column
	// types of the slice. Each column should have the same slice
	// length.
	//
	// Read returns the total number of records read, or an error. When
	// no more records are available, Read returns EOF. Read may return
	// EOF when n > 0. In this case, n records were read, but no more
	// are available.
	//
	// Read should never reuse any allocated memory in the frame;
	// its callers should not mutate the data returned.
	//
	// Read should not be called concurrently.
	Read(ctx context.Context, frame frame.Frame) (int, error)
}
```

Frames are pre-allocated and managed by the Bigslice runtime.
They are layed out in a columnar fashion,
so the underlying data layout can be exploited for locality.

# Frames

[Frames](https://godoc.org/github.com/grailbio/bigslice/frame#Frame)
are used within Bigslice to store data and operate on it.
Frames represent a rectangular data frame,
comprising one or more columns
and one or more rows.
Frames follow the semantics of Go's slices.
That is,
each frame is a descriptor of an underlying set of typed arrays.
Each frame stores a pointer to the underlying data,
together with its offset, length, and capacity.
Thus, frames may be appended, copied, and sub-sliced
in the manner of Go slices.
(However, the set of columns remain fixed once a Frame has been created.)
Frames also store the type of each column
so that operations may be type-checked at runtime.

Frames implement a columnar memory layout:
that is, 
each column in a Frame is an independent, contiguous array.
An an example, consider a 3-column Frame 
with types `A`, `B`, and `C`
of length 4.
This frame has the following memory layout: `AAAABBBBCCCC`.
(a row-based layout would have the layout `ABCABCABCABC`.)

In addition to managing storage,
Frames provide a set of [type-driven operations](https://godoc.org/github.com/grailbio/bigslice/frame#Ops)
that are required to implement various aspects of Bigslice.
For example,
operations are required for hashing and sorting data 
(e.g., for a reduce or group-by operation);
Ops also allow users to supply custom 
encoding and decoding functions
(by default, Bigslice will use [gob](https://godoc.org/encoding/gob)).
User-provided operations are provided through 
[frame.RegisterOps](https://godoc.org/github.com/grailbio/bigslice/frame#RegisterOps).

Frames thus provide a mechanism to efficiently 
and safely manage and operate on rectangular data.
Frames expose operations in a type-oblivious way,
so that algorithms can be generalized.
For example,
the Bigslice [combiner](https://github.com/grailbio/bigslice/blob/cafa2ff6e7ea96fa4d094a9f2149109825b3774a/exec/combiner.go#L148)
implements a hash table on top of frames,
without any knowledge of the types of the values it contains.

