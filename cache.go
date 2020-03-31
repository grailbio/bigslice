// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"context"

	"github.com/grailbio/bigslice/internal/slicecache"
	"github.com/grailbio/bigslice/slicefunc"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/slicetype"
)

type cacheSlice struct {
	name Name
	Slice
	cache *slicecache.FileShardCache
}

var _ slicecache.Cacheable = (*cacheSlice)(nil)

func (c *cacheSlice) Name() Name                                             { return c.name }
func (c *cacheSlice) NumDep() int                                            { return 1 }
func (c *cacheSlice) Dep(i int) Dep                                          { return Dep{c.Slice, false, nil, false} }
func (*cacheSlice) Combiner() slicefunc.Func                                 { return slicefunc.Nil }
func (c *cacheSlice) Reader(shard int, deps []sliceio.Reader) sliceio.Reader { return deps[0] }

func (c *cacheSlice) Cache() slicecache.ShardCache { return c.cache }

// Cache caches the output of a slice to the given file prefix.
// Cached data are stored as "prefix-nnnn-of-mmmm" for shards nnnn of
// mmmm. When the slice is computed, each shard is encoded and
// written to a separate file with this prefix. If all shards exist,
// then Cache shortcuts computation and instead reads directly from
// the previously computed output. The user must guarantee cache
// consistency: if the cache could be invalid (e.g., because of
// code changes), the user is responsible for removing existing
// cached files, or picking a different prefix that correctly
// represents the operation to be cached.
//
// Cache uses GRAIL's file library, so prefix may refer to URLs to a
// distributed object store such as S3.
func Cache(ctx context.Context, slice Slice, prefix string) Slice {
	shardCache := slicecache.NewFileShardCache(ctx, prefix, slice.NumShard())
	shardCache.RequireAllCached()
	return &cacheSlice{MakeName("cache"), slice, shardCache}
}

// CachePartial caches the output of the slice to the given file
// prefix (it uses the same file naming scheme as Cache). However, unlike
// Cache, if CachePartial finds incomplete cached results (from an
// earlier failed or interrupted run), it will use them and recompute only
// the missing data.
//
// WARNING: The user is responsible for ensuring slice's contents are
// deterministic between bigslice runs. If keys are non-deterministic, for
// example due to pseudorandom seeding based on time, or reading the state
// of a modifiable file in S3, CachePartial produces corrupt results.
//
// As with Cache, the user must guarantee cache consistency.
func CachePartial(ctx context.Context, slice Slice, prefix string) Slice {
	shardCache := slicecache.NewFileShardCache(ctx, prefix, slice.NumShard())
	return &cacheSlice{MakeName("cachepartial"), slice, shardCache}
}

type readCacheSlice struct {
	slicetype.Type
	name     Name
	numShard int
	cache    *slicecache.FileShardCache
}

func (r *readCacheSlice) Name() Name             { return r.name }
func (r *readCacheSlice) NumShard() int          { return r.numShard }
func (*readCacheSlice) ShardType() ShardType     { return HashShard }
func (*readCacheSlice) NumDep() int              { return 0 }
func (*readCacheSlice) Dep(i int) Dep            { panic("no deps") }
func (*readCacheSlice) Combiner() slicefunc.Func { return slicefunc.Nil }

func (r *readCacheSlice) Reader(shard int, _ []sliceio.Reader) sliceio.Reader {
	return r.cache.CacheReader(shard)
}

// ReadCache reads from an existing cache but does not write any cache itself.
// This may be useful if you want to reuse a cache from a previous computation
// and fail if it does not exist. typ is the type of the cached and returned
// slice.
func ReadCache(ctx context.Context, typ slicetype.Type, numShard int, prefix string) Slice {
	shardCache := slicecache.NewFileShardCache(ctx, prefix, numShard)
	shardCache.RequireAllCached()
	return &readCacheSlice{typ, MakeName("readcache"), numShard, shardCache}
}
