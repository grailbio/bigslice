// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigslice

import (
	"context"

	"github.com/grailbio/bigslice/internal/slicecache"
	"github.com/grailbio/bigslice/slicefunc"
	"github.com/grailbio/bigslice/sliceio"
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
func Cache(ctx context.Context, slice Slice, prefix string) (Slice, error) {
	shardCache, err := slicecache.NewFileShardCache(ctx, prefix, slice.NumShard())
	if err != nil {
		return nil, err
	}
	shardCache.RequireAllCached()
	return &cacheSlice{MakeName("cache"), slice, shardCache}, nil
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
func CachePartial(ctx context.Context, slice Slice, prefix string) (Slice, error) {
	shardCache, err := slicecache.NewFileShardCache(ctx, prefix, slice.NumShard())
	if err != nil {
		return nil, err
	}
	return &cacheSlice{MakeName("cachepartial"), slice, shardCache}, nil
}
