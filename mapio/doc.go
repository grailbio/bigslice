// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

/*
	Package mapio implements a sorted, on-disk map, similar to the
	SSTable data structure used in Bigtable [1], Cassandra [2], and
	others. Maps are read-only, and are produced by a Writer. Each
	Writer expects keys to be appended in lexicographic order. Buf
	provides a means of buffering writes to be sorted before appended to
	a Writer.

	Mapio's on-disk layout loosely follows that of LevelDB [3]. Each Map
	is a sequence of blocks; each block comprises a sequence of entries,
	followed by a trailer:

		block := blockEntry* blockTrailer
		blockEntry :=
			nshared:   uvarint           // number of bytes shared with previous key
			nunshared: uvarint           // number of new bytes in this entry's key
			nvalue:    uvarint           // number of bytes in value
			key:       uint8[nunshared]  // the (prefix compressed) key
			value:     uint8[nvalue]     // the entry's value
		blockTrailer :=
			restarts:  uint32[nrestart]  // array of key restarts
			nrestart:  uint32            // size of restart array
			type:      uint8             // block type (should be 0; reserved for future use)
			crc32:     uint32            // IEEE crc32 of contents and trailer

	Maps prefix compress each key by storing the number of bytes shared
	with the previous key. Maps contain a number of restart points:
	points at which the full key is specified (and nshared = 0). The
	restart point are stored in an array in the block trailer. This
	array can be used to perform binary search for keys.

	A Map is a sequence of data blocks, followed by an index block,
	followed by a trailer.

		map := block(data)* block(meta)* block(index) mapTrailer
		mapTrailer :=
			meta:	blockAddr[20]  // zero-padded address of the meta block index (tbd)
			index:  blockAddr[20]  // zero-padded address of index
			magic:	uint64         // magic (0xa8b2374e8558bc76)
		blockAddr :=
			offset: uvarint        // offset of block in map
			len:    uvarint        // length of block

	The index block contains one entry for each block in the map: each
	entry's key is the last key in that block; the entry's value is a
	blockAddr containing the position of that block. This arrangement
	allows the reader to binary search the index block then search the
	found block.

	[1] https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf
	[2] https://www.cs.cornell.edu/projects/ladis2009/papers/lakshman-ladis2009.pdf
	[3] https://github.com/google/leveldb
*/
package mapio
