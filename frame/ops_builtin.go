// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.

package frame

import (
	"math"

	"github.com/spaolacci/murmur3"
)

func init() {

	RegisterOps(func(slice []string) Ops {
		return Ops{
			Less: func(i, j int) bool { return slice[i] < slice[j] },
			HashWithSeed: func(i int, seed uint32) uint32 {
				return murmur3.Sum32WithSeed([]byte(slice[i]), seed)
			},
		}
	})

	RegisterOps(func(slice []uint) Ops {
		return Ops{
			Less: func(i, j int) bool { return slice[i] < slice[j] },
			HashWithSeed: func(i int, seed uint32) uint32 {
				return hash64(uint64(slice[i]), seed)
			},
		}
	})

	RegisterOps(func(slice []uint8) Ops {
		return Ops{
			Less: func(i, j int) bool { return slice[i] < slice[j] },
			HashWithSeed: func(i int, seed uint32) uint32 {
				return hash32(uint32(slice[i]), seed)
			},
		}
	})

	RegisterOps(func(slice []uint16) Ops {
		return Ops{
			Less: func(i, j int) bool { return slice[i] < slice[j] },
			HashWithSeed: func(i int, seed uint32) uint32 {
				return hash32(uint32(slice[i]), seed)
			},
		}
	})

	RegisterOps(func(slice []uint32) Ops {
		return Ops{
			Less: func(i, j int) bool { return slice[i] < slice[j] },
			HashWithSeed: func(i int, seed uint32) uint32 {
				return hash32(uint32(slice[i]), seed)
			},
		}
	})

	RegisterOps(func(slice []uint64) Ops {
		return Ops{
			Less: func(i, j int) bool { return slice[i] < slice[j] },
			HashWithSeed: func(i int, seed uint32) uint32 {
				return hash64(uint64(slice[i]), seed)
			},
		}
	})

	RegisterOps(func(slice []int) Ops {
		return Ops{
			Less: func(i, j int) bool { return slice[i] < slice[j] },
			HashWithSeed: func(i int, seed uint32) uint32 {
				return hash64(uint64(slice[i]), seed)
			},
		}
	})

	RegisterOps(func(slice []int8) Ops {
		return Ops{
			Less: func(i, j int) bool { return slice[i] < slice[j] },
			HashWithSeed: func(i int, seed uint32) uint32 {
				return hash32(uint32(slice[i]), seed)
			},
		}
	})

	RegisterOps(func(slice []int16) Ops {
		return Ops{
			Less: func(i, j int) bool { return slice[i] < slice[j] },
			HashWithSeed: func(i int, seed uint32) uint32 {
				return hash32(uint32(slice[i]), seed)
			},
		}
	})

	RegisterOps(func(slice []int32) Ops {
		return Ops{
			Less: func(i, j int) bool { return slice[i] < slice[j] },
			HashWithSeed: func(i int, seed uint32) uint32 {
				return hash32(uint32(slice[i]), seed)
			},
		}
	})

	RegisterOps(func(slice []int64) Ops {
		return Ops{
			Less: func(i, j int) bool { return slice[i] < slice[j] },
			HashWithSeed: func(i int, seed uint32) uint32 {
				return hash64(uint64(slice[i]), seed)
			},
		}
	})

	RegisterOps(func(slice []float32) Ops {
		return Ops{
			Less: func(i, j int) bool { return slice[i] < slice[j] },
			HashWithSeed: func(i int, seed uint32) uint32 {
				return hash32(math.Float32bits(slice[i]), seed)
			},
		}
	})

	RegisterOps(func(slice []float64) Ops {
		return Ops{
			Less: func(i, j int) bool { return slice[i] < slice[j] },
			HashWithSeed: func(i int, seed uint32) uint32 {
				return hash64(math.Float64bits(slice[i]), seed)
			},
		}
	})

	RegisterOps(func(slice []uintptr) Ops {
		return Ops{
			Less: func(i, j int) bool { return slice[i] < slice[j] },
			HashWithSeed: func(i int, seed uint32) uint32 {
				return hash64(uint64(slice[i]), seed)
			},
		}
	})

}

// Hash32 is the 32-bit integer hashing function from
// http://burtleburtle.net/bob/hash/integer.html. (Public domain.)
func hash32(x, seed uint32) uint32 {
	var b [4]byte
	b[0] = byte(x)
	b[1] = byte(x >> 8)
	b[2] = byte(x >> 16)
	b[3] = byte(x >> 24)
	return murmur3.Sum32WithSeed(b[:], seed)
}

// Hash64 uses hash32 to compute a 64-bit integer hash.
func hash64(x uint64, seed uint32) uint32 {
	var b [8]byte
	b[0] = byte(x)
	b[1] = byte(x >> 8)
	b[2] = byte(x >> 16)
	b[3] = byte(x >> 24)
	b[4] = byte(x >> 32)
	b[5] = byte(x >> 40)
	b[6] = byte(x >> 48)
	b[7] = byte(x >> 56)
	return murmur3.Sum32WithSeed(b[:], seed)
}
