// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reclaimer

import (
	"context"
	"expvar"
	"runtime"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/log"
)

// MemoryPollPeriod is the period with which memory is polled to see
// if the reclaimer needs to enter the reclamation state.
const memoryPollPeriod = 10 * time.Second

const maxInt = int(^uint(0) >> 1)

var (
	vars            = expvar.NewMap("reclaimer")
	pendingv, needv expvar.Int
)

func init() {
	vars.Set("pending", &pendingv)
	vars.Set("need", &needv)
}

// StartMemoryReclaimer starts a new memory monitor that reclaims
// resources from the provided reclaimer. Lo,  mid, and hi define
// watermarks for reclamation: if allocated memory usage (as reported
// by Go's runtime) exceeds the middle watermark, reclamation begins
// and does not end until memory usage usage falls below the low
// watermark. Reclamation proceeds by reclaiming one reclaimable at a
// time, and exponentially increasing the amount of reclamation
// performed as long as memory usage does not go below the low
// watermark. If memory usage increases beyond the high watermark,
// then every reclaimable is attempted.
func StartMemoryReclaimer(ctx context.Context, reclaimer *Reclaimer, lo, mid, hi uint64) {
	go memoryMonitor(ctx, reclaimer, lo, mid, hi)
}

func memoryMonitor(ctx context.Context, reclaimer *Reclaimer, lo, mid, hi uint64) {
	tick := time.NewTicker(memoryPollPeriod)
	done := make(chan *Reclaimable)
	for range tick.C {
		needv.Set(0)

		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		if stats.Alloc < mid {
			log.Debug.Printf("memory reclaimer: no need to reclaim: alloc:%s mid:%s",
				data.Size(stats.Alloc), data.Size(mid))
			continue
		}

		// Perform a single GC first to see if this brings us down to where
		// we need to be without any application reclamation. To avoid too
		// much thrashing, we apply the mid watermark again and not the
		// low.
		runtime.GC()
		runtime.ReadMemStats(&stats)
		if stats.Alloc < mid {
			log.Printf("memory reclaimer: skipping reclamation after GC: alloc:%s mid:%s",
				data.Size(stats.Alloc), data.Size(mid))
			continue
		}

		var (
			pending   int
			start     = time.Now()
			origstats = stats
			nreclaim  int
		)
		for stats.Alloc > lo || pending > 0 {
			var need int
			if stats.Alloc > hi {
				log.Printf("memory reclaimer: emergency reclamation: alloc:%s hi:%s",
					data.Size(stats.Alloc), data.Size(hi))
				need = maxInt
			} else if stats.Alloc > lo {
				// Double the amount of needed reclamations every 5 minutes.
				need = 1 << (uint(time.Since(start).Minutes()) / 5)
			} else {
				start = time.Now()
			}
			needv.Set(int64(need))
			if need > 0 {
				reclaimer.Pause()
			} else {
				reclaimer.Resume()
			}

			for pending < need {
				rec := reclaimer.Pop()
				if rec == nil {
					break
				}
				pendingv.Add(1)
				pending++
				go func() {
					rec.Lock()
					if rec.State != Idle && rec.State != Pause {
						rec.Unlock()
						done <- rec
					}
					rec.State = NeedsReclaim
					rec.Broadcast()
					for rec.State == NeedsReclaim {
						if err := rec.Wait(ctx); err != nil {
							rec.Unlock()
							return
						}
					}
					if rec.State != Reclaiming {
						rec.Unlock()
						done <- rec
					}
					if err := rec.Wait(ctx); err != nil {
						rec.Unlock()
						return
					}
					if rec.State == Idle {
						vars.Add("reclamations", 1)
					}
					rec.Unlock()
					done <- rec
				}()
			}

			log.Printf("memory reclaimer: reclaiming alloc:%s (diff):%s wasted:%s lo:%s mid:%s hi:%s pending:%d need:%d",
				data.Size(stats.Alloc), data.Size(stats.Alloc-origstats.Alloc),
				data.Size(stats.HeapInuse-stats.HeapAlloc),
				data.Size(lo), data.Size(mid), data.Size(hi),
				pending, need)

			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			case rec := <-done:
				reclaimer.Push(rec)
				nreclaim++
				pendingv.Add(-1)
				pending--
				// Perform another GC after each actual reclamation--there usually
				// is memory for the GC to reap in these cases.
				runtime.GC()
			}

			runtime.ReadMemStats(&stats)
		}
		log.Printf("memory reclaimer: completed reclamation: reclamations:%d alloc:%s (diff):%s",
			nreclaim, data.Size(stats.Alloc), data.Size(stats.Alloc-origstats.Alloc))
		reclaimer.Resume()
	}
}
