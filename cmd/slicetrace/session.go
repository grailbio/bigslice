// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/grailbio/base/limitbuf"
	"github.com/grailbio/bigslice/internal/trace"
)

// invocation represents an invocation of a bigslice Func.
type invocation struct {
	index    int
	location string
	args     string
}

// task represents statistics from the execution of a single task.
type task struct {
	invIndex int
	op       string
	shards   int
	shard    int
	// start is measured as a duration offset from the start of tracing.
	start    time.Duration
	duration time.Duration
	read     time.Duration
	write    time.Duration
}

// opStat represents useful statistics for a single op of a particular
// invocation.
type opStat struct {
	invIndex int
	op       string
	shards   int
	// start is measured as a duration offset from the start of tracing.
	start    time.Duration
	duration time.Duration
	total    time.Duration
	read     time.Duration
	write    time.Duration
	min      time.Duration
	q1       time.Duration
	q2       time.Duration
	q3       time.Duration
	max      time.Duration
}

// session represents the trace events from a bigslice session, interpreted for
// display of useful diagnostics.
type session struct {
	invs    []invocation
	tasks   []task
	opStats []opStat
}

// reInv is used to match the event name of "invocation" category events.
// The name is the invocation index, e.g. "4". If the invocation was exclusive,
// the names has "[x]" appended, e.g. "4[x]".
var reInv = regexp.MustCompile(`^(\d+)`)

// reTask is used to match the event name of "task" category events. The name is
// the full task name, from which we parse the invocation index, op name, shard
// number, and total number of shards. For example, the task name
// "inv2_reader_map@2000:132" will be parsed as invocation 2, op "reader_map",
// shards 2000, shard 132.
var reTask = regexp.MustCompile(`inv(\d+)_([^@]*)@(\d+):(\d+)`)

func newSession(events []trace.Event) *session {
	tasks := buildTasks(events)
	return &session{
		invs:    buildInvs(events),
		tasks:   tasks,
		opStats: buildOpStats(tasks),
	}
}

// Invs returns all of the invocations from the session s.
func (s *session) Invs() []invocation {
	return s.invs
}

// OpStats returns all of the op statistics for the invocation indicated by
// invIndex.
func (s *session) OpStats(invIndex int) []opStat {
	var opStats []opStat
	for _, opStat := range s.opStats {
		if opStat.invIndex != invIndex {
			continue
		}
		opStats = append(opStats, opStat)
	}
	sort.Slice(opStats, func(i, j int) bool {
		return opStats[i].start < opStats[j].start
	})
	return opStats
}

func buildInvs(events []trace.Event) []invocation {
	invByIndex := make(map[int]invocation)
	for _, event := range events {
		if event.Cat != "invocation" {
			continue
		}
		matches := reInv.FindStringSubmatch(event.Name)
		if matches == nil {
			log.Printf("could not parse name: %#v", event)
			continue
		}
		index, err := strconv.Atoi(matches[1])
		if err != nil {
			log.Printf("could not parse invocation index from name: %s", event.Name)
			continue
		}
		if _, ok := invByIndex[index]; ok {
			log.Printf("unexpected invocation index %q: %#v", event.Name, event)
			continue
		}
		var args []string
		for _, invArg := range event.Args["args"].([]interface{}) {
			args = append(args, fmt.Sprintf("\"%s\"", invArg.(string)))
		}
		invByIndex[index] = invocation{
			index:    index,
			location: event.Args["location"].(string),
			args:     truncatef(strings.Join(args, ", ")),
		}
	}
	invs := make([]invocation, 0, len(invByIndex))
	for _, inv := range invByIndex {
		invs = append(invs, inv)
	}
	sort.Slice(invs, func(i, j int) bool { return invs[i].index < invs[j].index })
	return invs
}

func buildTasks(events []trace.Event) []task {
	var tasks []task
	for _, event := range events {
		if event.Cat != "task" {
			continue
		}
		matches := reTask.FindStringSubmatch(event.Name)
		if matches == nil {
			log.Printf("could not parse name: %#v", event)
			continue
		}
		invIndex, err := strconv.Atoi(matches[1])
		if err != nil {
			log.Printf("could not parse invocation index from name: %s", event.Name)
			continue
		}
		op := matches[2]
		shards, err := strconv.Atoi(matches[3])
		if err != nil {
			log.Printf("could not parse shards from name: %s", event.Name)
			continue
		}
		shard, err := strconv.Atoi(matches[4])
		if err != nil {
			log.Printf("could not parse shard from name: %s", event.Name)
			continue
		}
		readDuration := int64(event.Args["readDuration"].(float64))
		writeDuration := int64(event.Args["writeDuration"].(float64))
		tasks = append(tasks, task{
			invIndex: invIndex,
			op:       op,
			shards:   shards,
			shard:    shard,
			start:    time.Duration(event.Ts * 1e3),
			duration: time.Duration(event.Dur * 1e3),
			read:     time.Duration(readDuration * 1e3),
			write:    time.Duration(writeDuration * 1e3),
		})
	}
	return tasks
}

func buildOpStats(tasks []task) []opStat {
	type invOp struct {
		invIndex int
		op       string
	}
	type accum struct {
		shards    int
		minStart  time.Duration
		maxEnd    time.Duration
		durations []time.Duration
		total     time.Duration
		read      time.Duration
		write     time.Duration
	}
	accums := make(map[invOp]*accum)
	for _, task := range tasks {
		key := invOp{task.invIndex, task.op}
		a, ok := accums[key]
		if !ok {
			a = &accum{
				minStart: 1<<63 - 1,
			}
			accums[key] = a
			// We expect all tasks to have the same total number of shards, so
			// we set it on the first task we see and then verify for all
			// subsequent tasks.
			a.shards = task.shards
		}
		if task.start < a.minStart {
			a.minStart = task.start
		}
		end := task.start + task.duration
		if a.maxEnd < end {
			a.maxEnd = end
		}
		if task.shards != a.shards {
			log.Fatalf("different total number of shards: got %d, want %d", task.shards, a.shards)
		}
		a.durations = append(a.durations, task.duration)
		a.total += task.duration
		a.read += task.read
		a.write += task.write
	}
	opStats := make([]opStat, 0, len(accums))
	for invOp, a := range accums {
		sort.Slice(a.durations, func(i, j int) bool {
			return a.durations[i] < a.durations[j]
		})
		// It is safe to call computeQuartiles, because a.durations is non-empty
		// by construction. a will only exist if there was a task, and if there
		// was a task, its duration was added to a.durations.
		q1, q2, q3 := computeQuartiles(a.durations)
		opStats = append(opStats, opStat{
			invIndex: invOp.invIndex,
			op:       invOp.op,
			shards:   a.shards,
			start:    a.minStart,
			duration: a.maxEnd - a.minStart,
			total:    a.total,
			read:     a.read,
			write:    a.write,
			min:      a.durations[0],
			q1:       q1,
			q2:       q2,
			q3:       q3,
			max:      a.durations[len(a.durations)-1],
		})
	}
	return opStats
}

func truncatef(v interface{}) string {
	b := limitbuf.NewLogger(80)
	fmt.Fprint(b, v)
	return b.String()
}
