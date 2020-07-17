package slicetest

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"

	"github.com/grailbio/bigslice"
	"github.com/grailbio/bigslice/exec"
)

// Print prints slice to stdout in a deterministic order. This is useful for use
// in slice function examples, as we can rely on the deterministic order in our
// expected output. Print uses local evaluation, so all user functions are
// executed within the same process. This makes it safe and convenient to use
// shared memory in slice operations.
func Print(slice bigslice.Slice) {
	fn := bigslice.Func(func() bigslice.Slice { return slice })
	sess := exec.Start(exec.Local)
	ctx := context.Background()
	res, err := sess.Run(ctx, fn)
	if err != nil {
		log.Panicf("unhandled error running session: %v", err)
	}
	var rows [][]reflect.Value
	vs := make([]reflect.Value, slice.NumOut())
	for i := range vs {
		vs[i] = reflect.New(slice.Out(i))
	}
	args := make([]interface{}, slice.NumOut())
	for i := range args {
		args[i] = vs[i].Interface()
	}
	scanner := res.Scanner()
	for scanner.Scan(ctx, args...) {
		row := make([]reflect.Value, len(args))
		for i := range row {
			row[i] = reflect.ValueOf(reflect.Indirect(vs[i]).Interface())
		}
		rows = append(rows, row)
	}
	if scanner.Err() != nil {
		log.Panicf("unhandled error scanning: %v", err)
	}
	canonicalize(rows)
	for _, row := range rows {
		strs := make([]string, len(row))
		for j := range strs {
			strs[j] = fmt.Sprintf("%v", row[j])
		}
		fmt.Println(strings.Join(strs, " "))
	}
}

// canonicalize deep sorts rows to make the order deterministic.
func canonicalize(rows [][]reflect.Value) {
	for _, row := range rows {
		for _, v := range row {
			valueCanonicalize(v)
		}
	}
	sort.Sort(canonical(rows))
}

// canonical provides a canonical sort of rows for printing. This used for
// printing slices in deterministic order.
type canonical [][]reflect.Value

func (c canonical) Len() int      { return len(c) }
func (c canonical) Swap(i, j int) { c[i], c[j] = c[j], c[i] }
func (c canonical) Less(i, j int) bool {
	for col := range c[i] {
		if valueLess(c[i][col], c[j][col]) {
			return true
		}
		if valueLess(c[j][col], c[i][col]) {
			return false
		}
	}
	return false
}

func valueLess(lhs, rhs reflect.Value) bool {
	switch lhs.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return lhs.Int() < rhs.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return lhs.Uint() < rhs.Uint()
	case reflect.String:
		return lhs.String() < rhs.String()
	case reflect.Slice:
		for i := 0; i < rhs.Len(); i++ {
			if lhs.Len() < i {
				return true
			}
			if valueLess(lhs.Index(i), rhs.Index(i)) {
				return true
			}
			if valueLess(lhs.Index(i), rhs.Index(i)) {
				return false
			}
		}
		return false
	}
	log.Panicf("cannot compare %v and %v", lhs.Kind(), rhs.Kind())
	return false
}

func valueCanonicalize(v reflect.Value) {
	switch v.Kind() {
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			valueCanonicalize(v.Index(i))
		}
		sort.Slice(v.Interface(), func(i, j int) bool {
			return valueLess(v.Index(i), v.Index(j))
		})
	}
}
