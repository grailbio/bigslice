package bigslice

import (
	"fmt"
	"reflect"

	"github.com/grailbio/base/must"
	"github.com/grailbio/bigslice/slicefunc"
	"github.com/grailbio/bigslice/sliceio"
	"github.com/grailbio/bigslice/typecheck"
)

func PushReader(nshard int, sinkRead interface{}, prags ...Pragma) Slice {
	fn, ok := slicefunc.Of(sinkRead)
	if !ok || fn.In.NumOut() != 2 || fn.In.Out(0).Kind() != reflect.Int {
		typecheck.Panicf(1, "pushreader: invalid reader function type %T", sinkRead)
	}

	var (
		sinkType      = fn.In.Out(1)
		errorType     = reflect.TypeOf((*error)(nil)).Elem()
		errorNilValue = reflect.Zero(errorType)
	)

	type state struct {
		sunkC chan []reflect.Value
		err   error
	}
	readerFuncImpl := func(args []reflect.Value) []reflect.Value {
		state := args[1].Interface().(*state)
		if state.sunkC == nil {
			state.sunkC = make(chan []reflect.Value, defaultChunksize)
			sinkImpl := func(args []reflect.Value) []reflect.Value {
				state.sunkC <- args
				return nil
			}
			sinkFunc := reflect.MakeFunc(sinkType, sinkImpl)
			go func() {
				defer close(state.sunkC)
				defer func() {
					if p := recover(); p != nil {
						state.err = fmt.Errorf("pushreader: panic from read: %v", p)
					}
				}()
				outs := reflect.ValueOf(sinkRead).Call([]reflect.Value{args[0], sinkFunc})
				if errI := outs[0].Interface(); errI != nil {
					state.err = errI.(error)
				}
			}()
		}

		var rows int
		for rows < args[2].Len() {
			row := <-state.sunkC
			if row == nil {
				state.err = sliceio.EOF
				break
			}
			must.True(len(row) == len(args[2:]), "%v, %v", len(row), len(args[2:]))
			for c := range row {
				args[2+c].Index(rows).Set(row[c])
			}
			rows++
		}
		errValue := errorNilValue
		if state.err != nil {
			errValue = reflect.ValueOf(state.err)
		}
		return []reflect.Value{reflect.ValueOf(rows), errValue}
	}
	readerFuncArgTypes := []reflect.Type{reflect.TypeOf(int(0)), reflect.TypeOf(&state{})}
	for i := 0; i < sinkType.NumIn(); i++ {
		readerFuncArgTypes = append(readerFuncArgTypes, reflect.SliceOf(sinkType.In(i)))
	}
	readerFuncType := reflect.FuncOf(readerFuncArgTypes,
		[]reflect.Type{reflect.TypeOf(int(0)), errorType}, false)
	readerFunc := reflect.MakeFunc(readerFuncType, readerFuncImpl)

	return ReaderFunc(nshard, readerFunc.Interface(), prags...)
}
