package bigslice

import (
	"fmt"
	"reflect"

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

	type chunkResult struct {
		n   int
		err error
	}
	type state struct {
		emptyC chan readerChunk
		sink   struct {
			filling readerChunk
			result  chunkResult
		}
		doneC chan chunkResult
	}
	readerFuncImpl := func(args []reflect.Value) []reflect.Value {
		var (
			shard             = args[0]
			state             = args[1].Interface().(*state)
			chunk readerChunk = args[2:]
		)
		if state.emptyC == nil {
			state.emptyC = make(chan readerChunk)
			state.doneC = make(chan chunkResult)
			sinkSend := func() {
				state.doneC <- state.sink.result
				state.sink.filling = nil
				state.sink.result = chunkResult{}
			}
			sinkImpl := func(args []reflect.Value) []reflect.Value {
				if state.sink.filling == nil {
					state.sink.filling = <-state.emptyC
				}
				state.sink.filling.SetRow(state.sink.result.n, args)
				state.sink.result.n++
				if state.sink.result.n == state.sink.filling.Len() {
					sinkSend()
				}
				return nil
			}
			sinkFunc := reflect.MakeFunc(sinkType, sinkImpl)
			go func() {
				defer close(state.emptyC) // Panic if another send is attempted.
				defer close(state.doneC)
				defer func() {
					if p := recover(); p != nil {
						state.sink.result.err = fmt.Errorf("pushreader: panic from read: %v", p)
					} else {
						state.sink.result.err = sliceio.EOF
					}
					// Make sure it's our turn to send our last result.
					if state.sink.filling == nil {
						state.sink.filling = <-state.emptyC
					}
					sinkSend()
				}()
				outs := reflect.ValueOf(sinkRead).Call([]reflect.Value{shard, sinkFunc})
				if errI := outs[0].Interface(); errI != nil {
					state.sink.result.err = errI.(error)
				}
			}()
		}
		state.emptyC <- chunk
		result := <-state.doneC
		errValue := errorNilValue
		if result.err != nil {
			errValue = reflect.ValueOf(result.err)
		}
		return []reflect.Value{reflect.ValueOf(result.n), errValue}
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

// TODO: Consider implementing Slice directly (instead of via ReaderFunc) and using Frame.
type readerChunk []reflect.Value

func (c readerChunk) Len() int { return c[0].Len() }

func (c readerChunk) SetRow(r int, vals []reflect.Value) {
	for col := 0; col < len(c); col++ {
		c[col].Index(r).Set(vals[col])
	}
}
