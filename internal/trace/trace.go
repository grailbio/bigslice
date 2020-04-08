package trace

import (
	"encoding/json"
	"io"
)

type T struct {
	Events []Event `json:"traceEvents"`
}

// traceEvent is an event in the Chrome tracing format. The fields are
// mirrored exactly. For more details, see:
//	https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview
type Event struct {
	Pid  int                    `json:"pid"`
	Tid  int                    `json:"tid"`
	Ts   int64                  `json:"ts"`
	Ph   string                 `json:"ph"`
	Dur  int64                  `json:"dur,omitempty"`
	Name string                 `json:"name"`
	Cat  string                 `json:"cat,omitempty"`
	Args map[string]interface{} `json:"args"`
}

func (t *T) Encode(w io.Writer) error {
	enc := json.NewEncoder(w)
	return enc.Encode(t)
}

func (t *T) Decode(r io.Reader) error {
	dec := json.NewDecoder(r)
	return dec.Decode(t)
}
