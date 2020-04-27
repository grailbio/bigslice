// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package trace

import (
	"encoding/json"
	"io"
)

// T represents the JSON object format in the Chrome tracing format. For more
// details, see:
// https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview
type T struct {
	Events []Event `json:"traceEvents"`
}

// Event is an event in the Chrome tracing format. The fields are mirrored
// exactly.
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

// Encode JSON encodes t into w.
func (t *T) Encode(w io.Writer) error {
	return json.NewEncoder(w).Encode(t)
}

// Decode decodes the JSON object format read from r into t. Call this with a t
// zero value.
func (t *T) Decode(r io.Reader) error {
	return json.NewDecoder(r).Decode(t)
}
