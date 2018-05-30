// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// +build ignore

// Gentype generates kernels for standard types.
// This program is run via "go generate".

package main

import (
	"bytes"
	"go/format"
	"io/ioutil"
	"log"
	"strings"
	"text/template"
)

var types = []string{
	"string",

	"uint",
	"uint8", // also byte
	"uint16",
	"uint32",
	"uint64",

	"int",
	"int8",
	"int16",
	"int32", // also rune
	"int64",

	"float32",
	"float64",

	"uintptr",
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("")

	type typeInfo struct {
		Type        string
		TypeCap     string
		ValueMethod string
	}
	infos := make([]typeInfo, len(types))
	for i, typ := range types {
		infos[i].Type = typ
		infos[i].TypeCap = strings.Title(typ)
		switch {
		case strings.HasPrefix(typ, "uint"):
			infos[i].ValueMethod = "Uint"
		case strings.HasPrefix(typ, "int"):
			infos[i].ValueMethod = "Int"
		case strings.HasPrefix(typ, "float"):
			infos[i].ValueMethod = "Float"
		case typ == "string":
			infos[i].ValueMethod = "String"
		default:
			log.Fatalf("no value method for type %s", typ)
		}
	}
	for _, file := range []string{"impl.go", "impl_test.go"} {
		tmpl, err := template.ParseFiles(file + "template")
		if err != nil {
			log.Fatal(err)
		}
		var b bytes.Buffer
		if err := tmpl.Execute(&b, infos); err != nil {
			log.Fatal(err)
		}
		src, err := format.Source(b.Bytes())
		if err != nil {
			log.Println(b.String())
			log.Fatalf("generated code is invalid: %v", err)
		}
		if err := ioutil.WriteFile(file, src, 0644); err != nil {
			log.Fatal(err)
		}
	}
}
