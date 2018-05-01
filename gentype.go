// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// +build ignore

// Gentype generates fast implementations for sorters and hashers.
// This program is run via "go generate".

package main

import (
	"bytes"
	"fmt"
	"go/format"
	"io/ioutil"
	"log"
	"strings"
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

	genSortImpl()
	genHashImpl()
}

func genSortImpl() {
	var g generator
	g.Printf("// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.\n")
	g.Printf("\n")
	g.Printf("package bigslice\n")
	g.Printf("\n")
	g.Printf(`import (
	"reflect"
	"sort"
)`)
	g.Printf("\n")

	g.Printf("func makeSorter(typ reflect.Type, col int) Sorter {\n")
	g.Printf("	switch typ.Kind() {\n")
	for _, typ := range types {
		g.Printf("	case reflect.%s:\n", strings.Title(typ))
		g.Printf("		return %sSorter(col)\n", typ)
	}
	g.Printf("	}\n")
	g.Printf("	return nil\n")
	g.Printf("}\n")
	g.Printf("\n")

	for _, typ := range types {
		var valueMethod string
		switch {
		case strings.HasPrefix(typ, "uint"):
			valueMethod = "Uint"
		case strings.HasPrefix(typ, "int"):
			valueMethod = "Int"
		case strings.HasPrefix(typ, "float"):
			valueMethod = "Float"
		case typ == "string":
			valueMethod = "String"
		default:
			log.Fatalf("no value method for type %s", typ)
		}
		g.Printf("type %sSorter int\n", typ)
		g.Printf("\n")
		g.Printf("func (col %sSorter) Sort(f Frame) {\n", typ)
		g.Printf("	v := f[col].Interface().([]%s)\n", typ)
		g.Printf("	sortFrame(f, func(i, j int) bool { return v[i] < v[j] })\n")
		g.Printf("}\n")
		g.Printf("\n")
		g.Printf("func (col %sSorter) Less(x Frame, i int, y Frame, j int) bool {\n", typ)
		g.Printf("	return x[col].Index(i).%s() < y[col].Index(j).%s()\n", valueMethod, valueMethod)
		g.Printf("}\n")
		g.Printf("\n")
		g.Printf("func (col %sSorter) IsSorted(f Frame) bool {\n", typ)
		g.Printf("	v := f[col].Interface().([]%s)\n", typ)
		g.Printf("	return sort.SliceIsSorted(v, func(i, j int) bool { return v[i] < v[j] })\n")
		g.Printf("}\n")
		g.Printf("\n")
	}

	src := g.Gofmt()
	if err := ioutil.WriteFile("sortimpl.go", src, 0644); err != nil {
		log.Fatal(err)
	}
}

func genHashImpl() {
	var g generator
	g.Printf("// THIS FILE WAS AUTOMATICALLY GENERATED. DO NOT EDIT.\n")
	g.Printf("\n")
	g.Printf("package bigslice\n")
	g.Printf("\n")
	g.Printf(`import (
	"hash/fnv"
	"math"
	"reflect"
)`)
	g.Printf("\n")
	g.Printf("func makeHasherGen(typ reflect.Type, col int) Hasher {\n")
	g.Printf("	switch typ.Kind() {\n")
	for _, typ := range types {
		g.Printf("	case reflect.%s:\n", strings.Title(typ))
		g.Printf("		return %sHasher(col)\n", typ)
	}
	g.Printf("	}\n")
	g.Printf("	return nil\n")
	g.Printf("}\n")
	g.Printf("\n")

	for _, typ := range types {
		g.Printf("type %sHasher int\n", typ)
		g.Printf("\n")
		g.Printf("func (col %sHasher) Hash(f Frame, sum []uint32) {\n", typ)
		g.Printf("	vec := f[col].Interface().([]%s)\n", typ)

		if typ == "string" {
			g.Printf("	h := fnv.New32a()\n")
			g.Printf("	for i := range sum {\n")
			g.Printf("		h.Write([]byte(vec[i]))\n")
			g.Printf("		sum[i] = h.Sum32()\n")
			g.Printf("		h.Reset()\n")
			g.Printf("	}\n")
		} else {

			g.Printf("	for i := range sum {\n")
			switch typ {
			case "float32":
				g.Printf("sum[i] = hash32(math.Float32bits(vec[i]))\n")
			case "uint8", "uint16", "uint32", "int8", "int16", "int32":
				g.Printf("sum[i] = hash32(uint32(vec[i]))\n")

			case "float64":
				g.Printf("sum[i] = hash64(math.Float64bits(vec[i]))\n")
			case "uint", "int", "uint64", "int64", "uintptr":
				g.Printf("sum[i] = hash64(uint64(vec[i]))\n")

			}
			g.Printf("	}\n")
		}
		g.Printf("}\n")
	}

	src := g.Gofmt()
	if err := ioutil.WriteFile("hashimpl.go", src, 0644); err != nil {
		log.Fatal(err)
	}

}

type generator struct {
	buf bytes.Buffer
}

func (g *generator) Printf(format string, args ...interface{}) {
	fmt.Fprintf(&g.buf, format, args...)
}

func (g *generator) Gofmt() []byte {
	src, err := format.Source(g.buf.Bytes())
	if err != nil {
		log.Println(g.buf.String())
		log.Fatalf("generated code is invalid: %s", err)
	}
	return src
}
