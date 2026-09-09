// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
)

func BenchmarkVariantExtract(b *testing.B) {
	const n = 1 << 17 // ParquetBatchSizeDefault
	mem := memory.DefaultAllocator
	ctx := compute.WithAllocator(context.Background(), mem)

	flat := func(t arrow.DataType) *extensions.VariantType {
		return shredStruct(arrow.Field{Name: "a", Type: t})
	}
	nested := func(t arrow.DataType) *extensions.VariantType {
		return shredStruct(arrow.Field{Name: "a", Type: arrow.StructOf(arrow.Field{Name: "b", Type: t})})
	}

	mkRows := func(shape, leaf string) []map[string]any {
		val := func(i int) any {
			if leaf == "string" {
				return "v" + strconv.Itoa(i)
			}

			return int64(i)
		}
		offType := func(i int) any {
			if leaf == "string" {
				return int64(i)
			}

			return "x"
		}
		rows := make([]map[string]any, n)
		for i := range rows {
			switch {
			case shape == "nested":
				rows[i] = map[string]any{"a": map[string]any{"b": val(i)}}
			case shape == "nulls" && i%100 == 0:
				rows[i] = nil
			case shape == "residual" && i%100 == 0:
				rows[i] = map[string]any{"a": offType(i)}
			default:
				rows[i] = map[string]any{"a": val(i)}
			}
		}

		return rows
	}

	leaves := []struct {
		name string
		dt   arrow.DataType
		typ  iceberg.PrimitiveType
	}{
		{"int64", arrow.PrimitiveTypes.Int64, iceberg.PrimitiveTypes.Int64},
		{"string", arrow.BinaryTypes.String, iceberg.PrimitiveTypes.String},
	}

	for _, leaf := range leaves {
		for _, shape := range []string{"clean", "nulls", "residual", "unshredded", "nested"} {
			var shred *extensions.VariantType
			path := "$.a"
			switch shape {
			case "unshredded":
				shred = nil
			case "nested":
				shred, path = nested(leaf.dt), "$.a.b"
			default:
				shred = flat(leaf.dt)
			}

			rec, col := buildVariantExtractRec(b, mem, shred, path, leaf.typ, mkRows(shape, leaf.name))
			varr := resolveVariantSource(rec, col.Term.Ref().Field().ID, col.SourcePath).(*extensions.VariantArray)
			typ := col.Term.Type().(iceberg.PrimitiveType)
			dt, _ := TypeToArrowType(typ, false, false)

			mode := "fallback"
			if ref := tryShreddedTypedColumn(varr, col.Term.VariantPath(), dt, mem); ref != nil {
				mode = "fastpath"
				ref.Release()
			}

			b.Run(fmt.Sprintf("%s/%s/%s/production", leaf.name, shape, mode), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					out, err := extractColumnValues(ctx, varr, col, typ, dt, mem)
					if err != nil {
						b.Fatal(err)
					}
					out.Release()
				}
			})
			b.Run(fmt.Sprintf("%s/%s/perrow", leaf.name, shape), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					out, err := extractColumnValuesPerRow(varr, col, dt, mem)
					if err != nil {
						b.Fatal(err)
					}
					out.Release()
				}
			})

			rec.Release()
		}
	}
}
