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

package table_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// BenchmarkAppendSorted appends 1M rows in 64K-row batches to an
// unpartitioned table and commits, for several default sort orders. The
// unsorted case is the control: the difference to it is the cost of
// sorting on write. It uses only the public API so the same benchmark
// can run on a base revision for comparison.
func BenchmarkAppendSorted(b *testing.B) {
	const (
		batches   = 16
		batchRows = 1 << 16
	)
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "id_nulls", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 3, Name: "name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 4, Name: "loc", Type: iceberg.NewStructType(
			iceberg.NestedField{ID: 5, Name: "lat", Type: iceberg.PrimitiveTypes.Float64},
		)},
		iceberg.NestedField{ID: 6, Name: "value", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 7, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	sc, err := table.SchemaToArrowSchema(schema, nil, false, false)
	if err != nil {
		b.Fatal(err)
	}

	mem := memory.NewGoAllocator()
	rng := rand.New(rand.NewPCG(1, 2))
	recs := make([]arrow.RecordBatch, batches)
	for n := range recs {
		bldr := array.NewRecordBuilder(mem, sc)
		loc := bldr.Field(3).(*array.StructBuilder)
		for range batchRows {
			bldr.Field(0).(*array.Int64Builder).Append(rng.Int64N(1 << 40))
			if rng.IntN(10) == 0 {
				bldr.Field(1).AppendNull()
			} else {
				bldr.Field(1).(*array.Int64Builder).Append(rng.Int64N(1 << 40))
			}
			bldr.Field(2).(*array.StringBuilder).Append(fmt.Sprintf("name-%012d", rng.Int64N(1<<40)))
			loc.Append(true)
			loc.FieldBuilder(0).(*array.Float64Builder).Append(rng.Float64()*180 - 90)
			bldr.Field(4).(*array.Float64Builder).Append(rng.Float64())
			bldr.Field(5).(*array.StringBuilder).Append(fmt.Sprintf("%064x", rng.Uint64()))
		}
		recs[n] = bldr.NewRecordBatch()
		bldr.Release()
	}
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	for _, tc := range []struct {
		name     string
		sourceID int
	}{
		{"none", 0},
		{"int64", 1},
		{"int64-nulls", 2},
		{"string", 3},
		{"nested-float64", 5},
	} {
		order := table.UnsortedSortOrder
		if tc.sourceID != 0 {
			order, err = table.NewSortOrder(1, []table.SortField{{
				SourceIDs: []int{tc.sourceID}, Transform: iceberg.IdentityTransform{},
				Direction: table.SortASC, NullOrder: table.NullsFirst,
			}})
			if err != nil {
				b.Fatal(err)
			}
		}

		b.Run("order="+tc.name, func(b *testing.B) {
			dir := filepath.ToSlash(b.TempDir())
			appendOnce := func(i int) error {
				location := fmt.Sprintf("%s/t%d", dir, i)
				meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, order, location,
					iceberg.Properties{table.PropertyFormatVersion: "2"})
				if err != nil {
					return err
				}
				tbl := table.New(table.Identifier{"db", "append_bench"}, meta, location+"/metadata/v1.metadata.json",
					func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
					&rowDeltaCatalog{metadata: meta})
				rdr, err := array.NewRecordReader(sc, recs)
				if err != nil {
					return err
				}
				defer rdr.Release()

				tx := tbl.NewTransaction()
				if err := tx.Append(b.Context(), rdr, nil); err != nil {
					return err
				}
				_, err = tx.Commit(b.Context())

				return err
			}

			// A revision whose writer rejects nested sort keys skips that case.
			if err := appendOnce(-1); err != nil {
				if strings.Contains(err.Error(), "not a top-level column") {
					b.Skip(err)
				}
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if err := appendOnce(i); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
