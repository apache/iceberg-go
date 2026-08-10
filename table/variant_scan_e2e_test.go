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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/driver/sqliteshim"
)

// TestVariantExtractScanEndToEnd covers variant predicate pushdown through the public API: write a shredded variant table, read it back via Scan.ToArrowTable with extract row filters.
func TestVariantExtractScanEndToEnd(t *testing.T) {
	ctx := context.Background()
	loc := "file://" + t.TempDir()

	cat, err := catalog.Load(ctx, "default", iceberg.Properties{
		"uri":          ":memory:",
		"type":         "sql",
		sql.DriverKey:  sqliteshim.ShimName,
		sql.DialectKey: string(sql.SQLite),
		"warehouse":    loc,
	})
	require.NoError(t, err)

	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}},
	)
	require.NoError(t, cat.CreateNamespace(ctx, table.Identifier{"ns"}, nil))
	tbl, err := cat.CreateTable(ctx, table.Identifier{"ns", "variant_scan"}, iceSchema,
		catalog.WithProperties(iceberg.Properties{
			table.PropertyFormatVersion:   "3",
			table.ParquetShredVariantsKey: "true",
		}),
		catalog.WithLocation(loc))
	require.NoError(t, err)

	arrSchema, err := table.SchemaToArrowSchema(iceSchema, nil, true, false)
	require.NoError(t, err)

	mem := memory.DefaultAllocator
	idb := array.NewInt64Builder(mem)
	defer idb.Release()
	vb := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer vb.Release()
	rows := []map[string]any{
		{"a": int64(1), "b": "x"},
		{"a": int64(2), "b": "y"},
		{"a": int64(3), "b": "z"},
		{"b": "only-b"}, // a absent
	}
	for i, m := range rows {
		idb.Append(int64(i))
		var b variant.Builder
		require.NoError(t, b.Append(m))
		v, err := b.Build()
		require.NoError(t, err)
		vb.Append(v)
	}
	idArr := idb.NewArray()
	defer idArr.Release()
	pArr := vb.NewArray()
	defer pArr.Release()
	rec := array.NewRecordBatch(arrSchema, []arrow.Array{idArr, pArr}, int64(len(rows)))
	defer rec.Release()

	rdr, err := array.NewRecordReader(arrSchema, []arrow.RecordBatch{rec})
	require.NoError(t, err)
	defer rdr.Release()
	tbl, err = tbl.Append(ctx, rdr, nil)
	require.NoError(t, err)

	ext := iceberg.Extract("payload", "$.a", iceberg.PrimitiveTypes.Int64)

	// idsFor reads the "id" column of a filtered scan.
	idsFor := func(filter iceberg.BooleanExpression) []int64 {
		scan := tbl.Scan(table.WithRowFilter(filter))
		result, err := scan.ToArrowTable(ctx)
		require.NoError(t, err)
		defer result.Release()
		col := result.Column(result.Schema().FieldIndices("id")[0]).Data()
		out := make([]int64, 0, result.NumRows())
		for _, ch := range col.Chunks() {
			c := ch.(*array.Int64)
			for i := 0; i < c.Len(); i++ {
				out = append(out, c.Value(i))
			}
		}

		return out
	}

	require.Equal(t, []int64{2}, idsFor(iceberg.LiteralPredicate(iceberg.OpEQ, ext, iceberg.NewLiteral(int64(3)))),
		"$.a == 3 selects only row id 2")
	require.ElementsMatch(t, []int64{0, 1, 2}, idsFor(iceberg.NotNull(ext)),
		"NotNull($.a) selects the three a-present rows")
	require.Equal(t, []int64{3}, idsFor(iceberg.IsNull(ext)),
		"IsNull($.a) selects the a-absent row")
}
