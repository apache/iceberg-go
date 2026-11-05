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
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/stats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/driver/sqliteshim"
)

// createNDVTable creates a fresh sqlite-backed table named name with the given
// schema and appends one record batch built from the JSON rows.
func createNDVTable(t *testing.T, name string, sc *iceberg.Schema, jsonRows []string) (*table.Table, context.Context) {
	t.Helper()
	ctx := context.Background()
	cat, err := catalog.Load(ctx, "default", iceberg.Properties{
		"uri":          ":memory:",
		"type":         "sql",
		sql.DriverKey:  sqliteshim.ShimName,
		sql.DialectKey: string(sql.SQLite),
		"warehouse":    "file://" + t.TempDir(),
	})
	require.NoError(t, err)

	ident := table.Identifier{"default", name}
	require.NoError(t, cat.CreateNamespace(ctx, catalog.NamespaceFromIdent(ident), nil))

	tbl, err := cat.CreateTable(ctx, ident, sc)
	require.NoError(t, err)

	arrowSchema, err := table.SchemaToArrowSchema(sc, nil, true, false)
	require.NoError(t, err)
	arrTable, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema,
		[]string{"[" + strings.Join(jsonRows, ",") + "]"})
	require.NoError(t, err)
	defer arrTable.Release()

	tbl, err = tbl.AppendTable(ctx, arrTable, arrTable.NumRows(), nil)
	require.NoError(t, err)

	return tbl, ctx
}

// ndvTestTable creates a fresh sqlite-backed table named tbl and appends rows:
// 100 distinct ids (1..100) and a category cycling through 3 distinct values.
func ndvTestTable(t *testing.T, name string) (*table.Table, context.Context) {
	t.Helper()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String, Required: true},
	)

	categories := []string{"north", "south", "east"}
	rows := make([]string, 0, 100)
	for i := 1; i <= 100; i++ {
		rows = append(rows, fmt.Sprintf(`{"id": %d, "category": %q}`, i, categories[i%len(categories)]))
	}

	return createNDVTable(t, name, sc, rows)
}

func TestComputeNDVsAllColumns(t *testing.T) {
	tbl, ctx := ndvTestTable(t, "ndv_all")

	results, err := tbl.ComputeNDVs(ctx, 0)
	require.NoError(t, err)
	require.Len(t, results, 2)

	// Both cardinalities are well below the nominal entry count, so the sketch
	// estimates are exact.
	assert.Equal(t, int64(100), results[1].NDV)
	assert.Equal(t, int64(3), results[2].NDV)

	// The serialized sketches carry the same estimate when read back.
	for _, r := range results {
		ndv, err := stats.NDVFromBytes(r.Sketch)
		require.NoError(t, err)
		assert.Equal(t, r.NDV, ndv)
	}
}

func TestComputeNDVsSelectedColumn(t *testing.T) {
	tbl, ctx := ndvTestTable(t, "ndv_selected")

	results, err := tbl.ComputeNDVs(ctx, 0, "category")
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, int64(3), results[2].NDV)
}

func TestComputeNDVsExcludesUnselectedColumns(t *testing.T) {
	tbl, ctx := ndvTestTable(t, "ndv_unselected")

	// Selecting only "category" yields exactly one sketch; the unselected "id"
	// column is absent from the results.
	results, err := tbl.ComputeNDVs(ctx, 0, "category")
	require.NoError(t, err)
	_, hasID := results[1]
	assert.False(t, hasID)
	_, hasCategory := results[2]
	assert.True(t, hasCategory)
}

func TestComputeNDVsUnknownColumnErrors(t *testing.T) {
	tbl, ctx := ndvTestTable(t, "ndv_unknown")

	// A column that does not exist in the schema fails projection rather than
	// being silently skipped.
	_, err := tbl.ComputeNDVs(ctx, 0, "does_not_exist")
	require.Error(t, err)
}

func TestComputeNDVsPrimitiveTypes(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "f_int", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "f_long", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 3, Name: "f_float", Type: iceberg.PrimitiveTypes.Float32, Required: true},
		iceberg.NestedField{ID: 4, Name: "f_double", Type: iceberg.PrimitiveTypes.Float64, Required: true},
		iceberg.NestedField{ID: 5, Name: "f_bool", Type: iceberg.PrimitiveTypes.Bool, Required: true},
		iceberg.NestedField{ID: 6, Name: "f_str", Type: iceberg.PrimitiveTypes.String, Required: true},
	)

	rows := make([]string, 0, 12)
	for i := range 12 {
		rows = append(rows, fmt.Sprintf(
			`{"f_int": %d, "f_long": %d, "f_float": %g, "f_double": %g, "f_bool": %t, "f_str": "s%d"}`,
			i%4, i, float64(i%3)+0.5, float64(i%5)+0.25, i%2 == 0, i%6))
	}

	tbl, ctx := createNDVTable(t, "ndv_types", sc, rows)

	results, err := tbl.ComputeNDVs(ctx, 0)
	require.NoError(t, err)

	// Every primitive type reaches its sketch through Iceberg single-value
	// serialization, and each column's cardinality is small enough to be exact.
	wantNDV := map[int]int64{1: 4, 2: 12, 3: 3, 4: 5, 5: 2, 6: 6}
	require.Len(t, results, len(wantNDV))
	for fieldID, want := range wantNDV {
		assert.Equal(t, want, results[fieldID].NDV, "field %d", fieldID)
	}
}

func TestComputeNDVsSkipsNullValues(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "maybe_val", Type: iceberg.PrimitiveTypes.Int64, Required: false},
	)

	// maybe_val has three distinct non-null values (10, 20, 30) alongside
	// duplicates and nulls; nulls must not count toward NDV.
	vals := []string{"10", "10", "20", "null", "null", "30"}
	rows := make([]string, 0, len(vals))
	for i, v := range vals {
		rows = append(rows, fmt.Sprintf(`{"id": %d, "maybe_val": %s}`, i, v))
	}

	tbl, ctx := createNDVTable(t, "ndv_nulls", sc, rows)

	results, err := tbl.ComputeNDVs(ctx, 0)
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.Equal(t, int64(len(vals)), results[1].NDV)
	assert.Equal(t, int64(3), results[2].NDV)
}
