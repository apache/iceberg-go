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
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/datasketches-go/theta"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/stats"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun/driver/sqliteshim"
)

// newNDVTable creates an empty sqlite-backed table named name.
func newNDVTable(t *testing.T, name string, sc *iceberg.Schema, props iceberg.Properties) (*table.Table, context.Context) {
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

	tbl, err := cat.CreateTable(ctx, ident, sc, catalog.WithProperties(props))
	require.NoError(t, err)

	return tbl, ctx
}

// appendNDVRows appends one data file built from the JSON rows, using the
// table's current schema.
func appendNDVRows(t *testing.T, ctx context.Context, tbl *table.Table, jsonRows []string) *table.Table {
	t.Helper()
	arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	require.NoError(t, err)
	arrTable, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema,
		[]string{"[" + strings.Join(jsonRows, ",") + "]"})
	require.NoError(t, err)
	defer arrTable.Release()

	tbl, err = tbl.AppendTable(ctx, arrTable, arrTable.NumRows(), nil)
	require.NoError(t, err)

	return tbl
}

// createNDVTable creates a fresh sqlite-backed table named name with the given
// schema and appends one data file built from the JSON rows.
func createNDVTable(t *testing.T, name string, sc *iceberg.Schema, jsonRows []string) (*table.Table, context.Context) {
	t.Helper()
	tbl, ctx := newNDVTable(t, name, sc, nil)

	return appendNDVRows(t, ctx, tbl, jsonRows), ctx
}

// ndvRows builds rows ids lo..hi with a category cycling through 3 values.
func ndvRows(lo, hi int) []string {
	categories := []string{"north", "south", "east"}
	rows := make([]string, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		rows = append(rows, fmt.Sprintf(`{"id": %d, "category": %q}`, i, categories[i%len(categories)]))
	}

	return rows
}

var ndvTestSchema = iceberg.NewSchema(0,
	iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String, Required: true},
)

// ndvTestTable creates a fresh sqlite-backed table named name and appends
// 100 distinct ids (1..100) and a category cycling through 3 distinct values.
func ndvTestTable(t *testing.T, name string) (*table.Table, context.Context) {
	t.Helper()

	return createNDVTable(t, name, ndvTestSchema, ndvRows(1, 100))
}

// ndvValues returns the NDV of each column, keyed by field ID.
func ndvValues(stats table.NDVStats) map[int]int64 {
	out := make(map[int]int64, len(stats.Columns))
	for id, r := range stats.Columns {
		out[id] = r.NDV
	}

	return out
}

func TestComputeNDVsAllColumns(t *testing.T) {
	tbl, ctx := ndvTestTable(t, "ndv_all")

	got, err := tbl.ComputeNDVs(ctx, 0)
	require.NoError(t, err)

	// The result names the snapshot it analyzed, so a statistics writer does
	// not have to resolve it again.
	require.NotNil(t, got.Snapshot)
	assert.Equal(t, tbl.CurrentSnapshot().SnapshotID, got.Snapshot.SnapshotID)
	assert.Equal(t, tbl.CurrentSnapshot().SequenceNumber, got.Snapshot.SequenceNumber)

	// Both cardinalities are well below the nominal entry count, so the sketch
	// estimates are exact.
	assert.Equal(t, map[int]int64{1: 100, 2: 3}, ndvValues(got))

	// The serialized sketches carry the same estimate when read back.
	for _, r := range got.Columns {
		ndv, err := stats.NDVFromBytes(r.Sketch)
		require.NoError(t, err)
		assert.Equal(t, r.NDV, ndv)
	}
}

func TestComputeNDVsSelectedColumn(t *testing.T) {
	tbl, ctx := ndvTestTable(t, "ndv_selected")

	// Only the named column is sketched; the unselected "id" is absent.
	got, err := tbl.ComputeNDVs(ctx, 0, "category")
	require.NoError(t, err)
	assert.Equal(t, map[int]int64{2: 3}, ndvValues(got))

	// Naming a column twice sketches it once.
	got, err = tbl.ComputeNDVs(ctx, 0, "category", "category")
	require.NoError(t, err)
	assert.Equal(t, map[int]int64{2: 3}, ndvValues(got))
}

func TestComputeNDVsUnknownColumnErrors(t *testing.T) {
	tbl, ctx := ndvTestTable(t, "ndv_unknown")

	// Names are checked against the snapshot schema before the scan is built,
	// so an unknown name, including "*" (which a scan would treat as every
	// column), is an invalid argument rather than a projection error.
	for _, name := range []string{"does_not_exist", "*", "ID"} {
		_, err := tbl.ComputeNDVs(ctx, 0, name)
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument, name)
		assert.ErrorContains(t, err, "can't find column", name)
	}
}

func TestComputeNDVsNoSnapshot(t *testing.T) {
	tbl, ctx := newNDVTable(t, "ndv_empty", ndvTestSchema, nil)
	require.Nil(t, tbl.CurrentSnapshot())

	// Like Java's EMPTY_RESULT: no snapshot means no sketches, not an empty
	// sketch per column.
	got, err := tbl.ComputeNDVs(ctx, 0)
	require.NoError(t, err)
	assert.Nil(t, got.Snapshot)
	assert.Empty(t, got.Columns)

	_, err = tbl.ComputeNDVs(ctx, 12345)
	require.ErrorIs(t, err, table.ErrInvalidOperation)
}

func TestComputeNDVsSkipsGeospatialColumns(t *testing.T) {
	geom, err := iceberg.GeometryTypeOf(iceberg.DefaultGeoCRS)
	require.NoError(t, err)
	geog, err := iceberg.GeographyTypeOf(iceberg.DefaultGeoCRS, "spherical")
	require.NoError(t, err)

	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "geom", Type: geom},
		iceberg.NestedField{ID: 3, Name: "geog", Type: geog},
	)
	tbl, ctx := newNDVTable(t, "ndv_geo", sc, iceberg.Properties{table.PropertyFormatVersion: "3"})
	tbl = appendNDVRows(t, ctx, tbl, []string{
		`{"id": 1, "geom": "01010000000000000000003e400000000000002440", "geog": null}`,
		`{"id": 2, "geom": "0101000000000000000000f03f0000000000000040", "geog": null}`,
	})

	// Iceberg defines no single-value serialization for geospatial values, so
	// the default set leaves them out instead of failing the whole call.
	got, err := tbl.ComputeNDVs(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, map[int]int64{1: 2}, ndvValues(got))

	for name, typ := range map[string]string{"geom": geom.String(), "geog": geog.String()} {
		_, err = tbl.ComputeNDVs(ctx, 0, name)
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument, name)
		assert.ErrorContains(t, err, "can't compute stats on "+typ+" column: "+name)
	}
}

// TestComputeNDVsTypes checks each primitive type end to end: values written
// through Arrow and read back by the scan must hash to the same sketch as their
// Iceberg single-value serialization, which table/stats pins against the Java
// fixtures. Each value appears twice plus a null, so NDV is the number of
// distinct non-empty values.
func TestComputeNDVsTypes(t *testing.T) {
	fixed4, err := iceberg.FixedTypeOf(4)
	require.NoError(t, err)
	dec, err := iceberg.DecimalTypeOf(9, 2)
	require.NoError(t, err)
	u1 := uuid.MustParse("f79c3e09-677c-4bbd-a479-3f349cb785e7")
	u2 := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	const ts1, ts2 = int64(1_600_000_000_000_000), int64(1_700_000_000_123_456)
	// Nanosecond values that differ only below the microsecond, so truncating
	// to micros would collapse them.
	const tsNs1, tsNs2 = int64(1_700_000_000_123_456_001), int64(1_700_000_000_123_456_002)

	tests := []struct {
		name string
		typ  iceberg.Type
		json []string // distinct values, JSON-encoded for Arrow
		lits []iceberg.Literal
	}{
		{
			"boolean", iceberg.PrimitiveTypes.Bool,
			[]string{"true", "false"},
			[]iceberg.Literal{iceberg.NewLiteral(true), iceberg.NewLiteral(false)},
		},
		{
			"int", iceberg.PrimitiveTypes.Int32,
			[]string{"-7", "0", "2147483647"},
			[]iceberg.Literal{iceberg.NewLiteral(int32(-7)), iceberg.NewLiteral(int32(0)), iceberg.NewLiteral(int32(2147483647))},
		},
		{
			"long", iceberg.PrimitiveTypes.Int64,
			[]string{"-5000000000", "1"},
			[]iceberg.Literal{iceberg.NewLiteral(int64(-5_000_000_000)), iceberg.NewLiteral(int64(1))},
		},
		{
			"float", iceberg.PrimitiveTypes.Float32,
			[]string{"0.5", "-1.25", "3"},
			[]iceberg.Literal{iceberg.NewLiteral(float32(0.5)), iceberg.NewLiteral(float32(-1.25)), iceberg.NewLiteral(float32(3))},
		},
		{
			"double", iceberg.PrimitiveTypes.Float64,
			[]string{"0.1", "1e300"},
			[]iceberg.Literal{iceberg.NewLiteral(0.1), iceberg.NewLiteral(1e300)},
		},
		{
			"string", iceberg.PrimitiveTypes.String,
			[]string{`""`, `"é"`, `"value"`},
			[]iceberg.Literal{iceberg.NewLiteral(""), iceberg.NewLiteral("é"), iceberg.NewLiteral("value")},
		},
		{
			"date", iceberg.PrimitiveTypes.Date,
			[]string{"18000", "-1"},
			[]iceberg.Literal{iceberg.NewLiteral(iceberg.Date(18000)), iceberg.NewLiteral(iceberg.Date(-1))},
		},
		{
			"time", iceberg.PrimitiveTypes.Time,
			[]string{"0", "86399999999"},
			[]iceberg.Literal{iceberg.NewLiteral(iceberg.Time(0)), iceberg.NewLiteral(iceberg.Time(86_399_999_999))},
		},
		{
			"timestamp", iceberg.PrimitiveTypes.Timestamp,
			[]string{strconv.FormatInt(ts1, 10), strconv.FormatInt(ts2, 10)},
			[]iceberg.Literal{iceberg.NewLiteral(iceberg.Timestamp(ts1)), iceberg.NewLiteral(iceberg.Timestamp(ts2))},
		},
		{
			"timestamptz", iceberg.PrimitiveTypes.TimestampTz,
			[]string{strconv.FormatInt(ts1, 10), strconv.FormatInt(ts2, 10)},
			[]iceberg.Literal{iceberg.NewLiteral(iceberg.Timestamp(ts1)), iceberg.NewLiteral(iceberg.Timestamp(ts2))},
		},
		{
			"timestamp_ns", iceberg.PrimitiveTypes.TimestampNs,
			[]string{strconv.FormatInt(tsNs1, 10), strconv.FormatInt(tsNs2, 10)},
			[]iceberg.Literal{iceberg.NewLiteral(iceberg.TimestampNano(tsNs1)), iceberg.NewLiteral(iceberg.TimestampNano(tsNs2))},
		},
		{
			"timestamptz_ns", iceberg.PrimitiveTypes.TimestampTzNs,
			[]string{strconv.FormatInt(tsNs1, 10), strconv.FormatInt(tsNs2, 10)},
			[]iceberg.Literal{iceberg.NewLiteral(iceberg.TimestampNano(tsNs1)), iceberg.NewLiteral(iceberg.TimestampNano(tsNs2))},
		},
		{
			"decimal", dec,
			[]string{`"1.23"`, `"-0.01"`, `"9999999.99"`},
			[]iceberg.Literal{
				iceberg.NewLiteral(iceberg.Decimal{Val: decimal128.FromI64(123), Scale: 2}),
				iceberg.NewLiteral(iceberg.Decimal{Val: decimal128.FromI64(-1), Scale: 2}),
				iceberg.NewLiteral(iceberg.Decimal{Val: decimal128.FromI64(999_999_999), Scale: 2}),
			},
		},
		{
			"uuid", iceberg.PrimitiveTypes.UUID,
			[]string{strconv.Quote(u1.String()), strconv.Quote(u2.String())},
			[]iceberg.Literal{iceberg.NewLiteral(u1), iceberg.NewLiteral(u2)},
		},
		{
			"binary", iceberg.PrimitiveTypes.Binary,
			[]string{`""`, `"AAEC"`},
			[]iceberg.Literal{iceberg.NewLiteral([]byte{}), iceberg.NewLiteral([]byte{0, 1, 2})},
		},
		{
			"fixed", fixed4,
			[]string{`"AAAAAA=="`, `"3q2+7w=="`},
			[]iceberg.Literal{
				iceberg.NewLiteral([]byte{0, 0, 0, 0}),
				iceberg.NewLiteral([]byte{0xde, 0xad, 0xbe, 0xef}),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Len(t, tc.lits, len(tc.json))
			sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "v", Type: tc.typ})

			rows := []string{`{"v": null}`}
			want := stats.NewSketch()
			for i, v := range tc.json {
				rows = append(rows, `{"v": `+v+`}`, `{"v": `+v+`}`)
				lit, err := tc.lits[i].To(tc.typ)
				require.NoError(t, err)
				b, err := lit.MarshalBinary()
				require.NoError(t, err)
				want.Add(b)
			}

			// Nanosecond timestamps need a V3 table; they are read through the
			// separate TimestampNano conversion branch.
			var props iceberg.Properties
			switch tc.typ.(type) {
			case iceberg.TimestampNsType, iceberg.TimestampTzNsType:
				props = iceberg.Properties{table.PropertyFormatVersion: "3"}
			}
			tbl, ctx := newNDVTable(t, "ndv_type_"+tc.name, sc, props)
			tbl = appendNDVRows(t, ctx, tbl, rows)
			got, err := tbl.ComputeNDVs(ctx, 0)
			require.NoError(t, err)
			require.Contains(t, got.Columns, 1)

			// DataSketches ignores empty byte arrays, as Java does (the Java
			// "string" fixture also leaves "" out of its count), so an empty
			// string or binary value is not counted.
			wantNDV := int64(len(tc.json))
			if tc.name == "string" || tc.name == "binary" {
				wantNDV--
			}
			assert.Equal(t, wantNDV, got.Columns[1].NDV)
			assertSameNDVSketch(t, want.Serialize(), got.Columns[1].Sketch)
		})
	}
}

// assertSameNDVSketch compares decoded sketch state; see table/stats for why
// raw bytes are not compared.
func assertSameNDVSketch(t *testing.T, want, got []byte) {
	t.Helper()
	w, err := theta.Decode(want, theta.DefaultSeed)
	require.NoError(t, err)
	g, err := theta.Decode(got, theta.DefaultSeed)
	require.NoError(t, err)
	assert.Equal(t, w.IsEmpty(), g.IsEmpty(), "empty")
	assert.Equal(t, w.Theta64(), g.Theta64(), "theta")
	assert.Equal(t, slices.Collect(w.All()), slices.Collect(g.All()), "retained hashes")
}

// TestComputeNDVsSnapshotContents checks that sketches cover exactly the live
// rows of the analyzed snapshot: rows spread over several data files, rows
// removed by position deletes, deletion vectors or equality deletes, and an
// older snapshot read with its own schema.
func TestComputeNDVsSnapshotContents(t *testing.T) {
	mor := func(version string) iceberg.Properties {
		return iceberg.Properties{
			table.PropertyFormatVersion: version,
			table.WriteDeleteModeKey:    table.WriteModeMergeOnRead,
		}
	}
	deleteIDsAbove := func(t *testing.T, ctx context.Context, tbl *table.Table, id int64) *table.Table {
		t.Helper()
		tbl, err := tbl.Delete(ctx, iceberg.GreaterThan(iceberg.Reference("id"), id), nil)
		require.NoError(t, err)

		return tbl
	}

	tests := []struct {
		name  string
		props iceberg.Properties
		// build populates the table and returns it with the snapshot to analyze
		// (0 for current).
		build func(t *testing.T, ctx context.Context, tbl *table.Table) (*table.Table, int64)
		want  map[int]int64
		// deletes reports how many delete files of one kind the analyzed
		// snapshot's tasks carry, so a delete case can't silently degrade to
		// copy-on-write.
		deletes func(table.FileScanTask) int
	}{
		{
			name: "several data files",
			build: func(t *testing.T, ctx context.Context, tbl *table.Table) (*table.Table, int64) {
				tbl = appendNDVRows(t, ctx, tbl, ndvRows(1, 40))
				tbl = appendNDVRows(t, ctx, tbl, ndvRows(31, 70)) // overlaps 31..40
				tbl = appendNDVRows(t, ctx, tbl, []string{`{"id": 500, "category": "west"}`})
				tasks, err := tbl.Scan().PlanFiles(ctx)
				require.NoError(t, err)
				require.Len(t, tasks, 3)

				return tbl, 0
			},
			want: map[int]int64{1: 71, 2: 4},
		},
		{
			name:  "position deletes",
			props: mor("2"),
			build: func(t *testing.T, ctx context.Context, tbl *table.Table) (*table.Table, int64) {
				tbl = appendNDVRows(t, ctx, tbl, ndvRows(1, 100))

				return deleteIDsAbove(t, ctx, tbl, 1), 0
			},
			want:    map[int]int64{1: 1, 2: 1},
			deletes: func(task table.FileScanTask) int { return len(task.DeleteFiles) },
		},
		{
			name:  "deletion vectors",
			props: mor("3"),
			build: func(t *testing.T, ctx context.Context, tbl *table.Table) (*table.Table, int64) {
				tbl = appendNDVRows(t, ctx, tbl, ndvRows(1, 100))

				return deleteIDsAbove(t, ctx, tbl, 2), 0
			},
			want:    map[int]int64{1: 2, 2: 2},
			deletes: func(task table.FileScanTask) int { return len(task.DeletionVectorFiles) },
		},
		{
			name:  "equality deletes",
			props: mor("2"),
			build: func(t *testing.T, ctx context.Context, tbl *table.Table) (*table.Table, int64) {
				tbl = appendNDVRows(t, ctx, tbl, ndvRows(1, 100))

				// Delete every "east" row (ids divisible by 3).
				eqSchema, err := table.SchemaToArrowSchema(
					iceberg.NewSchema(0, iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String, Required: true}),
					nil, true, false)
				require.NoError(t, err)
				eqPath := filepath.Join(t.TempDir(), "eq-del.parquet")
				writeParquetFile(t, eqPath, eqSchema, `[{"category": "east"}]`)
				info, err := os.Stat(eqPath)
				require.NoError(t, err)
				b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
					eqPath, iceberg.ParquetFile, nil, nil, nil, 1, info.Size())
				require.NoError(t, err)
				b.EqualityFieldIDs([]int{2})

				tx := tbl.NewTransaction()
				rd := tx.NewRowDelta(nil)
				rd.AddDeletes(b.Build())
				require.NoError(t, rd.Commit(ctx))
				tbl, err = tx.Commit(ctx)
				require.NoError(t, err)

				return tbl, 0
			},
			want:    map[int]int64{1: 67, 2: 2},
			deletes: func(task table.FileScanTask) int { return len(task.EqualityDeleteFiles) },
		},
		{
			name: "older snapshot after schema change",
			build: func(t *testing.T, ctx context.Context, tbl *table.Table) (*table.Table, int64) {
				tbl = appendNDVRows(t, ctx, tbl, ndvRows(1, 10))
				old := tbl.CurrentSnapshot().SnapshotID

				// Rename category and add a column, then write more rows. The old
				// snapshot is read with its own schema, and its sketches are keyed
				// by field ID, not the current name.
				tx := tbl.NewTransaction()
				require.NoError(t, tx.UpdateSchema(true, false).
					RenameColumn([]string{"category"}, "region").
					AddColumn([]string{"extra"}, iceberg.PrimitiveTypes.Int32, "", false, nil).
					Commit())
				tbl, err := tx.Commit(ctx)
				require.NoError(t, err)
				tbl = appendNDVRows(t, ctx, tbl, []string{
					`{"id": 11, "region": "west", "extra": 1}`,
					`{"id": 12, "region": "up", "extra": 2}`,
				})

				return tbl, old
			},
			want: map[int]int64{1: 10, 2: 3},
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tbl, ctx := newNDVTable(t, fmt.Sprintf("ndv_contents_%d", i), ndvTestSchema, tc.props)
			tbl, snapshotID := tc.build(t, ctx, tbl)

			if tc.deletes != nil {
				tasks, err := tbl.Scan().PlanFiles(ctx)
				require.NoError(t, err)
				n := 0
				for _, task := range tasks {
					n += tc.deletes(task)
				}
				require.Positive(t, n, "expected merge-on-read delete files")
			}

			got, err := tbl.ComputeNDVs(ctx, snapshotID)
			require.NoError(t, err)
			assert.Equal(t, tc.want, ndvValues(got))

			// Sketching one column at a time must give the same counts: deletes
			// keyed on an unselected column (equality deletes on category) still
			// apply when only id is read.
			for fieldID, want := range tc.want {
				sc, err := tbl.Scan(table.WithSnapshotID(got.Snapshot.SnapshotID)).Projection()
				require.NoError(t, err)
				field, ok := sc.FindFieldByID(fieldID)
				require.True(t, ok)
				one, err := tbl.ComputeNDVs(ctx, snapshotID, field.Name)
				require.NoError(t, err)
				assert.Equal(t, map[int]int64{fieldID: want}, ndvValues(one), field.Name)
			}

			require.NotNil(t, got.Snapshot)
			if snapshotID == 0 {
				snapshotID = tbl.CurrentSnapshot().SnapshotID
			}
			assert.Equal(t, snapshotID, got.Snapshot.SnapshotID)
		})
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

	got, err := tbl.ComputeNDVs(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, map[int]int64{1: int64(len(vals)), 2: 3}, ndvValues(got))
}

func TestComputeNDVsRejectsNonPrimitiveColumns(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "s", Required: true, Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 3, Name: "a", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			},
		}},
	)
	tbl, ctx := createNDVTable(t, "ndv_nested", sc, []string{
		`{"id": 1, "s": {"a": 10}}`,
		`{"id": 2, "s": {"a": 20}}`,
	})

	// Like Iceberg-Java, naming a column that can't be sketched is an error
	// rather than a silently missing result.
	_, err := tbl.ComputeNDVs(ctx, 0, "s")
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	assert.ErrorContains(t, err, "non-primitive")

	_, err = tbl.ComputeNDVs(ctx, 0, "s.a")
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	assert.ErrorContains(t, err, "nested")

	// The default column set skips the struct column.
	got, err := tbl.ComputeNDVs(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, map[int]int64{1: 2}, ndvValues(got))
}

// TestComputeNDVsMatchesJavaFixtures writes the values behind two Iceberg-Java
// fixtures in table/stats/testdata into a table and checks that ComputeNDVs
// produces the same sketch, covering the Arrow to single-value serialization
// path end to end. The rows mirror javaFixtures in table/stats/sketch_test.go:
// 1000 rows with every 11th null. Java merges three partition sketches, which
// in exact mode retains the same hashes as one sketch over all rows.
func TestComputeNDVsMatchesJavaFixtures(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "long", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "string", Type: iceberg.PrimitiveTypes.String},
	)

	rows := make([]string, 0, 1000)
	for i := range 1000 {
		if i%11 == 10 {
			rows = append(rows, `{"long": null, "string": null}`)

			continue
		}
		var str string
		switch i % 500 {
		case 0:
			str = ""
		case 1:
			str = "é"
		default:
			str = fmt.Sprintf("value-%d", i%500)
		}
		rows = append(rows, fmt.Sprintf(`{"long": %d, "string": %q}`,
			int64(i%600)*1_000_003-5_000_000_000, str))
	}

	tbl, ctx := createNDVTable(t, "ndv_java_fixtures", sc, rows)

	got, err := tbl.ComputeNDVs(ctx, 0)
	require.NoError(t, err)
	results := got.Columns

	for fieldID, fixture := range map[int]string{1: "long", 2: "string"} {
		t.Run(fixture, func(t *testing.T) {
			javaBytes, err := os.ReadFile(filepath.Join("stats", "testdata", fixture+".sk"))
			require.NoError(t, err)

			java, err := theta.Decode(javaBytes, theta.DefaultSeed)
			require.NoError(t, err)
			ours, err := theta.Decode(results[fieldID].Sketch, theta.DefaultSeed)
			require.NoError(t, err)

			javaNDV, err := stats.NDVFromBytes(javaBytes)
			require.NoError(t, err)
			assert.Equal(t, javaNDV, results[fieldID].NDV)

			// Decoded state is compared rather than raw bytes, as in
			// table/stats/sketch_test.go: DataSketches-Java sets a few
			// preamble fields differently.
			assert.Equal(t, java.IsEmpty(), ours.IsEmpty(), "empty")
			assert.Equal(t, java.Theta64(), ours.Theta64(), "theta")
			assert.Equal(t, slices.Collect(java.All()), slices.Collect(ours.All()), "retained hashes")
		})
	}
}

func TestComputeNDVsSkipsUnknownColumns(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "v", Type: iceberg.PrimitiveTypes.Unknown},
	)
	tbl, ctx := newNDVTable(t, "ndv_unknown_type", sc, iceberg.Properties{table.PropertyFormatVersion: "3"})
	tbl = appendNDVRows(t, ctx, tbl, []string{`{"id": 1, "v": null}`, `{"id": 2, "v": null}`})

	// An unknown column holds only nulls and is read as an Arrow Null array,
	// so there is nothing to sketch. The default set leaves it out instead of
	// failing the whole call.
	got, err := tbl.ComputeNDVs(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, map[int]int64{1: 2}, ndvValues(got))

	_, err = tbl.ComputeNDVs(ctx, 0, "v")
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	assert.ErrorContains(t, err, "can't compute stats on unknown column: v")
}

func TestComputeNDVsUsesSnapshotSchema(t *testing.T) {
	tbl, ctx := ndvTestTable(t, "ndv_schema_without_snapshot")
	snap := tbl.CurrentSnapshot()

	// A schema change creates no snapshot. The current snapshot's sketches must
	// follow that snapshot's schema, so the new column is not reported for a
	// snapshot that doesn't have it.
	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, false).
		AddColumn([]string{"extra"}, iceberg.PrimitiveTypes.Int32, "", false, nil).
		Commit())
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)
	require.Equal(t, snap.SnapshotID, tbl.CurrentSnapshot().SnapshotID)

	got, err := tbl.ComputeNDVs(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, snap.SnapshotID, got.Snapshot.SnapshotID)
	assert.Equal(t, map[int]int64{1: 100, 2: 3}, ndvValues(got))

	// The new column is not in the analyzed snapshot's schema.
	_, err = tbl.ComputeNDVs(ctx, 0, "extra")
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}
