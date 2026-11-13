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
	"encoding/json"
	"fmt"
	"iter"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func collectNormalizedChangelogRows(t *testing.T, records iter.Seq2[arrow.RecordBatch, error]) []changelogRow {
	t.Helper()
	var rows []changelogRow
	for rec, err := range records {
		require.NoError(t, err)
		for i := range int(rec.NumRows()) {
			row := changelogRow{}
			if id := rec.Schema().FieldIndices("id"); len(id) > 0 {
				row.id = rec.Column(id[0]).(*array.Int64).Value(i)
			}
			if data := rec.Schema().FieldIndices("data"); len(data) > 0 && !rec.Column(data[0]).IsNull(i) {
				row.data = rec.Column(data[0]).ValueStr(i)
			}
			n := int(rec.NumCols())
			row.op = rec.Column(n - 3).ValueStr(i)
			row.ordinal = int64(rec.Column(n - 2).(*array.Int32).Value(i))
			row.snapshot = rec.Column(n - 1).(*array.Int64).Value(i)
			rows = append(rows, row)
		}
		rec.Release()
	}

	return rows
}

func normalizationTable(t *testing.T, version int) (*table.Table, int64) {
	t.Helper()
	schema := iceberg.NewSchemaWithIdentifiers(0, []int{1},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String},
	)
	tbl := newChangelogTable(t, version, schema, iceberg.UnpartitionedSpec)
	tbl = writeNormalizationRows(t, tbl, `[{"id":1,"data":"same"},{"id":2,"data":"old"},{"id":3,"data":"removed"}]`, false)
	start := tbl.CurrentSnapshot().SnapshotID
	tbl = writeNormalizationRows(t, tbl, `[{"id":1,"data":"same"},{"id":2,"data":"new"},{"id":4,"data":"added"}]`, true)

	return tbl, start
}

func writeNormalizationRows(t *testing.T, tbl *table.Table, rows string, overwrite bool) *table.Table {
	t.Helper()
	schema, err := table.SchemaToArrowSchemaWithOptions(tbl.Schema(), table.ArrowSchemaOptions{})
	require.NoError(t, err)
	data, err := array.TableFromJSON(memory.DefaultAllocator, schema, []string{rows})
	require.NoError(t, err)
	defer data.Release()
	reader := array.NewTableReader(data, -1)
	defer reader.Release()
	if overwrite {
		tbl, err = tbl.Overwrite(t.Context(), reader, nil, table.WithOverwriteConcurrency(1))
	} else {
		tbl, err = tbl.Append(t.Context(), reader, nil)
	}
	require.NoError(t, err)

	return tbl
}

func TestNormalizedChangelogCarryoversAndUpdateImages(t *testing.T) {
	for _, version := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			tbl, start := normalizationTable(t, version)
			for _, updates := range []bool{false, true} {
				scan := tbl.NewIncrementalChangelogScan(table.WithArrowBatchSize(1)).FromSnapshotExclusive(start)
				schema, records, err := scan.ToNormalizedArrowRecords(t.Context(), table.ChangelogNormalizationOptions{ComputeUpdates: updates})
				require.NoError(t, err)
				require.Equal(t, []string{"id", "data", table.ChangelogChangeTypeColumn, table.ChangelogChangeOrdinalColumn, table.ChangelogCommitSnapshotIDColumn}, schemaFieldNames(schema))
				got := clearChangelogSnapshot(collectNormalizedChangelogRows(t, records))
				if updates {
					require.Equal(t, []changelogRow{
						{id: 2, data: "old", op: "UPDATE_BEFORE"},
						{id: 2, data: "new", op: "UPDATE_AFTER"},
						{id: 3, data: "removed", op: "DELETE"},
						{id: 4, data: "added", op: "INSERT"},
					}, got)
				} else {
					require.Equal(t, []changelogRow{
						{id: 2, data: "old", op: "DELETE"},
						{id: 3, data: "removed", op: "DELETE"},
						{id: 2, data: "new", op: "INSERT"},
						{id: 4, data: "added", op: "INSERT"},
					}, got)
				}
			}
		})
	}
}

func TestNormalizedChangelogProjectionDoesNotHideUpdates(t *testing.T) {
	tbl, start := normalizationTable(t, 3)
	for _, updates := range []bool{false, true} {
		scan := tbl.NewIncrementalChangelogScan(table.WithSelectedFields("id")).FromSnapshotExclusive(start)
		_, records, err := scan.ToNormalizedArrowRecords(t.Context(), table.ChangelogNormalizationOptions{ComputeUpdates: updates})
		require.NoError(t, err)
		got := collectNormalizedChangelogRows(t, records)
		require.Len(t, got, 4)
		if updates {
			require.Equal(t, "UPDATE_BEFORE", got[0].op)
			require.Equal(t, "UPDATE_AFTER", got[1].op)
		} else {
			require.Equal(t, []int64{2, 3}, changelogIDs(got, table.ChangelogOpDelete))
			require.Equal(t, []int64{2, 4}, changelogIDs(got, table.ChangelogOpInsert))
		}
	}
	_, records, err := tbl.NewIncrementalChangelogScan(table.WithSelectedFields("data")).FromSnapshotExclusive(start).
		ToNormalizedArrowRecords(t.Context(), table.ChangelogNormalizationOptions{ComputeUpdates: true})
	require.NoError(t, err)
	got := collectNormalizedChangelogRows(t, records)
	require.Equal(t, "old", got[0].data)
	require.Equal(t, "new", got[1].data)
	require.Equal(t, "UPDATE_BEFORE", got[0].op)
	require.Equal(t, "UPDATE_AFTER", got[1].op)
}

func TestNormalizedChangelogFiltersAfterComputingUpdates(t *testing.T) {
	tbl, start := normalizationTable(t, 2)
	for _, tt := range []struct{ data, op string }{{"old", "UPDATE_BEFORE"}, {"new", "UPDATE_AFTER"}, {"same", ""}} {
		scan := tbl.NewIncrementalChangelogScan(
			table.WithSelectedFields("id"),
			table.WithRowFilter(iceberg.EqualTo(iceberg.Reference("data"), tt.data)),
		).FromSnapshotExclusive(start)
		_, records, err := scan.ToNormalizedArrowRecords(t.Context(), table.ChangelogNormalizationOptions{ComputeUpdates: true})
		require.NoError(t, err)
		got := collectNormalizedChangelogRows(t, records)
		if tt.op == "" {
			require.Empty(t, got)
		} else {
			require.Len(t, got, 1)
			require.Equal(t, int64(2), got[0].id)
			require.Equal(t, tt.op, got[0].op)
		}
	}
}

func TestNormalizedChangelogLimitAppliesAfterCarryoverRemoval(t *testing.T) {
	tbl, start := normalizationTable(t, 3)
	for _, limit := range []int64{0, 1, 2} {
		_, records, err := tbl.NewIncrementalChangelogScan(table.WithLimit(limit)).FromSnapshotExclusive(start).
			ToNormalizedArrowRecords(t.Context(), table.ChangelogNormalizationOptions{ComputeUpdates: true})
		require.NoError(t, err)
		got := collectNormalizedChangelogRows(t, records)
		require.Len(t, got, int(limit))
		if limit > 0 {
			require.Equal(t, int64(2), got[0].id, "the unchanged id=1 must not leak as DELETE")
			require.Equal(t, "UPDATE_BEFORE", got[0].op)
		}
		if limit > 1 {
			require.Equal(t, "UPDATE_AFTER", got[1].op)
		}
	}
}

func TestNormalizedChangelogKeepsSnapshotBoundaries(t *testing.T) {
	tbl, start := normalizationTable(t, 3)
	firstUpdate := tbl.CurrentSnapshot().SnapshotID
	tbl = writeNormalizationRows(t, tbl, `[{"id":1,"data":"same"},{"id":2,"data":"old"},{"id":4,"data":"added"}]`, true)
	_, records, err := tbl.NewIncrementalChangelogScan().FromSnapshotExclusive(start).
		ToNormalizedArrowRecords(t.Context(), table.ChangelogNormalizationOptions{ComputeUpdates: true})
	require.NoError(t, err)
	got := collectNormalizedChangelogRows(t, records)
	require.Len(t, got, 6)
	require.Equal(t, firstUpdate, got[0].snapshot)
	require.Equal(t, firstUpdate, got[1].snapshot)
	require.Equal(t, tbl.CurrentSnapshot().SnapshotID, got[4].snapshot)
	require.Equal(t, tbl.CurrentSnapshot().SnapshotID, got[5].snapshot)
	require.Equal(t, "UPDATE_BEFORE", got[4].op)
	require.Equal(t, "new", got[4].data)
	require.Equal(t, "UPDATE_AFTER", got[5].op)
	require.Equal(t, "old", got[5].data)
	require.Equal(t, int64(1), got[4].ordinal)
}

func TestNormalizedChangelogReleasesBuffers(t *testing.T) {
	tbl, start := normalizationTable(t, 3)
	for _, tt := range []struct {
		name      string
		opts      table.ChangelogNormalizationOptions
		stopEarly bool
		cancel    bool
		wantErr   error
	}{
		{name: "drain", opts: table.ChangelogNormalizationOptions{ComputeUpdates: true}},
		{name: "early break", opts: table.ChangelogNormalizationOptions{ComputeUpdates: true}, stopEarly: true},
		{name: "byte cap", opts: table.ChangelogNormalizationOptions{MaxBufferedBytes: 1}, wantErr: table.ErrChangelogBufferLimit},
		{name: "row cap", opts: table.ChangelogNormalizationOptions{MaxBufferedRows: 1}, wantErr: table.ErrChangelogBufferLimit},
		{name: "cancel before iteration", cancel: true, wantErr: context.Canceled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			ctx, cancel := context.WithCancel(compute.WithAllocator(t.Context(), mem))
			defer cancel()
			_, records, err := tbl.NewIncrementalChangelogScan(table.WithArrowBatchSize(1)).FromSnapshotExclusive(start).
				ToNormalizedArrowRecords(ctx, tt.opts)
			require.NoError(t, err)
			if tt.cancel {
				cancel()
			}
			var readErr error
			var emitted int
			for rec, err := range records {
				if err != nil {
					readErr = err

					break
				}
				emitted += int(rec.NumRows())
				rec.Release()
				if tt.stopEarly {
					break
				}
			}
			if tt.wantErr != nil {
				require.ErrorIs(t, readErr, tt.wantErr)
				require.Zero(t, emitted, "a failed snapshot must not produce partial output")
			} else {
				require.NoError(t, readErr)
				require.Positive(t, emitted)
			}
		})
	}
}

func TestNormalizedChangelogEmptyTableAndValidation(t *testing.T) {
	tbl := newV2RowLineageTestTable(t)
	schema, records, err := tbl.NewIncrementalChangelogScan().ToNormalizedArrowRecords(t.Context(), table.ChangelogNormalizationOptions{})
	require.NoError(t, err)
	require.Len(t, schema.Fields(), 5)
	require.Empty(t, collectNormalizedChangelogRows(t, records))
	for _, opts := range []table.ChangelogNormalizationOptions{
		{MaxBufferedBytes: -1},
		{MaxBufferedRows: -1},
		{IdentifierFields: []string{"id"}},
		{ComputeUpdates: true},
		{ComputeUpdates: true, IdentifierFields: []string{"missing"}},
		{ComputeUpdates: true, IdentifierFields: []string{"id", "id"}},
	} {
		_, _, err := tbl.NewIncrementalChangelogScan().ToNormalizedArrowRecords(t.Context(), opts)
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	}
	var scan *table.IncrementalChangelogScan
	_, _, err = scan.ToNormalizedArrowRecords(t.Context(), table.ChangelogNormalizationOptions{})
	require.ErrorIs(t, err, table.ErrInvalidOperation)
}

func TestNormalizedChangelogNestedValuesAndOwnership(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "values", Type: iceberg.NewListType(3, iceberg.PrimitiveTypes.Binary, true)},
		iceberg.NestedField{ID: 4, Name: "attributes", Type: iceberg.NewMapType(5, iceberg.PrimitiveTypes.String, 6, iceberg.NewListType(7, iceberg.PrimitiveTypes.Binary, false), true)},
		iceberg.NestedField{ID: 8, Name: "details", Type: iceberg.NewStructType(
			iceberg.NestedField{ID: 9, Name: "name", Type: iceberg.PrimitiveTypes.String},
			iceberg.NestedField{ID: 10, Name: "items", Type: iceberg.NewListType(11, iceberg.PrimitiveTypes.String, false)},
		)},
	)
	tbl := newChangelogTable(t, 2, schema, iceberg.UnpartitionedSpec)
	tbl = writeNormalizationRows(t, tbl, `[
		{"id":1,"values":["YQ==","Yg=="],"attributes":[{"key":"x","value":["YQ==",null]}],"details":{"name":"old","items":["a","b"]}},
		{"id":2,"values":[],"attributes":[],"details":null},
		{"id":3,"values":null,"attributes":null,"details":{"name":null,"items":null}}
	]`, false)
	start := tbl.CurrentSnapshot().SnapshotID
	tbl = writeNormalizationRows(t, tbl, `[
		{"id":1,"values":["YQ==","Yg=="],"attributes":[{"key":"x","value":["YQ==",null]}],"details":{"name":"new","items":["a","b"]}},
		{"id":2,"values":[],"attributes":[],"details":null},
		{"id":4,"values":["Yw=="],"attributes":[],"details":null}
	]`, true)
	for _, early := range []bool{false, true} {
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		ctx := compute.WithAllocator(t.Context(), mem)
		_, records, err := tbl.NewIncrementalChangelogScan(table.WithArrowBatchSize(2)).FromSnapshotExclusive(start).
			ToNormalizedArrowRecords(ctx, table.ChangelogNormalizationOptions{ComputeUpdates: true, IdentifierFields: []string{"id"}})
		require.NoError(t, err)
		var ids []int64
		var values []map[string]any
		for rec, err := range records {
			require.NoError(t, err)
			ids = append(ids, rec.Column(0).(*array.Int64).Int64Values()...)
			require.False(t, rec.Schema().Field(1).Type.(*arrow.ListType).ElemField().Nullable)
			require.False(t, rec.Schema().Field(2).Type.(*arrow.MapType).ItemField().Nullable)
			// Materialize every nested value, including NULL structs followed by
			// another row, to exercise variable-width offsets in output builders.
			encoded, err := rec.MarshalJSON()
			require.NoError(t, err)
			var rows []map[string]any
			require.NoError(t, json.Unmarshal(encoded, &rows))
			for _, row := range rows {
				delete(row, table.ChangelogChangeTypeColumn)
				delete(row, table.ChangelogChangeOrdinalColumn)
				delete(row, table.ChangelogCommitSnapshotIDColumn)
				values = append(values, row)
			}
			rec.Release()
			if early {
				break
			}
		}
		if !early {
			require.Equal(t, []int64{1, 1, 3, 4}, ids)
			var expected []map[string]any
			require.NoError(t, json.Unmarshal([]byte(`[
				{"id":1,"values":["YQ==","Yg=="],"attributes":[{"key":"x","value":["YQ==",null]}],"details":{"name":"old","items":["a","b"]}},
				{"id":1,"values":["YQ==","Yg=="],"attributes":[{"key":"x","value":["YQ==",null]}],"details":{"name":"new","items":["a","b"]}},
				{"id":3,"values":null,"attributes":null,"details":{"name":null,"items":null}},
				{"id":4,"values":["Yw=="],"attributes":[],"details":null}
			]`), &expected))
			require.Equal(t, expected, values)
		}
		mem.AssertSize(t, 0)
	}
}

func TestNormalizedChangelogVariantExtractFilter(t *testing.T) {
	tbl, _ := setupVariantScanTable(t, "normalized_changes", []map[string]any{
		{"status": "active"}, {"status": "inactive"},
	})
	for _, tt := range []struct {
		value string
		want  []int64
	}{{"active", []int64{0}}, {"INSERT", nil}} {
		filter := iceberg.EqualTo(iceberg.Extract("payload", "$.status", iceberg.PrimitiveTypes.String), tt.value)
		mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
		_, records, err := tbl.NewIncrementalChangelogScan(table.WithRowFilter(filter), table.WithSelectedFields("id")).
			ToNormalizedArrowRecords(compute.WithAllocator(t.Context(), mem), table.ChangelogNormalizationOptions{})
		require.NoError(t, err)
		rows := collectNormalizedChangelogRows(t, records)
		var ids []int64
		for _, row := range rows {
			ids = append(ids, row.id)
		}
		require.Equal(t, tt.want, ids)
		mem.AssertSize(t, 0)
	}
}

func TestNormalizedChangelogLargeTypesAndLineage(t *testing.T) {
	tbl, start := normalizationTable(t, 3)
	_, records, err := tbl.NewIncrementalChangelogScan(
		table.WithOptions(iceberg.Properties{table.ScanOptionArrowUseLargeTypes: "true"}),
		table.WithRowLineage(),
	).FromSnapshotExclusive(start).ToNormalizedArrowRecords(t.Context(), table.ChangelogNormalizationOptions{ComputeUpdates: true})
	require.NoError(t, err)
	var got []changelogRow
	for rec, err := range records {
		require.NoError(t, err)
		require.Equal(t, arrow.LARGE_STRING, rec.Column(1).DataType().ID())
		require.Equal(t, arrow.LARGE_STRING, rec.Column(int(rec.NumCols())-3).DataType().ID())
		require.Len(t, rec.Schema().FieldIndices("_row_id"), 1)
		require.Len(t, rec.Schema().FieldIndices("_last_updated_sequence_number"), 1)
		one := func(yield func(arrow.RecordBatch, error) bool) { yield(rec, nil) }
		got = append(got, collectNormalizedChangelogRows(t, one)...)
	}
	require.Len(t, got, 4, "lineage changes must not prevent carryover removal")
}

func TestNormalizedChangelogRowLevelDeletes(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			tbl := appendChangelogRows(t, t.Context(), newChangelogMORTable(t, version))
			start := tbl.CurrentSnapshot().SnapshotID
			tbl = deleteChangelogID(t, t.Context(), tbl, 2)
			_, records, err := tbl.NewIncrementalChangelogScan().FromSnapshotExclusive(start).
				ToNormalizedArrowRecords(t.Context(), table.ChangelogNormalizationOptions{ComputeUpdates: true, IdentifierFields: []string{"id"}})
			require.NoError(t, err)
			got := collectNormalizedChangelogRows(t, records)
			require.Len(t, got, 1)
			require.Equal(t, int64(2), got[0].id)
			require.Equal(t, "DELETE", got[0].op)
		})
	}
}

func TestNormalizedChangelogEmptyResultsStillValidateRange(t *testing.T) {
	tbl, _ := normalizationTable(t, 2)
	for _, opts := range [][]table.ScanOption{
		{table.WithLimit(0)}, {table.WithRowFilter(iceberg.AlwaysFalse{})},
	} {
		_, _, err := tbl.NewIncrementalChangelogScan(opts...).ToSnapshot(-123).
			ToNormalizedArrowRecords(t.Context(), table.ChangelogNormalizationOptions{})
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	}
}

func TestNormalizedChangelogNestedCompositeIdentifiers(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "identity", Type: iceberg.NewStructType(
			iceberg.NestedField{ID: 3, Name: "code", Type: iceberg.PrimitiveTypes.String},
		)},
		iceberg.NestedField{ID: 4, Name: "tenant", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 5, Name: "data", Type: iceberg.PrimitiveTypes.String},
	)
	tbl := newChangelogTable(t, 2, schema, iceberg.UnpartitionedSpec)
	tbl = writeNormalizationRows(t, tbl, `[
		{"id":1,"identity":null,"tenant":"a","data":"old"},
		{"id":2,"identity":{"code":""},"tenant":"a","data":"same"},
		{"id":3,"identity":{"code":"x"},"tenant":"b","data":"changed key"}
	]`, false)
	start := tbl.CurrentSnapshot().SnapshotID
	tbl = writeNormalizationRows(t, tbl, `[
		{"id":4,"identity":null,"tenant":"a","data":"new"},
		{"id":2,"identity":{"code":""},"tenant":"a","data":"same"},
		{"id":3,"identity":{"code":"x"},"tenant":"c","data":"changed key"}
	]`, true)
	opts := table.ChangelogNormalizationOptions{ComputeUpdates: true, IdentifierFields: []string{"IDENTITY.CODE", "TENANT"}}
	_, _, err := tbl.NewIncrementalChangelogScan().ToNormalizedArrowRecords(t.Context(), opts)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	_, records, err := tbl.NewIncrementalChangelogScan(table.WithCaseSensitive(false), table.WithSelectedFields("id", "data")).
		FromSnapshotExclusive(start).ToNormalizedArrowRecords(t.Context(), opts)
	require.NoError(t, err)
	got := collectNormalizedChangelogRows(t, records)
	require.Equal(t, []changelogRow{
		{id: 1, data: "old", op: "UPDATE_BEFORE"},
		{id: 4, data: "new", op: "UPDATE_AFTER"},
		{id: 3, data: "changed key", op: "DELETE"},
		{id: 3, data: "changed key", op: "INSERT"},
	}, clearChangelogSnapshot(got))
}

func TestNormalizedChangelogReadFailureDoesNotEmitPartialSnapshot(t *testing.T) {
	tbl, start := normalizationTable(t, 3)
	scan := tbl.NewIncrementalChangelogScan(table.WithArrowBatchSize(1)).FromSnapshotExclusive(start)
	tasks, err := scan.PlanFiles(t.Context())
	require.NoError(t, err)
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	_, records, err := scan.ToNormalizedArrowRecords(compute.WithAllocator(t.Context(), mem), table.ChangelogNormalizationOptions{})
	require.NoError(t, err)
	for _, task := range tasks {
		if task.Operation() == table.ChangelogOpInsert {
			require.NoError(t, os.Remove(task.ScanTask().File.FilePath()))
		}
	}
	var readErr error
	for rec, err := range records {
		if err != nil {
			readErr = err

			break
		}
		rec.Release()
		t.Fatal("a partially read snapshot must not emit changes")
	}
	require.Error(t, readErr)
}

func TestNormalizedChangelogPhysicalAndVariantNullAreDifferent(t *testing.T) {
	schema := iceberg.NewSchemaWithIdentifiers(0, []int{1},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}},
	)
	tbl := newChangelogTable(t, 3, schema, iceberg.UnpartitionedSpec)
	write := func(present, overwrite bool) {
		arrowSchema, err := table.SchemaToArrowSchemaWithOptions(tbl.Schema(), table.ArrowSchemaOptions{})
		require.NoError(t, err)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		defer builder.Release()
		builder.Field(0).(*array.Int64Builder).Append(1)
		payload := builder.Field(1).(*extensions.VariantBuilder)
		if present {
			var b variant.Builder
			require.NoError(t, b.Append(nil))
			null, err := b.Build()
			require.NoError(t, err)
			payload.Append(null)
		} else {
			payload.AppendNull()
		}
		rec := builder.NewRecordBatch()
		defer rec.Release()
		reader, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{rec})
		require.NoError(t, err)
		defer reader.Release()
		if overwrite {
			tbl, err = tbl.Overwrite(t.Context(), reader, nil)
		} else {
			tbl, err = tbl.Append(t.Context(), reader, nil)
		}
		require.NoError(t, err)
	}
	write(false, false)
	start := tbl.CurrentSnapshot().SnapshotID
	write(true, true)
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	_, records, err := tbl.NewIncrementalChangelogScan(table.WithArrowBatchSize(1)).FromSnapshotExclusive(start).
		ToNormalizedArrowRecords(compute.WithAllocator(t.Context(), mem), table.ChangelogNormalizationOptions{ComputeUpdates: true})
	require.NoError(t, err)
	var nulls []bool
	var ops []string
	for rec, err := range records {
		require.NoError(t, err)
		nulls = append(nulls, rec.Column(1).(array.ExtensionArray).Storage().IsNull(0))
		ops = append(ops, rec.Column(int(rec.NumCols())-3).ValueStr(0))
		rec.Release()
	}
	require.Equal(t, []bool{true, false}, nulls)
	require.Equal(t, []string{"UPDATE_BEFORE", "UPDATE_AFTER"}, ops)
}

func TestNormalizedChangelogSchemaEvolutionAndDefaults(t *testing.T) {
	tbl, _ := normalizationTable(t, 3)
	start := tbl.CurrentSnapshot().SnapshotID
	tx := tbl.NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, false).
		AddColumn([]string{"extra"}, iceberg.PrimitiveTypes.String, "", false, iceberg.StringLiteral("x")).Commit())
	tbl, err := tx.Commit(t.Context())
	require.NoError(t, err)
	tbl = writeNormalizationRows(t, tbl, `[
		{"id":1,"data":"same","extra":"x"},
		{"id":2,"data":"newer","extra":"x"},
		{"id":4,"data":"added","extra":"x"}
	]`, true)
	_, records, err := tbl.NewIncrementalChangelogScan(table.WithSelectedFields("id", "extra")).FromSnapshotExclusive(start).
		ToNormalizedArrowRecords(t.Context(), table.ChangelogNormalizationOptions{ComputeUpdates: true})
	require.NoError(t, err)
	var ops []string
	for rec, err := range records {
		require.NoError(t, err)
		for row := range int(rec.NumRows()) {
			require.Equal(t, int64(2), rec.Column(0).(*array.Int64).Value(row))
			require.Equal(t, "x", rec.Column(1).ValueStr(row))
			ops = append(ops, rec.Column(int(rec.NumCols())-3).ValueStr(row))
		}
		rec.Release()
	}
	require.Equal(t, []string{"UPDATE_BEFORE", "UPDATE_AFTER"}, ops)
}

func TestNormalizedChangelogEqualityDeleteAndReplacementRows(t *testing.T) {
	tbl := appendChangelogRows(t, t.Context(), newChangelogMORTable(t, 2))
	start := tbl.CurrentSnapshot().SnapshotID
	dataSchema, err := table.SchemaToArrowSchemaWithOptions(tbl.Schema(), table.ArrowSchemaOptions{IncludeFieldIDs: true})
	require.NoError(t, err)
	dataPath := tbl.Location() + "/data/replacement.parquet"
	writeParquetFile(t, dataPath, dataSchema, `[{"id":2,"data":"new"}]`)
	dataStat, err := os.Stat(dataPath)
	require.NoError(t, err)
	data, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		dataPath, iceberg.ParquetFile, nil, nil, nil, 1, dataStat.Size())
	require.NoError(t, err)
	eqSchema, err := table.SchemaToArrowSchemaWithOptions(iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true}), table.ArrowSchemaOptions{IncludeFieldIDs: true})
	require.NoError(t, err)
	eqPath := tbl.Location() + "/data/equality-delete.parquet"
	writeParquetFile(t, eqPath, eqSchema, `[{"id":2}]`)
	eqStat, err := os.Stat(eqPath)
	require.NoError(t, err)
	deletes, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		eqPath, iceberg.ParquetFile, nil, nil, nil, 1, eqStat.Size())
	require.NoError(t, err)
	deletes.EqualityFieldIDs([]int{1})
	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddRows(data.Build()).AddDeletes(deletes.Build()).Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	_, records, err := tbl.NewIncrementalChangelogScan(table.WithSelectedFields("data")).FromSnapshotExclusive(start).
		ToNormalizedArrowRecords(t.Context(), table.ChangelogNormalizationOptions{ComputeUpdates: true, IdentifierFields: []string{"id"}})
	require.NoError(t, err)
	require.Equal(t, []changelogRow{
		{data: "b", op: "UPDATE_BEFORE"}, {data: "new", op: "UPDATE_AFTER"},
	}, clearChangelogSnapshot(collectNormalizedChangelogRows(t, records)))
}
