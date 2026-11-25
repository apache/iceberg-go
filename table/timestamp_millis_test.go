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
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	parquetschema "github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeInt64TimestampParquet(
	t *testing.T,
	path string,
	tsNode parquetschema.Node,
	props *parquet.WriterProperties,
	ids []int64,
	tsVals []int64,
) {
	t.Helper()
	require.Equal(t, len(ids), len(tsVals))

	idNode, err := parquetschema.NewPrimitiveNode("id", parquet.Repetitions.Required, parquet.Types.Int64, -1, 1)
	require.NoError(t, err)
	root, err := parquetschema.NewGroupNode("schema", parquet.Repetitions.Required,
		parquetschema.FieldList{idNode, tsNode}, -1)
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	f, err := os.Create(path)
	require.NoError(t, err)

	pw := file.NewParquetWriter(f, root, file.WithWriterProps(props))
	rgw, err := pw.AppendRowGroupChecked()
	require.NoError(t, err)

	cw, err := rgw.NextColumn()
	require.NoError(t, err)
	_, err = cw.(*file.Int64ColumnChunkWriter).WriteBatch(ids, nil, nil)
	require.NoError(t, err)
	require.NoError(t, cw.Close())

	cw, err = rgw.NextColumn()
	require.NoError(t, err)
	_, err = cw.(*file.Int64ColumnChunkWriter).WriteBatch(tsVals, nil, nil)
	require.NoError(t, err)
	require.NoError(t, cw.Close())

	require.NoError(t, rgw.Close())
	require.NoError(t, pw.Close())
}

func newTimestampMillisTestTable(t *testing.T, iceSchema *iceberg.Schema, spec *iceberg.PartitionSpec) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	meta, err := table.NewMetadata(iceSchema, spec, table.UnsortedSortOrder, location, iceberg.Properties{
		table.PropertyFormatVersion: "2",
	})
	require.NoError(t, err)

	return table.New(
		table.Identifier{"db", "ts_millis_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		&rowDeltaCatalog{metadata: meta},
	)
}

func TestAddDataFilesByPathTimestampMillisBoundsAndPartition(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(1500 * time.Millisecond)
	t2 := t0.Add(12 * time.Hour)
	const expectedDay = iceberg.Date(19723) // 2024-01-01 in days since 1970-01-01

	logicalNode := func(unit parquetschema.TimeUnitType, utc bool) parquetschema.Node {
		node, err := parquetschema.NewPrimitiveNodeLogical("ts", parquet.Repetitions.Required,
			parquetschema.NewTimestampLogicalType(utc, unit), parquet.Types.Int64, -1, 2)
		require.NoError(t, err)

		return node
	}
	convertedNode, err := parquetschema.NewPrimitiveNodeConverted("ts", parquet.Repetitions.Required,
		parquet.Types.Int64, parquetschema.ConvertedTypes.TimestampMillis, 0, 0, 0, 2)
	require.NoError(t, err)

	tests := []struct {
		name      string
		node      parquetschema.Node
		icebergTy iceberg.PrimitiveType
		perMilli  int64
	}{
		{"millis logical timestamp", logicalNode(parquetschema.TimeUnitMillis, false), iceberg.PrimitiveTypes.Timestamp, 1},
		{"millis logical timestamptz", logicalNode(parquetschema.TimeUnitMillis, true), iceberg.PrimitiveTypes.TimestampTz, 1},
		{"millis converted type only", convertedNode, iceberg.PrimitiveTypes.TimestampTz, 1},
		{"micros control timestamp", logicalNode(parquetschema.TimeUnitMicros, false), iceberg.PrimitiveTypes.Timestamp, 1000},
		{"micros control timestamptz", logicalNode(parquetschema.TimeUnitMicros, true), iceberg.PrimitiveTypes.TimestampTz, 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer alloc.AssertSize(t, 0)
			ctx := compute.WithAllocator(t.Context(), alloc)

			iceSchema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
				iceberg.NestedField{ID: 2, Name: "ts", Type: tt.icebergTy, Required: true},
			)
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{2},
				FieldID:   1000,
				Name:      "ts_day",
				Transform: iceberg.DayTransform{},
			})
			tbl := newTimestampMillisTestTable(t, iceSchema, &spec)

			dataPath := filepath.Join(tbl.Location(), "data", "data-001.parquet")
			writeInt64TimestampParquet(t, dataPath, tt.node,
				parquet.NewWriterProperties(parquet.WithStats(true)),
				[]int64{1, 2, 3},
				[]int64{t0.UnixMilli() * tt.perMilli, t1.UnixMilli() * tt.perMilli, t2.UnixMilli() * tt.perMilli},
			)

			tx := tbl.NewTransaction()
			require.NoError(t, tx.AddDataFilesByPath(ctx, []string{filepath.ToSlash(dataPath)}, nil, false))
			tbl, err := tx.Commit(ctx)
			require.NoError(t, err)

			tasks, err := tbl.Scan().PlanFiles(ctx)
			require.NoError(t, err)
			require.Len(t, tasks, 1)

			df := tasks[0].File
			assert.Equal(t, expectedDay, df.Partition()[1000])

			loLit, err := iceberg.LiteralFromBytes(tt.icebergTy, df.LowerBoundValues()[2])
			require.NoError(t, err)
			hiLit, err := iceberg.LiteralFromBytes(tt.icebergTy, df.UpperBoundValues()[2])
			require.NoError(t, err)
			assert.Equal(t, iceberg.Timestamp(t0.UnixMicro()), loLit.(iceberg.TypedLiteral[iceberg.Timestamp]).Value())
			assert.Equal(t, iceberg.Timestamp(t2.UnixMicro()), hiLit.(iceberg.TypedLiteral[iceberg.Timestamp]).Value())
		})
	}

	t.Run("overflow omits manifest bounds", func(t *testing.T) {
		alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer alloc.AssertSize(t, 0)
		ctx := compute.WithAllocator(t.Context(), alloc)

		iceSchema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
		)
		tbl := newTimestampMillisTestTable(t, iceSchema, iceberg.UnpartitionedSpec)

		const hugeMillis = math.MaxInt64/1000 + 1
		dataPath := filepath.Join(tbl.Location(), "data", "overflow.parquet")
		writeInt64TimestampParquet(t, dataPath, logicalNode(parquetschema.TimeUnitMillis, false),
			parquet.NewWriterProperties(parquet.WithStats(true)),
			[]int64{1, 2},
			[]int64{0, hugeMillis},
		)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddDataFilesByPath(ctx, []string{filepath.ToSlash(dataPath)}, nil, false))
		tbl, err := tx.Commit(ctx)
		require.NoError(t, err)

		tasks, err := tbl.Scan().PlanFiles(ctx)
		require.NoError(t, err)
		require.Len(t, tasks, 1)

		df := tasks[0].File
		assert.NotContains(t, df.LowerBoundValues(), 2)
		assert.NotContains(t, df.UpperBoundValues(), 2)
		assert.Contains(t, df.LowerBoundValues(), 1)
		assert.Contains(t, df.UpperBoundValues(), 1)
	})
}

func writeArrowParquetFile(
	t *testing.T,
	alloc memory.Allocator,
	path string,
	arrowSc *arrow.Schema,
	rec arrow.RecordBatch,
	props *parquet.WriterProperties,
) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	f, err := os.Create(path)
	require.NoError(t, err)

	tbl := array.NewTableFromRecords(arrowSc, []arrow.RecordBatch{rec})
	defer tbl.Release()

	require.NoError(t, pqarrow.WriteTable(tbl, f, rec.NumRows(), props,
		pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(alloc))))
}

func TestScanTimestampMillisRowFiltersAndProjection(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(1500 * time.Millisecond)
	t2 := t0.Add(12 * time.Hour)

	fieldIDMeta := func(id string) arrow.Metadata {
		return arrow.MetadataFrom(map[string]string{"PARQUET:field_id": id})
	}

	typeCases := []struct {
		name      string
		unit      arrow.TimeUnit
		tz        string
		icebergTy iceberg.PrimitiveType
	}{
		{"millis timestamp", arrow.Millisecond, "", iceberg.PrimitiveTypes.Timestamp},
		{"millis timestamptz", arrow.Millisecond, "UTC", iceberg.PrimitiveTypes.TimestampTz},
		{"micros control timestamp", arrow.Microsecond, "", iceberg.PrimitiveTypes.Timestamp},
		{"micros control timestamptz", arrow.Microsecond, "UTC", iceberg.PrimitiveTypes.TimestampTz},
	}

	propCases := []struct {
		name     string
		hasBloom bool
		props    *parquet.WriterProperties
	}{
		{
			name: "no stats dict bloom",
			props: parquet.NewWriterProperties(
				parquet.WithStats(false),
				parquet.WithDictionaryDefault(false),
			),
		},
		{
			name:     "with stats dict bloom",
			hasBloom: true,
			props: parquet.NewWriterProperties(
				parquet.WithStats(true),
				parquet.WithDictionaryDefault(true),
				parquet.WithDictionaryCostFallbackFor("ts", false),
				parquet.WithBloomFilterEnabledFor("ts", true),
			),
		},
	}

	for _, tc := range typeCases {
		for _, pc := range propCases {
			t.Run(tc.name+"/"+pc.name, func(t *testing.T) {
				alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
				if tc.unit != arrow.Microsecond || !pc.hasBloom {
					// arrow-go's bloom filter reader allocates its bitset from the
					// reader allocator with no Release method.
					defer alloc.AssertSize(t, 0)
				}
				ctx := compute.WithAllocator(t.Context(), alloc)

				iceSchema := iceberg.NewSchema(0,
					iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
					iceberg.NestedField{ID: 2, Name: "ts", Type: tc.icebergTy, Required: false},
				)
				tbl := newTimestampMillisTestTable(t, iceSchema, iceberg.UnpartitionedSpec)

				arrowSc := arrow.NewSchema([]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false, Metadata: fieldIDMeta("1")},
					{Name: "ts", Type: &arrow.TimestampType{Unit: tc.unit, TimeZone: tc.tz}, Nullable: true, Metadata: fieldIDMeta("2")},
				}, nil)

				toUnit := func(tm time.Time) arrow.Timestamp {
					if tc.unit == arrow.Millisecond {
						return arrow.Timestamp(tm.UnixMilli())
					}

					return arrow.Timestamp(tm.UnixMicro())
				}

				rb := array.NewRecordBuilder(alloc, arrowSc)
				rb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4}, nil)
				tsB := rb.Field(1).(*array.TimestampBuilder)
				tsB.Append(toUnit(t0))
				tsB.Append(toUnit(t1))
				tsB.Append(toUnit(t2))
				tsB.AppendNull()
				rec := rb.NewRecordBatch()
				rb.Release()

				dataPath := filepath.Join(tbl.Location(), "data", "data-001.parquet")
				writeArrowParquetFile(t, alloc, dataPath, arrowSc, rec, pc.props)
				rec.Release()

				tx := tbl.NewTransaction()
				require.NoError(t, tx.AddDataFilesByPath(ctx, []string{filepath.ToSlash(dataPath)}, nil, false))
				tbl, err := tx.Commit(ctx)
				require.NoError(t, err)

				type row struct {
					id   int64
					ts   int64
					null bool
				}
				scanRows := func(filter iceberg.BooleanExpression) []row {
					opts := []table.ScanOption{table.WithSelectedFields("id", "ts")}
					if filter != nil {
						opts = append(opts, table.WithRowFilter(filter))
					}
					sc, itr, err := tbl.Scan(opts...).ToArrowRecords(ctx)
					require.NoError(t, err)
					tsFieldType := sc.Field(1).Type.(*arrow.TimestampType)
					assert.Equal(t, arrow.Microsecond, tsFieldType.Unit)
					assert.Equal(t, tc.tz, tsFieldType.TimeZone)

					var out []row
					for batch, err := range itr {
						require.NoError(t, err)
						idCol := batch.Column(0).(*array.Int64)
						tsCol := batch.Column(1).(*array.Timestamp)
						assert.Equal(t, arrow.Microsecond, tsCol.DataType().(*arrow.TimestampType).Unit)
						for i := range int(batch.NumRows()) {
							r := row{id: idCol.Value(i), null: tsCol.IsNull(i)}
							if !r.null {
								r.ts = int64(tsCol.Value(i))
							}
							out = append(out, r)
						}
						batch.Release()
					}

					return out
				}

				ref := iceberg.Reference("ts")
				assert.Equal(t, []row{
					{id: 1, ts: t0.UnixMicro()},
					{id: 2, ts: t1.UnixMicro()},
					{id: 3, ts: t2.UnixMicro()},
					{id: 4, null: true},
				}, scanRows(nil))
				assert.Equal(t, []row{
					{id: 2, ts: t1.UnixMicro()},
				}, scanRows(iceberg.EqualTo(ref, iceberg.Timestamp(t1.UnixMicro()))))
				assert.Equal(t, []row{
					{id: 2, ts: t1.UnixMicro()},
					{id: 3, ts: t2.UnixMicro()},
				}, scanRows(iceberg.GreaterThanEqual(ref, iceberg.Timestamp(t1.UnixMicro()))))
				assert.Equal(t, []row{
					{id: 1, ts: t0.UnixMicro()},
				}, scanRows(iceberg.LessThan(ref, iceberg.Timestamp(t1.UnixMicro()))))
				assert.Equal(t, []row{
					{id: 4, null: true},
				}, scanRows(iceberg.IsNull(ref)))
				assert.Equal(t, []row{
					{id: 1, ts: t0.UnixMicro()},
					{id: 2, ts: t1.UnixMicro()},
					{id: 3, ts: t2.UnixMicro()},
				}, scanRows(iceberg.NotNull(ref)))
			})
		}
	}
}

func TestScanTimestampMillisNestedStruct(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(1500 * time.Millisecond)
	t2 := t0.Add(12 * time.Hour)

	fieldIDMeta := func(id string) arrow.Metadata {
		return arrow.MetadataFrom(map[string]string{"PARQUET:field_id": id})
	}

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), alloc)

	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{
			ID:       2,
			Name:     "s",
			Required: false,
			Type: iceberg.NewStructType(
				iceberg.NestedField{ID: 3, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp, Required: false},
			),
		},
	)
	tbl := newTimestampMillisTestTable(t, iceSchema, iceberg.UnpartitionedSpec)

	arrowSc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false, Metadata: fieldIDMeta("1")},
		{
			Name: "s",
			Type: arrow.StructOf(
				arrow.Field{
					Name:     "ts",
					Type:     &arrow.TimestampType{Unit: arrow.Millisecond},
					Nullable: true,
					Metadata: fieldIDMeta("3"),
				},
			),
			Nullable: true,
			Metadata: fieldIDMeta("2"),
		},
	}, nil)

	rb := array.NewRecordBuilder(alloc, arrowSc)
	rb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	sb := rb.Field(1).(*array.StructBuilder)
	for _, tm := range []time.Time{t0, t1, t2} {
		sb.Append(true)
		sb.FieldBuilder(0).(*array.TimestampBuilder).Append(arrow.Timestamp(tm.UnixMilli()))
	}
	rec := rb.NewRecordBatch()
	rb.Release()

	dataPath := filepath.Join(tbl.Location(), "data", "nested.parquet")
	writeArrowParquetFile(t, alloc, dataPath, arrowSc, rec,
		parquet.NewWriterProperties(parquet.WithStats(false), parquet.WithDictionaryDefault(false)))
	rec.Release()

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(ctx, []string{filepath.ToSlash(dataPath)}, nil, false))
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)

	_, itr, err := tbl.Scan(
		table.WithSelectedFields("id", "s"),
		table.WithRowFilter(iceberg.GreaterThanEqual(iceberg.Reference("s.ts"), iceberg.Timestamp(t1.UnixMicro()))),
	).ToArrowRecords(ctx)
	require.NoError(t, err)

	var ids, vals []int64
	for batch, err := range itr {
		require.NoError(t, err)
		idCol := batch.Column(0).(*array.Int64)
		tsCol := batch.Column(1).(*array.Struct).Field(0).(*array.Timestamp)
		assert.Equal(t, arrow.Microsecond, tsCol.DataType().(*arrow.TimestampType).Unit)
		for i := range int(batch.NumRows()) {
			ids = append(ids, idCol.Value(i))
			vals = append(vals, int64(tsCol.Value(i)))
		}
		batch.Release()
	}

	assert.Equal(t, []int64{2, 3}, ids)
	assert.Equal(t, []int64{t1.UnixMicro(), t2.UnixMicro()}, vals)
}

func TestCommitEqualityDeletesTimestampMillis(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(1500 * time.Millisecond)
	t2 := t0.Add(12 * time.Hour)

	fieldIDMeta := func(id string) arrow.Metadata {
		return arrow.MetadataFrom(map[string]string{"PARQUET:field_id": id})
	}

	tests := []struct {
		name      string
		tz        string
		icebergTy iceberg.PrimitiveType
	}{
		{"timestamp", "", iceberg.PrimitiveTypes.Timestamp},
		{"timestamptz", "UTC", iceberg.PrimitiveTypes.TimestampTz},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer alloc.AssertSize(t, 0)
			ctx := compute.WithAllocator(t.Context(), alloc)

			iceSchema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
				iceberg.NestedField{ID: 2, Name: "ts", Type: tt.icebergTy, Required: true},
			)
			tbl := newTimestampMillisTestTable(t, iceSchema, iceberg.UnpartitionedSpec)

			dataSc := arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false, Metadata: fieldIDMeta("1")},
				{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: tt.tz}, Nullable: false, Metadata: fieldIDMeta("2")},
			}, nil)

			rb := array.NewRecordBuilder(alloc, dataSc)
			rb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
			rb.Field(1).(*array.TimestampBuilder).AppendValues(
				[]arrow.Timestamp{arrow.Timestamp(t0.UnixMilli()), arrow.Timestamp(t1.UnixMilli()), arrow.Timestamp(t2.UnixMilli())}, nil)
			dataRec := rb.NewRecordBatch()
			rb.Release()

			dataPath := filepath.Join(tbl.Location(), "data", "data-001.parquet")
			writeArrowParquetFile(t, alloc, dataPath, dataSc, dataRec,
				parquet.NewWriterProperties(parquet.WithStats(true)))
			dataRec.Release()

			tx := tbl.NewTransaction()
			require.NoError(t, tx.AddDataFilesByPath(ctx, []string{filepath.ToSlash(dataPath)}, nil, false))
			tbl, err := tx.Commit(ctx)
			require.NoError(t, err)

			delSc, err := table.SchemaToArrowSchema(
				iceberg.NewSchema(0, iceberg.NestedField{ID: 2, Name: "ts", Type: tt.icebergTy, Required: true}),
				nil, true, false)
			require.NoError(t, err)

			delB := array.NewRecordBuilder(alloc, delSc)
			delB.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(t1.UnixMicro()))
			delRec := delB.NewRecordBatch()
			delB.Release()

			delItr := func(yield func(arrow.RecordBatch, error) bool) {
				yield(delRec, nil)
			}

			tx2 := tbl.NewTransaction()
			require.NoError(t, tx2.CommitEqualityDeletes(ctx, []int{2}, delItr, nil))
			delRec.Release()
			tbl, err = tx2.Commit(ctx)
			require.NoError(t, err)

			_, itr, err := tbl.Scan(table.WithSelectedFields("id", "ts")).ToArrowRecords(ctx)
			require.NoError(t, err)

			var ids, vals []int64
			for batch, err := range itr {
				require.NoError(t, err)
				idCol := batch.Column(0).(*array.Int64)
				tsCol := batch.Column(1).(*array.Timestamp)
				for i := range int(batch.NumRows()) {
					ids = append(ids, idCol.Value(i))
					vals = append(vals, int64(tsCol.Value(i)))
				}
				batch.Release()
			}

			assert.Equal(t, []int64{1, 3}, ids)
			assert.Equal(t, []int64{t0.UnixMicro(), t2.UnixMicro()}, vals)
		})
	}
}

func TestScanTimestampMillisOverflowReturnsError(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), alloc)

	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
	)
	tbl := newTimestampMillisTestTable(t, iceSchema, iceberg.UnpartitionedSpec)

	tsNode, err := parquetschema.NewPrimitiveNodeLogical("ts", parquet.Repetitions.Required,
		parquetschema.NewTimestampLogicalType(false, parquetschema.TimeUnitMillis), parquet.Types.Int64, -1, 2)
	require.NoError(t, err)

	dataPath := filepath.Join(tbl.Location(), "data", "overflow.parquet")
	writeInt64TimestampParquet(t, dataPath, tsNode,
		parquet.NewWriterProperties(parquet.WithStats(false)),
		[]int64{1, 2},
		[]int64{1_000, math.MaxInt64/1000 + 1},
	)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(ctx, []string{filepath.ToSlash(dataPath)}, nil, false))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)

	_, itr, err := tbl.Scan(table.WithSelectedFields("id", "ts")).ToArrowRecords(ctx)
	require.NoError(t, err)

	var scanErr error
	for batch, err := range itr {
		if err != nil {
			scanErr = err

			break
		}
		batch.Release()
	}
	require.ErrorIs(t, scanErr, iceberg.ErrInvalidArgument)
}

func TestScanTimestampMillisWithDictionaryColumns(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(1500 * time.Millisecond)
	t2 := t0.Add(12 * time.Hour)

	fieldIDMeta := func(id string) arrow.Metadata {
		return arrow.MetadataFrom(map[string]string{"PARQUET:field_id": id})
	}

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), alloc)

	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "label", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 3, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
	)
	tbl := newTimestampMillisTestTable(t, iceSchema, iceberg.UnpartitionedSpec)

	arrowSc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false, Metadata: fieldIDMeta("1")},
		{Name: "label", Type: arrow.BinaryTypes.String, Nullable: false, Metadata: fieldIDMeta("2")},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Millisecond}, Nullable: false, Metadata: fieldIDMeta("3")},
	}, nil)

	rb := array.NewRecordBuilder(alloc, arrowSc)
	rb.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	rb.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b", "a"}, nil)
	rb.Field(2).(*array.TimestampBuilder).AppendValues(
		[]arrow.Timestamp{
			arrow.Timestamp(t0.UnixMilli()),
			arrow.Timestamp(t1.UnixMilli()),
			arrow.Timestamp(t2.UnixMilli()),
		}, nil)
	rec := rb.NewRecordBatch()
	rb.Release()

	dataPath := filepath.Join(tbl.Location(), "data", "dict_and_millis.parquet")
	writeArrowParquetFile(t, alloc, dataPath, arrowSc, rec,
		parquet.NewWriterProperties(parquet.WithDictionaryDefault(true)))
	rec.Release()

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFilesByPath(ctx, []string{filepath.ToSlash(dataPath)}, nil, false))
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)

	outSchema, itr, err := tbl.Scan(
		table.WithSelectedFields("id", "label", "ts"),
		table.WithDictionaryColumns("label"),
		table.WithRowFilter(iceberg.GreaterThanEqual(iceberg.Reference("ts"), iceberg.Timestamp(t1.UnixMicro()))),
	).ToArrowRecords(ctx)
	require.NoError(t, err)

	assert.Equal(t, arrow.DICTIONARY, outSchema.Field(1).Type.ID())
	tsType, ok := outSchema.Field(2).Type.(*arrow.TimestampType)
	require.True(t, ok)
	assert.Equal(t, arrow.Microsecond, tsType.Unit)

	var ids, vals []int64
	var labels []string
	for batch, err := range itr {
		require.NoError(t, err)
		idCol := batch.Column(0).(*array.Int64)
		labelCol, ok := batch.Column(1).(*array.Dictionary)
		require.True(t, ok, "expected dictionary-encoded label column, got %T", batch.Column(1))
		tsCol := batch.Column(2).(*array.Timestamp)
		assert.Equal(t, arrow.Microsecond, tsCol.DataType().(*arrow.TimestampType).Unit)
		for i := range int(batch.NumRows()) {
			ids = append(ids, idCol.Value(i))
			labels = append(labels, labelCol.ValueStr(i))
			vals = append(vals, int64(tsCol.Value(i)))
		}
		batch.Release()
	}

	assert.Equal(t, []int64{2, 3}, ids)
	assert.Equal(t, []string{"b", "a"}, labels)
	assert.Equal(t, []int64{t1.UnixMicro(), t2.UnixMicro()}, vals)
}
