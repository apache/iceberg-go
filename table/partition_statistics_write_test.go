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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/twmb/avro/ocf"
)

func statsTestPartitionType() *iceberg.StructType {
	return &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 1000, Name: "day", Type: iceberg.PrimitiveTypes.Int32},
		{ID: 1001, Name: "region", Type: iceberg.PrimitiveTypes.String},
	}}
}

func TestPartitionStatisticsSchemaV1(t *testing.T) {
	sc := PartitionStatisticsSchemaV1(statsTestPartitionType())
	require.Equal(t, []string{
		"partition", "spec_id", "data_record_count", "data_file_count",
		"total_data_file_size_in_bytes", "total_record_count",
	}, testFieldNames(sc))
	fields := sc.Fields()
	require.Equal(t, []int{1, 2, 3, 4, 5, 10}, []int{
		fields[0].ID, fields[1].ID, fields[2].ID,
		fields[3].ID, fields[4].ID, fields[5].ID,
	})
	for index := range 5 {
		require.True(t, fields[index].Required)
	}
	require.False(t, fields[5].Required)
	require.Equal(t, 1000, fields[0].Type.(*iceberg.StructType).FieldList[0].ID)
}

func TestPartitionStatisticValuesNullFirstNumericAndBinary(t *testing.T) {
	for _, tc := range []struct {
		a, b any
		want int
	}{
		{nil, int32(2), -1},
		{int32(2), nil, 1},
		{nil, nil, 0},
		{int32(-2), int32(10), -1},
		{int64(10), int64(-2), 1},
		{uint64(1), uint64(2), -1},
		{float64(-1.5), float64(0), -1},
		{float64(math.Copysign(0, -1)), float64(0), -1},
		{float64(0), float64(math.Copysign(0, -1)), 1},
		{float32(math.Copysign(0, -1)), float32(0), -1},
		{math.Inf(-1), -math.MaxFloat64, -1},
		{math.NaN(), math.Inf(1), 1},
		{math.Inf(1), math.NaN(), -1},
		{math.NaN(), math.NaN(), 0},
		{float32(math.NaN()), float32(math.NaN()), 0},
		{float32(math.NaN()), float32(math.Inf(1)), 1},
		{false, true, -1},
		{"10", "2", -1},
		{[]byte{0, 255}, []byte{1}, -1},
		{[2]byte{0, 255}, [2]byte{1, 0}, -1},
		{[]byte{1, 2}, []byte{1, 2}, 0},
		{uuid.MustParse("80000000-0000-0000-0000-000000000000"), uuid.MustParse("7fffffff-ffff-ffff-ffff-ffffffffffff"), -1},
		{uuid.MustParse("00000000-0000-0000-8000-000000000000"), uuid.MustParse("00000000-0000-0000-7fff-ffffffffffff"), -1},
		{iceberg.Decimal{Val: decimal128.FromI64(-120), Scale: 2}, iceberg.Decimal{Val: decimal128.FromI64(30), Scale: 2}, -1},
		{iceberg.Decimal{Val: decimal128.FromI64(30), Scale: 2}, iceberg.Decimal{Val: decimal128.FromI64(30), Scale: 2}, 0},
		{iceberg.Decimal{Val: decimal128.FromI64(120), Scale: 2}, iceberg.Decimal{Val: decimal128.FromI64(30), Scale: 2}, 1},
	} {
		got, err := comparePartitionStatisticValues(tc.a, tc.b)
		require.NoError(t, err, "compare %v to %v", tc.a, tc.b)
		require.Equal(t, tc.want, got)
	}
	_, err := comparePartitionStatisticValues(struct{ X int }{}, struct{ X int }{})
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)
	_, err = comparePartitionStatisticValues(
		iceberg.Decimal{Val: decimal128.FromI64(10), Scale: 1},
		iceberg.Decimal{Val: decimal128.FromI64(10), Scale: 2})
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestSortPartitionStatisticsByTupleThenSpecID(t *testing.T) {
	pt := statsTestPartitionType()
	rows := []*inspectPartitionAggregate{
		{partition: map[int]any{1000: int32(10), 1001: "z"}, specID: 3},
		{partition: map[int]any{1000: int32(2), 1001: "z"}, specID: 4},
		{partition: map[int]any{1000: int32(-1), 1001: "a"}, specID: 1},
		{partition: map[int]any{1000: nil, 1001: "z"}, specID: 2},
		{partition: map[int]any{1000: int32(2), 1001: "z"}, specID: 1},
		{partition: map[int]any{1000: int32(2), 1001: "a"}, specID: 5},
	}
	require.NoError(t, sortPartitionStatistics(rows, pt))
	require.Equal(t, []int32{2, 1, 5, 1, 4, 3}, []int32{
		rows[0].specID, rows[1].specID, rows[2].specID,
		rows[3].specID, rows[4].specID, rows[5].specID,
	})
	require.Nil(t, rows[0].partition[1000])
	require.EqualValues(t, -1, rows[1].partition[1000])
	require.EqualValues(t, 10, rows[5].partition[1000])
}

func TestSortPartitionStatisticsFloatAndUUID(t *testing.T) {
	t.Run("float", func(t *testing.T) {
		pt := &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 1000, Name: "value", Type: iceberg.PrimitiveTypes.Float64},
		}}
		rows := []*inspectPartitionAggregate{
			{partition: map[int]any{1000: math.NaN()}, specID: 6},
			{partition: map[int]any{1000: float64(0)}, specID: 3},
			{partition: map[int]any{1000: math.Inf(1)}, specID: 5},
			{partition: map[int]any{1000: math.Copysign(0, -1)}, specID: 2},
			{partition: map[int]any{1000: math.Inf(-1)}, specID: 1},
			{partition: map[int]any{1000: math.Float64frombits(0xfff8000000000001)}, specID: 7},
		}
		require.NoError(t, sortPartitionStatistics(rows, pt))
		require.Equal(t, []int32{1, 2, 3, 5, 6, 7}, []int32{
			rows[0].specID, rows[1].specID, rows[2].specID,
			rows[3].specID, rows[4].specID, rows[5].specID,
		})
		require.True(t, math.Signbit(rows[1].partition[1000].(float64)))
		require.False(t, math.Signbit(rows[2].partition[1000].(float64)))
		require.True(t, math.IsNaN(rows[4].partition[1000].(float64)))
	})
	t.Run("uuid", func(t *testing.T) {
		pt := &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 1000, Name: "value", Type: iceberg.PrimitiveTypes.UUID},
		}}
		rows := []*inspectPartitionAggregate{
			{partition: map[int]any{1000: uuid.MustParse("7fffffff-ffff-ffff-0000-000000000000")}, specID: 4},
			{partition: map[int]any{1000: uuid.MustParse("00000000-0000-0000-7fff-ffffffffffff")}, specID: 3},
			{partition: map[int]any{1000: uuid.MustParse("80000000-0000-0000-0000-000000000000")}, specID: 1},
			{partition: map[int]any{1000: uuid.MustParse("00000000-0000-0000-8000-000000000000")}, specID: 2},
		}
		require.NoError(t, sortPartitionStatistics(rows, pt))
		require.Equal(t, []int32{1, 2, 3, 4}, []int32{
			rows[0].specID, rows[1].specID, rows[2].specID, rows[3].specID,
		})
	})
}

func TestPartitionStatisticsParquetWrite(t *testing.T) {
	ctx := t.Context()
	fs := iceio.NewMemFS()
	path := "mem://bucket/table/metadata/v1-stats.parquet"
	rows := []*inspectPartitionAggregate{
		{
			specID: 0, partition: map[int]any{1000: int32(-3), 1001: "a"},
			dataRecordCount: 17, dataFileCount: 2, dataFileSize: 2048,
		},
		{
			specID: 1, partition: map[int]any{1000: int32(10), 1001: nil},
			dataRecordCount: 3, dataFileCount: 1, dataFileSize: 512,
		},
	}
	size, err := writePartitionStatisticsParquet(ctx, fs, path, statsTestPartitionType(), rows)
	require.NoError(t, err)
	require.Positive(t, size)

	f, err := fs.Open(ctx, path)
	require.NoError(t, err)
	data, err := io.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.EqualValues(t, len(data), size)
	require.True(t, bytes.HasPrefix(data, []byte("PAR1")))
	require.True(t, bytes.HasSuffix(data, []byte("PAR1")))
	reader, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer reader.Close()
	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	table, err := arrowReader.ReadTable(ctx)
	require.NoError(t, err)
	defer table.Release()
	require.EqualValues(t, 2, table.NumRows())
	partitions := table.Column(0).Data().Chunk(0).(*array.Struct)
	require.Equal(t, int32(-3), partitions.Field(0).(*array.Int32).Value(0))
	require.Equal(t, int32(10), partitions.Field(0).(*array.Int32).Value(1))
	require.True(t, partitions.Field(1).IsNull(1))
	require.Equal(t, int64(17), table.Column(2).Data().Chunk(0).(*array.Int64).Value(0))
	require.Equal(t, int64(3), table.Column(5).Data().Chunk(0).(*array.Int64).Value(1))

	// Cancellation must not leave an incomplete metadata file.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	failedPath := "mem://bucket/table/metadata/cancelled.parquet"
	_, err = writePartitionStatisticsParquet(cancelled, fs, failedPath, statsTestPartitionType(), rows)
	require.ErrorIs(t, err, context.Canceled)
	_, err = fs.Open(ctx, failedPath)
	require.Error(t, err)
}

func TestCollectPartitionStatisticsFromLiveManifests(t *testing.T) {
	spec := partitionedSpec()
	const snapshotID = int64(1)
	sequenceNumber := int64(1)
	entry := func(name string, partition int32, count int64, status iceberg.ManifestEntryStatus) iceberg.ManifestEntry {
		file := newTestDataFileWithCount(
			t, spec, "mem://default/table-location/data/"+name+".parquet",
			map[int]any{1000: partition}, count)

		return iceberg.NewManifestEntry(status, new(snapshotID),
			&sequenceNumber, &sequenceNumber, file)
	}
	tbl := inspectFilesTableWithManifests(t, spec, inspectManifestSpec{
		content: iceberg.ManifestContentData,
		entries: []iceberg.ManifestEntry{
			entry("first", 2, 5, iceberg.EntryStatusADDED),
			entry("second", 2, 7, iceberg.EntryStatusADDED),
			entry("third", 10, 3, iceberg.EntryStatusADDED),
			entry("removed", 10, 100, iceberg.EntryStatusDELETED),
		},
	})
	ctx := t.Context()
	fs, err := tbl.FS(ctx)
	require.NoError(t, err)
	pt, err := inspectPartitionType(tbl.Metadata())
	require.NoError(t, err)

	rows, err := collectPartitionStatistics(ctx, fs, *tbl.CurrentSnapshot(), pt, tbl.Metadata().PartitionSpecs())
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.NoError(t, sortPartitionStatistics(rows, pt))
	require.Equal(t, int32(2), rows[0].partition[1000])
	require.Equal(t, int64(12), rows[0].dataRecordCount)
	require.Equal(t, int32(2), rows[0].dataFileCount)
	require.Equal(t, int32(10), rows[1].partition[1000])
	require.Equal(t, int64(3), rows[1].dataRecordCount)
	require.Equal(t, int32(1), rows[1].dataFileCount)
}

func TestCollectPartitionStatisticsDecimalFromManifest(t *testing.T) {
	decimalType := iceberg.MustDecimalTypeOf(10, 2)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "amount", Transform: iceberg.IdentityTransform{},
	})
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "amount", Type: decimalType, Required: true,
	})
	partitionType := &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 1000, Name: "amount", Type: decimalType},
	}}
	amount := iceberg.Decimal{Val: decimal128.FromI64(1234), Scale: 2}
	const snapshotID = int64(7)
	files := []iceberg.DataFile{
		newTestDataFileWithCount(t, spec, "mem://stats/decimal-a.parquet", map[int]any{1000: amount}, 4),
		newTestDataFileWithCount(t, spec, "mem://stats/decimal-b.parquet", map[int]any{1000: iceberg.DecimalLiteral(amount)}, 6),
	}
	entries := make([]iceberg.ManifestEntry, 0, len(files))
	for _, df := range files {
		entries = append(entries, iceberg.NewManifestEntry(iceberg.EntryStatusADDED, new(snapshotID), nil, nil, df))
	}

	const manifestPath = "mem://stats/decimal-manifest.avro"
	var encoded bytes.Buffer
	_, err := iceberg.WriteManifest(manifestPath, &encoded, 1, spec, schema, snapshotID, entries)
	require.NoError(t, err)
	fs := iceio.NewMemFS()
	require.NoError(t, fs.WriteFile(t.Context(), manifestPath, encoded.Bytes()))

	snapshot := Snapshot{SnapshotID: snapshotID, ManifestLocations: []string{manifestPath}}
	rows, err := collectPartitionStatistics(t.Context(), fs, snapshot, partitionType, []iceberg.PartitionSpec{spec})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, amount, rows[0].partition[1000])
	require.Equal(t, int64(10), rows[0].dataRecordCount)
	require.Equal(t, int32(2), rows[0].dataFileCount)
	require.NoError(t, sortPartitionStatistics(rows, partitionType))

	const statsPath = "mem://stats/decimal-stats.parquet"
	_, err = writePartitionStatisticsParquet(t.Context(), fs, statsPath, partitionType, rows)
	require.NoError(t, err)
	stored, err := fs.Open(t.Context(), statsPath)
	require.NoError(t, err)
	content, err := io.ReadAll(stored)
	require.NoError(t, err)
	require.NoError(t, stored.Close())
	reader, err := file.NewParquetReader(bytes.NewReader(content))
	require.NoError(t, err)
	defer reader.Close()
	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	result, err := arrowReader.ReadTable(t.Context())
	require.NoError(t, err)
	defer result.Release()
	require.EqualValues(t, 1, result.NumRows())
	partition := result.Column(0).Data().Chunk(0).(*array.Struct)
	require.Equal(t, amount.Val, partition.Field(0).(*array.Decimal128).Value(0))
}

func TestTransactionPartitionStatisticsRegistration(t *testing.T) {
	tbl := inspectTestTable(Identifier{"db", "stats"}, map[string]SnapshotRef{
		MainBranch: {SnapshotID: 103, Type: BranchRef},
	})
	tx := tbl.NewTransaction()
	stats := PartitionStatisticsFile{
		SnapshotID:      103,
		StatisticsPath:  "s3://warehouse/db/stats/metadata/stats.parquet",
		FileSizeInBytes: 512,
	}

	require.NoError(t, tx.SetPartitionStatistics(stats))
	meta, err := tx.meta.Build()
	require.NoError(t, err)
	require.Equal(t, []PartitionStatisticsFile{stats}, slices.Collect(meta.PartitionStatistics()))

	replacement := stats
	replacement.FileSizeInBytes = 1024
	require.NoError(t, tx.SetPartitionStatistics(replacement))
	meta, err = tx.meta.Build()
	require.NoError(t, err)
	require.Equal(t, []PartitionStatisticsFile{replacement}, slices.Collect(meta.PartitionStatistics()))

	require.NoError(t, tx.RemovePartitionStatistics(stats.SnapshotID))
	meta, err = tx.meta.Build()
	require.NoError(t, err)
	require.Empty(t, slices.Collect(meta.PartitionStatistics()))

	require.ErrorIs(t, tx.SetPartitionStatistics(PartitionStatisticsFile{
		SnapshotID: 999, StatisticsPath: stats.StatisticsPath, FileSizeInBytes: 512,
	}), ErrSnapshotNotFound)
	require.ErrorIs(t, tx.SetPartitionStatistics(PartitionStatisticsFile{
		SnapshotID: 103, FileSizeInBytes: 512,
	}), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, tx.SetPartitionStatistics(PartitionStatisticsFile{
		SnapshotID: 103, StatisticsPath: stats.StatisticsPath, FileSizeInBytes: -1,
	}), iceberg.ErrInvalidArgument)
	var nilTx *Transaction
	require.Error(t, nilTx.RemovePartitionStatistics(103))
	require.Error(t, nilTx.SetPartitionStatistics(stats))
}

func TestComputePartitionStatisticsRejectsNonV1AndMissingSnapshot(t *testing.T) {
	tbl := inspectTestTable(Identifier{"db", "stats"}, map[string]SnapshotRef{
		MainBranch: {SnapshotID: 103, Type: BranchRef},
	})
	tx := tbl.NewTransaction()
	_, err := tx.ComputePartitionStatistics(t.Context())
	require.True(t, errors.Is(err, iceberg.ErrNotImplemented))

	tx.meta.formatVersion = 1
	_, err = tx.ComputePartitionStatistics(t.Context())
	// The fixture's schema is unpartitioned. Fail before any filesystem IO.
	require.True(t, errors.Is(err, iceberg.ErrNotImplemented))
}

type statsSchemaHistory struct {
	Metadata
	current *iceberg.Schema
	schemas []*iceberg.Schema
	specs   []iceberg.PartitionSpec
}

func (m statsSchemaHistory) CurrentSchema() *iceberg.Schema          { return m.current }
func (m statsSchemaHistory) Schemas() []*iceberg.Schema              { return m.schemas }
func (m statsSchemaHistory) PartitionSpecs() []iceberg.PartitionSpec { return m.specs }

func TestPartitionStatisticsRejectsDroppedAndPromotedSources(t *testing.T) {
	old := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32})
	promoted := iceberg.NewSchema(2, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64})
	dropped := iceberg.NewSchema(3, iceberg.NestedField{ID: 2, Name: "other", Type: iceberg.PrimitiveTypes.String})
	meta := statsSchemaHistory{current: old, schemas: []*iceberg.Schema{old}, specs: []iceberg.PartitionSpec{partitionedSpec()}}
	require.NoError(t, validatePartitionStatisticsSourceTypes(meta))
	meta.current, meta.schemas = promoted, []*iceberg.Schema{old, promoted}
	require.ErrorIs(t, validatePartitionStatisticsSourceTypes(meta), iceberg.ErrNotImplemented)
	meta.specs = []iceberg.PartitionSpec{iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "bucket", Transform: iceberg.BucketTransform{NumBuckets: 4},
	})}
	require.NoError(t, validatePartitionStatisticsSourceTypes(meta))
	meta.current, meta.schemas = dropped, []*iceberg.Schema{old, dropped}
	require.ErrorIs(t, validatePartitionStatisticsSourceTypes(meta), iceberg.ErrNotImplemented)
}

func TestComputePartitionStatisticsWritesAndStages(t *testing.T) {
	spec := partitionedSpec()
	const snapshotID = int64(1)
	sequenceNumber := int64(1)
	data := newTestDataFileWithCount(t, spec, "mem://default/table-location/data/a.parquet", map[int]any{1000: int32(7)}, 12)
	entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, new(snapshotID), &sequenceNumber, &sequenceNumber, data)
	tbl := inspectFilesTableWithManifests(t, spec, inspectManifestSpec{content: iceberg.ManifestContentData, entries: []iceberg.ManifestEntry{entry}})
	base := tbl.Metadata().(*metadataV2).commonMetadata
	base.FormatVersion = 1
	fs, err := tbl.FS(t.Context())
	require.NoError(t, err)
	tbl = New(tbl.Identifier(), &metadataV1{commonMetadata: base}, "metadata.json", func(context.Context) (iceio.IO, error) { return fs, nil }, nil)
	tx := tbl.NewTransaction()
	stats, err := tx.ComputePartitionStatistics(t.Context())
	require.NoError(t, err)
	require.Equal(t, snapshotID, stats.SnapshotID)
	metadata, err := tx.meta.Build()
	require.NoError(t, err)
	require.Equal(t, []PartitionStatisticsFile{*stats}, slices.Collect(metadata.PartitionStatistics()))
	f, err := fs.Open(t.Context(), stats.StatisticsPath)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func TestCollectPartitionStatisticsRejectsDuplicateLiveFiles(t *testing.T) {
	spec := partitionedSpec()
	const snapshotID = int64(1)
	seq := int64(1)
	file := newTestDataFileWithCount(t, spec,
		"mem://default/table-location/data/duplicate.parquet",
		map[int]any{1000: int32(2)}, 5)
	entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, new(snapshotID),
		&seq, &seq, file)
	tbl := inspectFilesTableWithManifests(t, spec, inspectManifestSpec{
		content: iceberg.ManifestContentData,
		entries: []iceberg.ManifestEntry{entry, entry},
	})
	fs, err := tbl.FS(t.Context())
	require.NoError(t, err)
	pt, err := inspectPartitionType(tbl.Metadata())
	require.NoError(t, err)

	_, err = collectPartitionStatistics(t.Context(), fs, *tbl.CurrentSnapshot(), pt, tbl.Metadata().PartitionSpecs())
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "duplicate live data file")
}

func TestPartitionStatisticsEmbeddedManifests(t *testing.T) {
	fs := iceio.NewMemFS()
	spec0 := partitionedSpec()
	spec7 := iceberg.NewPartitionSpecID(7, iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{}})
	write := func(path string, spec iceberg.PartitionSpec, name string, value any, count int64) {
		file := newTestDataFileWithCount(t, spec, "mem://stats/"+name+".parquet", map[int]any{1000: value}, count)
		entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, new(int64(1)), nil, nil, file)
		var out bytes.Buffer
		_, err := iceberg.WriteManifest(path, &out, 1, spec, simpleSchema(), 1, []iceberg.ManifestEntry{entry})
		require.NoError(t, err)
		require.NoError(t, fs.WriteFile(t.Context(), path, out.Bytes()))
	}
	write("mem://stats/spec0.avro", spec0, "a", int32(3), 10)
	write("mem://stats/spec7.avro", spec7, "b", int32(3), 20)
	write("mem://stats/null.avro", spec7, "null", nil, 1)
	snapshot := Snapshot{SnapshotID: 1, ManifestLocations: []string{"mem://stats/spec0.avro", "mem://stats/spec7.avro", "mem://stats/null.avro"}}
	pt := &iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 1000, Name: "id", Type: iceberg.Int32Type{}}}}
	rows, err := collectPartitionStatistics(t.Context(), fs, snapshot, pt, []iceberg.PartitionSpec{spec0, spec7})
	require.NoError(t, err)
	require.Len(t, rows, 3)
	require.NoError(t, sortPartitionStatistics(rows, pt))
	require.Nil(t, rows[0].partition[1000])
	require.Equal(t, int32(7), rows[0].specID)
	require.Equal(t, int32(0), rows[1].specID)
	require.Equal(t, int64(10), rows[1].dataRecordCount)
	require.Equal(t, int32(7), rows[2].specID)
	require.Equal(t, int64(20), rows[2].dataRecordCount)
	statsPath := "mem://stats/partition-stats.parquet"
	_, err = writePartitionStatisticsParquet(t.Context(), fs, statsPath, pt, rows)
	require.NoError(t, err)
	statsFile, err := fs.Open(t.Context(), statsPath)
	require.NoError(t, err)
	encoded, err := io.ReadAll(statsFile)
	require.NoError(t, err)
	require.NoError(t, statsFile.Close())
	parquetReader, err := file.NewParquetReader(bytes.NewReader(encoded))
	require.NoError(t, err)
	defer parquetReader.Close()
	arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	result, err := arrowReader.ReadTable(t.Context())
	require.NoError(t, err)
	defer result.Release()
	require.EqualValues(t, 3, result.NumRows())
	require.Equal(t, []int32{7, 0, 7}, result.Column(1).Data().Chunk(0).(*array.Int32).Int32Values())
	_, err = collectPartitionStatistics(t.Context(), fs, snapshot, pt, []iceberg.PartitionSpec{spec0})
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	wrong := iceberg.NewPartitionSpecID(7, iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.BucketTransform{NumBuckets: 4}})
	_, err = collectPartitionStatistics(t.Context(), fs, snapshot, pt, []iceberg.PartitionSpec{spec0, wrong})
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestPartitionStatisticsEmbeddedManifestWithoutSpecID(t *testing.T) {
	fs := iceio.NewMemFS()
	spec := partitionedSpec()
	file := newTestDataFileWithCount(t, spec, "mem://stats/a.parquet", map[int]any{1000: int32(3)}, 10)
	entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, new(int64(1)), nil, nil, file)
	var original bytes.Buffer
	_, err := iceberg.WriteManifest("mem://stats/a.avro", &original, 1, spec, simpleSchema(), 1, []iceberg.ManifestEntry{entry})
	require.NoError(t, err)
	reader, err := ocf.NewReader(bytes.NewReader(original.Bytes()))
	require.NoError(t, err)
	metadata := make(map[string][]byte)
	for k, v := range reader.Metadata() {
		if k != "partition-spec-id" && k != "avro.schema" && k != "avro.codec" {
			metadata[k] = v
		}
	}
	schema := reader.Schema()
	require.NoError(t, reader.Close())
	writeEmpty := func(path string) {
		var rewritten bytes.Buffer
		writer, err := ocf.NewWriter(&rewritten, schema, ocf.WithMetadata(metadata))
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		require.NoError(t, fs.WriteFile(t.Context(), path, rewritten.Bytes()))
	}
	const missingID = "mem://stats/missing.avro"
	writeEmpty(missingID)
	pt := &iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 1000, Name: "id", Type: iceberg.Int32Type{}}}}
	snapshot := Snapshot{SnapshotID: 1, ManifestLocations: []string{missingID}}
	rows, err := collectPartitionStatistics(t.Context(), fs, snapshot, pt, []iceberg.PartitionSpec{spec})
	require.NoError(t, err)
	require.Empty(t, rows)

	fields := make([]iceberg.PartitionField, 0, spec.NumFields())
	for _, field := range spec.Fields() {
		fields = append(fields, field)
	}
	duplicateSpec := iceberg.NewPartitionSpecID(spec.ID()+7, fields...)
	_, err = collectPartitionStatistics(t.Context(), fs, snapshot, pt, []iceberg.PartitionSpec{spec, duplicateSpec})
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)

	wrongFields := slices.Clone(fields)
	wrongFields[0].Transform = iceberg.BucketTransform{NumBuckets: 4}
	wrongSpec := iceberg.NewPartitionSpecID(spec.ID(), wrongFields...)
	_, err = collectPartitionStatistics(t.Context(), fs, snapshot, pt, []iceberg.PartitionSpec{wrongSpec})
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)

	// Historical v1 partition fields may also omit their field IDs.
	var rawFields []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(metadata["partition-spec"], &rawFields))
	for _, field := range rawFields {
		delete(field, "field-id")
	}
	metadata["partition-spec"], err = json.Marshal(rawFields)
	require.NoError(t, err)
	const missingBoth = "mem://stats/missing-both.avro"
	writeEmpty(missingBoth)
	snapshot.ManifestLocations = []string{missingBoth}
	rows, err = collectPartitionStatistics(t.Context(), fs, snapshot, pt, []iceberg.PartitionSpec{spec})
	require.NoError(t, err)
	require.Empty(t, rows)

	// The generic manifest reader requires an ID, but v1 field IDs are
	// still optional even if partition-spec-id is present.
	metadata["partition-spec-id"] = []byte(strconv.Itoa(spec.ID()))
	const missingFieldIDs = "mem://stats/missing-field-ids.avro"
	writeEmpty(missingFieldIDs)
	snapshot.ManifestLocations = []string{missingFieldIDs}
	rows, err = collectPartitionStatistics(t.Context(), fs, snapshot, pt, []iceberg.PartitionSpec{spec})
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestPartitionStatisticsRequiresPartitionFieldIDs(t *testing.T) {
	spec := partitionedSpec()
	require.NoError(t, validatePartitionStatisticsTuple(spec, "file", map[int]any{1000: nil}))
	require.NoError(t, validatePartitionStatisticsTuple(spec, "file", map[int]any{1000: int32(3)}))
	require.ErrorIs(t, validatePartitionStatisticsTuple(spec, "file", nil), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, validatePartitionStatisticsTuple(spec, "file", map[int]any{1001: int32(3)}), iceberg.ErrInvalidArgument)
}

func TestComputePartitionStatisticsEmbeddedManifest(t *testing.T) {
	fs := iceio.NewMemFS()
	spec := partitionedSpec()
	f := newTestDataFileWithCount(t, spec, "mem://table/data/a.parquet", map[int]any{1000: int32(7)}, 12)
	entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, new(int64(1)), nil, nil, f)
	var out bytes.Buffer
	const path = "mem://table/metadata/manifest.avro"
	_, err := iceberg.WriteManifest(path, &out, 1, spec, simpleSchema(), 1, []iceberg.ManifestEntry{entry})
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(t.Context(), path, out.Bytes()))
	metadata, err := NewMetadata(simpleSchema(), &spec, UnsortedSortOrder, "mem://table", iceberg.Properties{PropertyFormatVersion: "1"})
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)
	require.NoError(t, builder.AddSnapshot(&Snapshot{SnapshotID: 1, TimestampMs: time.Now().UnixMilli() + 1000, ManifestLocations: []string{path}}))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, 1, BranchRef))
	metadata, err = builder.Build()
	require.NoError(t, err)
	tbl := New(Identifier{"db", "table"}, metadata, "mem://table/metadata.json", func(context.Context) (iceio.IO, error) { return fs, nil }, nil)
	tx := tbl.NewTransaction()
	stats, err := tx.ComputePartitionStatistics(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(1), stats.SnapshotID)
	result, err := tx.meta.Build()
	require.NoError(t, err)
	require.Equal(t, []PartitionStatisticsFile{*stats}, slices.Collect(result.PartitionStatistics()))
	stored, err := fs.Open(t.Context(), stats.StatisticsPath)
	require.NoError(t, err)
	require.NoError(t, stored.Close())
}
