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
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
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
	for index := 0; index < 5; index++ {
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
		{false, true, -1},
		{"10", "2", -1},
		{[]byte{0, 255}, []byte{1}, -1},
		{[2]byte{0, 255}, [2]byte{1, 0}, -1},
		{[]byte{1, 2}, []byte{1, 2}, 0},
	} {
		got, err := comparePartitionStatisticValues(tc.a, tc.b)
		require.NoError(t, err, "compare %v to %v", tc.a, tc.b)
		require.Equal(t, tc.want, got)
	}
	_, err := comparePartitionStatisticValues(math.NaN(), 1.0)
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)
	_, err = comparePartitionStatisticValues(struct{ X int }{}, struct{ X int }{})
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)
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

	data, err := fs.ReadFile(path)
	require.NoError(t, err)
	require.EqualValues(t, len(data), size)
	require.True(t, bytes.HasPrefix(data, []byte("PAR1")))
	require.True(t, bytes.HasSuffix(data, []byte("PAR1")))

	// Cancellation must not leave an incomplete metadata file.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	failedPath := "mem://bucket/table/metadata/cancelled.parquet"
	_, err = writePartitionStatisticsParquet(cancelled, fs, failedPath, statsTestPartitionType(), rows)
	require.ErrorIs(t, err, context.Canceled)
	_, err = fs.ReadFile(failedPath)
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

	rows, err := collectPartitionStatistics(ctx, fs, *tbl.CurrentSnapshot(), pt)
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

func TestUpdatePartitionStatisticsRejectsNonV1AndMissingSnapshot(t *testing.T) {
	tbl := inspectTestTable(Identifier{"db", "stats"}, map[string]SnapshotRef{
		MainBranch: {SnapshotID: 103, Type: BranchRef},
	})
	tx := tbl.NewTransaction()
	_, err := tx.UpdatePartitionStatistics(t.Context())
	require.True(t, errors.Is(err, iceberg.ErrNotImplemented))

	tx.meta.formatVersion = 1
	_, err = tx.UpdatePartitionStatistics(t.Context())
	// The fixture's schema is unpartitioned. Fail before any filesystem IO.
	require.True(t, errors.Is(err, iceberg.ErrNotImplemented))
}
