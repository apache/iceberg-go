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
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/codec"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeBucketDayAppendTable(t *testing.T, mem memory.Allocator, ids []int64, micros int64) arrow.Table {
	t.Helper()
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	bldr := array.NewRecordBuilder(mem, arrSchema)
	defer bldr.Release()
	for _, id := range ids {
		bldr.Field(0).(*array.Int64Builder).Append(id)
		bldr.Field(1).(*array.TimestampBuilder).Append(arrow.Timestamp(micros))
		bldr.Field(2).(*array.StringBuilder).Append(fmt.Sprintf("row-%d", id))
	}
	rec := bldr.NewRecordBatch()
	defer rec.Release()

	return array.NewTableFromRecords(rec.Schema(), []arrow.RecordBatch{rec})
}

// A days(ts) partition value can reach the manifest-entry encoder as a Go
// time.Time — a date-logical value decoded from a manifest whose iceberg
// partition field result type (Int32Type for the day transform) carries no
// hint to normalize it, e.g. via NewDataFileBuilder. The encoder must still
// write the int days the avro field expects rather than rejecting time.Time.
// Regression test for distributed compaction failing to commit rewrites of
// bucket()/days()-partitioned tables.
func TestEncodeDataFile_DatePartitionFromTimeTime(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
	)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1001, Transform: iceberg.DayTransform{}, Name: "ts_day"},
	)
	day := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	b, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData,
		"s3://bucket/data/ts_day=2026-01-02/f.parquet", iceberg.ParquetFile,
		map[int]any{1001: day}, nil, nil, 10, 100)
	require.NoError(t, err)
	df := b.Build()

	blob, err := codec.EncodeDataFile(df, spec, schema, 2)
	require.NoError(t, err, "encoding a date partition held as time.Time")

	dec, err := codec.DecodeDataFile(blob, spec, schema, 2)
	require.NoError(t, err)
	const epochDays = int32(20455) // 2026-01-02
	assert.EqualValues(t, epochDays, dec.Partition()[1001])
}

// End-to-end guard for the distributed-compaction commit path on a
// bucket()/days()-partitioned table: execute each group, round-trip the
// resulting data files through the codec (the leader<->peer wire leg), then
// stage and commit the rewrite.
func TestRewriteDataFiles_BucketDaysPartition_DistributedCommit(t *testing.T) {
	mem := memory.DefaultAllocator
	location := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
		iceberg.NestedField{ID: 3, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Transform: iceberg.BucketTransform{NumBuckets: 4}, Name: "id_bucket"},
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1001, Transform: iceberg.DayTransform{}, Name: "ts_day"},
	)
	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)
	tbl := table.New(
		table.Identifier{"db", "bucket_day"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		&rowDeltaCatalog{metadata: meta},
	)

	const dayMicros = int64(24 * 60 * 60 * 1_000_000)
	for i := range 6 {
		base := int64(i * 20)
		ids := make([]int64, 20)
		for j := range ids {
			ids[j] = base + int64(j)
		}
		at := makeBucketDayAppendTable(t, mem, ids, int64(i%2)*dayMicros)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.AppendTable(t.Context(), at, at.NumRows(), nil))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
		at.Release()
	}

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	filesBefore := len(tasks)
	plan, err := defaultTestCompactionCfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Groups)

	wireSpec := tbl.Spec()
	version := tbl.Metadata().Version()
	tx := tbl.NewTransaction()
	rw := tx.NewRewrite(iceberg.Properties{})
	for _, g := range plan.Groups {
		res, execErr := table.ExecuteCompactionGroup(t.Context(), tbl, table.CompactionTaskGroup{
			PartitionKey:   g.PartitionKey,
			Tasks:          g.Tasks,
			TotalSizeBytes: g.TotalSizeBytes,
		})
		require.NoError(t, execErr)
		oldFiles := make([]iceberg.DataFile, 0, len(g.Tasks))
		for _, tk := range g.Tasks {
			oldFiles = append(oldFiles, tk.File)
		}
		rw.Apply(
			wireRoundTrip(t, oldFiles, wireSpec, schema, version),
			wireRoundTrip(t, res.NewDataFiles, wireSpec, schema, version),
			nil,
		)
	}
	require.NoError(t, rw.Commit(t.Context()), "staging rewrite with codec-round-tripped files")

	out, err := tx.Commit(t.Context())
	require.NoError(t, err)
	newTasks, err := out.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	assert.Less(t, len(newTasks), filesBefore)
}

func wireRoundTrip(t *testing.T, dfs []iceberg.DataFile, spec iceberg.PartitionSpec, schema *iceberg.Schema, version int) []iceberg.DataFile {
	t.Helper()
	out := make([]iceberg.DataFile, 0, len(dfs))
	for _, df := range dfs {
		blob, err := codec.EncodeDataFile(df, spec, schema, version)
		require.NoError(t, err, "encode data file")
		dec, err := codec.DecodeDataFile(blob, spec, schema, version)
		require.NoError(t, err, "decode data file")
		out = append(out, dec)
	}

	return out
}
