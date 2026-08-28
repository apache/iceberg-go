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
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/metrics"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeSplitParquetFile(t testing.TB, path string, sc *arrow.Schema, jsonData string) {
	t.Helper()

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc, strings.NewReader(jsonData))
	require.NoError(t, err)
	defer rec.Release()

	fs := iceio.LocalFS{}
	fw, err := fs.Create(path)
	require.NoError(t, err)
	defer fw.Close()

	data := array.NewTableFromRecords(sc, []arrow.RecordBatch{rec})
	defer data.Release()

	props := parquet.NewWriterProperties(parquet.WithStats(true))
	require.NoError(t, pqarrow.WriteTable(data, fw, rec.NumRows(), props, pqarrow.DefaultWriterProps()))
}

func splitTestDataFile(t *testing.T, format iceberg.FileFormat, size int64, offsets []int64) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec,
		iceberg.EntryContentData,
		"mem://table/data.parquet",
		format,
		nil,
		nil,
		nil,
		100,
		size,
	)
	require.NoError(t, err)
	if offsets != nil {
		builder.SplitOffsets(offsets)
	}

	return builder.Build()
}

func dataFileWithSplitOffsets(t *testing.T, source iceberg.DataFile, offsets []int64) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec,
		source.ContentType(),
		source.FilePath(),
		source.FileFormat(),
		nil,
		nil,
		nil,
		source.Count(),
		source.FileSizeBytes(),
	)
	require.NoError(t, err)

	return builder.SplitOffsets(offsets).Build()
}

func TestSplitParquetScanTask(t *testing.T) {
	firstRowID := int64(10)
	file := splitTestDataFile(t, iceberg.ParquetFile, 100, []int64{8, 40, 70})
	task := FileScanTask{
		File:        file,
		DeleteFiles: []iceberg.DataFile{file},
		Start:       0,
		Length:      100,
		Residual:    iceberg.AlwaysTrue{},
		FirstRowID:  &firstRowID,
	}

	got, split := splitParquetScanTask(task, 50)
	require.True(t, split)
	require.Len(t, got, 3)

	assert.Equal(t, []int64{40, 30, 30}, []int64{got[0].Length, got[1].Length, got[2].Length})
	assert.Equal(t, []int64{0, 40, 70}, []int64{got[0].Start, got[1].Start, got[2].Start})
	for _, split := range got {
		assert.Equal(t, task.File, split.File)
		assert.Equal(t, task.DeleteFiles, split.DeleteFiles)
		assert.Equal(t, task.Residual, split.Residual)
		assert.Equal(t, task.FirstRowID, split.FirstRowID)
	}
}

func TestSplitParquetScanTaskCoversSparseOffsets(t *testing.T) {
	tests := []struct {
		name    string
		offsets []int64
		starts  []int64
		lengths []int64
	}{
		{
			name:    "omitted leading row group",
			offsets: []int64{40, 70},
			starts:  []int64{0, 70},
			lengths: []int64{70, 30},
		},
		{
			name:    "omitted interior row group",
			offsets: []int64{8, 70},
			starts:  []int64{0, 70},
			lengths: []int64{70, 30},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := splitTestDataFile(t, iceberg.ParquetFile, 100, tt.offsets)
			task := FileScanTask{File: file, Start: 0, Length: file.FileSizeBytes()}

			got, split := splitParquetScanTask(task, 50)
			require.True(t, split)
			require.Len(t, got, len(tt.starts))
			assert.Equal(t, tt.starts, []int64{got[0].Start, got[1].Start})
			assert.Equal(t, tt.lengths, []int64{got[0].Length, got[1].Length})
			assert.Equal(t, file.FileSizeBytes(), got[len(got)-1].Start+got[len(got)-1].Length)
		})
	}
}

func TestSplitParquetScanTaskCoalescesRangesToTarget(t *testing.T) {
	file := splitTestDataFile(t, iceberg.ParquetFile, 80, []int64{10, 20, 30, 40, 50, 60, 70})
	task := FileScanTask{File: file, Start: 0, Length: file.FileSizeBytes()}

	got, split := splitParquetScanTask(task, 25)
	require.True(t, split)
	require.Len(t, got, 4)
	assert.Equal(t, []int64{0, 20, 40, 60}, []int64{got[0].Start, got[1].Start, got[2].Start, got[3].Start})
	assert.Equal(t, []int64{20, 20, 20, 20}, []int64{got[0].Length, got[1].Length, got[2].Length, got[3].Length})
}

func TestSplitParquetScanTaskKeepsUnsafeTasksIntact(t *testing.T) {
	baseFile := splitTestDataFile(t, iceberg.ParquetFile, 100, []int64{8, 40, 70})

	tests := []struct {
		name   string
		file   iceberg.DataFile
		task   FileScanTask
		target int64
	}{
		{
			name:   "small file",
			file:   splitTestDataFile(t, iceberg.ParquetFile, 100, []int64{8, 40, 70}),
			target: 100,
		},
		{
			name:   "no split offsets",
			file:   splitTestDataFile(t, iceberg.ParquetFile, 100, nil),
			target: 50,
		},
		{
			name:   "negative offset",
			file:   splitTestDataFile(t, iceberg.ParquetFile, 100, []int64{-1, 40}),
			target: 50,
		},
		{
			name:   "non increasing offsets",
			file:   splitTestDataFile(t, iceberg.ParquetFile, 100, []int64{8, 40, 40}),
			target: 50,
		},
		{
			name:   "offset at file end",
			file:   splitTestDataFile(t, iceberg.ParquetFile, 100, []int64{8, 100}),
			target: 50,
		},
		{
			name:   "partial task",
			file:   baseFile,
			task:   FileScanTask{File: baseFile, Start: 8, Length: 92},
			target: 50,
		},
		{
			name:   "non parquet file",
			file:   splitTestDataFile(t, iceberg.AvroFile, 100, []int64{8, 40, 70}),
			target: 50,
		},
		{
			name:   "invalid target",
			file:   baseFile,
			target: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := tt.task
			if task.File == nil {
				task = FileScanTask{File: tt.file, Start: 0, Length: tt.file.FileSizeBytes()}
			}

			got, split := splitParquetScanTask(task, tt.target)
			assert.False(t, split)
			assert.Nil(t, got)
		})
	}
}

func TestPlanFilesSplitsLargeParquetFileAndReadsEachRowOnce(t *testing.T) {
	ctx := context.Background()
	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String},
	)
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, location,
		iceberg.Properties{
			PropertyFormatVersion:   "3",
			ParquetRowGroupLimitKey: "2",
			ReadSplitTargetSizeKey:  "1",
		})
	require.NoError(t, err)

	tbl := New(Identifier{"db", "split"}, meta, location+"/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		&countingCatalog{metadata: meta})
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},` +
			`{"id":5,"data":"e"},{"id":6,"data":"f"},{"id":7,"data":"g"},{"id":8,"data":"h"}]`,
	})
	require.NoError(t, err)
	defer data.Release()

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	reporter := &metrics.InMemoryReporter{}
	tasks, err := tbl.Scan(WithReporter(reporter)).PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, tasks, 4, "one task should be planned per row group")
	reports := reporter.Reports()
	require.Len(t, reports, 1)
	report, ok := reports[0].(metrics.ScanReport)
	require.True(t, ok)
	assert.Equal(t, int64(1), report.Metrics.ResultDataFiles.Value,
		"split tasks must still report one data file")
	assert.Equal(t, tasks[0].File.FileSizeBytes(), report.Metrics.TotalFileSizeInBytes.Value,
		"split tasks must report the original file size once")
	for i, task := range tasks {
		assert.Equal(t, tasks[0].File.FilePath(), task.File.FilePath())
		assert.Positive(t, task.Length)
		if i > 0 {
			assert.Equal(t, tasks[i-1].Start+tasks[i-1].Length, task.Start)
		}
	}

	fullOffsets := tasks[0].File.SplitOffsets()
	require.Len(t, fullOffsets, 4)
	sparseFile := dataFileWithSplitOffsets(t, tasks[0].File, []int64{fullOffsets[1], fullOffsets[3]})
	sparseTask := FileScanTask{File: sparseFile, Start: 0, Length: sparseFile.FileSizeBytes()}
	sparseTasks, split := splitParquetScanTask(sparseTask, 1)
	require.True(t, split)
	require.Len(t, sparseTasks, 2)

	_, sparseRecords, err := tbl.Scan().ReadTasks(ctx, sparseTasks)
	require.NoError(t, err)
	seen := make(map[int64]int)
	for record, readErr := range sparseRecords {
		require.NoError(t, readErr)
		ids := record.Column(record.Schema().FieldIndices("id")[0]).(*array.Int64)
		for i := range ids.Len() {
			seen[ids.Value(i)]++
		}
		record.Release()
	}
	wantSeen := map[int64]int{1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 1, 7: 1, 8: 1}
	assert.Equal(t, wantSeen, seen,
		"sparse split offsets must retain leading and interior row groups")

	result, err := tbl.Scan(WithRowLineage()).ToArrowTable(ctx)
	require.NoError(t, err)
	defer result.Release()

	assert.EqualValues(t, 8, result.NumRows())
	idIdx := result.Schema().FieldIndices("id")
	rowIDIdx := result.Schema().FieldIndices(iceberg.RowIDColumnName)
	require.Len(t, idIdx, 1)
	require.Len(t, rowIDIdx, 1)
	idChunks := result.Column(idIdx[0]).Data().Chunks()
	rowIDChunks := result.Column(rowIDIdx[0]).Data().Chunks()
	require.Equal(t, len(idChunks), len(rowIDChunks))

	gotRowIDs := make(map[int64]int64, 8)
	for chunk := range idChunks {
		ids := idChunks[chunk].(*array.Int64)
		rowIDs := rowIDChunks[chunk].(*array.Int64)
		require.Equal(t, ids.Len(), rowIDs.Len())
		for i := range ids.Len() {
			gotRowIDs[ids.Value(i)] = rowIDs.Value(i)
		}
	}
	assert.Equal(t, map[int64]int64{1: 0, 2: 1, 3: 2, 4: 3, 5: 4, 6: 5, 7: 6, 8: 7}, gotRowIDs)

	posDelPath := tbl.Location() + "/data/pos-del.parquet"
	posSc, err := SchemaToArrowSchema(iceberg.PositionalDeleteSchema, nil, true, false)
	require.NoError(t, err)
	writeSplitParquetFile(t, posDelPath, posSc, fmt.Sprintf(
		`[{"file_path":%q,"pos":1},{"file_path":%q,"pos":6}]`,
		tasks[0].File.FilePath(), tasks[0].File.FilePath()))
	posDelBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec,
		iceberg.EntryContentPosDeletes,
		posDelPath,
		iceberg.ParquetFile,
		nil,
		nil,
		nil,
		2,
		256,
	)
	require.NoError(t, err)

	tasksWithDeletes := make([]FileScanTask, len(tasks))
	for i, task := range tasks {
		tasksWithDeletes[i] = task
		tasksWithDeletes[i].DeleteFiles = []iceberg.DataFile{posDelBuilder.Build()}
	}
	_, records, err := tbl.Scan(WithRowLineage()).ReadTasks(ctx, tasksWithDeletes)
	require.NoError(t, err)
	gotRowIDs = make(map[int64]int64, 6)
	for record, err := range records {
		require.NoError(t, err)
		ids := record.Column(record.Schema().FieldIndices("id")[0]).(*array.Int64)
		rowIDs := record.Column(record.Schema().FieldIndices(iceberg.RowIDColumnName)[0]).(*array.Int64)
		for i := range int(record.NumRows()) {
			gotRowIDs[ids.Value(i)] = rowIDs.Value(i)
		}
		record.Release()
	}
	assert.Equal(t, map[int64]int64{1: 0, 3: 2, 4: 3, 5: 4, 6: 5, 8: 7}, gotRowIDs,
		"position deletes must use original positions across split tasks")

	dvWriter := dv.NewDVWriter(iceio.LocalFS{}, func(specID int32) *iceberg.PartitionSpec {
		if specID == 0 {
			return iceberg.UnpartitionedSpec
		}

		return nil
	})
	require.NoError(t, dvWriter.Add(tasks[0].File.FilePath(), []int64{2, 5}, 0, nil))
	dvFiles, err := dvWriter.Flush(ctx, tbl.Location()+"/data/scan-split.puffin")
	require.NoError(t, err)

	tasksWithDVs := make([]FileScanTask, len(tasks))
	for i, task := range tasks {
		tasksWithDVs[i] = task
		tasksWithDVs[i].DeletionVectorFiles = dvFiles
	}
	_, records, err = tbl.Scan(WithRowLineage()).ReadTasks(ctx, tasksWithDVs)
	require.NoError(t, err)
	gotRowIDs = make(map[int64]int64, 6)
	for record, err := range records {
		require.NoError(t, err)
		ids := record.Column(record.Schema().FieldIndices("id")[0]).(*array.Int64)
		rowIDs := record.Column(record.Schema().FieldIndices(iceberg.RowIDColumnName)[0]).(*array.Int64)
		for i := range int(record.NumRows()) {
			gotRowIDs[ids.Value(i)] = rowIDs.Value(i)
		}
		record.Release()
	}
	assert.Equal(t, map[int64]int64{1: 0, 2: 1, 4: 3, 5: 4, 7: 6, 8: 7}, gotRowIDs,
		"deletion vectors must use original positions across split tasks")
}
