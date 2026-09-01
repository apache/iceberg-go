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
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func manyRowsJSON(n int) string {
	var sb strings.Builder
	sb.WriteString("[")
	for i := range n {
		if i > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, `{"id":%d,"data":"row-%d"}`, i, i)
	}
	sb.WriteString("]")

	return sb.String()
}

func TestWithArrowBatchSizeCapsDecodedBatches(t *testing.T) {
	const numRows = 100
	const batchSize = 7
	tbl := buildV3TableWithRows(t, manyRowsJSON(numRows))

	_, records, err := tbl.Scan(WithArrowBatchSize(batchSize)).ToArrowRecords(t.Context())
	require.NoError(t, err)

	var totalRows, batches int64
	for rec, err := range records {
		require.NoError(t, err)
		assert.LessOrEqual(t, rec.NumRows(), int64(batchSize))
		totalRows += rec.NumRows()
		batches++
		rec.Release()
	}
	assert.Equal(t, int64(numRows), totalRows)
	assert.Greater(t, batches, int64(numRows/batchSize))

	// Control: the default batch size returns all rows in one batch.
	_, records, err = tbl.Scan().ToArrowRecords(t.Context())
	require.NoError(t, err)
	batches = 0
	for rec, err := range records {
		require.NoError(t, err)
		batches++
		rec.Release()
	}
	assert.Equal(t, int64(1), batches)
}

func TestWithArrowBatchSizeIgnoresNonPositive(t *testing.T) {
	tbl := buildV3TableWithRows(t, manyRowsJSON(3))

	scan := tbl.Scan(WithArrowBatchSize(0), WithArrowBatchSize(-5))
	assert.Empty(t, scan.options.Get(ParquetBatchSizeKey, ""))
}

func TestWithArrowBatchSizeDoesNotMutateCallerOptions(t *testing.T) {
	tbl := buildV3TableWithRows(t, manyRowsJSON(3))

	callerOpts := iceberg.Properties{"include_empty_files": "true"}
	scan := tbl.Scan(WithOptions(callerOpts), WithArrowBatchSize(9))
	assert.Equal(t, "9", scan.options.Get(ParquetBatchSizeKey, ""))
	assert.Empty(t, callerOpts[ParquetBatchSizeKey])
	assert.Equal(t, "true", scan.options.Get("include_empty_files", ""))
}

func TestRecordQueueCapacityDefaultAndOverride(t *testing.T) {
	f := &writerFactory{}
	assert.Equal(t, rollingDataWriterQueueCapacity, f.recordQueueCapacity())

	f.recordBufferSize = 8
	assert.Equal(t, 8, f.recordQueueCapacity())

	f.recordBufferSize = -1
	assert.Equal(t, rollingDataWriterQueueCapacity, f.recordQueueCapacity())
}

func TestWriteRecordTuningOptions(t *testing.T) {
	var cfg writeRecordConfig
	WithRecordBatchBufferSize(16)(&cfg)
	WithParquetRowGroupLimit(1000)(&cfg)
	assert.Equal(t, 16, cfg.recordBatchBufferSize)
	assert.Equal(t, 1000, cfg.parquetRowGroupLimit)

	WithRecordBatchBufferSize(0)(&cfg)
	WithParquetRowGroupLimit(-1)(&cfg)
	assert.Equal(t, 16, cfg.recordBatchBufferSize, "non-positive values are ignored")
	assert.Equal(t, 1000, cfg.parquetRowGroupLimit)
}

func TestCompactionGroupTuningOptions(t *testing.T) {
	var cfg compactionGroupConfig
	for _, opt := range []CompactionGroupOption{
		WithCompactionReadBatchSize(1024),
		WithCompactionRecordBatchBufferSize(4),
		WithCompactionParquetRowGroupLimit(500),
	} {
		opt(&cfg)
	}
	assert.Equal(t, int64(1024), cfg.readBatchSize)
	assert.Equal(t, 4, cfg.recordBatchBufferSize)
	assert.Equal(t, 500, cfg.parquetRowGroupLimit)

	for _, opt := range []CompactionGroupOption{
		WithCompactionReadBatchSize(0),
		WithCompactionRecordBatchBufferSize(-2),
		WithCompactionParquetRowGroupLimit(0),
	} {
		opt(&cfg)
	}
	assert.Equal(t, int64(1024), cfg.readBatchSize, "non-positive values are ignored")
	assert.Equal(t, 4, cfg.recordBatchBufferSize)
	assert.Equal(t, 500, cfg.parquetRowGroupLimit)
}

func TestWithParquetRowGroupLimitBoundsRowGroups(t *testing.T) {
	tbl := buildV3TableWithRows(t, `[{"id":0,"data":"seed"}]`)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	const numRows = 100
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{manyRowsJSON(numRows)})
	require.NoError(t, err)
	defer data.Release()

	rdr := array.NewTableReader(data, numRows)
	defer rdr.Release()

	var paths []string
	for df, err := range WriteRecords(t.Context(), tbl, arrowSchema,
		array.IterFromReader(rdr), WithParquetRowGroupLimit(10)) {
		require.NoError(t, err)
		paths = append(paths, df.FilePath())
	}
	require.NotEmpty(t, paths)

	var rowGroups int
	for _, path := range paths {
		pf, err := file.OpenParquetFile(strings.TrimPrefix(path, "file://"), false)
		require.NoError(t, err)
		for rg := range pf.NumRowGroups() {
			assert.LessOrEqual(t, pf.RowGroup(rg).NumRows(), int64(10))
		}
		rowGroups += pf.NumRowGroups()
		require.NoError(t, pf.Close())
	}
	assert.GreaterOrEqual(t, rowGroups, numRows/10)
}
