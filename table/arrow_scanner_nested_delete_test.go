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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadDeletesForPathsRejectsNestedBloomFieldIDCollision(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	defer mem.AssertSize(t, 0)

	fields := PositionalDeleteArrowSchema.Fields()
	fields = append(fields, arrow.Field{Name: "row", Type: arrow.ListOfField(arrow.Field{
		Name: "element", Type: arrow.BinaryTypes.String,
		Metadata: fields[0].Metadata,
	})})
	schema := arrow.NewSchema(fields, nil)
	record := mustLoadRecordBatchFromJSON(schema,
		`[{"file_path":"data.parquet","pos":1,"row":["unrelated"]}]`)
	defer record.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.RecordBatch{record})
	defer tbl.Release()

	fs := iceio.NewMemFS()
	const deletePath = "mem://bucket/deletes/nested-bloom.parquet"
	writer, err := fs.Create(deletePath)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(tbl, writer, 1,
		parquet.NewWriterProperties(parquet.WithStats(true), parquet.WithBloomFilterEnabled(true)),
		pqarrow.DefaultWriterProps()))
	require.NoError(t, writer.Close())

	file := newPosDeleteFile(t, deletePath, 1, 128)
	allDeletes, err := readDeletesForPaths(ctx, fs, file, nil)
	require.NoError(t, err)
	defer releasePosDeletes(allDeletes)
	assert.Equal(t, []int64{1}, int64Values(allDeletes["data.parquet"]))

	filtered, err := readDeletesForPaths(ctx, fs, file, map[string]struct{}{"data.parquet": {}})
	defer releasePosDeletes(filtered)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.Nil(t, filtered)
}
