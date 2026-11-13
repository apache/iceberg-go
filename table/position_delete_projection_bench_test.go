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
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

// This measures the complete local merge-on-read Delete, including planning,
// position-delete output, manifests and an in-memory catalog commit. Each
// iteration starts from the same immutable snapshot with a fresh catalog.
func BenchmarkMergeOnReadDeleteProjection(b *testing.B) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String})
	tbl := newProjectedDeleteTable(b, schema, iceberg.Properties{
		table.ParquetCompressionKey: "uncompressed", "parquet.enable.dictionary": "false",
		table.ParquetPageRowLimitKey: "128",
	})
	arrowSchema, err := table.SchemaToArrowSchema(schema, nil, true, false)
	require.NoError(b, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	payload := strings.Repeat("x", 8192)
	for i := range 8192 {
		builder.Field(0).(*array.Int64Builder).Append(int64(i))
		builder.Field(1).(*array.StringBuilder).Append(strconv.Itoa(i) + payload)
	}
	record := builder.NewRecordBatch()
	builder.Release()
	reader, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{record})
	require.NoError(b, err)
	tbl, err = tbl.Append(b.Context(), reader, nil)
	reader.Release()
	record.Release()
	require.NoError(b, err)
	fsF := func(context.Context) (icebergio.IO, error) { return icebergio.LocalFS{}, nil }
	filter := iceberg.EqualTo(iceberg.Reference("id"), int64(4096))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		cat := &concurrentTestCatalog{metadata: tbl.Metadata(), location: tbl.MetadataLocation(), fsF: fsF}
		current := table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), fsF, cat)
		updated, err := current.Delete(b.Context(), filter, nil)
		require.NoError(b, err)
		require.Equal(b, "1", updated.CurrentSnapshot().Summary.Properties["added-position-deletes"])
	}
}
