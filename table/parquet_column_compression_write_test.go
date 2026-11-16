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
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestParquetColumnCompressionAppendAfterRename(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "customer name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	location := filepath.ToSlash(t.TempDir())
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location, iceberg.Properties{
		table.PropertyFormatVersion:                                     "2",
		table.ParquetCompressionKey:                                     "zstd",
		table.ParquetCompressionColumnCodecKeyPrefix + ".customer name": "snappy",
	})
	require.NoError(t, err)
	cat := &concurrentTestCatalog{metadata: meta, location: location + "/metadata/v1.metadata.json", fsF: branchPlanningFS}
	tbl := table.New(branchPlanningIdent, meta, cat.location, branchPlanningFS, cat)

	appendJSON := func(rows string) {
		arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
		require.NoError(t, err)
		data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{rows})
		require.NoError(t, err)
		defer data.Release()
		reader := array.NewTableReader(data, -1)
		defer reader.Release()
		tbl, err = tbl.Append(t.Context(), reader, nil)
		require.NoError(t, err)
	}
	appendJSON(`[{"customer name":"Ann","payload":"a"}]`)

	txn := tbl.NewTransaction()
	require.NoError(t, txn.UpdateSchema(true, false).RenameColumn([]string{"customer name"}, "full name").Commit())
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, "snappy", tbl.Properties()[table.ParquetCompressionColumnCodecKeyPrefix+".full name"])
	appendJSON(`[{"full name":"Bob","payload":"b"}]`)

	_, records, err := tbl.Scan().ToArrowRecords(t.Context())
	require.NoError(t, err)
	var names []string
	for record, err := range records {
		require.NoError(t, err)
		for i := range int(record.NumRows()) {
			names = append(names, record.Column(0).ValueStr(i))
		}
		record.Release()
	}
	require.ElementsMatch(t, []string{"Ann", "Bob"}, names)

	paths, _ := liveFilesOnRef(t, tbl, table.MainBranch)
	require.Len(t, paths, 2)
	seen := map[string]bool{}
	for _, path := range paths {
		input, err := os.Open(path)
		require.NoError(t, err)
		reader, err := file.NewParquetReader(input)
		require.NoError(t, err)
		meta := reader.MetaData()
		name := meta.Schema.Column(0).Path()
		seen[name] = true
		col, err := meta.RowGroup(0).ColumnChunk(0)
		require.NoError(t, err)
		require.Equal(t, compress.Codecs.Snappy, col.Compression())
		other, err := meta.RowGroup(0).ColumnChunk(1)
		require.NoError(t, err)
		require.Equal(t, compress.Codecs.Zstd, other.Compression())
		require.NoError(t, reader.Close())
	}
	require.True(t, seen["customer_x20name"])
	require.True(t, seen["full_x20name"])
}
