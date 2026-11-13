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
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

// A writer may leave an identity partition column out of its data files, as
// Delta and Hive-style layouts do. The spec's column projection reads such a
// field from the file's partition value (rule 1), required or not.
func TestScanFillsIdentityPartitionColumnMissingFromDataFile(t *testing.T) {
	ctx := context.Background()
	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Name: "region", Transform: iceberg.IdentityTransform{},
	})
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, location, nil)
	require.NoError(t, err)
	tbl := New(Identifier{"db", "identity"}, meta, location+"/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, &countingCatalog{metadata: meta})

	// Each file stores only id; its region lives in the manifest.
	idOnly := arrow.NewSchema([]arrow.Field{{
		Name: "id", Type: arrow.PrimitiveTypes.Int64,
		Metadata: arrow.NewMetadata([]string{ArrowParquetFieldIDKey}, []string{"1"}),
	}}, nil)
	files := make([]iceberg.DataFile, 0, 2)
	for _, part := range []struct {
		region string
		ids    string
	}{{"east", `[{"id":1},{"id":2}]`}, {"west", `[{"id":3}]`}} {
		data, err := array.TableFromJSON(memory.DefaultAllocator, idOnly, []string{part.ids})
		require.NoError(t, err)
		path := location + "/data/" + part.region + ".parquet"
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		f, err := os.Create(path)
		require.NoError(t, err)
		require.NoError(t, pqarrow.WriteTable(data, f, 1024, nil, pqarrow.DefaultWriterProps()))
		info, err := os.Stat(path)
		require.NoError(t, err)

		b, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, path, iceberg.ParquetFile,
			map[int]any{1000: part.region}, nil, nil, data.NumRows(), info.Size())
		require.NoError(t, err)
		files = append(files, b.Build())
		data.Release()
	}
	txn := tbl.NewTransaction()
	require.NoError(t, txn.AddDataFiles(ctx, files, nil))
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	result, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer result.Release()

	got := map[int64]string{}
	reader := array.NewTableReader(result, -1)
	defer reader.Release()
	for reader.Next() {
		rec := reader.RecordBatch()
		ids := rec.Column(0).(*array.Int64)
		regions := rec.Column(1).(*array.String)
		for i := range int(rec.NumRows()) {
			got[ids.Value(i)] = regions.Value(i)
		}
	}
	require.Equal(t, map[int64]string{1: "east", 2: "east", 3: "west"}, got)

	// A filter on the partition column prunes by it and still fills it.
	filtered, err := tbl.Scan(WithRowFilter(iceberg.EqualTo(iceberg.Reference("region"), "west"))).ToArrowTable(ctx)
	require.NoError(t, err)
	defer filtered.Release()
	require.EqualValues(t, 1, filtered.NumRows())
}

// Partition values come back from the manifest as literals; a decimal one
// must still fill the column rather than fail the scan.
func TestScanFillsDecimalIdentityPartitionColumnMissingFromDataFile(t *testing.T) {
	ctx := context.Background()
	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "amount", Type: iceberg.MustDecimalTypeOf(9, 2)},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Name: "amount", Transform: iceberg.IdentityTransform{},
	})
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, location, nil)
	require.NoError(t, err)
	tbl := New(Identifier{"db", "identity_decimal"}, meta, location+"/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, &countingCatalog{metadata: meta})

	idOnly := arrow.NewSchema([]arrow.Field{{
		Name: "id", Type: arrow.PrimitiveTypes.Int64,
		Metadata: arrow.NewMetadata([]string{ArrowParquetFieldIDKey}, []string{"1"}),
	}}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, idOnly, []string{`[{"id":1},{"id":2}]`})
	require.NoError(t, err)
	defer data.Release()
	path := location + "/data/amount.parquet"
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	f, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(data, f, 1024, nil, pqarrow.DefaultWriterProps()))
	info, err := os.Stat(path)
	require.NoError(t, err)

	value := iceberg.Decimal{Val: decimal128.FromI64(1234), Scale: 2}
	b, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, path, iceberg.ParquetFile,
		map[int]any{1000: value}, nil, nil, data.NumRows(), info.Size())
	require.NoError(t, err)
	txn := tbl.NewTransaction()
	require.NoError(t, txn.AddDataFiles(ctx, []iceberg.DataFile{b.Build()}, nil))
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	result, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer result.Release()
	require.EqualValues(t, 2, result.NumRows())
	for _, chunk := range result.Column(1).Data().Chunks() {
		amounts := chunk.(*array.Decimal128)
		for i := range amounts.Len() {
			require.False(t, amounts.IsNull(i))
			require.Equal(t, "12.34", amounts.ValueStr(i))
		}
	}
}
