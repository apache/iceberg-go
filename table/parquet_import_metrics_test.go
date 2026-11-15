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
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writePartialMetricsParquet(t *testing.T, schema *iceberg.Schema, groups [][]*int32, omit string) string {
	t.Helper()
	arrowSchema, err := table.SchemaToArrowSchema(schema, nil, true, false)
	require.NoError(t, err)
	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(arrowSchema, &buf, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	col := builder.Field(0).(*array.Int32Builder)
	for _, group := range groups {
		for _, v := range group {
			if v == nil {
				col.AppendNull()
			} else {
				col.Append(*v)
			}
		}
		batch := builder.NewRecordBatch()
		require.NoError(t, writer.Write(batch))
		batch.Release()
	}
	require.NoError(t, writer.Close())
	data := buf.Bytes()
	reader, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	footer := reader.MetaData()
	require.Len(t, footer.RowGroups, len(groups))
	for rg, rowGroup := range footer.RowGroups {
		if rg == 0 && omit != "all_max" {
			continue
		}
		stats := rowGroup.Columns[0].MetaData.Statistics
		require.NotNil(t, stats.NullCount)
		switch omit {
		case "null_count":
			require.NotEmpty(t, stats.MinValue)
			require.NotEmpty(t, stats.MaxValue)
			stats.NullCount = nil
		case "min_max":
			require.NotEmpty(t, stats.MinValue)
			require.NotEmpty(t, stats.MaxValue)
			stats.Min, stats.Max, stats.MinValue, stats.MaxValue = nil, nil, nil, nil
		case "max", "all_max":
			require.NotEmpty(t, stats.MinValue)
			require.NotEmpty(t, stats.MaxValue)
			stats.Max, stats.MaxValue = nil, nil
		}
	}
	encodedFooter, err := footer.Serialize(t.Context())
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	footerSize := int(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
	fileData := append(bytes.Clone(data[:len(data)-8-footerSize]), encodedFooter...)
	fileData = binary.LittleEndian.AppendUint32(fileData, uint32(len(encodedFooter)))
	fileData = append(fileData, "PAR1"...)
	path := filepath.Join(t.TempDir(), "input.parquet")
	require.NoError(t, os.WriteFile(path, fileData, 0o600))

	return path
}

func TestImportedParquetPartialMetrics(t *testing.T) {
	ten, hundred := int32(10), int32(100)
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "amount", Type: iceberg.PrimitiveTypes.Int32})
	for _, tc := range []struct {
		name, omit             string
		groups                 [][]*int32
		nullKnown, boundsKnown bool
		nullCount              int64
		want100                int64
		delete100              bool
	}{
		{"complete", "", [][]*int32{{&ten}, {&hundred, nil}}, true, true, 1, 1, false},
		{"missing_null_count", "null_count", [][]*int32{{&ten}, {&hundred, nil}}, false, true, 0, 1, false},
		{"missing_min_max", "min_max", [][]*int32{{&ten}, {&hundred, nil}}, true, false, 1, 1, false},
		{"missing_only_max", "max", [][]*int32{{&ten}, {&hundred, nil}}, true, false, 1, 1, false},
		{"all_null_chunk", "", [][]*int32{{&ten}, {nil}}, true, true, 1, 0, false},
		{"delete_missing_null_count", "null_count", [][]*int32{{&hundred}, {&hundred, nil}}, false, true, 0, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			path := writePartialMetricsParquet(t, schema, tc.groups, tc.omit)
			cat, err := hadoop.NewCatalog(ctx, "local", t.TempDir(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, cat.Close()) })
			require.NoError(t, cat.CreateNamespace(ctx, catalog.ToIdentifier("db"), nil))
			tbl, err := cat.CreateTable(ctx, catalog.ToIdentifier("db", "amounts"), schema, catalog.WithProperties(iceberg.Properties{table.WriteDeleteModeKey: table.WriteModeCopyOnWrite}))
			require.NoError(t, err)
			txn := tbl.NewTransaction()
			require.NoError(t, txn.AddDataFilesByPath(ctx, []string{path}, nil, false))
			tbl, err = txn.Commit(ctx)
			require.NoError(t, err)
			raw, err := tbl.Scan().ToArrowTable(ctx)
			require.NoError(t, err)
			require.EqualValues(t, len(tc.groups[0])+len(tc.groups[1]), raw.NumRows())
			require.EqualValues(t, 1, raw.Column(0).NullN())
			raw.Release()
			files, err := tbl.Scan().PlanFiles(ctx)
			require.NoError(t, err)
			require.Len(t, files, 1)
			imported := files[0].File
			count, ok := imported.NullValueCounts()[1]
			assert.Equal(t, tc.nullKnown, ok)
			if tc.nullKnown && ok {
				assert.Equal(t, tc.nullCount, count)
			}
			_, lower := imported.LowerBoundValues()[1]
			_, upper := imported.UpperBoundValues()[1]
			assert.Equal(t, tc.boundsKnown, lower)
			assert.Equal(t, tc.boundsKnown, upper)
			if tc.boundsKnown && lower && upper {
				require.Equal(t, binary.LittleEndian.AppendUint32(nil, uint32(*tc.groups[0][0])), imported.LowerBoundValues()[1])
				max := *tc.groups[0][0]
				if tc.want100 > 0 {
					max = hundred
				}
				require.Equal(t, binary.LittleEndian.AppendUint32(nil, uint32(max)), imported.UpperBoundValues()[1])
			}
			for _, filter := range []struct {
				expr iceberg.BooleanExpression
				want int64
			}{
				{iceberg.IsNull(iceberg.Reference("amount")), 1},
				{iceberg.EqualTo(iceberg.Reference("amount"), hundred), tc.want100},
			} {
				result, err := tbl.Scan(table.WithRowFilter(filter.expr)).ToArrowTable(ctx)
				require.NoError(t, err)
				assert.EqualValues(t, filter.want, result.NumRows(), "%s", filter.expr)
				result.Release()
			}
			if tc.delete100 {
				tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("amount"), hundred), nil)
				require.NoError(t, err)
				result, err := tbl.Scan().ToArrowTable(ctx)
				require.NoError(t, err)
				defer result.Release()
				require.EqualValues(t, 1, result.NumRows())
				require.EqualValues(t, 1, result.Column(0).NullN())
			}
		})
	}
}

func TestPartialParquetBoundsPartitionInference(t *testing.T) {
	value := int32(7)
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "amount", Type: iceberg.PrimitiveTypes.Int32})
	spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "amount", Transform: iceberg.IdentityTransform{}})
	path := writePartialMetricsParquet(t, schema, [][]*int32{{&value}, {&value}}, "min_max")
	_, err := table.FileToDataFile(t.Context(), iceio.LocalFS{}, path, schema, spec, 0, nil)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "incomplete Parquet bounds")
	dataFile, err := table.FileToDataFileWithOptions(t.Context(), iceio.LocalFS{}, path, schema, spec, 0, nil, table.WithFilePartitionValues(map[int]any{1000: value}))
	require.NoError(t, err)
	require.Equal(t, value, dataFile.Partition()[1000])

	for _, wrong := range []any{int32(8), nil} {
		_, err := table.FileToDataFileWithOptions(t.Context(), iceio.LocalFS{}, path, schema, spec, 0, nil, table.WithFilePartitionValues(map[int]any{1000: wrong}))
		require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
		require.ErrorContains(t, err, "does not match Parquet bounds")
	}

	path = writePartialMetricsParquet(t, schema, [][]*int32{{&value}, {&value}}, "all_max")
	_, err = table.FileToDataFile(t.Context(), iceio.LocalFS{}, path, schema, spec, 0, nil)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "incomplete Parquet bounds")
}
