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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
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

func writePartialMetricsParquet[T int32 | string](t *testing.T, schema *iceberg.Schema, groups [][]*T, omit string) string {
	t.Helper()
	arrowSchema, err := table.SchemaToArrowSchema(schema, nil, true, false)
	require.NoError(t, err)
	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(arrowSchema, &buf, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	col := builder.Field(0)
	for _, group := range groups {
		for _, v := range group {
			if v == nil {
				col.AppendNull()
			} else {
				require.NoError(t, col.AppendValueFromString(fmt.Sprint(*v)))
			}
		}
		batch := builder.NewRecordBatch()
		require.NoError(t, writer.Write(batch))
		batch.Release()
	}
	require.NoError(t, writer.Close())
	data := buf.Bytes()
	path := filepath.Join(t.TempDir(), "input.parquet")
	if omit == "" {
		require.NoError(t, os.WriteFile(path, data, 0o600))

		return path
	}

	reader, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	footer := reader.MetaData()
	require.Len(t, footer.RowGroups, len(groups))
	if omit == "legacy" || omit == "legacy_max" {
		footer.ColumnOrders = nil
	}
	for rg, rowGroup := range footer.RowGroups {
		chunk := rowGroup.Columns[0].MetaData
		stats := chunk.Statistics
		require.NotNil(t, stats.NullCount)
		if omit == "legacy" {
			require.NotEmpty(t, stats.Min)
			require.NotEmpty(t, stats.Max)
			stats.MinValue, stats.MaxValue = nil, nil
		}
		switch omit {
		case "first_statistics":
			if rg == 0 {
				chunk.Statistics = nil
			}
		case "statistics":
			if rg > 0 {
				chunk.Statistics = nil
			}
		case "all_statistics":
			chunk.Statistics = nil
		}
		if rg == 0 && omit != "all_max" {
			continue
		}
		switch omit {
		case "legacy_max":
			require.NotEmpty(t, stats.Min)
			require.NotEmpty(t, stats.Max)
			stats.Max = nil
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
		{"legacy_complete", "legacy", [][]*int32{{&ten}, {&hundred, nil}}, true, true, 1, 1, false},
		{"legacy_missing_only_max", "legacy_max", [][]*int32{{&ten}, {&hundred, nil}}, true, false, 1, 1, false},
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

func TestMissingParquetStatisticsPartitionInference(t *testing.T) {
	value := int32(7)
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "amount", Type: iceberg.PrimitiveTypes.Int32})
	spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "amount", Transform: iceberg.IdentityTransform{}})
	for _, omit := range []string{"first_statistics", "statistics", "all_statistics"} {
		t.Run(omit, func(t *testing.T) {
			path := writePartialMetricsParquet(t, schema, [][]*int32{{&value}, {&value}}, omit)
			_, err := table.FileToDataFile(t.Context(), iceio.LocalFS{}, path, schema, spec, 0, nil)
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
			require.ErrorContains(t, err, "incomplete Parquet bounds")

			dataFile, err := table.FileToDataFileWithOptions(t.Context(), iceio.LocalFS{}, path, schema, spec, 0, nil, table.WithFilePartitionValues(map[int]any{1000: value}))
			require.NoError(t, err)
			assert.Equal(t, value, dataFile.Partition()[1000])
			assert.NotContains(t, dataFile.NullValueCounts(), 1)
			assert.NotContains(t, dataFile.LowerBoundValues(), 1)
			assert.NotContains(t, dataFile.UpperBoundValues(), 1)
			if omit != "all_statistics" {
				_, err = table.FileToDataFileWithOptions(t.Context(), iceio.LocalFS{}, path, schema, spec, 0, nil, table.WithFilePartitionValues(map[int]any{1000: int32(8)}))
				require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
				require.ErrorContains(t, err, "does not match Parquet bounds")
			}
		})
	}
}

func TestPartitionInferenceWithoutBoundMetrics(t *testing.T) {
	value := int32(7)
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "amount", Type: iceberg.PrimitiveTypes.Int32})
	spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "amount", Transform: iceberg.IdentityTransform{}})
	for _, mode := range []string{"none", "counts"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			path := writePartialMetricsParquet(t, schema, [][]*int32{{&value}, {&value}}, "")
			cat, err := hadoop.NewCatalog(ctx, "local", t.TempDir(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, cat.Close()) })
			require.NoError(t, cat.CreateNamespace(ctx, catalog.ToIdentifier("db"), nil))
			tbl, err := cat.CreateTable(ctx, catalog.ToIdentifier("db", "amounts"), schema,
				catalog.WithPartitionSpec(&spec),
				catalog.WithProperties(iceberg.Properties{table.MetricsModeColumnConfPrefix + ".amount": mode}))
			require.NoError(t, err)
			txn := tbl.NewTransaction()
			require.NoError(t, txn.AddDataFilesByPath(ctx, []string{path}, nil, false))
			tbl, err = txn.Commit(ctx)
			require.NoError(t, err)

			files, err := tbl.Scan().PlanFiles(ctx)
			require.NoError(t, err)
			require.Len(t, files, 1)
			imported := files[0].File
			assert.Equal(t, map[int]any{1000: value}, imported.Partition())
			assert.NotContains(t, imported.LowerBoundValues(), 1)
			assert.NotContains(t, imported.UpperBoundValues(), 1)
			if mode == "counts" {
				assert.Equal(t, map[int]int64{1: 2}, imported.ValueCounts())
				assert.Equal(t, map[int]int64{1: 0}, imported.NullValueCounts())
			} else {
				assert.NotContains(t, imported.ValueCounts(), 1)
				assert.NotContains(t, imported.NullValueCounts(), 1)
			}

			result, err := tbl.Scan(table.WithRowFilter(iceberg.EqualTo(iceberg.Reference("amount"), value))).ToArrowTable(ctx)
			require.NoError(t, err)
			defer result.Release()
			assert.EqualValues(t, 2, result.NumRows())
		})
	}
}

func TestPartitionInferenceRejectsNullAndNonNullValues(t *testing.T) {
	value := int32(7)
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "amount", Type: iceberg.PrimitiveTypes.Int32})
	spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "amount", Transform: iceberg.IdentityTransform{}})
	for _, mode := range []string{"full", "none", "counts"} {
		t.Run(mode, func(t *testing.T) {
			props := iceberg.Properties{table.MetricsModeColumnConfPrefix + ".amount": mode}
			path := writePartialMetricsParquet(t, schema, [][]*int32{{nil}, {&value}}, "")
			_, err := table.FileToDataFile(t.Context(), iceio.LocalFS{}, path, schema, spec, 0, props)
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
			require.ErrorContains(t, err, "both null and non-null values")
		})
	}
}

func TestPartitionInferenceWithoutBoundMetricsRequiresFooterBounds(t *testing.T) {
	value := int32(7)
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "amount", Type: iceberg.PrimitiveTypes.Int32})
	spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "amount", Transform: iceberg.IdentityTransform{}})
	for _, mode := range []string{"none", "counts"} {
		t.Run(mode, func(t *testing.T) {
			props := iceberg.Properties{table.MetricsModeColumnConfPrefix + ".amount": mode}
			path := writePartialMetricsParquet(t, schema, [][]*int32{{&value}, {&value}}, "min_max")
			_, err := table.FileToDataFile(t.Context(), iceio.LocalFS{}, path, schema, spec, 0, props)
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
			require.ErrorContains(t, err, "incomplete Parquet bounds")

			_, err = table.FileToDataFileWithOptions(t.Context(), iceio.LocalFS{}, path, schema, spec, 0, props, table.WithFilePartitionValues(map[int]any{1000: int32(8)}))
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
			require.ErrorContains(t, err, "does not match Parquet bounds")

			dataFile, err := table.FileToDataFileWithOptions(t.Context(), iceio.LocalFS{}, path, schema, spec, 0, props, table.WithFilePartitionValues(map[int]any{1000: value}))
			require.NoError(t, err)
			assert.Equal(t, value, dataFile.Partition()[1000])
			assert.NotContains(t, dataFile.LowerBoundValues(), 1)
			assert.NotContains(t, dataFile.UpperBoundValues(), 1)
		})
	}
}

func TestEmptyParquetRowGroupsPartitionInference(t *testing.T) {
	value := int32(7)
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "amount", Type: iceberg.PrimitiveTypes.Int32})
	spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "amount", Transform: iceberg.IdentityTransform{}})
	path := writePartialMetricsParquet(t, schema, [][]*int32{nil, {&value}, nil}, "")

	reader, err := file.OpenParquetFile(path, false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })
	groups := reader.MetaData().RowGroups
	require.Len(t, groups, 3)
	for _, i := range []int{0, 2} {
		require.Zero(t, groups[i].NumRows)
		require.Zero(t, groups[i].Columns[0].MetaData.NumValues)
		require.Nil(t, groups[i].Columns[0].MetaData.Statistics)
	}

	dataFile, err := table.FileToDataFile(t.Context(), iceio.LocalFS{}, path, schema, spec, 0, nil)
	require.NoError(t, err)
	assert.Equal(t, value, dataFile.Partition()[1000])
	assert.Equal(t, map[int]int64{1: 1}, dataFile.ValueCounts())
	assert.Equal(t, map[int]int64{1: 0}, dataFile.NullValueCounts())
	bound := binary.LittleEndian.AppendUint32(nil, uint32(value))
	assert.Equal(t, bound, dataFile.LowerBoundValues()[1])
	assert.Equal(t, bound, dataFile.UpperBoundValues()[1])
}

func TestImportedParquetStringBounds(t *testing.T) {
	first, last := "apple", "pear"
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "fruit", Type: iceberg.PrimitiveTypes.String})
	for _, omit := range []string{"", "max"} {
		path := writePartialMetricsParquet(t, schema, [][]*string{{&first}, {&last, nil}}, omit)
		dataFile, err := table.FileToDataFile(t.Context(), iceio.LocalFS{}, path, schema, *iceberg.UnpartitionedSpec, 0, nil)
		require.NoError(t, err)
		assert.EqualValues(t, 1, dataFile.NullValueCounts()[1])
		if omit == "" {
			assert.Equal(t, []byte(first), dataFile.LowerBoundValues()[1])
			assert.Equal(t, []byte(last), dataFile.UpperBoundValues()[1])
		} else {
			assert.NotContains(t, dataFile.LowerBoundValues(), 1)
			assert.NotContains(t, dataFile.UpperBoundValues(), 1)
		}
	}
}

func TestWriteNullPartitionWithoutParquetStatistics(t *testing.T) {
	ctx := t.Context()
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "amount", Type: iceberg.PrimitiveTypes.Int32})
	spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "amount", Transform: iceberg.IdentityTransform{}})
	cat, err := hadoop.NewCatalog(ctx, "local", t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cat.Close()) })
	require.NoError(t, cat.CreateNamespace(ctx, catalog.ToIdentifier("db"), nil))
	tbl, err := cat.CreateTable(ctx, catalog.ToIdentifier("db", "amounts"), schema,
		catalog.WithPartitionSpec(&spec),
		catalog.WithProperties(iceberg.Properties{table.ParquetColumnStatsEnabledKeyPrefix + ".amount": "false"}))
	require.NoError(t, err)

	arrowSchema, err := table.SchemaToArrowSchema(schema, nil, true, false)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	builder.Field(0).AppendNull()
	record := builder.NewRecordBatch()
	defer record.Release()
	reader, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{record})
	require.NoError(t, err)
	defer reader.Release()
	tbl, err = tbl.Append(ctx, reader, nil)
	require.NoError(t, err)
	files, err := tbl.Scan().PlanFiles(ctx)
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.Equal(t, map[int]any{1000: nil}, files[0].File.Partition())
	assert.NotContains(t, files[0].File.NullValueCounts(), 1)
}
