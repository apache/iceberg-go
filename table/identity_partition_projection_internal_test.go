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
	"iter"
	"os"
	"path/filepath"
	"sort"
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

// identityPartitionTestTable returns an empty table at a fresh location.
func identityPartitionTestTable(t *testing.T, schema *iceberg.Schema, spec iceberg.PartitionSpec, props iceberg.Properties) *Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, location, props)
	require.NoError(t, err)

	return New(Identifier{"db", "identity"}, meta, location+"/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, &countingCatalog{metadata: meta})
}

// addIDOnlyFile commits a data file that stores only the id column (field ID
// 1), with partition as its partition data under the table's current spec.
func addIDOnlyFile(t *testing.T, tbl *Table, name string, partition map[int]any, ids string) *Table {
	t.Helper()

	idOnly := arrow.NewSchema([]arrow.Field{{
		Name: "id", Type: arrow.PrimitiveTypes.Int64,
		Metadata: arrow.NewMetadata([]string{ArrowParquetFieldIDKey}, []string{"1"}),
	}}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, idOnly, []string{ids})
	require.NoError(t, err)
	defer data.Release()

	path := tbl.Location() + "/data/" + name + ".parquet"
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	f, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(data, f, 1024, nil, pqarrow.DefaultWriterProps()))
	info, err := os.Stat(path)
	require.NoError(t, err)

	b, err := iceberg.NewDataFileBuilder(tbl.Spec(), iceberg.EntryContentData, path, iceberg.ParquetFile,
		partition, nil, nil, data.NumRows(), info.Size())
	require.NoError(t, err)
	if tbl.Metadata().FormatVersion() >= 3 {
		b.FirstRowID(tbl.Metadata().NextRowID())
	}
	txn := tbl.NewTransaction()
	require.NoError(t, txn.AddDataFiles(t.Context(), []iceberg.DataFile{b.Build()}, nil))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	return tbl
}

// scanIDAndColumn returns each row of a two-column (id, value) scan as
// "id:value", sorted, with "id:<null>" for a null value.
func scanIDAndColumn(t *testing.T, tbl *Table, opts ...ScanOption) []string {
	t.Helper()

	result, err := tbl.Scan(opts...).ToArrowTable(t.Context())
	require.NoError(t, err)
	defer result.Release()

	rows := []string{}
	reader := array.NewTableReader(result, -1)
	defer reader.Release()
	for reader.Next() {
		rec := reader.RecordBatch()
		ids := rec.Column(0).(*array.Int64)
		for i := range int(rec.NumRows()) {
			value := "<null>"
			if !rec.Column(1).IsNull(i) {
				value = rec.Column(1).ValueStr(i)
			}
			rows = append(rows, fmt.Sprintf("%d:%s", ids.Value(i), value))
		}
	}
	sort.Strings(rows)

	return rows
}

// regionTestTable has id 1 in both partitions, in files that store only id.
func regionTestTable(t *testing.T, props iceberg.Properties) *Table {
	t.Helper()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Name: "region", Transform: iceberg.IdentityTransform{},
	})
	tbl := identityPartitionTestTable(t, schema, spec, props)
	tbl = addIDOnlyFile(t, tbl, "east", map[int]any{1000: "east"}, `[{"id":1},{"id":2}]`)

	return addIDOnlyFile(t, tbl, "west", map[int]any{1000: "west"}, `[{"id":1},{"id":3}]`)
}

// A row filter must see the partition value of a column the file does not
// store, not a null. A scan takes each file's residual from planning, which
// substitutes the partition value already; this pins that it stays so.
func TestFilterOnIdentityPartitionColumnMissingFromDataFile(t *testing.T) {
	tbl := regionTestTable(t, nil)
	region, id := iceberg.Reference("region"), iceberg.Reference("id")
	westOne := iceberg.NewAnd(iceberg.EqualTo(region, "west"), iceberg.EqualTo(id, int64(1)))

	tests := []struct {
		name   string
		filter iceberg.BooleanExpression
		want   []string
	}{
		{"and", westOne, []string{"1:west"}},
		{"not and", iceberg.NewNot(westOne), []string{"1:east", "2:east", "3:west"}},
		{"not equal", iceberg.NotEqualTo(region, "east"), []string{"1:west", "3:west"}},
		{
			"or", iceberg.NewOr(iceberg.EqualTo(region, "east"), iceberg.EqualTo(id, int64(3))),
			[]string{"1:east", "2:east", "3:west"},
		},
		{"in", iceberg.IsIn(region, "west", "north"), []string{"1:west", "3:west"}},
		{"not in", iceberg.NotIn(region, "west", "north"), []string{"1:east", "2:east"}},
		{"starts with", iceberg.StartsWith(region, "ea"), []string{"1:east", "2:east"}},
		{"is null", iceberg.IsNull(region), []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, scanIDAndColumn(t, tbl, WithRowFilter(tt.filter)))
		})
	}
}

// Delete evaluates its filter per file: copy-on-write keeps the rows that do
// not match, and merge-on-read writes position deletes for the rows that do.
func TestDeleteOnIdentityPartitionColumnMissingFromDataFile(t *testing.T) {
	for _, mode := range []string{WriteModeCopyOnWrite, WriteModeMergeOnRead} {
		t.Run(mode, func(t *testing.T) {
			tbl := regionTestTable(t, iceberg.Properties{
				PropertyFormatVersion: "2",
				WriteDeleteModeKey:    mode,
			})
			filter := iceberg.NewAnd(
				iceberg.EqualTo(iceberg.Reference("region"), "west"),
				iceberg.EqualTo(iceberg.Reference("id"), int64(1)))

			txn := tbl.NewTransaction()
			require.NoError(t, txn.Delete(t.Context(), filter, nil))
			tbl, err := txn.Commit(t.Context())
			require.NoError(t, err)

			require.Equal(t, []string{"1:east", "2:east", "3:west"}, scanIDAndColumn(t, tbl))
		})
	}
}

// An equality delete keyed on the partition column compares the partition
// value, so it matches rows of a file that does not store the column.
func TestEqualityDeleteOnIdentityPartitionColumnMissingFromDataFile(t *testing.T) {
	tbl := regionTestTable(t, iceberg.Properties{PropertyFormatVersion: "2"})

	deleteSchema, err := SchemaToArrowSchema(tbl.Schema(), nil, true, false)
	require.NoError(t, err)
	deletes, err := array.TableFromJSON(memory.DefaultAllocator, deleteSchema,
		[]string{`[{"id":1,"region":"west"}]`})
	require.NoError(t, err)
	defer deletes.Release()
	records := func(yield func(arrow.RecordBatch, error) bool) {
		reader := array.NewTableReader(deletes, -1)
		defer reader.Release()
		for reader.Next() {
			if !yield(reader.RecordBatch(), nil) {
				return
			}
		}
	}

	txn := tbl.NewTransaction()
	files, err := txn.WriteEqualityDeletes(t.Context(), []int{1, 2}, iter.Seq2[arrow.RecordBatch, error](records))
	require.NoError(t, err)
	require.NoError(t, txn.NewRowDelta(nil).AddDeletes(files...).Commit(t.Context()))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	require.Equal(t, []string{"1:east", "2:east", "3:west"}, scanIDAndColumn(t, tbl))
}

// A manifest keeps the partition type the file was written with. After the
// column is promoted, the narrower value must be widened, not put under the
// wider field as is.
func TestScanWidensPromotedIdentityPartitionColumnMissingFromDataFile(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "ratio", Type: iceberg.PrimitiveTypes.Float32},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Name: "ratio", Transform: iceberg.IdentityTransform{},
	})
	tbl := identityPartitionTestTable(t, schema, spec, nil)
	tbl = addIDOnlyFile(t, tbl, "ratio", map[int]any{1000: float32(1.5)}, `[{"id":1},{"id":2}]`)

	txn := tbl.NewTransaction()
	require.NoError(t, txn.UpdateSchema(true, false).UpdateColumn([]string{"ratio"}, ColumnUpdate{
		FieldType: iceberg.Optional[iceberg.Type]{Val: iceberg.PrimitiveTypes.Float64, Valid: true},
	}).Commit())
	tbl, err := txn.Commit(t.Context())
	require.NoError(t, err)

	require.Equal(t, []string{"1:1.5", "2:1.5"}, scanIDAndColumn(t, tbl))
	require.Equal(t, []string{"1:1.5", "2:1.5"}, scanIDAndColumn(t, tbl,
		WithRowFilter(iceberg.EqualTo(iceberg.Reference("ratio"), 1.5))))
	require.Empty(t, scanIDAndColumn(t, tbl,
		WithRowFilter(iceberg.GreaterThan(iceberg.Reference("ratio"), 1.5))))
}

// The partition value comes from the spec the file was written with, not the
// table's current one, and it wins over the column's initial-default: a null
// partition value stays null. Only a file with no partition value for the
// column takes the default.
func TestIdentityPartitionValueUsesFileSpecAndWinsOverInitialDefault(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String, InitialDefault: "unknown"},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Name: "region", Transform: iceberg.IdentityTransform{},
	})
	tbl := identityPartitionTestTable(t, schema, spec, iceberg.Properties{PropertyFormatVersion: "3"})
	tbl = addIDOnlyFile(t, tbl, "east", map[int]any{1000: "east"}, `[{"id":1}]`)
	tbl = addIDOnlyFile(t, tbl, "null", map[int]any{1000: nil}, `[{"id":2}]`)

	txn := tbl.NewTransaction()
	require.NoError(t, txn.UpdateSpec(true).RemoveField("region").Commit())
	tbl, err := txn.Commit(t.Context())
	require.NoError(t, err)
	require.True(t, tbl.Spec().IsUnpartitioned())
	tbl = addIDOnlyFile(t, tbl, "unpartitioned", nil, `[{"id":3}]`)

	region := iceberg.Reference("region")
	tests := []struct {
		name   string
		filter iceberg.BooleanExpression
		want   []string
	}{
		{"no filter", iceberg.AlwaysTrue{}, []string{"1:east", "2:<null>", "3:unknown"}},
		{"partition value", iceberg.EqualTo(region, "east"), []string{"1:east"}},
		{"initial-default", iceberg.EqualTo(region, "unknown"), []string{"3:unknown"}},
		{"is null", iceberg.IsNull(region), []string{"2:<null>"}},
		{"not null", iceberg.NotNull(region), []string{"1:east", "3:unknown"}},
		{"not equal", iceberg.NotEqualTo(region, "east"), []string{"3:unknown"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, scanIDAndColumn(t, tbl, WithRowFilter(tt.filter)))
		})
	}
}
