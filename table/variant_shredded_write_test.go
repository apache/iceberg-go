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
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	pqschema "github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildVariantBatch makes one record batch of n rows {id, payload} where payload
// is a uniform object {a: bigInt, b: "row-i"} in the non-shredded layout.
func buildVariantBatch(t *testing.T, mem memory.Allocator, arrSchema *arrow.Schema, start, n int) arrow.RecordBatch {
	t.Helper()
	idb := array.NewInt64Builder(mem)
	defer idb.Release()
	vb := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer vb.Release()

	for i := start; i < start+n; i++ {
		idb.Append(int64(i))
		var b variant.Builder
		require.NoError(t, b.Append(map[string]any{"a": int64(5_000_000_000 + i), "b": "row"}))
		v, err := b.Build()
		require.NoError(t, err)
		vb.Append(v)
	}
	idArr := idb.NewArray()
	defer idArr.Release()
	payloadArr := vb.NewArray()
	defer payloadArr.Release()

	return array.NewRecordBatch(arrSchema, []arrow.Array{idArr, payloadArr}, int64(n))
}

// payloadHasTypedValue opens a written parquet file and reports whether the
// top-level "payload" variant group has a typed_value child (i.e. was shredded).
func payloadHasTypedValue(t *testing.T, path string) bool {
	t.Helper()
	path = strings.TrimPrefix(path, "file://")
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	pf, err := file.NewParquetReader(f)
	require.NoError(t, err)
	defer pf.Close()

	root := pf.MetaData().Schema.Root()
	idx := root.FieldIndexByName("payload")
	require.GreaterOrEqual(t, idx, 0, "payload field must exist")
	payload, ok := root.Field(idx).(*pqschema.GroupNode)
	require.True(t, ok, "payload must be a group node")
	for i := 0; i < payload.NumFields(); i++ {
		if payload.Field(i).Name() == "typed_value" {
			return true
		}
	}

	return false
}

// payloadTypedValuePhysicalType returns the Parquet physical type of the scalar
// payload->typed_value column, used to assert the spec's per-type physical mapping.
func payloadTypedValuePhysicalType(t *testing.T, path string) parquet.Type {
	t.Helper()
	path = strings.TrimPrefix(path, "file://")
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	pf, err := file.NewParquetReader(f)
	require.NoError(t, err)
	defer pf.Close()

	root := pf.MetaData().Schema.Root()
	payload := root.Field(root.FieldIndexByName("payload")).(*pqschema.GroupNode)
	for i := 0; i < payload.NumFields(); i++ {
		if tv, ok := payload.Field(i).(*pqschema.PrimitiveNode); ok && tv.Name() == "typed_value" {
			return tv.PhysicalType()
		}
	}
	t.Fatal("payload has no scalar typed_value")

	return parquet.Types.Undefined
}

func writeVariantTable(t *testing.T, props iceberg.Properties) []iceberg.DataFile {
	t.Helper()
	mem := memory.DefaultAllocator

	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}, Required: false},
	)
	arrSchema, err := SchemaToArrowSchema(iceSchema, nil, true, false)
	require.NoError(t, err)

	// Two batches of 4 rows; with buffer-size 4 the first fills the bootstrap
	// buffer (inference + open + replay) and the second writes in steady state.
	rec1 := buildVariantBatch(t, mem, arrSchema, 0, 4)
	defer rec1.Release()
	rec2 := buildVariantBatch(t, mem, arrSchema, 4, 4)
	defer rec2.Release()

	itr := func(yield func(arrow.RecordBatch, error) bool) {
		if !yield(rec1, nil) {
			return
		}
		yield(rec2, nil)
	}

	loc := strings.ReplaceAll(t.TempDir(), "\\", "/")
	meta, err := NewMetadata(iceSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder, loc, props)
	require.NoError(t, err)
	metaBuilder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	writeUUID := uuid.New()
	args := recordWritingArgs{
		sc:        arrSchema,
		itr:       itr,
		fs:        iceio.LocalFS{},
		writeUUID: &writeUUID,
		counter: func(yield func(int) bool) {
			for i := 0; ; i++ {
				if !yield(i) {
					break
				}
			}
		},
	}
	factory, err := newWriterFactory(loc, args, metaBuilder, iceSchema, 512*1024*1024)
	require.NoError(t, err)

	var files []iceberg.DataFile
	for df, err := range unpartitionedWrite(context.Background(), factory, args.itr) {
		require.NoError(t, err)
		files = append(files, df)
	}

	return files
}

// TestShreddedVariantWriteRoundTrip writes a variant table with shredding on,
// asserts the produced file is physically shredded, and that every row reads
// back to its original value through the standard reader path.
func TestShreddedVariantWriteRoundTrip(t *testing.T) {
	files := writeVariantTable(t, iceberg.Properties{
		PropertyFormatVersion:       "3",
		ParquetShredVariantsKey:     "true",
		ParquetVariantBufferSizeKey: "4",
	})
	require.Len(t, files, 1)
	var total int64
	for _, df := range files {
		total += df.Count()
	}
	require.Equal(t, int64(8), total, "all 8 rows written")

	path := files[0].FilePath()
	assert.True(t, payloadHasTypedValue(t, path), "payload must be shredded (typed_value child)")

	// Read back and verify values round-trip.
	p := strings.TrimPrefix(path, "file://")
	f, err := os.Open(p)
	require.NoError(t, err)
	defer f.Close()
	tbl, err := pqarrow.ReadTable(context.Background(), f, nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	defer tbl.Release()

	col := tbl.Column(tbl.Schema().FieldIndices("payload")[0]).Data().Chunk(0).(*extensions.VariantArray)
	require.Equal(t, 8, col.Len())
	for i := 0; i < col.Len(); i++ {
		v, err := col.Value(i)
		require.NoError(t, err)
		j, err := v.MarshalJSON()
		require.NoError(t, err)
		assert.JSONEq(t, `{"a":`+strconv.Itoa(5_000_000_000+i)+`,"b":"row"}`, string(j))
	}
}

// TestVariantWriteDefaultNotShredded confirms the default (property off) write
// path is unchanged: the file has no typed_value child.
func TestVariantWriteDefaultNotShredded(t *testing.T) {
	files := writeVariantTable(t, iceberg.Properties{PropertyFormatVersion: "3"})
	require.Len(t, files, 1)
	assert.False(t, payloadHasTypedValue(t, files[0].FilePath()),
		"default write must not shred (no typed_value)")
}

func infiniteCounter() func(func(int) bool) {
	return func(yield func(int) bool) {
		for i := 0; ; i++ {
			if !yield(i) {
				break
			}
		}
	}
}

// TestShreddedVariantWriteNoLeak uses a tiny target so every file rolls, and the
// checked allocator verifies no leak across the bootstrap -> roll -> re-bootstrap cycle.
func TestShreddedVariantWriteNoLeak(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)

	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}},
	)
	arrSchema, err := SchemaToArrowSchema(iceSchema, nil, true, false)
	require.NoError(t, err)

	const nBatches = 4
	batches := make([]arrow.RecordBatch, nBatches)
	for i := range batches {
		batches[i] = buildVariantBatch(t, mem, arrSchema, i*3, 3)
	}
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()

	idx := 0
	itr := func(yield func(arrow.RecordBatch, error) bool) {
		for idx < len(batches) {
			b := batches[idx]
			idx++
			if !yield(b, nil) {
				return
			}
		}
	}

	loc := strings.ReplaceAll(t.TempDir(), "\\", "/")
	meta, err := NewMetadata(iceSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder, loc, iceberg.Properties{
		PropertyFormatVersion:       "3",
		ParquetShredVariantsKey:     "true",
		ParquetVariantBufferSizeKey: "2",
	})
	require.NoError(t, err)
	metaBuilder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	writeUUID := uuid.New()
	args := recordWritingArgs{sc: arrSchema, itr: itr, fs: iceio.LocalFS{}, writeUUID: &writeUUID, counter: infiniteCounter()}
	// 1-byte target forces a roll after every file.
	factory, err := newWriterFactory(loc, args, metaBuilder, iceSchema, 1)
	require.NoError(t, err)

	var nFiles int
	for df, err := range unpartitionedWrite(ctx, factory, args.itr) {
		require.NoError(t, err)
		nFiles++
		assert.True(t, payloadHasTypedValue(t, df.FilePath()), "each rolled file must be shredded")
	}
	require.Greater(t, nFiles, 1, "tiny target should roll into multiple files (re-bootstrap each)")
}

// TestShreddedVariantPartitionedWrite verifies shredding works through the
// fanout writer: each partition's RollingDataWriter bootstraps independently and
// produces its own shredded file.
func TestShreddedVariantPartitionedWrite(t *testing.T) {
	mem := memory.DefaultAllocator

	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "p", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}},
	)
	arrSchema, err := SchemaToArrowSchema(iceSchema, nil, true, false)
	require.NoError(t, err)

	pb := array.NewInt64Builder(mem)
	defer pb.Release()
	vb := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer vb.Release()
	for i := 0; i < 8; i++ {
		pb.Append(int64(i % 2)) // partition values 0 and 1
		var b variant.Builder
		require.NoError(t, b.Append(map[string]any{"a": int64(5_000_000_000 + i), "b": "row"}))
		v, err := b.Build()
		require.NoError(t, err)
		vb.Append(v)
	}
	pArr := pb.NewInt64Array()
	defer pArr.Release()
	payloadArr := vb.NewArray()
	defer payloadArr.Release()
	rec := array.NewRecordBatch(arrSchema, []arrow.Array{pArr, payloadArr}, 8)
	defer rec.Release()

	itr := func(yield func(arrow.RecordBatch, error) bool) { yield(rec, nil) }

	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "p",
	})
	loc := strings.ReplaceAll(t.TempDir(), "\\", "/")
	meta, err := NewMetadata(iceSchema, &spec, UnsortedSortOrder, loc, iceberg.Properties{
		PropertyFormatVersion:   "3",
		ParquetShredVariantsKey: "true",
	})
	require.NoError(t, err)
	metaBuilder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	writeUUID := uuid.New()
	args := recordWritingArgs{sc: arrSchema, itr: itr, fs: iceio.LocalFS{}, writeUUID: &writeUUID, counter: infiniteCounter()}
	factory, err := newWriterFactory(loc, args, metaBuilder, iceSchema, 512*1024*1024)
	require.NoError(t, err)

	writer := newPartitionedFanoutWriter(spec, iceSchema, args.itr, factory)
	var files []iceberg.DataFile
	for df, err := range writer.Write(context.Background(), 2) {
		require.NoError(t, err)
		files = append(files, df)
	}

	require.Len(t, files, 2, "two partition values -> two files")
	for _, df := range files {
		assert.True(t, payloadHasTypedValue(t, df.FilePath()), "each partition's file must be shredded")
	}
}

func writeScalarVariantTable(t *testing.T, build func(*variant.Builder) error, n int) ([]iceberg.DataFile, string) {
	t.Helper()
	mem := memory.DefaultAllocator
	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}},
	)
	arrSchema, err := SchemaToArrowSchema(iceSchema, nil, true, false)
	require.NoError(t, err)

	mk := func() variant.Value {
		var b variant.Builder
		require.NoError(t, build(&b))
		v, err := b.Build()
		require.NoError(t, err)

		return v
	}
	want, err := mk().MarshalJSON()
	require.NoError(t, err)

	idb := array.NewInt64Builder(mem)
	defer idb.Release()
	vb := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer vb.Release()
	for i := 0; i < n; i++ {
		idb.Append(int64(i))
		vb.Append(mk())
	}
	idArr := idb.NewArray()
	defer idArr.Release()
	pArr := vb.NewArray()
	defer pArr.Release()
	rec := array.NewRecordBatch(arrSchema, []arrow.Array{idArr, pArr}, int64(n))
	defer rec.Release()

	itr := func(yield func(arrow.RecordBatch, error) bool) { yield(rec, nil) }
	loc := strings.ReplaceAll(t.TempDir(), "\\", "/")
	meta, err := NewMetadata(iceSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder, loc, iceberg.Properties{
		PropertyFormatVersion:   "3",
		ParquetShredVariantsKey: "true",
	})
	require.NoError(t, err)
	mb, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	u := uuid.New()
	args := recordWritingArgs{sc: arrSchema, itr: itr, fs: iceio.LocalFS{}, writeUUID: &u, counter: infiniteCounter()}
	factory, err := newWriterFactory(loc, args, mb, iceSchema, 512*1024*1024)
	require.NoError(t, err)

	var files []iceberg.DataFile
	for df, err := range unpartitionedWrite(context.Background(), factory, args.itr) {
		require.NoError(t, err)
		files = append(files, df)
	}

	return files, string(want)
}

// TestShreddedVariantWriteScalarTypes writes a top-level decimal and a timestamp
// variant column through the real writer to Parquet and reads them back, covering
// the Parquet logical-type round-trip for non-int/string leaves.
func TestShreddedVariantWriteScalarTypes(t *testing.T) {
	cases := []struct {
		name     string
		build    func(*variant.Builder) error
		physical parquet.Type
	}{
		{"decimal", func(b *variant.Builder) error { return b.AppendDecimal4(2, decimal.Decimal32(12345)) }, parquet.Types.Int32},
		{"timestamp", func(b *variant.Builder) error {
			return b.AppendTimestamp(arrow.Timestamp(1700000000000000), true, true)
		}, parquet.Types.Int64},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			files, want := writeScalarVariantTable(t, c.build, 6)
			require.Len(t, files, 1)
			assert.Truef(t, payloadHasTypedValue(t, files[0].FilePath()), "%s column must be shredded", c.name)
			// Spec (VariantShredding.md): decimal4->INT32, timestamp(micros)->INT64.
			assert.Equalf(t, c.physical, payloadTypedValuePhysicalType(t, files[0].FilePath()),
				"%s typed_value must use the spec physical type", c.name)

			p := strings.TrimPrefix(files[0].FilePath(), "file://")
			f, err := os.Open(p)
			require.NoError(t, err)
			defer f.Close()
			tbl, err := pqarrow.ReadTable(context.Background(), f, nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
			require.NoError(t, err)
			defer tbl.Release()

			col := tbl.Column(tbl.Schema().FieldIndices("payload")[0]).Data().Chunk(0).(*extensions.VariantArray)
			require.Equal(t, 6, col.Len())
			for i := 0; i < col.Len(); i++ {
				v, err := col.Value(i)
				require.NoError(t, err)
				got, err := v.MarshalJSON()
				require.NoError(t, err)
				assert.JSONEqf(t, want, string(got), "%s row %d round-trip", c.name, i)
			}
		})
	}
}

// TestShreddedVariantWriteRowConservation verifies every row survives bootstrap ->
// replay -> roll -> re-bootstrap exactly once (buffer size 5 crosses the 3-row batches).
func TestShreddedVariantWriteRowConservation(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)

	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}},
	)
	arrSchema, err := SchemaToArrowSchema(iceSchema, nil, true, false)
	require.NoError(t, err)

	const nBatches, batchRows = 5, 3
	const total = nBatches * batchRows
	batches := make([]arrow.RecordBatch, nBatches)
	for i := range batches {
		batches[i] = buildVariantBatch(t, mem, arrSchema, i*batchRows, batchRows)
	}
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()

	idx := 0
	itr := func(yield func(arrow.RecordBatch, error) bool) {
		for idx < len(batches) {
			b := batches[idx]
			idx++
			if !yield(b, nil) {
				return
			}
		}
	}

	loc := strings.ReplaceAll(t.TempDir(), "\\", "/")
	meta, err := NewMetadata(iceSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder, loc, iceberg.Properties{
		PropertyFormatVersion:       "3",
		ParquetShredVariantsKey:     "true",
		ParquetVariantBufferSizeKey: "5", // crosses the 3-row batch boundary
	})
	require.NoError(t, err)
	metaBuilder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	writeUUID := uuid.New()
	args := recordWritingArgs{sc: arrSchema, itr: itr, fs: iceio.LocalFS{}, writeUUID: &writeUUID, counter: infiniteCounter()}
	factory, err := newWriterFactory(loc, args, metaBuilder, iceSchema, 1) // 1-byte target rolls every file
	require.NoError(t, err)

	seen := make(map[int64]int, total)
	var files int
	for df, err := range unpartitionedWrite(ctx, factory, args.itr) {
		require.NoError(t, err)
		files++
		assert.True(t, payloadHasTypedValue(t, df.FilePath()), "each rolled file must be shredded")

		p := strings.TrimPrefix(df.FilePath(), "file://")
		f, err := os.Open(p)
		require.NoError(t, err)
		// Read-back uses the default allocator: this test proves write-path row
		// conservation, not reader leaks (mem.AssertSize already guards the write).
		tbl, err := pqarrow.ReadTable(context.Background(), f, nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		require.NoError(t, err)
		idCol := tbl.Column(tbl.Schema().FieldIndices("id")[0]).Data()
		for _, ch := range idCol.Chunks() {
			chunk := ch.(*array.Int64)
			for r := 0; r < chunk.Len(); r++ {
				seen[chunk.Value(r)]++
			}
		}
		tbl.Release()
		f.Close()
	}

	require.Greater(t, files, 1, "tiny target should roll into multiple files")
	require.Len(t, seen, total, "every distinct row id must appear")
	for i := int64(0); i < total; i++ {
		assert.Equalf(t, 1, seen[i], "row id %d must appear exactly once (no loss/duplication)", i)
	}
}

// TestShreddedVariantWriteLargeDecimal writes a precision > 18 decimal, which shreds
// to a FIXED_LEN_BYTE_ARRAY typed_value, and round-trips it through real Parquet.
func TestShreddedVariantWriteLargeDecimal(t *testing.T) {
	dec, err := decimal.Decimal128FromString("12345678901234567890123", 23, 0) // 23 digits
	require.NoError(t, err)
	build := func(b *variant.Builder) error { return b.AppendDecimal16(0, dec) }

	files, want := writeScalarVariantTable(t, build, 6)
	require.Len(t, files, 1)
	assert.True(t, payloadHasTypedValue(t, files[0].FilePath()), "decimal column must be shredded")
	assert.Equal(t, parquet.Types.FixedLenByteArray, payloadTypedValuePhysicalType(t, files[0].FilePath()),
		"precision > 18 must use FIXED_LEN_BYTE_ARRAY")

	p := strings.TrimPrefix(files[0].FilePath(), "file://")
	f, err := os.Open(p)
	require.NoError(t, err)
	defer f.Close()
	tbl, err := pqarrow.ReadTable(context.Background(), f, nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	defer tbl.Release()

	col := tbl.Column(tbl.Schema().FieldIndices("payload")[0]).Data().Chunk(0).(*extensions.VariantArray)
	require.Equal(t, 6, col.Len())
	for i := 0; i < col.Len(); i++ {
		v, err := col.Value(i)
		require.NoError(t, err)
		got, err := v.MarshalJSON()
		require.NoError(t, err)
		assert.JSONEqf(t, want, string(got), "row %d round-trip", i)
	}
}
