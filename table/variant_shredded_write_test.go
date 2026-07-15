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
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
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

// payloadAValues reads the "a" field of every variant row in the file's payload column.
func payloadAValues(t *testing.T, path string) []int64 {
	t.Helper()
	path = strings.TrimPrefix(path, "file://")
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	tbl, err := pqarrow.ReadTable(context.Background(), f, nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	defer tbl.Release()
	col := tbl.Column(tbl.Schema().FieldIndices("payload")[0]).Data().Chunk(0).(*extensions.VariantArray)
	out := make([]int64, 0, col.Len())
	for i := 0; i < col.Len(); i++ {
		v, err := col.Value(i)
		require.NoError(t, err)
		j, err := v.MarshalJSON()
		require.NoError(t, err)
		var m struct {
			A int64 `json:"a"`
		}
		require.NoError(t, json.Unmarshal(j, &m))
		out = append(out, m.A)
	}

	return out
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

// TestShreddedVariantWriteRoundTrip writes a shredded variant table and asserts the
// file is physically shredded and every row reads back to its original value.
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

// TestShreddedVariantPartitionedWrite verifies shredding through the fanout writer:
// each partition bootstraps independently and produces its own shredded file.
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
	seen := map[int64]int{}
	for _, df := range files {
		assert.True(t, payloadHasTypedValue(t, df.FilePath()), "each partition's file must be shredded")
		for _, a := range payloadAValues(t, df.FilePath()) {
			seen[a]++
		}
	}
	require.Len(t, seen, 8, "all 8 rows present across the partition files")
	for i := 0; i < 8; i++ {
		assert.Equalf(t, 1, seen[int64(5_000_000_000+i)], "row a=%d present exactly once", 5_000_000_000+i)
	}
}

// TestShreddedVariantPartitionedDecimalRace runs a partitioned shredded-decimal write
// with many small batches so partition writers open files concurrently. Under -race it
// fails if NewFileWriter appends StoreDecimalAsInteger into the shared write-props slice.
func TestShreddedVariantPartitionedDecimalRace(t *testing.T) {
	mem := memory.DefaultAllocator
	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "p", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}},
	)
	arrSchema, err := SchemaToArrowSchema(iceSchema, nil, true, false)
	require.NoError(t, err)

	const nPart = 16
	mkBatch := func() arrow.RecordBatch {
		pb := array.NewInt64Builder(mem)
		defer pb.Release()
		vb := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
		defer vb.Release()
		for i := 0; i < nPart*4; i++ {
			pb.Append(int64(i % nPart))
			var b variant.Builder
			require.NoError(t, b.AppendDecimal4(2, decimal.Decimal32(12345))) // shreds to Decimal128
			v, err := b.Build()
			require.NoError(t, err)
			vb.Append(v)
		}
		pArr := pb.NewInt64Array()
		defer pArr.Release()
		payloadArr := vb.NewArray()
		defer payloadArr.Release()

		return array.NewRecordBatch(arrSchema, []arrow.Array{pArr, payloadArr}, int64(nPart*4))
	}
	batches := make([]arrow.RecordBatch, 40)
	for i := range batches {
		batches[i] = mkBatch()
		defer batches[i].Release()
	}
	itr := func(yield func(arrow.RecordBatch, error) bool) {
		for _, b := range batches {
			if !yield(b, nil) {
				return
			}
		}
	}

	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Transform: iceberg.IdentityTransform{}, Name: "p",
	})
	loc := strings.ReplaceAll(t.TempDir(), "\\", "/")
	meta, err := NewMetadata(iceSchema, &spec, UnsortedSortOrder, loc, iceberg.Properties{
		PropertyFormatVersion:       "3",
		ParquetShredVariantsKey:     "true",
		ParquetVariantBufferSizeKey: "3", // tiny buffer -> partitions flush mid-stream, concurrently
	})
	require.NoError(t, err)
	mb, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	u := uuid.New()
	args := recordWritingArgs{sc: arrSchema, itr: itr, fs: iceio.LocalFS{}, writeUUID: &u, counter: infiniteCounter()}
	factory, err := newWriterFactory(loc, args, mb, iceSchema, 512*1024*1024)
	require.NoError(t, err)

	writer := newPartitionedFanoutWriter(spec, iceSchema, args.itr, factory)
	files := 0
	for df, err := range writer.Write(context.Background(), 8) {
		require.NoError(t, err)
		assert.True(t, payloadHasTypedValue(t, df.FilePath()), "each partition file must be shredded")
		files++
	}
	require.Equal(t, nPart, files, "one file per partition")
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

// TestShreddedVariantWriteScalarTypes writes a top-level decimal and timestamp variant
// through the real writer to Parquet and reads them back (physical-type + round-trip).
func TestShreddedVariantWriteScalarTypes(t *testing.T) {
	cases := []struct {
		name     string
		build    func(*variant.Builder) error
		physical parquet.Type
	}{
		{"decimal", func(b *variant.Builder) error { return b.AppendDecimal4(2, decimal.Decimal32(12345)) }, parquet.Types.Int32},
		{"decimal-int64", func(b *variant.Builder) error { return b.AppendDecimal8(0, decimal.Decimal64(123456789012)) }, parquet.Types.Int64},
		{"timestamp", func(b *variant.Builder) error {
			return b.AppendTimestamp(arrow.Timestamp(1700000000000000), true, true)
		}, parquet.Types.Int64},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			files, want := writeScalarVariantTable(t, c.build, 6)
			require.Len(t, files, 1)
			assert.Truef(t, payloadHasTypedValue(t, files[0].FilePath()), "%s column must be shredded", c.name)
			// Spec (VariantShredding.md): decimal4->INT32, decimal8->INT64, timestamp(micros)->INT64.
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

// TestShreddedVariantWriteMixedTypeField writes an object field that is int64 in most
// rows and string in a few. Inference shreds it as int64 (the majority); the minority
// string rows must round-trip through the unshredded residual, not be dropped.
func TestShreddedVariantWriteMixedTypeField(t *testing.T) {
	mem := memory.DefaultAllocator
	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}},
	)
	arrSchema, err := SchemaToArrowSchema(iceSchema, nil, true, false)
	require.NoError(t, err)

	const nRows, nStr = 10, 2 // rows [nRows-nStr, nRows) carry a string f; the rest int64
	idb := array.NewInt64Builder(mem)
	defer idb.Release()
	vb := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer vb.Release()
	for i := 0; i < nRows; i++ {
		idb.Append(int64(i))
		var b variant.Builder
		if i < nRows-nStr {
			require.NoError(t, b.Append(map[string]any{"f": int64(1000 + i)}))
		} else {
			require.NoError(t, b.Append(map[string]any{"f": "s" + strconv.Itoa(i)}))
		}
		v, err := b.Build()
		require.NoError(t, err)
		vb.Append(v)
	}
	idArr := idb.NewArray()
	defer idArr.Release()
	pArr := vb.NewArray()
	defer pArr.Release()
	rec := array.NewRecordBatch(arrSchema, []arrow.Array{idArr, pArr}, nRows)
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
	require.Len(t, files, 1)
	assert.True(t, payloadHasTypedValue(t, files[0].FilePath()), "f shreds as int64 (majority)")

	p := strings.TrimPrefix(files[0].FilePath(), "file://")
	f, err := os.Open(p)
	require.NoError(t, err)
	defer f.Close()
	tbl, err := pqarrow.ReadTable(context.Background(), f, nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	defer tbl.Release()
	col := tbl.Column(tbl.Schema().FieldIndices("payload")[0]).Data().Chunk(0).(*extensions.VariantArray)
	require.Equal(t, nRows, col.Len())
	for i := 0; i < col.Len(); i++ {
		v, err := col.Value(i)
		require.NoError(t, err)
		got, err := v.MarshalJSON()
		require.NoError(t, err)
		want := `{"f":` + strconv.Itoa(1000+i) + `}`
		if i >= nRows-nStr {
			want = `{"f":"s` + strconv.Itoa(i) + `"}`
		}
		assert.JSONEqf(t, want, string(got), "row %d (mixed-type minority must round-trip)", i)
	}

	// f shreds as int64 but the string rows land in the residual value column, so its
	// typed stats do not cover all values: the bound must be dropped (Java value() rule).
	_, hasLower := files[0].LowerBoundValues()[2]
	assert.False(t, hasLower, "partial-shred field must not emit a variant bound")
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

// columnPhysicalType returns the Parquet physical type of a top-level leaf column.
func columnPhysicalType(t *testing.T, path, name string) parquet.Type {
	t.Helper()
	path = strings.TrimPrefix(path, "file://")
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	pf, err := file.NewParquetReader(f)
	require.NoError(t, err)
	defer pf.Close()
	sc := pf.MetaData().Schema
	for i := 0; i < sc.NumColumns(); i++ {
		if col := sc.Column(i); col.Name() == name {
			return col.PhysicalType()
		}
	}
	t.Fatalf("column %q not found", name)

	return parquet.Types.Undefined
}

// TestShreddedVariantWriteRegularDecimalStats checks that with shredding on but no
// shredded decimal, a regular decimal column keeps FLBA and its bounds stay correct.
func TestShreddedVariantWriteRegularDecimalStats(t *testing.T) {
	mem := memory.DefaultAllocator
	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "price", Type: iceberg.DecimalTypeOf(9, 2)},
		iceberg.NestedField{ID: 3, Name: "payload", Type: iceberg.VariantType{}},
	)
	arrSchema, err := SchemaToArrowSchema(iceSchema, nil, true, false)
	require.NoError(t, err)

	idb := array.NewInt64Builder(mem)
	defer idb.Release()
	decb := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 9, Scale: 2})
	defer decb.Release()
	vb := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer vb.Release()
	for i := 1; i <= 6; i++ {
		idb.Append(int64(i))
		decb.Append(decimal128.FromI64(int64(i * 100))) // 1.00 .. 6.00
		var b variant.Builder
		require.NoError(t, b.Append(map[string]any{"a": int64(5_000_000_000 + i)}))
		v, err := b.Build()
		require.NoError(t, err)
		vb.Append(v)
	}
	idArr := idb.NewArray()
	defer idArr.Release()
	decArr := decb.NewArray()
	defer decArr.Release()
	pArr := vb.NewArray()
	defer pArr.Release()
	rec := array.NewRecordBatch(arrSchema, []arrow.Array{idArr, decArr, pArr}, 6)
	defer rec.Release()

	loc := strings.ReplaceAll(t.TempDir(), "\\", "/")
	meta, err := NewMetadata(iceSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder, loc, iceberg.Properties{
		PropertyFormatVersion:   "3",
		ParquetShredVariantsKey: "true",
	})
	require.NoError(t, err)
	mb, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	u := uuid.New()
	itr := func(yield func(arrow.RecordBatch, error) bool) { yield(rec, nil) }
	args := recordWritingArgs{sc: arrSchema, itr: itr, fs: iceio.LocalFS{}, writeUUID: &u, counter: infiniteCounter()}
	factory, err := newWriterFactory(loc, args, mb, iceSchema, 512*1024*1024)
	require.NoError(t, err)

	var files []iceberg.DataFile
	for df, err := range unpartitionedWrite(context.Background(), factory, args.itr) {
		require.NoError(t, err)
		files = append(files, df)
	}
	require.Len(t, files, 1)

	// The variant shreds no decimal, so per-file gating leaves StoreDecimalAsInteger
	// off and the regular decimal column keeps its default FIXED_LEN_BYTE_ARRAY.
	assert.Equal(t, parquet.Types.FixedLenByteArray, columnPhysicalType(t, files[0].FilePath(), "price"))

	decTyp := iceberg.DecimalTypeOf(9, 2)
	lb, err := iceberg.LiteralFromBytes(decTyp, files[0].LowerBoundValues()[2])
	require.NoError(t, err)
	ub, err := iceberg.LiteralFromBytes(decTyp, files[0].UpperBoundValues()[2])
	require.NoError(t, err)
	assert.Equal(t, decimal128.FromI64(100), lb.(iceberg.DecimalLiteral).Value().Val, "min bound 1.00")
	assert.Equal(t, decimal128.FromI64(600), ub.(iceberg.DecimalLiteral).Value().Val, "max bound 6.00")
}

// TestShreddedVariantWriteRegularDecimalWithShreddedDecimal is the co-occurrence case:
// a file that shreds a variant decimal (so StoreDecimalAsInteger is on file-global) AND
// carries a regular decimal column. The regular column is then INT-encoded, and this
// pins that its manifest bounds stay correct under that encoding.
func TestShreddedVariantWriteRegularDecimalWithShreddedDecimal(t *testing.T) {
	mem := memory.DefaultAllocator
	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "price", Type: iceberg.DecimalTypeOf(9, 2)},
		iceberg.NestedField{ID: 3, Name: "payload", Type: iceberg.VariantType{}},
	)
	arrSchema, err := SchemaToArrowSchema(iceSchema, nil, true, false)
	require.NoError(t, err)

	idb := array.NewInt64Builder(mem)
	defer idb.Release()
	decb := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 9, Scale: 2})
	defer decb.Release()
	vb := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer vb.Release()
	for i := 1; i <= 6; i++ {
		idb.Append(int64(i))
		decb.Append(decimal128.FromI64(int64(i * 100))) // 1.00 .. 6.00
		var b variant.Builder
		require.NoError(t, b.AppendDecimal4(2, decimal.Decimal32(12345))) // variant shreds a decimal
		v, err := b.Build()
		require.NoError(t, err)
		vb.Append(v)
	}
	idArr := idb.NewArray()
	defer idArr.Release()
	decArr := decb.NewArray()
	defer decArr.Release()
	pArr := vb.NewArray()
	defer pArr.Release()
	rec := array.NewRecordBatch(arrSchema, []arrow.Array{idArr, decArr, pArr}, 6)
	defer rec.Release()

	loc := strings.ReplaceAll(t.TempDir(), "\\", "/")
	meta, err := NewMetadata(iceSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder, loc, iceberg.Properties{
		PropertyFormatVersion:   "3",
		ParquetShredVariantsKey: "true",
	})
	require.NoError(t, err)
	mb, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	u := uuid.New()
	itr := func(yield func(arrow.RecordBatch, error) bool) { yield(rec, nil) }
	args := recordWritingArgs{sc: arrSchema, itr: itr, fs: iceio.LocalFS{}, writeUUID: &u, counter: infiniteCounter()}
	factory, err := newWriterFactory(loc, args, mb, iceSchema, 512*1024*1024)
	require.NoError(t, err)

	var files []iceberg.DataFile
	for df, err := range unpartitionedWrite(context.Background(), factory, args.itr) {
		require.NoError(t, err)
		files = append(files, df)
	}
	require.Len(t, files, 1)

	// The variant shreds a decimal, so per-file gating turns StoreDecimalAsInteger on
	// file-global and the regular decimal column is INT-encoded.
	assert.True(t, payloadHasTypedValue(t, files[0].FilePath()), "payload decimal must be shredded")
	assert.Equal(t, parquet.Types.Int32, columnPhysicalType(t, files[0].FilePath(), "price"))

	// Bounds are derived from the Arrow values, so they must be correct regardless of
	// the INT physical encoding.
	decTyp := iceberg.DecimalTypeOf(9, 2)
	lb, err := iceberg.LiteralFromBytes(decTyp, files[0].LowerBoundValues()[2])
	require.NoError(t, err)
	ub, err := iceberg.LiteralFromBytes(decTyp, files[0].UpperBoundValues()[2])
	require.NoError(t, err)
	assert.Equal(t, decimal128.FromI64(100), lb.(iceberg.DecimalLiteral).Value().Val, "min bound 1.00")
	assert.Equal(t, decimal128.FromI64(600), ub.(iceberg.DecimalLiteral).Value().Val, "max bound 6.00")
}

// TestShreddedVariantClusteredWrite verifies shredding through the clustered writer.
func TestShreddedVariantClusteredWrite(t *testing.T) {
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
		pb.Append(int64(i / 4)) // clustered: 0,0,0,0,1,1,1,1
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
	itr := func(yield func(arrow.RecordBatch, error) bool) { yield(rec, nil) }
	args := recordWritingArgs{sc: arrSchema, itr: itr, fs: iceio.LocalFS{}, writeUUID: &writeUUID, counter: infiniteCounter()}
	factory, err := newWriterFactory(loc, args, metaBuilder, iceSchema, 512*1024*1024)
	require.NoError(t, err)

	var files []iceberg.DataFile
	for df, err := range clusteredPartitionedWrite(context.Background(), spec, iceSchema, factory, args.itr) {
		require.NoError(t, err)
		files = append(files, df)
	}
	require.Len(t, files, 2, "two partition values -> two files")
	seen := map[int64]int{}
	for _, df := range files {
		assert.True(t, payloadHasTypedValue(t, df.FilePath()), "each clustered partition's file must be shredded")
		for _, a := range payloadAValues(t, df.FilePath()) {
			seen[a]++
		}
	}
	require.Len(t, seen, 8, "all 8 rows present across the partition files")
	for i := 0; i < 8; i++ {
		assert.Equalf(t, 1, seen[int64(5_000_000_000+i)], "row a=%d present exactly once", 5_000_000_000+i)
	}
}

// TestShreddedVariantWriteLargeBootstrapBatch feeds one batch far bigger than the
// buffer (3000 rows, buffer 100) with a 1-byte target file size. The bounded buffer
// flushes every shredBufferRows and rolls, so the batch splits across many files;
// whole-batch buffering would replay all 3000 rows into a single file. Also checks
// row conservation and no leak (checked allocator).
func TestShreddedVariantWriteLargeBootstrapBatch(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(context.Background(), mem)

	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}},
	)
	arrSchema, err := SchemaToArrowSchema(iceSchema, nil, true, false)
	require.NoError(t, err)

	const nRows = 3000
	rec := buildVariantBatch(t, mem, arrSchema, 0, nRows) // one batch, far bigger than the buffer
	defer rec.Release()
	itr := func(yield func(arrow.RecordBatch, error) bool) { yield(rec, nil) }

	loc := strings.ReplaceAll(t.TempDir(), "\\", "/")
	meta, err := NewMetadata(iceSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder, loc, iceberg.Properties{
		PropertyFormatVersion:       "3",
		ParquetShredVariantsKey:     "true",
		ParquetVariantBufferSizeKey: "100",
	})
	require.NoError(t, err)
	mb, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	u := uuid.New()
	args := recordWritingArgs{sc: arrSchema, itr: itr, fs: iceio.LocalFS{}, writeUUID: &u, counter: infiniteCounter()}
	factory, err := newWriterFactory(loc, args, mb, iceSchema, 1) // 1-byte target: bounded buffer forces a split
	require.NoError(t, err)

	seen := map[int64]int{}
	files := 0
	for df, err := range unpartitionedWrite(ctx, factory, args.itr) {
		require.NoError(t, err)
		files++
		assert.True(t, payloadHasTypedValue(t, df.FilePath()), "file must be shredded")
		p := strings.TrimPrefix(df.FilePath(), "file://")
		f, err := os.Open(p)
		require.NoError(t, err)
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
	// Bounded buffer -> many files; whole-batch buffering would yield exactly one.
	assert.Greater(t, files, 1, "large batch must split across files, not buffer whole")
	require.Len(t, seen, nRows, "all rows present across files")
	for i := int64(0); i < nRows; i++ {
		assert.Equalf(t, 1, seen[i], "row id %d present exactly once", i)
	}
}

// TestInferShreddingSkipsUndecodable pins C4: a present-but-undecodable variant value
// is skipped and logged, not fatal - inference still succeeds from the good rows.
func TestInferShreddingSkipsUndecodable(t *testing.T) {
	mem := memory.DefaultAllocator
	vt := extensions.NewDefaultVariantType()

	var b variant.Builder
	require.NoError(t, b.Append(map[string]any{"a": int64(5_000_000_000)}))
	good, err := b.Build()
	require.NoError(t, err)
	metaBytes, valBytes := good.Metadata().Bytes(), good.Bytes()

	// storage struct<metadata,value>: 3 good rows + 1 undecodable (1-byte metadata
	// fails variant.NewMetadata "too short"). All rows non-null.
	sb := array.NewStructBuilder(mem, vt.StorageType().(*arrow.StructType))
	defer sb.Release()
	metaB := sb.FieldBuilder(0).(*array.BinaryBuilder)
	valB := sb.FieldBuilder(1).(*array.BinaryBuilder)
	for i := 0; i < 3; i++ {
		sb.Append(true)
		metaB.Append(metaBytes)
		valB.Append(valBytes)
	}
	sb.Append(true)
	metaB.Append([]byte{0x01})
	valB.Append(valBytes)
	storage := sb.NewStructArray()
	defer storage.Release()

	varr := array.NewExtensionArrayWithStorage(vt, storage).(*extensions.VariantArray)
	defer varr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "payload", Type: vt, Nullable: true}}, nil)
	rec := array.NewRecordBatch(schema, []arrow.Array{varr}, 4)
	defer rec.Release()

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	inferred := inferShreddingFromBatches([]arrow.RecordBatch{rec}, 100)

	require.Contains(t, inferred, 0, "inference must succeed despite one undecodable row")
	st, ok := inferred[0].(*arrow.StructType)
	require.True(t, ok)
	_, found := st.FieldsByName("a")
	assert.True(t, found, "schema inferred from the good rows")
	assert.Contains(t, buf.String(), "skipped", "the skip is logged, not silent")
}

// TestShreddedVariantWriteChildStats asserts the written data file carries variant
// child bounds under the parent field id: an object of the shredded fields' min/max
// keyed by normalized JSON path. writeVariantTable rows are {a: int64(5e9+i), b: "row"}.
func TestShreddedVariantWriteChildStats(t *testing.T) {
	files := writeVariantTable(t, iceberg.Properties{
		PropertyFormatVersion:   "3",
		ParquetShredVariantsKey: "true",
	})
	require.Len(t, files, 1)
	df := files[0]

	const variantField = 2 // payload

	buildObj := func(a int64) []byte {
		var b variant.Builder
		start := b.Offset()
		entries := []variant.FieldEntry{b.NextField(start, "$['a']")}
		require.NoError(t, b.AppendInt(a))
		entries = append(entries, b.NextField(start, "$['b']"))
		require.NoError(t, b.AppendString("row"))
		require.NoError(t, b.FinishObject(start, entries))
		v, err := b.Build()
		require.NoError(t, err)

		return append(append([]byte{}, v.Metadata().Bytes()...), v.Bytes()...)
	}

	assert.Equal(t, buildObj(5_000_000_000), df.LowerBoundValues()[variantField], "lower bound object")
	assert.Equal(t, buildObj(5_000_000_007), df.UpperBoundValues()[variantField], "upper bound object")
}

// TestShreddedVariantWriteNullFieldKeepsBound: int64+null field still gets a bound.
func TestShreddedVariantWriteNullFieldKeepsBound(t *testing.T) {
	mem := memory.DefaultAllocator
	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}},
	)
	arrSchema, err := SchemaToArrowSchema(iceSchema, nil, true, false)
	require.NoError(t, err)

	const nRows, nNull = 10, 3
	idb := array.NewInt64Builder(mem)
	defer idb.Release()
	vb := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer vb.Release()
	for i := 0; i < nRows; i++ {
		idb.Append(int64(i))
		var b variant.Builder
		if i < nRows-nNull {
			require.NoError(t, b.Append(map[string]any{"f": int64(1000 + i)}))
		} else {
			require.NoError(t, b.Append(map[string]any{"f": nil}))
		}
		v, err := b.Build()
		require.NoError(t, err)
		vb.Append(v)
	}
	idArr := idb.NewArray()
	defer idArr.Release()
	pArr := vb.NewArray()
	defer pArr.Release()
	rec := array.NewRecordBatch(arrSchema, []arrow.Array{idArr, pArr}, nRows)
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
	require.Len(t, files, 1)

	// f has int64 + null (no conflict): its bound is kept and covers the non-null range.
	buildObj := func(fv int64) []byte {
		var b variant.Builder
		start := b.Offset()
		entries := []variant.FieldEntry{b.NextField(start, "$['f']")}
		require.NoError(t, b.AppendInt(fv))
		require.NoError(t, b.FinishObject(start, entries))
		v, err := b.Build()
		require.NoError(t, err)

		return append(append([]byte{}, v.Metadata().Bytes()...), v.Bytes()...)
	}
	assert.Equal(t, buildObj(1000), files[0].LowerBoundValues()[2])
	assert.Equal(t, buildObj(1006), files[0].UpperBoundValues()[2])
}

func TestShreddedVariantWriteFloatBound(t *testing.T) {
	mem := memory.DefaultAllocator
	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.VariantType{}},
	)
	arrSchema, err := SchemaToArrowSchema(iceSchema, nil, true, false)
	require.NoError(t, err)

	const nRows = 8
	idb := array.NewInt64Builder(mem)
	defer idb.Release()
	vb := extensions.NewVariantBuilder(mem, extensions.NewDefaultVariantType())
	defer vb.Release()
	for i := 0; i < nRows; i++ {
		idb.Append(int64(i))
		var b variant.Builder
		require.NoError(t, b.Append(map[string]any{"g": 1.5 + float64(i)}))
		v, err := b.Build()
		require.NoError(t, err)
		vb.Append(v)
	}
	idArr := idb.NewArray()
	defer idArr.Release()
	pArr := vb.NewArray()
	defer pArr.Release()
	rec := array.NewRecordBatch(arrSchema, []arrow.Array{idArr, pArr}, nRows)
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
	require.Len(t, files, 1)

	buildObj := func(g float64) []byte {
		var b variant.Builder
		start := b.Offset()
		entries := []variant.FieldEntry{b.NextField(start, "$['g']")}
		require.NoError(t, b.AppendFloat64(g))
		require.NoError(t, b.FinishObject(start, entries))
		v, err := b.Build()
		require.NoError(t, err)

		return append(append([]byte{}, v.Metadata().Bytes()...), v.Bytes()...)
	}
	assert.Equal(t, buildObj(1.5), files[0].LowerBoundValues()[2])
	assert.Equal(t, buildObj(8.5), files[0].UpperBoundValues()[2])
}

// TestShreddedVariantWriteScalarBounds pins the emitted bound for each shredded scalar
// type (uniform rows, so min==max): the root "$" bound must equal the re-encoded value.
func TestShreddedVariantWriteScalarBounds(t *testing.T) {
	buildRootBound := func(build func(*variant.Builder) error) []byte {
		var b variant.Builder
		start := b.Offset()
		entries := []variant.FieldEntry{b.NextField(start, "$")}
		require.NoError(t, build(&b))
		require.NoError(t, b.FinishObject(start, entries))
		v, err := b.Build()
		require.NoError(t, err)

		return append(append([]byte{}, v.Metadata().Bytes()...), v.Bytes()...)
	}

	uid := uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff")
	cases := []struct {
		name  string
		build func(*variant.Builder) error
	}{
		{"bool", func(b *variant.Builder) error { return b.AppendBool(true) }},
		{"int-small", func(b *variant.Builder) error { return b.AppendInt(1000) }},
		{"int64", func(b *variant.Builder) error { return b.AppendInt(5_000_000_000) }},
		{"float32", func(b *variant.Builder) error { return b.AppendFloat32(1.5) }},
		{"float64", func(b *variant.Builder) error { return b.AppendFloat64(2.5) }},
		{"string", func(b *variant.Builder) error { return b.AppendString("hi") }},
		{"binary", func(b *variant.Builder) error { return b.AppendBinary([]byte{1, 2, 3}) }},
		{"date", func(b *variant.Builder) error { return b.AppendDate(arrow.Date32(19000)) }},
		{"time", func(b *variant.Builder) error { return b.AppendTimeMicro(arrow.Time64(3600000000)) }},
		{"timestamp-tz", func(b *variant.Builder) error {
			return b.AppendTimestamp(arrow.Timestamp(1700000000000000), true, true)
		}},
		{"timestamp-ntz", func(b *variant.Builder) error {
			return b.AppendTimestamp(arrow.Timestamp(1700000000000000), true, false)
		}},
		{"uuid", func(b *variant.Builder) error { return b.AppendUUID(uid) }},
		{"decimal4", func(b *variant.Builder) error { return b.AppendDecimal4(2, decimal.Decimal32(12345)) }},
		{"decimal8", func(b *variant.Builder) error { return b.AppendDecimal8(0, decimal.Decimal64(123456789012)) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			files, _ := writeScalarVariantTable(t, c.build, 4)
			require.Len(t, files, 1)
			require.Truef(t, payloadHasTypedValue(t, files[0].FilePath()), "%s must shred", c.name)
			want := buildRootBound(c.build)
			assert.Equalf(t, want, files[0].LowerBoundValues()[2], "%s lower bound", c.name)
			assert.Equalf(t, want, files[0].UpperBoundValues()[2], "%s upper bound", c.name)
		})
	}
}
