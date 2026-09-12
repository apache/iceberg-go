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
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	parquetschema "github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildRowGroupMetricsMetadata(t testing.TB, rowGroups, columns int, withStats bool) *metadata.FileMetaData {
	t.Helper()
	fields := make(parquetschema.FieldList, columns)
	for i := range fields {
		fields[i] = parquetschema.NewByteArrayNode(fmt.Sprintf("field_%d", i), parquet.Repetitions.Required, int32(i+1))
	}
	root, err := parquetschema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	if err != nil {
		t.Fatal(err)
	}

	builder := metadata.NewFileMetadataBuilder(parquetschema.NewSchema(root), parquet.NewWriterProperties(), nil)
	for rowGroup := range rowGroups {
		rg := builder.AppendRowGroup()
		rg.SetNumRows(1)
		for column := range columns {
			chunk := rg.NextColumnChunk()
			if withStats {
				var stats metadata.EncodedStatistics
				stats.SetMin([]byte("a"))
				stats.SetMax([]byte("z"))
				stats.SetNullCount(0)
				chunk.SetStats(stats)
			}
			if err := chunk.Finish(metadata.ChunkMetaInfo{
				NumValues:        1,
				DataPageOffset:   int64(100 + rowGroup*columns + column),
				IndexPageOffset:  -1,
				CompressedSize:   8,
				UncompressedSize: 8,
			}, false, false, metadata.EncodingStats{}); err != nil {
				t.Fatal(err)
			}
		}
		if err := rg.Finish(int64(columns*8), int16(rowGroup)); err != nil {
			t.Fatal(err)
		}
	}

	meta, err := builder.Finish()
	if err != nil {
		t.Fatal(err)
	}

	return meta
}

// buildDecimalRowGroupMetadata builds a single row group holding one decimal
// column with the supplied plain-encoded min and max statistics. The min and
// max are kept distinct so that confusing the lower bound with the upper one is
// detectable.
func buildDecimalRowGroupMetadata(t testing.TB, physical parquet.Type, typeLen int, precision, scale int32, minEnc, maxEnc []byte) *metadata.FileMetaData {
	t.Helper()
	node, err := parquetschema.NewPrimitiveNodeLogical("decimal", parquet.Repetitions.Required,
		parquetschema.NewDecimalLogicalType(precision, scale), physical, typeLen, 1)
	require.NoError(t, err)
	root, err := parquetschema.NewGroupNode("schema", parquet.Repetitions.Required,
		parquetschema.FieldList{node}, -1)
	require.NoError(t, err)

	size := int64(len(minEnc) + len(maxEnc))
	builder := metadata.NewFileMetadataBuilder(parquetschema.NewSchema(root), parquet.NewWriterProperties(), nil)
	rg := builder.AppendRowGroup()
	rg.SetNumRows(2)
	chunk := rg.NextColumnChunk()
	var stats metadata.EncodedStatistics
	stats.SetMin(minEnc)
	stats.SetMax(maxEnc)
	stats.SetNullCount(0)
	chunk.SetStats(stats)
	require.NoError(t, chunk.Finish(metadata.ChunkMetaInfo{
		NumValues:        2,
		DataPageOffset:   100,
		IndexPageOffset:  -1,
		CompressedSize:   size,
		UncompressedSize: size,
	}, false, false, metadata.EncodingStats{}))
	require.NoError(t, rg.Finish(size, 0))

	meta, err := builder.Finish()
	require.NoError(t, err)

	return meta
}

func testDecimalRowGroup(t *testing.T, meta *metadata.FileMetaData, field iceberg.Type, pred iceberg.BooleanExpression) bool {
	t.Helper()
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "decimal", Type: field, Required: true,
	})
	expr, err := iceberg.BindExpr(schema, pred, true)
	require.NoError(t, err)
	eval := &inclusiveMetricsEval{expr: expr}
	keep, err := eval.TestRowGroup(meta.RowGroup(0), []int{0})
	require.NoError(t, err)

	return keep
}

// decimalOf builds a decimal predicate value from an unscaled value at the
// given scale. Named to avoid shadowing by the package's many "dec" locals.
func decimalOf(unscaled int64, scale int) iceberg.Decimal {
	return iceberg.Decimal{Val: decimal128.FromI64(unscaled), Scale: scale}
}

// Parquet writes INT32/INT64-backed decimal statistics little-endian, while an
// Iceberg bound is big-endian two's complement. Decoding the raw stat bytes as
// an Iceberg bound therefore yields a wildly wrong value and prunes row groups
// that do match, so those bounds are byte-reversed first. A
// FIXED_LEN_BYTE_ARRAY-backed decimal is big-endian in Parquet's plain encoding
// too, so it needs no conversion. Each case asserts in both directions: a row
// group that can match must survive, and one that cannot must be pruned, so
// neither a corrupted bound nor a silently dropped one passes.
// See apache/iceberg-go#1876.
func TestInclusiveMetricsEvalIntBackedDecimalRowGroup(t *testing.T) {
	// Unscaled bounds of the synthetic row group: -6.59 through 123.45 at
	// scale 2. The negative minimum exercises the sign-bit decode path.
	const (
		minUnscaled int64 = -659
		maxUnscaled int64 = 12345
		scale             = 2
	)

	// Big-endian two's complement, 16 bytes, as Parquet stores a
	// FIXED_LEN_BYTE_ARRAY decimal and as Iceberg expects a bound.
	flbaMin := bytes.Repeat([]byte{0xff}, 16)
	flbaMin[14], flbaMin[15] = 0xfd, 0x6d
	flbaMax := make([]byte, 16)
	flbaMax[14], flbaMax[15] = 0x30, 0x39

	tests := []struct {
		name           string
		physical       parquet.Type
		typeLen        int
		precision      int
		minEnc, maxEnc []byte
	}{
		{
			name:     "INT32-backed decimal",
			physical: parquet.Types.Int32, typeLen: -1, precision: 9,
			minEnc: []byte{0x6d, 0xfd, 0xff, 0xff}, // -659, little-endian
			maxEnc: []byte{0x39, 0x30, 0x00, 0x00}, // 12345, little-endian
		},
		{
			name:     "INT64-backed decimal",
			physical: parquet.Types.Int64, typeLen: -1, precision: 18,
			minEnc: []byte{0x6d, 0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			maxEnc: []byte{0x39, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			// Control: already big-endian, so reversing it would break it.
			name:     "FIXED_LEN_BYTE_ARRAY decimal",
			physical: parquet.Types.FixedLenByteArray, typeLen: 16, precision: 38,
			minEnc: flbaMin, maxEnc: flbaMax,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := buildDecimalRowGroupMetadata(t, tt.physical, tt.typeLen,
				int32(tt.precision), scale, tt.minEnc, tt.maxEnc)
			field := iceberg.DecimalTypeOf(tt.precision, scale)
			ref := iceberg.Reference("decimal")

			keep := func(pred iceberg.BooleanExpression) bool {
				return testDecimalRowGroup(t, meta, field, pred)
			}

			// One positive and one negative assertion per bound. The
			// "keeps" catch a bound decoded wrongly or swapped with its
			// partner; the "prunes" catch a bound dropped rather than
			// converted, which would silently disable decimal pruning.
			assert.True(t, keep(iceberg.EqualTo(ref, decimalOf(minUnscaled, scale))),
				"pruned a row group whose minimum matches")
			assert.True(t, keep(iceberg.EqualTo(ref, decimalOf(maxUnscaled, scale))),
				"pruned a row group whose maximum matches")
			assert.False(t, keep(iceberg.LessThan(ref, decimalOf(minUnscaled, scale))),
				"failed to prune using the lower bound")
			assert.False(t, keep(iceberg.GreaterThan(ref, decimalOf(maxUnscaled, scale))),
				"failed to prune using the upper bound")

			// Equality one step outside each bound. Redundant against every
			// regression we could think of, kept as cheap insurance: equality
			// just past a bound is the most common real query shape, and an
			// off-by-one there would otherwise rest on LessThan/GreaterThan
			// alone.
			assert.False(t, keep(iceberg.EqualTo(ref, decimalOf(minUnscaled-1, scale))),
				"failed to prune a value one below the lower bound")
			assert.False(t, keep(iceberg.EqualTo(ref, decimalOf(maxUnscaled+1, scale))),
				"failed to prune a value one above the upper bound")
		})
	}
}

// TestInclusiveMetricsEvalRealParquetDecimalRowGroup covers the same ground
// without hand-written statistics: it writes a real Parquet file with an
// INT32-backed decimal column and prunes against the metadata the writer
// produced, so the fixture above cannot drift from what Parquet actually emits.
func TestInclusiveMetricsEvalRealParquetDecimalRowGroup(t *testing.T) {
	const (
		precision, scale = 9, 2
		minUnscaled      = -659
		maxUnscaled      = 12345
	)

	node, err := parquetschema.NewPrimitiveNodeLogical("decimal", parquet.Repetitions.Required,
		parquetschema.NewDecimalLogicalType(precision, scale), parquet.Types.Int32, -1, 1)
	require.NoError(t, err)
	root, err := parquetschema.NewGroupNode("schema", parquet.Repetitions.Required,
		parquetschema.FieldList{node}, -1)
	require.NoError(t, err)

	var buf bytes.Buffer
	w := file.NewParquetWriter(&buf, root,
		file.WithWriterProps(parquet.NewWriterProperties(parquet.WithStats(true))))
	rgw, err := w.AppendRowGroupChecked()
	require.NoError(t, err)
	cw, err := rgw.NextColumn()
	require.NoError(t, err)
	_, err = cw.(*file.Int32ColumnChunkWriter).WriteBatch([]int32{minUnscaled, maxUnscaled}, nil, nil)
	require.NoError(t, err)
	require.NoError(t, cw.Close())
	require.NoError(t, rgw.Close())
	require.NoError(t, w.Close())

	rdr, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rdr.Close()) })

	meta := rdr.MetaData()
	chunk, err := meta.RowGroup(0).ColumnChunk(0)
	require.NoError(t, err)
	stats, err := chunk.Statistics()
	require.NoError(t, err)
	require.True(t, intBackedDecimal(stats.Descr()),
		"writer did not produce an INT32-backed decimal, so this test no longer covers the bug")

	field := iceberg.DecimalTypeOf(precision, scale)
	ref := iceberg.Reference("decimal")
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "decimal", Type: field, Required: true,
	})

	keep := func(pred iceberg.BooleanExpression) bool {
		expr, err := iceberg.BindExpr(schema, pred, true)
		require.NoError(t, err)
		eval := &inclusiveMetricsEval{expr: expr}
		result, err := eval.TestRowGroup(meta.RowGroup(0), []int{0})
		require.NoError(t, err)

		return result
	}

	assert.True(t, keep(iceberg.EqualTo(ref, decimalOf(minUnscaled, scale))),
		"pruned a row group holding the written minimum")
	assert.False(t, keep(iceberg.GreaterThan(ref, decimalOf(maxUnscaled, scale))),
		"failed to prune above the written maximum")
}

func TestInclusiveMetricsEvalRowGroupMetricsLifecycle(t *testing.T) {
	withStats := buildRowGroupMetricsMetadata(t, 1, 2, true)
	withoutStats := buildRowGroupMetricsMetadata(t, 1, 2, false)
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "field_0", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "field_1", Type: iceberg.PrimitiveTypes.String},
	)
	expr, err := iceberg.BindExpr(schema,
		iceberg.NotStartsWith(iceberg.Reference("field_0"), "a"), true)
	require.NoError(t, err)
	eval := &inclusiveMetricsEval{expr: expr}

	firstKeep, err := eval.TestRowGroup(withoutStats.RowGroup(0), []int{0, 1})
	require.NoError(t, err)
	assert.True(t, firstKeep)
	assert.NotNil(t, eval.valueCounts)
	assert.NotNil(t, eval.nullCounts)
	assert.Nil(t, eval.lowerBounds)
	assert.Nil(t, eval.upperBounds)
	assert.False(t, eval.mayContainNull(1))

	keep, err := eval.TestRowGroup(withStats.RowGroup(0), []int{0, 1})
	require.NoError(t, err)
	assert.True(t, keep)
	assert.Len(t, eval.valueCounts, 2)
	assert.Len(t, eval.nullCounts, 2)
	assert.Len(t, eval.lowerBounds, 2)
	assert.Len(t, eval.upperBounds, 2)

	keep, err = eval.TestRowGroup(withoutStats.RowGroup(0), []int{0, 1})
	require.NoError(t, err)
	assert.Equal(t, firstKeep, keep)
	assert.False(t, eval.mayContainNull(1))
	assert.NotNil(t, eval.valueCounts)
	assert.NotNil(t, eval.nullCounts)
	assert.NotNil(t, eval.lowerBounds)
	assert.NotNil(t, eval.upperBounds)
	assert.Empty(t, eval.valueCounts)
	assert.Empty(t, eval.nullCounts)
	assert.Empty(t, eval.lowerBounds)
	assert.Empty(t, eval.upperBounds)
}
