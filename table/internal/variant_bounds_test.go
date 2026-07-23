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

package internal

import (
	"bytes"
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizedVariantPathEscaping(t *testing.T) {
	for _, tt := range []struct {
		name   string
		fields []string
		want   string
	}{
		{"root", nil, "$"},
		{"plain", []string{"event_type"}, "$['event_type']"},
		{"dotted name kept literal", []string{"user.name"}, "$['user.name']"},
		{"nested", []string{"location", "latitude"}, "$['location']['latitude']"},
		{"single quote escaped", []string{"o'brien"}, `$['o\'brien']`},
		{"backslash escaped", []string{`a\b`}, `$['a\\b']`},
		{"newline escaped", []string{"a\nb"}, `$['a\nb']`},
		{"other control char hex-escaped", []string{"a\x01b"}, `$['a\u0001b']`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, normalizedVariantPath(tt.fields))
		})
	}
}

func TestEnumerateVariantLeavesNestedObject(t *testing.T) {
	// Object {a:int64, n:int16, location:{latitude:float64}, tags:[]string}
	inner := arrow.StructOf(arrow.Field{Name: "latitude", Type: arrow.PrimitiveTypes.Float64})
	obj := arrow.StructOf(
		arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int16},
		arrow.Field{Name: "location", Type: inner},
		arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
	)
	vt := extensions.NewShreddedVariantType(obj)

	leaves := enumerateVariantLeaves([]string{"payload"}, vt.TypedValue())

	got := map[string]variantLeaf{}
	for _, l := range leaves {
		got[l.jsonPath] = l
	}

	// scalar + narrow-int + nested-object leaves present; array leaf skipped.
	require.Contains(t, got, "$['a']")
	require.Contains(t, got, "$['n']")
	require.Contains(t, got, "$['location']['latitude']")
	assert.NotContains(t, got, "$['tags']")
	assert.Len(t, leaves, 3)

	assert.Equal(t, "payload.typed_value.a.typed_value", got["$['a']"].typedPath)
	assert.Equal(t, []string{"payload.value", "payload.typed_value.a.value"}, got["$['a']"].valuePaths)
	assert.Equal(t, iceberg.PrimitiveTypes.Int64, got["$['a']"].icebergType)
	assert.Equal(t, iceberg.PrimitiveTypes.Int32, got["$['n']"].icebergType)
	assert.Equal(t, "payload.typed_value.location.typed_value.latitude.typed_value", got["$['location']['latitude']"].typedPath)
	// nested leaf carries the full ancestor residual chain: root, location, latitude.
	assert.Equal(t, []string{"payload.value", "payload.typed_value.location.value", "payload.typed_value.location.typed_value.latitude.value"}, got["$['location']['latitude']"].valuePaths)
	assert.Equal(t, iceberg.PrimitiveTypes.Float64, got["$['location']['latitude']"].icebergType)
}

func TestEnumerateVariantLeavesRootScalar(t *testing.T) {
	vt := extensions.NewShreddedVariantType(arrow.PrimitiveTypes.Int64)
	leaves := enumerateVariantLeaves([]string{"payload"}, vt.TypedValue())
	require.Len(t, leaves, 1)
	assert.Equal(t, "$", leaves[0].jsonPath)
	assert.Equal(t, "payload.typed_value", leaves[0].typedPath)
	assert.Equal(t, []string{"payload.value"}, leaves[0].valuePaths)
	assert.Equal(t, iceberg.PrimitiveTypes.Int64, leaves[0].icebergType)
}

func TestSerializeVariantBounds(t *testing.T) {
	fields := []variantFieldBound{
		// deliberately out of order to prove sorting.
		{jsonPath: "$['name']", icebergType: iceberg.PrimitiveTypes.String, lower: iceberg.NewLiteral("aa"), upper: iceberg.NewLiteral("zz")},
		{jsonPath: "$['a']", icebergType: iceberg.PrimitiveTypes.Int64, lower: iceberg.NewLiteral(int64(2)), upper: iceberg.NewLiteral(int64(9))},
	}

	lower, upper, ok, err := serializeVariantBounds(fields)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, lower)
	require.NotEmpty(t, upper)

	// Decode via the builder Value directly (on-disk bytes concatenate metadata+value).
	lv, err := boundsObjectValue(fields, true)
	require.NoError(t, err)
	uv, err := boundsObjectValue(fields, false)
	require.NoError(t, err)

	lj, err := lv.MarshalJSON()
	require.NoError(t, err)
	uj, err := uv.MarshalJSON()
	require.NoError(t, err)

	assert.JSONEq(t, `{"$['a']":2,"$['name']":"aa"}`, string(lj))
	assert.JSONEq(t, `{"$['a']":9,"$['name']":"zz"}`, string(uj))

	// lower and upper carry the same keys (same field set).
	lo := lv.Value().(variant.ObjectValue)
	uo := uv.Value().(variant.ObjectValue)
	assert.Equal(t, lo.NumElements(), uo.NumElements())
}

func TestSerializeVariantBoundsEmpty(t *testing.T) {
	_, _, ok, err := serializeVariantBounds(nil)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestIsNaNLiteral(t *testing.T) {
	assert.True(t, isNaNLiteral(iceberg.NewLiteral(float32(math.NaN()))))
	assert.True(t, isNaNLiteral(iceberg.NewLiteral(math.NaN())))
	assert.False(t, isNaNLiteral(iceberg.NewLiteral(float32(1.5))))
	assert.False(t, isNaNLiteral(iceberg.NewLiteral(1.5)))
	assert.False(t, isNaNLiteral(iceberg.NewLiteral(int64(3))))
}

func le64(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))

	return b
}

// buildShreddedVariantMeta builds a 2-row-group FileMetaData with caller-set typed_value stats per row group.
func buildShreddedVariantMeta(t *testing.T, tv0, tv1 metadata.EncodedStatistics) *metadata.FileMetaData {
	t.Helper()

	payload, err := schema.NewGroupNode("payload", parquet.Repetitions.Optional, schema.FieldList{
		schema.NewByteArrayNode("metadata", parquet.Repetitions.Optional, -1),
		schema.NewByteArrayNode("value", parquet.Repetitions.Optional, -1),
		schema.NewInt64Node("typed_value", parquet.Repetitions.Optional, -1),
	}, -1)
	require.NoError(t, err)
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{payload}, -1)
	require.NoError(t, err)

	fmb := metadata.NewFileMetadataBuilder(schema.NewSchema(root), parquet.NewWriterProperties(), nil)

	var metaStats, valStats metadata.EncodedStatistics
	metaStats.SetNullCount(0)
	valStats.SetNullCount(2) // residual all-null

	info := metadata.ChunkMetaInfo{NumValues: 2, DataPageOffset: 4, IndexPageOffset: -1, CompressedSize: 8, UncompressedSize: 8}
	setChunk := func(rg *metadata.RowGroupMetaDataBuilder, st metadata.EncodedStatistics) {
		c := rg.NextColumnChunk()
		if st.IsSet() {
			c.SetStats(st)
		}
		require.NoError(t, c.Finish(info, false, false, metadata.EncodingStats{}))
	}

	for i, tv := range []metadata.EncodedStatistics{tv0, tv1} {
		rg := fmb.AppendRowGroup()
		setChunk(rg, metaStats)
		setChunk(rg, valStats)
		setChunk(rg, tv)
		require.NoError(t, rg.Finish(24, int16(i)))
	}

	fmd, err := fmb.Finish()
	require.NoError(t, err)

	return fmd
}

func TestCollectVariantBoundsIncompleteRowGroup(t *testing.T) {
	arrowSchema := arrow.NewSchema([]arrow.Field{{
		Name:     "payload",
		Type:     extensions.NewShreddedVariantType(arrow.PrimitiveTypes.Int64),
		Nullable: true,
	}}, nil)
	colMapping := map[string]int{"payload": 1}
	statsCols := map[int]StatisticsCollector{1: {Mode: MetricsMode{Typ: MetricModeFull}}}

	minmax := func() metadata.EncodedStatistics {
		var e metadata.EncodedStatistics
		e.SetMin(le64(5))
		e.SetMax(le64(9))
		e.SetNullCount(0)

		return e
	}
	nonNullNoMinMax := func() metadata.EncodedStatistics {
		var e metadata.EncodedStatistics
		e.SetNullCount(0) // 2 non-null values, no min/max

		return e
	}
	allNull := func() metadata.EncodedStatistics {
		var e metadata.EncodedStatistics
		e.SetNullCount(2)

		return e
	}

	for _, tt := range []struct {
		name      string
		tv0, tv1  metadata.EncodedStatistics
		wantBound bool
	}{
		{"non-null row group without min/max drops bound", nonNullNoMinMax(), minmax(), false},
		{"row group with no stats drops bound", metadata.EncodedStatistics{}, minmax(), false},
		{"all-null row group keeps bound", allNull(), minmax(), true},
		{"both row groups valued keeps bound", minmax(), minmax(), true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			meta := buildShreddedVariantMeta(t, tt.tv0, tt.tv1)
			lo, hi := parquetFormat{}.collectVariantBounds(meta, arrowSchema, colMapping, statsCols)
			_, hasLo := lo[1]
			_, hasHi := hi[1]
			assert.Equal(t, tt.wantBound, hasLo, "lower bound presence")
			assert.Equal(t, tt.wantBound, hasHi, "upper bound presence")
		})
	}
}

func TestCollectVariantBoundsMetricsMode(t *testing.T) {
	arrowSchema := arrow.NewSchema([]arrow.Field{{
		Name:     "payload",
		Type:     extensions.NewShreddedVariantType(arrow.PrimitiveTypes.Int64),
		Nullable: true,
	}}, nil)
	colMapping := map[string]int{"payload": 1}

	minmax := func() metadata.EncodedStatistics {
		var e metadata.EncodedStatistics
		e.SetMin(le64(5))
		e.SetMax(le64(9))
		e.SetNullCount(0)

		return e
	}
	meta := buildShreddedVariantMeta(t, minmax(), minmax())

	for _, tt := range []struct {
		name      string
		statsCols map[int]StatisticsCollector
		wantBound bool
	}{
		{"full emits", map[int]StatisticsCollector{1: {Mode: MetricsMode{Typ: MetricModeFull}}}, true},
		{"truncate emits", map[int]StatisticsCollector{1: {Mode: MetricsMode{Typ: MetricModeTruncate, Len: 16}}}, true},
		{"none skips", map[int]StatisticsCollector{1: {Mode: MetricsMode{Typ: MetricModeNone}}}, false},
		{"counts skips", map[int]StatisticsCollector{1: {Mode: MetricsMode{Typ: MetricModeCounts}}}, false},
		{"missing entry skips", map[int]StatisticsCollector{}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lo, hi := parquetFormat{}.collectVariantBounds(meta, arrowSchema, colMapping, tt.statsCols)
			_, hasLo := lo[1]
			_, hasHi := hi[1]
			assert.Equal(t, tt.wantBound, hasLo, "lower bound presence")
			assert.Equal(t, tt.wantBound, hasHi, "upper bound presence")
		})
	}
}

func le64f(v float64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, math.Float64bits(v))

	return b
}

func TestCollectVariantBoundsNestedAncestorResidual(t *testing.T) {
	// payload variant shredded as { location: { latitude: float64 } }.
	inner := arrow.StructOf(arrow.Field{Name: "latitude", Type: arrow.PrimitiveTypes.Float64})
	obj := arrow.StructOf(arrow.Field{Name: "location", Type: inner})
	arrowSchema := arrow.NewSchema([]arrow.Field{{
		Name: "payload", Type: extensions.NewShreddedVariantType(obj), Nullable: true,
	}}, nil)
	colMapping := map[string]int{"payload": 1}
	statsCols := map[int]StatisticsCollector{1: {Mode: MetricsMode{Typ: MetricModeFull}}}

	lat, err := schema.NewGroupNode("latitude", parquet.Repetitions.Optional, schema.FieldList{
		schema.NewByteArrayNode("value", parquet.Repetitions.Optional, -1),
		schema.NewFloat64Node("typed_value", parquet.Repetitions.Optional, -1),
	}, -1)
	require.NoError(t, err)
	latTyped, err := schema.NewGroupNode("typed_value", parquet.Repetitions.Optional, schema.FieldList{lat}, -1)
	require.NoError(t, err)
	loc, err := schema.NewGroupNode("location", parquet.Repetitions.Optional, schema.FieldList{
		schema.NewByteArrayNode("value", parquet.Repetitions.Optional, -1),
		latTyped,
	}, -1)
	require.NoError(t, err)
	payloadTyped, err := schema.NewGroupNode("typed_value", parquet.Repetitions.Optional, schema.FieldList{loc}, -1)
	require.NoError(t, err)
	payload, err := schema.NewGroupNode("payload", parquet.Repetitions.Optional, schema.FieldList{
		schema.NewByteArrayNode("metadata", parquet.Repetitions.Optional, -1),
		schema.NewByteArrayNode("value", parquet.Repetitions.Optional, -1),
		payloadTyped,
	}, -1)
	require.NoError(t, err)
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{payload}, -1)
	require.NoError(t, err)

	allNull := func() metadata.EncodedStatistics {
		var e metadata.EncodedStatistics
		e.SetNullCount(2)

		return e
	}
	latMinMax := func() metadata.EncodedStatistics {
		var e metadata.EncodedStatistics
		e.SetMin(le64f(1.5))
		e.SetMax(le64f(8.5))
		e.SetNullCount(0)

		return e
	}
	info := metadata.ChunkMetaInfo{NumValues: 2, DataPageOffset: 4, IndexPageOffset: -1, CompressedSize: 8, UncompressedSize: 8}

	// locValStats is the ancestor (location) residual value column under test.
	// Column order (depth-first leaves): payload.metadata, payload.value,
	// location.value, latitude.value, latitude.typed_value.
	build := func(locValStats metadata.EncodedStatistics) *metadata.FileMetaData {
		fmb := metadata.NewFileMetadataBuilder(schema.NewSchema(root), parquet.NewWriterProperties(), nil)
		set := func(rg *metadata.RowGroupMetaDataBuilder, st metadata.EncodedStatistics) {
			c := rg.NextColumnChunk()
			if st.IsSet() {
				c.SetStats(st)
			}
			require.NoError(t, c.Finish(info, false, false, metadata.EncodingStats{}))
		}
		rg := fmb.AppendRowGroup()
		set(rg, allNull())   // payload.metadata
		set(rg, allNull())   // payload.value (root residual, benign)
		set(rg, locValStats) // location.value (ancestor residual under test)
		set(rg, allNull())   // latitude.value (leaf residual, benign)
		set(rg, latMinMax()) // latitude.typed_value (produces the bound)
		require.NoError(t, rg.Finish(40, 0))
		fmd, err := fmb.Finish()
		require.NoError(t, err)

		return fmd
	}

	// location residual has non-null (non-object location) values: latitude bound must drop.
	nonNull := metadata.EncodedStatistics{}
	nonNull.SetNullCount(0)
	lo, hi := parquetFormat{}.collectVariantBounds(build(nonNull), arrowSchema, colMapping, statsCols)
	assert.NotContains(t, lo, 1, "latitude bound must drop when the location ancestor residual has non-null values")
	assert.NotContains(t, hi, 1)

	// location residual all-null: latitude bound kept.
	lo2, hi2 := parquetFormat{}.collectVariantBounds(build(allNull()), arrowSchema, colMapping, statsCols)
	assert.Contains(t, lo2, 1, "latitude bound kept when the location ancestor residual is all-null")
	assert.Contains(t, hi2, 1)
}

func TestTruncateVariantBound(t *testing.T) {
	for _, tt := range []struct {
		name      string
		typ       iceberg.PrimitiveType
		lower     iceberg.Literal
		upper     iceberg.Literal
		truncLen  int
		wantLower any
		wantUpper any // nil means the upper bound is dropped
	}{
		{
			name:      "string truncates lower and increments upper",
			typ:       iceberg.PrimitiveTypes.String,
			lower:     iceberg.NewLiteral(strings.Repeat("a", 40)),
			upper:     iceberg.NewLiteral(strings.Repeat("a", 40)),
			truncLen:  8,
			wantLower: strings.Repeat("a", 8),
			wantUpper: strings.Repeat("a", 7) + "b",
		},
		{
			name:      "string upper dropped when it cannot be incremented",
			typ:       iceberg.PrimitiveTypes.String,
			lower:     iceberg.NewLiteral(strings.Repeat("\U0010FFFF", 40)),
			upper:     iceberg.NewLiteral(strings.Repeat("\U0010FFFF", 40)),
			truncLen:  8,
			wantLower: strings.Repeat("\U0010FFFF", 8),
			wantUpper: nil,
		},
		{
			name:      "binary truncates lower and increments upper",
			typ:       iceberg.PrimitiveTypes.Binary,
			lower:     iceberg.NewLiteral(bytes.Repeat([]byte{0x01}, 40)),
			upper:     iceberg.NewLiteral(bytes.Repeat([]byte{0x01}, 40)),
			truncLen:  8,
			wantLower: bytes.Repeat([]byte{0x01}, 8),
			wantUpper: append(bytes.Repeat([]byte{0x01}, 7), 0x02),
		},
		{
			name:      "binary upper dropped when it cannot be incremented",
			typ:       iceberg.PrimitiveTypes.Binary,
			lower:     iceberg.NewLiteral(bytes.Repeat([]byte{0xff}, 40)),
			upper:     iceberg.NewLiteral(bytes.Repeat([]byte{0xff}, 40)),
			truncLen:  8,
			wantLower: bytes.Repeat([]byte{0xff}, 8),
			wantUpper: nil,
		},
		{
			name:      "full leaves bounds unchanged",
			typ:       iceberg.PrimitiveTypes.String,
			lower:     iceberg.NewLiteral("short"),
			upper:     iceberg.NewLiteral("zzzz"),
			truncLen:  0,
			wantLower: "short",
			wantUpper: "zzzz",
		},
		{
			name:      "short value is not truncated or dropped",
			typ:       iceberg.PrimitiveTypes.String,
			lower:     iceberg.NewLiteral("hi"),
			upper:     iceberg.NewLiteral("hi"),
			truncLen:  8,
			wantLower: "hi",
			wantUpper: "hi",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lo, hi := truncateVariantBound(tt.typ, tt.lower, tt.upper, tt.truncLen)
			assert.Equal(t, tt.wantLower, lo.Any())
			if tt.wantUpper == nil {
				assert.Nil(t, hi, "upper bound dropped")
			} else {
				require.NotNil(t, hi)
				assert.Equal(t, tt.wantUpper, hi.Any())
			}
		})
	}
}

func TestSerializeVariantBoundsDropsNilUpper(t *testing.T) {
	// One field keeps both bounds; the second has a dropped (nil) upper.
	both := []variantFieldBound{
		{jsonPath: "$['a']", icebergType: iceberg.PrimitiveTypes.Int64, lower: iceberg.NewLiteral(int64(1)), upper: iceberg.NewLiteral(int64(9))},
		{jsonPath: "$['b']", icebergType: iceberg.PrimitiveTypes.String, lower: iceberg.NewLiteral("aaaaaaaa"), upper: nil},
	}
	lower, upper, ok, err := serializeVariantBounds(both)
	require.NoError(t, err)
	require.True(t, ok)

	// Same fields but $['b'] never had an upper: the upper object must match dropping it entirely.
	lowerOnlyB := []variantFieldBound{
		{jsonPath: "$['a']", icebergType: iceberg.PrimitiveTypes.Int64, lower: iceberg.NewLiteral(int64(1)), upper: iceberg.NewLiteral(int64(9))},
	}
	_, wantUpper, _, err := serializeVariantBounds(lowerOnlyB)
	require.NoError(t, err)
	assert.Equal(t, wantUpper, upper, "dropped upper must be omitted from the upper object")
	assert.NotEqual(t, lower, upper, "lower still carries $['b']")
}
