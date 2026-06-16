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
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dynamicOverwriteSchema() *iceberg.Schema {
	return iceberg.NewSchema(
		0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "sub", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 4, Name: "score", Type: iceberg.PrimitiveTypes.Float64, Required: false},
		iceberg.NestedField{ID: 5, Name: "score32", Type: iceberg.PrimitiveTypes.Float32, Required: false},
	)
}

func identityField(sourceID, fieldID int, name string) iceberg.PartitionField {
	return iceberg.PartitionField{SourceIDs: []int{sourceID}, FieldID: fieldID, Name: name, Transform: iceberg.IdentityTransform{}}
}

func identitySpec(fields ...iceberg.PartitionField) iceberg.PartitionSpec {
	return iceberg.NewPartitionSpec(fields...)
}

func TestBuildPartitionMatchPredicate_EmptyInput(t *testing.T) {
	spec := identitySpec(identityField(1, 1000, "id"))

	expr, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), nil)
	require.NoError(t, err)
	assert.True(t, expr.Equals(iceberg.AlwaysFalse{}), "empty input should match nothing, got %s", expr)
}

func TestBuildPartitionMatchPredicate_SingleField(t *testing.T) {
	spec := identitySpec(identityField(1, 1000, "id"))

	expr, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{
		{1000: int32(5)},
	})
	require.NoError(t, err)

	want := iceberg.EqualTo(iceberg.Reference("id"), int32(5))
	assert.True(t, expr.Equals(want), "want %s, got %s", want, expr)
}

func TestBuildPartitionMatchPredicate_MultipleFields(t *testing.T) {
	spec := identitySpec(
		identityField(1, 1000, "id"),
		identityField(2, 1001, "category"),
	)

	expr, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{
		{1000: int32(5), 1001: "books"},
	})
	require.NoError(t, err)

	want := iceberg.NewAnd(
		iceberg.EqualTo(iceberg.Reference("id"), int32(5)),
		iceberg.EqualTo(iceberg.Reference("category"), "books"),
	)
	assert.True(t, expr.Equals(want), "want %s, got %s", want, expr)
}

func TestBuildPartitionMatchPredicate_MultiplePartitions(t *testing.T) {
	spec := identitySpec(identityField(1, 1000, "id"))

	expr, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{
		{1000: int32(5)},
		{1000: int32(7)},
	})
	require.NoError(t, err)

	want := iceberg.NewOr(
		iceberg.EqualTo(iceberg.Reference("id"), int32(5)),
		iceberg.EqualTo(iceberg.Reference("id"), int32(7)),
	)
	assert.True(t, expr.Equals(want), "want %s, got %s", want, expr)
}

func TestBuildPartitionMatchPredicate_MultipleFieldsAndPartitions(t *testing.T) {
	spec := identitySpec(
		identityField(1, 1000, "id"),
		identityField(2, 1001, "category"),
	)

	expr, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{
		{1000: int32(5), 1001: "books"},
		{1000: int32(7), 1001: "science"},
	})
	require.NoError(t, err)

	want := iceberg.NewOr(
		iceberg.NewAnd(
			iceberg.EqualTo(iceberg.Reference("id"), int32(5)),
			iceberg.EqualTo(iceberg.Reference("category"), "books"),
		),
		iceberg.NewAnd(
			iceberg.EqualTo(iceberg.Reference("id"), int32(7)),
			iceberg.EqualTo(iceberg.Reference("category"), "science"),
		),
	)
	assert.True(t, expr.Equals(want), "want %s, got %s", want, expr)
}

func TestBuildPartitionMatchPredicate_NullValue(t *testing.T) {
	spec := identitySpec(identityField(2, 1001, "category"))

	// Both an explicit nil and a missing key represent a null partition value.
	cases := []struct {
		name       string
		partitions []map[int]any
	}{
		{"explicit nil", []map[int]any{{1001: nil}}},
		{"missing key", []map[int]any{{}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), tc.partitions)
			require.NoError(t, err)

			want := iceberg.IsNull(iceberg.Reference("category"))
			assert.True(t, expr.Equals(want), "want %s, got %s", want, expr)
		})
	}
}

func TestBuildPartitionMatchPredicate_NullAndNonNullInSameTuple(t *testing.T) {
	spec := identitySpec(
		identityField(1, 1000, "id"),
		identityField(2, 1001, "category"),
	)

	expr, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{
		{1000: int32(5), 1001: nil},
	})
	require.NoError(t, err)

	want := iceberg.NewAnd(
		iceberg.EqualTo(iceberg.Reference("id"), int32(5)),
		iceberg.IsNull(iceberg.Reference("category")),
	)
	assert.True(t, expr.Equals(want), "want %s, got %s", want, expr)
}

func TestBuildPartitionMatchPredicate_NaNValue(t *testing.T) {
	// x == NaN is never true, so a NaN partition value must become IsNaN, for
	// both float widths.
	cases := []struct {
		name  string
		field iceberg.PartitionField
		col   string
		nan   any
	}{
		{"float64", identityField(4, 1003, "score"), "score", math.NaN()},
		{"float32", identityField(5, 1004, "score32"), "score32", float32(math.NaN())},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := identitySpec(tc.field)

			expr, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{
				{tc.field.FieldID: tc.nan},
			})
			require.NoError(t, err)

			want := iceberg.IsNaN(iceberg.Reference(tc.col))
			assert.True(t, expr.Equals(want), "want %s, got %s", want, expr)
		})
	}
}

func TestBuildPartitionMatchPredicate_NaNDeduplicates(t *testing.T) {
	spec := identitySpec(identityField(4, 1003, "score"))

	// NaN has many valid bit patterns; distinct payloads must still collapse to a
	// single IsNaN clause (the dedup key uses a sentinel, not the raw float bits).
	nan1 := math.Float64frombits(0x7FF8000000000001)
	nan2 := math.Float64frombits(0x7FF8000000000002)
	require.True(t, math.IsNaN(nan1) && math.IsNaN(nan2))

	expr, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{
		{1003: nan1},
		{1003: nan2},
	})
	require.NoError(t, err)

	want := iceberg.IsNaN(iceberg.Reference("score"))
	assert.True(t, expr.Equals(want), "distinct NaN payloads should collapse to one clause, got %s", expr)
}

func TestBuildPartitionMatchPredicate_DeduplicatesPartitions(t *testing.T) {
	spec := identitySpec(identityField(1, 1000, "id"))

	// Many data files land in the same partition; the predicate must not repeat it.
	expr, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{
		{1000: int32(5)},
		{1000: int32(5)},
		{1000: int32(5)},
	})
	require.NoError(t, err)

	want := iceberg.EqualTo(iceberg.Reference("id"), int32(5))
	assert.True(t, expr.Equals(want), "duplicates should collapse to one clause, got %s", expr)
}

func TestBuildPartitionMatchPredicate_DeduplicatesMultiFieldPartitions(t *testing.T) {
	spec := identitySpec(
		identityField(1, 1000, "id"),
		identityField(2, 1001, "category"),
	)

	expr, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{
		{1000: int32(5), 1001: "books"},
		{1000: int32(5), 1001: "books"},
	})
	require.NoError(t, err)

	want := iceberg.NewAnd(
		iceberg.EqualTo(iceberg.Reference("id"), int32(5)),
		iceberg.EqualTo(iceberg.Reference("category"), "books"),
	)
	assert.True(t, expr.Equals(want), "identical multi-field tuples should collapse, got %s", expr)
}

// Distinct tuples whose values contain the dedup separators ('/' and '=') must
// not collide into a single clause. A naive "%d=%v"-joined signature would map
// both of these to "1001=5/1002=three/1002=x" and silently drop one partition.
func TestBuildPartitionMatchPredicate_NoSeparatorCollision(t *testing.T) {
	spec := identitySpec(
		identityField(2, 1001, "category"),
		identityField(3, 1002, "sub"),
	)

	expr, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{
		{1001: "5/1002=three", 1002: "x"},
		{1001: "5", 1002: "three/1002=x"},
	})
	require.NoError(t, err)

	want := iceberg.NewOr(
		iceberg.NewAnd(
			iceberg.EqualTo(iceberg.Reference("category"), "5/1002=three"),
			iceberg.EqualTo(iceberg.Reference("sub"), "x"),
		),
		iceberg.NewAnd(
			iceberg.EqualTo(iceberg.Reference("category"), "5"),
			iceberg.EqualTo(iceberg.Reference("sub"), "three/1002=x"),
		),
	)
	assert.True(t, expr.Equals(want), "distinct tuples must not be deduped, got %s", expr)
}

func TestBuildPartitionMatchPredicate_NonIdentityTransformRejected(t *testing.T) {
	cases := []struct {
		name      string
		transform iceberg.Transform
	}{
		{"bucket", iceberg.BucketTransform{NumBuckets: 4}},
		{"truncate", iceberg.TruncateTransform{Width: 10}},
		{"day", iceberg.DayTransform{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := identitySpec(iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_part", Transform: tc.transform})

			_, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{{1000: int32(1)}})
			require.Error(t, err)
			assert.ErrorIs(t, err, iceberg.ErrNotImplemented)
		})
	}
}

func TestBuildPartitionMatchPredicate_UnknownSourceID(t *testing.T) {
	// Partition field points at a source id that is not present in the schema.
	spec := identitySpec(identityField(999, 1000, "ghost"))

	_, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{{1000: int32(1)}})
	require.Error(t, err)
	assert.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestBuildPartitionMatchPredicate_UnsupportedValueType(t *testing.T) {
	spec := identitySpec(identityField(1, 1000, "id"))

	_, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{
		{1000: struct{}{}},
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestLiteralForPartitionValue(t *testing.T) {
	id := uuid.New()

	cases := []struct {
		name string
		in   any
		want iceberg.Literal
	}{
		{"bool", true, iceberg.NewLiteral(true)},
		{"int32", int32(5), iceberg.NewLiteral(int32(5))},
		{"int64", int64(5), iceberg.NewLiteral(int64(5))},
		{"int", 5, iceberg.NewLiteral(int64(5))},
		{"float32", float32(1.5), iceberg.NewLiteral(float32(1.5))},
		{"float64", float64(1.5), iceberg.NewLiteral(float64(1.5))},
		{"string", "books", iceberg.NewLiteral("books")},
		{"bytes", []byte{1, 2, 3}, iceberg.NewLiteral([]byte{1, 2, 3})},
		{"date", iceberg.Date(100), iceberg.NewLiteral(iceberg.Date(100))},
		{"time", iceberg.Time(100), iceberg.NewLiteral(iceberg.Time(100))},
		{"timestamp", iceberg.Timestamp(100), iceberg.NewLiteral(iceberg.Timestamp(100))},
		{"timestampNano", iceberg.TimestampNano(100), iceberg.NewLiteral(iceberg.TimestampNano(100))},
		{"uuid", id, iceberg.NewLiteral(id)},
		// DataFile.Partition() decodes decimal fields into a DecimalLiteral, which
		// already satisfies iceberg.Literal and must pass through unchanged.
		{"decimal literal passthrough", iceberg.DecimalLiteral{Val: decimal128.FromI64(123), Scale: 2}, iceberg.DecimalLiteral{Val: decimal128.FromI64(123), Scale: 2}},
		// A raw iceberg.Decimal (defensive, hand-constructed) is wrapped via NewLiteral.
		{"raw decimal", iceberg.Decimal{Val: decimal128.FromI64(123), Scale: 2}, iceberg.NewLiteral(iceberg.Decimal{Val: decimal128.FromI64(123), Scale: 2})},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := literalForPartitionValue(tc.in)
			require.NoError(t, err)
			assert.True(t, got.Equals(tc.want), "want %s, got %s", tc.want, got)
		})
	}

	t.Run("unsupported", func(t *testing.T) {
		_, err := literalForPartitionValue(struct{}{})
		require.Error(t, err)
		assert.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	})
}
