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
	"time"

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

func specWithFields(fields ...iceberg.PartitionField) iceberg.PartitionSpec {
	return iceberg.NewPartitionSpec(fields...)
}

type partitionPredicateRow []any

func (r partitionPredicateRow) Size() int       { return len(r) }
func (r partitionPredicateRow) Get(pos int) any { return r[pos] }
func (r partitionPredicateRow) Set(pos int, val any) {
	r[pos] = val
}

func timestampAtUTC(year, month, day, hour, minute, second, microsecond int) iceberg.Timestamp {
	return iceberg.Timestamp(time.Date(year, time.Month(month), day, hour, minute, second,
		microsecond*int(time.Microsecond), time.UTC).UnixMicro())
}

func dateAtUTC(year, month, day int) iceberg.Date {
	return iceberg.Date(time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC).Unix() /
		int64((24*time.Hour)/time.Second))
}

func TestBuildPartitionMatchPredicate_EmptyInput(t *testing.T) {
	spec := specWithFields(identityField(1, 1000, "id"))

	expr, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), nil)
	require.NoError(t, err)
	assert.True(t, expr.Equals(iceberg.AlwaysFalse{}), "empty input should match nothing, got %s", expr)
}

func TestBuildPartitionMatchPredicate_SingleField(t *testing.T) {
	spec := specWithFields(identityField(1, 1000, "id"))

	expr, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{
		{1000: int32(5)},
	})
	require.NoError(t, err)

	want := iceberg.EqualTo(iceberg.Reference("id"), int32(5))
	assert.True(t, expr.Equals(want), "want %s, got %s", want, expr)
}

func TestBuildPartitionMatchPredicate_MultipleFields(t *testing.T) {
	spec := specWithFields(
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
	spec := specWithFields(identityField(1, 1000, "id"))

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
	spec := specWithFields(
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
	spec := specWithFields(identityField(2, 1001, "category"))

	expr, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{{1001: nil}})
	require.NoError(t, err)

	want := iceberg.IsNull(iceberg.Reference("category"))
	assert.True(t, expr.Equals(want), "want %s, got %s", want, expr)
}

func TestBuildPartitionMatchPredicate_MissingFieldID(t *testing.T) {
	spec := specWithFields(identityField(2, 1001, "category"))

	_, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{{}})
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	assert.Contains(t, err.Error(), "missing from partition tuple")
}

func TestBuildPartitionMatchPredicate_NullAndNonNullInSameTuple(t *testing.T) {
	spec := specWithFields(
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
			spec := specWithFields(tc.field)

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
	spec := specWithFields(identityField(4, 1003, "score"))

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
	spec := specWithFields(identityField(1, 1000, "id"))

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
	spec := specWithFields(
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
	spec := specWithFields(
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

func TestBuildPartitionMatchPredicate_UsesNonIdentityTransform(t *testing.T) {
	cases := []struct {
		name      string
		field     iceberg.PartitionField
		source    string
		value     any
		wantValue iceberg.Literal
	}{
		{
			name:      "bucket",
			field:     iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_part", Transform: iceberg.BucketTransform{NumBuckets: 4}},
			source:    "id",
			value:     int32(1),
			wantValue: iceberg.Int32Literal(1),
		},
		{
			name:      "truncate",
			field:     iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1000, Name: "category_part", Transform: iceberg.TruncateTransform{Width: 3}},
			source:    "category",
			value:     "boo",
			wantValue: iceberg.StringLiteral("boo"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := specWithFields(tc.field)

			expr, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{{1000: tc.value}})
			require.NoError(t, err)

			want := iceberg.LiteralPredicate(iceberg.OpEQ,
				iceberg.NewUnboundTransform(tc.field.Transform, iceberg.Reference(tc.source)),
				tc.wantValue,
			)
			assert.True(t, expr.Equals(want), "want %s, got %s", want, expr)
		})
	}
}

func bucketValueForInt32(t *testing.T, transform iceberg.BucketTransform, value int32) int32 {
	t.Helper()

	result := transform.Apply(iceberg.Optional[iceberg.Literal]{
		Valid: true,
		Val:   iceberg.Int32Literal(value),
	})
	require.True(t, result.Valid)

	return result.Val.(iceberg.Int32Literal).Value()
}

func TestBuildPartitionMatchPredicate_EvaluatesTransforms(t *testing.T) {
	bucketTransform := iceberg.BucketTransform{NumBuckets: 4}
	bucketPartition := bucketValueForInt32(t, bucketTransform, 1)
	bucketNonMatch := int32(2)
	for bucketValueForInt32(t, bucketTransform, bucketNonMatch) == bucketPartition {
		bucketNonMatch++
	}

	cases := []struct {
		name            string
		sourceType      iceberg.Type
		transform       iceberg.Transform
		partitionValue  any
		matchingValues  []any
		nonMatchingVals []any
	}{
		{
			name:            "bucket",
			sourceType:      iceberg.PrimitiveTypes.Int32,
			transform:       bucketTransform,
			partitionValue:  bucketPartition,
			matchingValues:  []any{int32(1)},
			nonMatchingVals: []any{bucketNonMatch},
		},
		{
			name:            "truncate",
			sourceType:      iceberg.PrimitiveTypes.String,
			transform:       iceberg.TruncateTransform{Width: 3},
			partitionValue:  "boo",
			matchingValues:  []any{"boo", "books", "booster"},
			nonMatchingVals: []any{"bar", "science"},
		},
		{
			name:           "year",
			sourceType:     iceberg.PrimitiveTypes.Timestamp,
			transform:      iceberg.YearTransform{},
			partitionValue: int32(50), // 2020 - 1970
			matchingValues: []any{
				timestampAtUTC(2020, 1, 1, 0, 0, 0, 0),
				timestampAtUTC(2020, 12, 31, 23, 59, 59, 999999),
			},
			nonMatchingVals: []any{
				timestampAtUTC(2019, 12, 31, 23, 59, 59, 999999),
				timestampAtUTC(2021, 1, 1, 0, 0, 0, 0),
			},
		},
		{
			name:           "month",
			sourceType:     iceberg.PrimitiveTypes.Timestamp,
			transform:      iceberg.MonthTransform{},
			partitionValue: int32(601), // 2020-02, relative to 1970-01
			matchingValues: []any{
				timestampAtUTC(2020, 2, 1, 0, 0, 0, 0),
				timestampAtUTC(2020, 2, 29, 23, 59, 59, 999999),
			},
			nonMatchingVals: []any{
				timestampAtUTC(2020, 1, 31, 23, 59, 59, 999999),
				timestampAtUTC(2020, 3, 1, 0, 0, 0, 0),
			},
		},
		{
			name:           "day",
			sourceType:     iceberg.PrimitiveTypes.Timestamp,
			transform:      iceberg.DayTransform{},
			partitionValue: dateAtUTC(2020, 2, 29),
			matchingValues: []any{
				timestampAtUTC(2020, 2, 29, 0, 0, 0, 0),
				timestampAtUTC(2020, 2, 29, 23, 59, 59, 999999),
			},
			nonMatchingVals: []any{
				timestampAtUTC(2020, 2, 28, 23, 59, 59, 999999),
				timestampAtUTC(2020, 3, 1, 0, 0, 0, 0),
			},
		},
		{
			name:           "hour",
			sourceType:     iceberg.PrimitiveTypes.Timestamp,
			transform:      iceberg.HourTransform{},
			partitionValue: int32(439714), // 2020-02-29 10:00 UTC, relative to 1970
			matchingValues: []any{
				timestampAtUTC(2020, 2, 29, 10, 0, 0, 0),
				timestampAtUTC(2020, 2, 29, 10, 59, 59, 999999),
			},
			nonMatchingVals: []any{
				timestampAtUTC(2020, 2, 29, 9, 59, 59, 999999),
				timestampAtUTC(2020, 2, 29, 11, 0, 0, 0),
			},
		},
		{
			name:           "hour before epoch",
			sourceType:     iceberg.PrimitiveTypes.Timestamp,
			transform:      iceberg.HourTransform{},
			partitionValue: int32(-1), // 1969-12-31 23:00 UTC
			matchingValues: []any{
				timestampAtUTC(1969, 12, 31, 23, 0, 0, 0),
				timestampAtUTC(1969, 12, 31, 23, 59, 59, 999999),
			},
			nonMatchingVals: []any{
				timestampAtUTC(1969, 12, 31, 22, 59, 59, 999999),
				timestampAtUTC(1970, 1, 1, 0, 0, 0, 0),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := iceberg.NewSchema(0, iceberg.NestedField{
				ID: 1, Name: "value", Type: tc.sourceType,
			})
			spec := specWithFields(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "value_part", Transform: tc.transform,
			})

			expr, err := BuildPartitionMatchPredicate(spec, schema, []map[int]any{{1000: tc.partitionValue}})
			require.NoError(t, err)

			eval, err := iceberg.ExpressionEvaluator(schema, expr, true)
			require.NoError(t, err)

			for _, value := range tc.matchingValues {
				matched, err := eval(partitionPredicateRow{value})
				require.NoError(t, err)
				assert.True(t, matched, "source value %v should match partition value %v", value, tc.partitionValue)
			}
			for _, value := range tc.nonMatchingVals {
				matched, err := eval(partitionPredicateRow{value})
				require.NoError(t, err)
				assert.False(t, matched, "source value %v should not match partition value %v", value, tc.partitionValue)
			}
		})
	}
}

func TestBuildPartitionMatchPredicate_EvaluatesBucketCollision(t *testing.T) {
	transform := iceberg.BucketTransform{NumBuckets: 4}
	valuesByBucket := make(map[int32][]int32)
	var collisionBucket int32
	var collisionValues []int32

	for value := range int32(10000) {
		bucket := bucketValueForInt32(t, transform, value)
		valuesByBucket[bucket] = append(valuesByBucket[bucket], value)
		if len(valuesByBucket[bucket]) == 2 {
			collisionBucket = bucket
			collisionValues = valuesByBucket[bucket]

			break
		}
	}
	require.Len(t, collisionValues, 2)
	assert.NotEqual(t, collisionValues[0], collisionValues[1])

	nonMatchingValue := int32(-1)
	for value := range int32(10000) {
		if bucketValueForInt32(t, transform, value) != collisionBucket {
			nonMatchingValue = value

			break
		}
	}
	require.NotEqual(t, int32(-1), nonMatchingValue)

	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32,
	})
	spec := specWithFields(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id_bucket", Transform: transform,
	})
	expr, err := BuildPartitionMatchPredicate(spec, schema, []map[int]any{{1000: collisionBucket}})
	require.NoError(t, err)

	eval, err := iceberg.ExpressionEvaluator(schema, expr, true)
	require.NoError(t, err)
	for _, value := range collisionValues {
		matched, err := eval(partitionPredicateRow{value})
		require.NoError(t, err)
		assert.True(t, matched, "source value %d should match bucket %d", value, collisionBucket)
	}
	matched, err := eval(partitionPredicateRow{nonMatchingValue})
	require.NoError(t, err)
	assert.False(t, matched, "source value %d should not match bucket %d", nonMatchingValue, collisionBucket)
}

func TestBuildPartitionMatchPredicate_EvaluatesTransformedNull(t *testing.T) {
	cases := []struct {
		name          string
		sourceType    iceberg.Type
		transform     iceberg.Transform
		nonNil        any
		nonNilMatches bool
	}{
		{name: "bucket", sourceType: iceberg.PrimitiveTypes.Int32, transform: iceberg.BucketTransform{NumBuckets: 4}, nonNil: int32(1), nonNilMatches: false},
		{name: "truncate", sourceType: iceberg.PrimitiveTypes.String, transform: iceberg.TruncateTransform{Width: 3}, nonNil: "books", nonNilMatches: false},
		{name: "year", sourceType: iceberg.PrimitiveTypes.Timestamp, transform: iceberg.YearTransform{}, nonNil: timestampAtUTC(2020, 1, 1, 0, 0, 0, 0), nonNilMatches: false},
		{name: "month", sourceType: iceberg.PrimitiveTypes.Timestamp, transform: iceberg.MonthTransform{}, nonNil: timestampAtUTC(2020, 1, 1, 0, 0, 0, 0), nonNilMatches: false},
		{name: "day", sourceType: iceberg.PrimitiveTypes.Timestamp, transform: iceberg.DayTransform{}, nonNil: timestampAtUTC(2020, 1, 1, 0, 0, 0, 0), nonNilMatches: false},
		{name: "hour", sourceType: iceberg.PrimitiveTypes.Timestamp, transform: iceberg.HourTransform{}, nonNil: timestampAtUTC(2020, 1, 1, 0, 0, 0, 0), nonNilMatches: false},
		{name: "void", sourceType: iceberg.PrimitiveTypes.String, transform: iceberg.VoidTransform{}, nonNil: "books", nonNilMatches: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := iceberg.NewSchema(0, iceberg.NestedField{
				ID: 1, Name: "value", Type: tc.sourceType,
			})
			spec := specWithFields(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "value_part", Transform: tc.transform,
			})

			expr, err := BuildPartitionMatchPredicate(spec, schema, []map[int]any{{1000: nil}})
			require.NoError(t, err)

			eval, err := iceberg.ExpressionEvaluator(schema, expr, true)
			require.NoError(t, err)

			matched, err := eval(partitionPredicateRow{nil})
			require.NoError(t, err)
			assert.True(t, matched, "null source value should match null partition value")

			matched, err = eval(partitionPredicateRow{tc.nonNil})
			require.NoError(t, err)
			assert.Equal(t, tc.nonNilMatches, matched, "unexpected match for non-null source value")
		})
	}
}

func TestBuildPartitionMatchPredicate_VoidBindsAlwaysTrue(t *testing.T) {
	spec := specWithFields(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1001, Name: "category_void", Transform: iceberg.VoidTransform{},
	})

	expr, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{{1001: nil}})
	require.NoError(t, err)

	bound, err := iceberg.BindExpr(dynamicOverwriteSchema(), expr, true)
	require.NoError(t, err)
	assert.True(t, bound.Equals(iceberg.AlwaysTrue{}), "void-only predicate should bind to AlwaysTrue, got %s", bound)
}

func TestBuildPartitionMatchPredicate_CoercesRawDayValue(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Timestamp,
	})
	spec := specWithFields(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "value_day", Transform: iceberg.DayTransform{},
	})
	rawDay := int32(dateAtUTC(2020, 2, 29))

	expr, err := BuildPartitionMatchPredicate(spec, schema, []map[int]any{{1000: rawDay}})
	require.NoError(t, err)

	want := iceberg.LiteralPredicate(iceberg.OpEQ,
		iceberg.NewUnboundTransform(iceberg.DayTransform{}, iceberg.Reference("value")),
		iceberg.DateLiteral(rawDay),
	)
	assert.True(t, expr.Equals(want), "want %s, got %s", want, expr)

	eval, err := iceberg.ExpressionEvaluator(schema, expr, true)
	require.NoError(t, err)

	matched, err := eval(partitionPredicateRow{timestampAtUTC(2020, 2, 29, 12, 0, 0, 0)})
	require.NoError(t, err)
	assert.True(t, matched)
}

func TestBuildPartitionMatchPredicate_UsesNestedSourcePath(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 10, Name: "location", Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 11, Name: "category", Type: iceberg.PrimitiveTypes.String},
			},
		}},
		iceberg.NestedField{ID: 12, Name: "category", Type: iceberg.PrimitiveTypes.String},
	)
	spec := specWithFields(iceberg.PartitionField{
		SourceIDs: []int{11}, FieldID: 1000, Name: "category_part",
		Transform: iceberg.TruncateTransform{Width: 3},
	})

	expr, err := BuildPartitionMatchPredicate(spec, schema, []map[int]any{{1000: "boo"}})
	require.NoError(t, err)
	_, err = iceberg.BindExpr(schema, expr, true)
	require.NoError(t, err)

	want := iceberg.EqualTo(
		iceberg.NewUnboundTransform(iceberg.TruncateTransform{Width: 3}, iceberg.Reference("location.category")),
		"boo",
	)
	assert.True(t, expr.Equals(want), "want %s, got %s", want, expr)
}

func TestBuildPartitionMatchPredicate_RecognizesPointerIdentityTransform(t *testing.T) {
	spec := specWithFields(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id_part",
		Transform: &iceberg.IdentityTransform{},
	})

	expr, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{{1000: int32(1)}})
	require.NoError(t, err)

	want := iceberg.EqualTo(iceberg.Reference("id"), int32(1))
	assert.True(t, expr.Equals(want), "want %s, got %s", want, expr)
}

func TestBuildPartitionMatchPredicate_UnknownSourceID(t *testing.T) {
	// Partition field points at a source id that is not present in the schema.
	spec := specWithFields(identityField(999, 1000, "ghost"))

	_, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{{1000: int32(1)}})
	require.Error(t, err)
	assert.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestBuildPartitionMatchPredicate_UnsupportedValueType(t *testing.T) {
	spec := specWithFields(identityField(1, 1000, "id"))

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
			got, err := LiteralForPartitionValue(tc.in)
			require.NoError(t, err)
			assert.True(t, got.Equals(tc.want), "want %s, got %s", tc.want, got)
		})
	}

	t.Run("unsupported", func(t *testing.T) {
		_, err := LiteralForPartitionValue(struct{}{})
		require.Error(t, err)
		assert.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	})
}

func TestBuildPartitionMatchPredicate_RejectsInvalidTransforms(t *testing.T) {
	unknown, err := iceberg.ParseTransform("future_transform")
	require.NoError(t, err)
	for _, tc := range []struct {
		name      string
		transform iceberg.Transform
		want      error
	}{
		{name: "nil", want: iceberg.ErrInvalidArgument},
		{name: "typed nil identity", transform: (*iceberg.IdentityTransform)(nil), want: iceberg.ErrInvalidArgument},
		{name: "typed nil bucket", transform: (*iceberg.BucketTransform)(nil), want: iceberg.ErrInvalidArgument},
		{name: "unknown", transform: unknown, want: iceberg.ErrNotImplemented},
		{name: "invalid source type", transform: iceberg.DayTransform{}, want: iceberg.ErrInvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := specWithFields(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "id_part", Transform: tc.transform,
			})
			_, err := BuildPartitionMatchPredicate(spec, dynamicOverwriteSchema(), []map[int]any{{1000: int32(1)}})
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestBuildPartitionMatchPredicate_RejectsImpossiblePartitionValues(t *testing.T) {
	cases := []struct {
		name      string
		schema    *iceberg.Schema
		transform iceberg.Transform
		value     any
	}{
		{
			name:      "bucket value outside range",
			schema:    iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Int32}),
			transform: iceberg.BucketTransform{NumBuckets: 4},
			value:     int32(4),
		},
		{
			name:      "truncate value is not fixed point",
			schema:    iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.String}),
			transform: iceberg.TruncateTransform{Width: 3},
			value:     "books",
		},
		{
			name:      "void value is not nil",
			schema:    iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.String}),
			transform: iceberg.VoidTransform{},
			value:     "books",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := specWithFields(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "value_part", Transform: tc.transform,
			})
			_, err := BuildPartitionMatchPredicate(spec, tc.schema, []map[int]any{{1000: tc.value}})
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
		})
	}
}
