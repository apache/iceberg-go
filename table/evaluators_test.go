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
	"math"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	IntMinValue, IntMaxValue int32 = 30, 79
)

func TestManifestEvaluator(t *testing.T) {
	var (
		IntMin, IntMax       = []byte{byte(IntMinValue), 0x00, 0x00, 0x00}, []byte{byte(IntMaxValue), 0x00, 0x00, 0x00}
		StringMin, StringMax = []byte("a"), []byte("z")
		FloatMin, _          = iceberg.Float32Literal(0).MarshalBinary()
		FloatMax, _          = iceberg.Float32Literal(20).MarshalBinary()
		DblMin, _            = iceberg.Float64Literal(0).MarshalBinary()
		DblMax, _            = iceberg.Float64Literal(20).MarshalBinary()
		NanTrue, NanFalse    = true, false

		testSchema = iceberg.NewSchema(1,
			iceberg.NestedField{
				ID: 1, Name: "id",
				Type: iceberg.PrimitiveTypes.Int32, Required: true,
			},
			iceberg.NestedField{
				ID: 2, Name: "all_nulls_missing_nan",
				Type: iceberg.PrimitiveTypes.String, Required: false,
			},
			iceberg.NestedField{
				ID: 3, Name: "some_nulls",
				Type: iceberg.PrimitiveTypes.String, Required: false,
			},
			iceberg.NestedField{
				ID: 4, Name: "no_nulls",
				Type: iceberg.PrimitiveTypes.String, Required: false,
			},
			iceberg.NestedField{
				ID: 5, Name: "float",
				Type: iceberg.PrimitiveTypes.Float32, Required: false,
			},
			iceberg.NestedField{
				ID: 6, Name: "all_nulls_double",
				Type: iceberg.PrimitiveTypes.Float64, Required: false,
			},
			iceberg.NestedField{
				ID: 7, Name: "all_nulls_no_nans",
				Type: iceberg.PrimitiveTypes.Float32, Required: false,
			},
			iceberg.NestedField{
				ID: 8, Name: "all_nans",
				Type: iceberg.PrimitiveTypes.Float64, Required: false,
			},
			iceberg.NestedField{
				ID: 9, Name: "both_nan_and_null",
				Type: iceberg.PrimitiveTypes.Float32, Required: false,
			},
			iceberg.NestedField{
				ID: 10, Name: "no_nan_or_null",
				Type: iceberg.PrimitiveTypes.Float64, Required: false,
			},
			iceberg.NestedField{
				ID: 11, Name: "all_nulls_missing_nan_float",
				Type: iceberg.PrimitiveTypes.Float32, Required: false,
			},
			iceberg.NestedField{
				ID: 12, Name: "all_same_value_or_null",
				Type: iceberg.PrimitiveTypes.String, Required: false,
			},
			iceberg.NestedField{
				ID: 13, Name: "no_nulls_same_value_a",
				Type: iceberg.PrimitiveTypes.Binary, Required: false,
			},
		)
	)

	partFields := make([]iceberg.PartitionField, 0, testSchema.NumFields())
	for _, f := range testSchema.Fields() {
		partFields = append(partFields, iceberg.PartitionField{
			Name:      f.Name,
			SourceID:  f.ID,
			FieldID:   f.ID,
			Transform: iceberg.IdentityTransform{},
		})
	}

	spec := iceberg.NewPartitionSpec(partFields...)
	manifestNoStats := iceberg.NewManifestFile(1, "", 0, 0, 0).Build()
	manifest := iceberg.NewManifestFile(1, "", 0, 0, 0).Partitions(
		[]iceberg.FieldSummary{
			{ // id
				ContainsNull: false,
				ContainsNaN:  nil,
				LowerBound:   &IntMin,
				UpperBound:   &IntMax,
			},
			{ // all_nulls_missing_nan
				ContainsNull: true,
				ContainsNaN:  nil,
				LowerBound:   nil,
				UpperBound:   nil,
			},
			{ // some_nulls
				ContainsNull: true,
				ContainsNaN:  nil,
				LowerBound:   &StringMin,
				UpperBound:   &StringMax,
			},
			{ // no_nulls
				ContainsNull: false,
				ContainsNaN:  nil,
				LowerBound:   &StringMin,
				UpperBound:   &StringMax,
			},
			{ // float
				ContainsNull: true,
				ContainsNaN:  nil,
				LowerBound:   &FloatMin,
				UpperBound:   &FloatMax,
			},
			{ // all_nulls_double
				ContainsNull: true,
				ContainsNaN:  nil,
				LowerBound:   nil,
				UpperBound:   nil,
			},
			{ // all_nulls_no_nans
				ContainsNull: true,
				ContainsNaN:  &NanFalse,
				LowerBound:   nil,
				UpperBound:   nil,
			},
			{ // all_nans
				ContainsNull: false,
				ContainsNaN:  &NanTrue,
				LowerBound:   nil,
				UpperBound:   nil,
			},
			{ // both_nan_and_null
				ContainsNull: true,
				ContainsNaN:  &NanTrue,
				LowerBound:   nil,
				UpperBound:   nil,
			},
			{ // no_nan_or_null
				ContainsNull: false,
				ContainsNaN:  &NanFalse,
				LowerBound:   &DblMin,
				UpperBound:   &DblMax,
			},
			{ // all_nulls_missing_nan_float
				ContainsNull: true,
				ContainsNaN:  nil,
				LowerBound:   nil,
				UpperBound:   nil,
			},
			{ // all_same_value_or_null
				ContainsNull: true,
				ContainsNaN:  nil,
				LowerBound:   &StringMin,
				UpperBound:   &StringMin,
			},
			{ // no_nulls_same_value_a
				ContainsNull: false,
				ContainsNaN:  nil,
				LowerBound:   &StringMin,
				UpperBound:   &StringMin,
			},
		}).Build()

	t.Run("all nulls", func(t *testing.T) {
		tests := []struct {
			field    string
			expected bool
			msg      string
		}{
			{"all_nulls_missing_nan", false, "should skip: all nulls column with non-floating type contains all null"},
			{"all_nulls_missing_nan_float", true, "should read: no NaN information may indicate presence of NaN value"},
			{"some_nulls", true, "should read: column with some nulls contains a non-null value"},
			{"no_nulls", true, "should read: non-null column contains a non-null value"},
		}

		for _, tt := range tests {
			eval, err := newManifestEvaluator(spec, testSchema,
				iceberg.NotNull(iceberg.Reference(tt.field)), true)
			require.NoError(t, err)

			result, err := eval(manifest)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result, tt.msg)
		}
	})

	t.Run("no nulls", func(t *testing.T) {
		tests := []struct {
			field    string
			expected bool
			msg      string
		}{
			{"all_nulls_missing_nan", true, "should read: at least one null value in all null column"},
			{"some_nulls", true, "should read: column with some nulls contains a null value"},
			{"no_nulls", false, "should skip: non-null column contains no null values"},
			{"both_nan_and_null", true, "should read: both_nan_and_null column contains no null values"},
		}

		for _, tt := range tests {
			eval, err := newManifestEvaluator(spec, testSchema,
				iceberg.IsNull(iceberg.Reference(tt.field)), true)
			require.NoError(t, err)

			result, err := eval(manifest)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result, tt.msg)
		}
	})

	t.Run("is nan", func(t *testing.T) {
		tests := []struct {
			field    string
			expected bool
			msg      string
		}{
			{"float", true, "should read: no information on if there are nan values in float column"},
			{"all_nulls_double", true, "should read: no NaN information may indicate presence of NaN value"},
			{"all_nulls_missing_nan_float", true, "should read: no NaN information may indicate presence of NaN value"},
			{"all_nulls_no_nans", false, "should skip: no nan column doesn't contain nan value"},
			{"all_nans", true, "should read: all_nans column contains nan value"},
			{"both_nan_and_null", true, "should read: both_nan_and_null column contains nan value"},
			{"no_nan_or_null", false, "should skip: no_nan_or_null column doesn't contain nan value"},
		}

		for _, tt := range tests {
			eval, err := newManifestEvaluator(spec, testSchema,
				iceberg.IsNaN(iceberg.Reference(tt.field)), true)
			require.NoError(t, err)

			result, err := eval(manifest)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result, tt.msg)
		}
	})

	t.Run("not nan", func(t *testing.T) {
		tests := []struct {
			field    string
			expected bool
			msg      string
		}{
			{"float", true, "should read: no information on if there are nan values in float column"},
			{"all_nulls_double", true, "should read: all null column contains non nan value"},
			{"all_nulls_no_nans", true, "should read: no_nans column contains non nan value"},
			{"all_nans", false, "should skip: all nans columndoesn't contain non nan value"},
			{"both_nan_and_null", true, "should read: both_nan_and_null nans column contains non nan value"},
			{"no_nan_or_null", true, "should read: no_nan_or_null column contains non nan value"},
		}

		for _, tt := range tests {
			eval, err := newManifestEvaluator(spec, testSchema,
				iceberg.NotNaN(iceberg.Reference(tt.field)), true)
			require.NoError(t, err)

			result, err := eval(manifest)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result, tt.msg)
		}
	})

	t.Run("test missing stats", func(t *testing.T) {
		exprs := []iceberg.BooleanExpression{
			iceberg.LessThan(iceberg.Reference("id"), int32(5)),
			iceberg.LessThanEqual(iceberg.Reference("id"), int32(30)),
			iceberg.EqualTo(iceberg.Reference("id"), int32(70)),
			iceberg.GreaterThan(iceberg.Reference("id"), int32(78)),
			iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(90)),
			iceberg.NotEqualTo(iceberg.Reference("id"), int32(101)),
			iceberg.IsNull(iceberg.Reference("id")),
			iceberg.NotNull(iceberg.Reference("id")),
			iceberg.IsNaN(iceberg.Reference("float")),
			iceberg.NotNaN(iceberg.Reference("float")),
		}

		for _, tt := range exprs {
			eval, err := newManifestEvaluator(spec, testSchema, tt, true)
			require.NoError(t, err)

			result, err := eval(manifestNoStats)
			require.NoError(t, err)
			assert.Truef(t, result, "should read when missing stats for expr: %s", tt)
		}
	})

	t.Run("test exprs", func(t *testing.T) {
		tests := []struct {
			expr   iceberg.BooleanExpression
			expect bool
			msg    string
		}{
			{
				iceberg.NewNot(iceberg.LessThan(iceberg.Reference("id"), int32(IntMinValue-25))),
				true, "should read: not(false)",
			},
			{
				iceberg.NewNot(iceberg.GreaterThan(iceberg.Reference("id"), int32(IntMinValue-25))),
				false, "should skip: not(true)",
			},
			{
				iceberg.NewAnd(
					iceberg.LessThan(iceberg.Reference("id"), int32(IntMinValue-25)),
					iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(IntMinValue-30))),
				false, "should skip: and(false, true)",
			},
			{
				iceberg.NewAnd(
					iceberg.LessThan(iceberg.Reference("id"), int32(IntMinValue-25)),
					iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(IntMaxValue+1))),
				false, "should skip: and(false, false)",
			},
			{
				iceberg.NewAnd(
					iceberg.GreaterThan(iceberg.Reference("id"), int32(IntMinValue-25)),
					iceberg.LessThanEqual(iceberg.Reference("id"), int32(IntMinValue))),
				true, "should read: and(true, true)",
			},
			{
				iceberg.NewOr(
					iceberg.LessThan(iceberg.Reference("id"), int32(IntMinValue-25)),
					iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(IntMaxValue+1))),
				false, "should skip: or(false, false)",
			},
			{
				iceberg.NewOr(
					iceberg.LessThan(iceberg.Reference("id"), int32(IntMinValue-25)),
					iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(IntMaxValue-19))),
				true, "should read: or(false, true)",
			},
			{
				iceberg.LessThan(iceberg.Reference("some_nulls"), "1"), false,
				"should not read: id range below lower bound",
			},
			{
				iceberg.LessThan(iceberg.Reference("some_nulls"), "b"), true,
				"should read: lower bound in range",
			},
			{
				iceberg.LessThan(iceberg.Reference("float"), 15.50), true,
				"should read: lower bound in range",
			},
			{
				iceberg.LessThan(iceberg.Reference("no_nan_or_null"), 15.50), true,
				"should read: lower bound in range",
			},
			{
				iceberg.LessThanEqual(iceberg.Reference("no_nulls_same_value_a"), "a"), true,
				"should read: lower bound in range",
			},
			{
				iceberg.LessThan(iceberg.Reference("id"), int32(IntMinValue-25)), false,
				"should not read: id range below lower bound (5 < 30)",
			},
			{
				iceberg.LessThan(iceberg.Reference("id"), int32(IntMinValue)), false,
				"should not read: id range below lower bound (30 is not < 30)",
			},
			{
				iceberg.LessThan(iceberg.Reference("id"), int32(IntMinValue+1)), true,
				"should read: one possible id",
			},
			{
				iceberg.LessThan(iceberg.Reference("id"), int32(IntMaxValue)), true,
				"should read: many possible ids",
			},
			{
				iceberg.LessThanEqual(iceberg.Reference("id"), int32(IntMinValue-25)), false,
				"should not read: id range below lower bound (5 < 30)",
			},
			{
				iceberg.LessThanEqual(iceberg.Reference("id"), int32(IntMinValue-1)), false,
				"should not read: id range below lower bound 29 < 30",
			},
			{
				iceberg.LessThanEqual(iceberg.Reference("id"), int32(IntMinValue)), true,
				"should read: one possible id",
			},
			{
				iceberg.LessThanEqual(iceberg.Reference("id"), int32(IntMaxValue)), true,
				"should read: many possible ids",
			},
			{
				iceberg.GreaterThan(iceberg.Reference("id"), int32(IntMaxValue+6)), false,
				"should not read: id range above upper bound (85 < 79)",
			},
			{
				iceberg.GreaterThan(iceberg.Reference("id"), int32(IntMaxValue)), false,
				"should not read: id range above upper bound (79 is not > 79)",
			},
			{
				iceberg.GreaterThan(iceberg.Reference("id"), int32(IntMaxValue-1)), true,
				"should read: one possible id",
			},
			{
				iceberg.GreaterThan(iceberg.Reference("id"), int32(IntMaxValue-4)), true,
				"should read: many possible ids",
			},
			{
				iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(IntMaxValue+6)), false,
				"should not read: id range is above upper bound (85 < 79)",
			},
			{
				iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(IntMaxValue+1)), false,
				"should not read: id range above upper bound (80 > 79)",
			},
			{
				iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(IntMaxValue)), true,
				"should read: one possible id",
			},
			{
				iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(IntMaxValue)), true,
				"should read: many possible ids",
			},
			{
				iceberg.EqualTo(iceberg.Reference("id"), int32(IntMinValue-25)), false,
				"should not read: id below lower bound",
			},
			{
				iceberg.EqualTo(iceberg.Reference("id"), int32(IntMinValue-1)), false,
				"should not read: id below lower bound",
			},
			{
				iceberg.EqualTo(iceberg.Reference("id"), int32(IntMinValue)), true,
				"should read: id equal to lower bound",
			},
			{
				iceberg.EqualTo(iceberg.Reference("id"), int32(IntMaxValue-4)), true,
				"should read: id between lower and upper bounds",
			},
			{
				iceberg.EqualTo(iceberg.Reference("id"), int32(IntMaxValue)), true,
				"should read: id equal to upper bound",
			},
			{
				iceberg.EqualTo(iceberg.Reference("id"), int32(IntMaxValue+1)), false,
				"should not read: id above upper bound",
			},
			{
				iceberg.EqualTo(iceberg.Reference("id"), int32(IntMaxValue+6)), false,
				"should not read: id above upper bound",
			},
			{
				iceberg.NotEqualTo(iceberg.Reference("id"), int32(IntMinValue-25)), true,
				"should read: id below lower bound",
			},
			{
				iceberg.NotEqualTo(iceberg.Reference("id"), int32(IntMinValue-1)), true,
				"should read: id below lower bound",
			},
			{
				iceberg.NotEqualTo(iceberg.Reference("id"), int32(IntMinValue)), true,
				"should read: id equal to lower bound",
			},
			{
				iceberg.NotEqualTo(iceberg.Reference("id"), int32(IntMaxValue-4)), true,
				"should read: id between lower and upper bounds",
			},
			{
				iceberg.NotEqualTo(iceberg.Reference("id"), int32(IntMaxValue)), true,
				"should read: id equal to upper bound",
			},
			{
				iceberg.NotEqualTo(iceberg.Reference("id"), int32(IntMaxValue+1)), true,
				"should read: id above upper bound",
			},
			{
				iceberg.NotEqualTo(iceberg.Reference("id"), int32(IntMaxValue+6)), true,
				"should read: id above upper bound",
			},
			{
				iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("id"), int32(IntMinValue-25))), true,
				"should read: id below lower bound",
			},
			{
				iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("id"), int32(IntMinValue-1))), true,
				"should read: id below lower bound",
			},
			{
				iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("id"), int32(IntMinValue))), true,
				"should read: id equal to lower bound",
			},
			{
				iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("id"), int32(IntMaxValue-4))), true,
				"should read: id between lower and upper bounds",
			},
			{
				iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("id"), int32(IntMaxValue))), true,
				"should read: id equal to upper bound",
			},
			{
				iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("id"), int32(IntMaxValue+1))), true,
				"should read: id above upper bound",
			},
			{
				iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("id"), int32(IntMaxValue+6))), true,
				"should read: id above upper bound",
			},
			{
				iceberg.IsIn(iceberg.Reference("id"), int32(IntMinValue-25), IntMinValue-24), false,
				"should not read: id below lower bound (5 < 30, 6 < 30)",
			},
			{
				iceberg.IsIn(iceberg.Reference("id"), int32(IntMinValue-2), IntMinValue-1), false,
				"should not read: id below lower bound (28 < 30, 29 < 30)",
			},
			{
				iceberg.IsIn(iceberg.Reference("id"), int32(IntMinValue-1), IntMinValue), true,
				"should read: id equal to lower bound (30 == 30)",
			},
			{
				iceberg.IsIn(iceberg.Reference("id"), int32(IntMaxValue-4), IntMaxValue-3), true,
				"should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)",
			},
			{
				iceberg.IsIn(iceberg.Reference("id"), int32(IntMaxValue), IntMaxValue+1), true,
				"should read: id equal to upper bound (79 == 79)",
			},
			{
				iceberg.IsIn(iceberg.Reference("id"), int32(IntMaxValue+1), IntMaxValue+2), false,
				"should not read: id above upper bound (80 > 79, 81 > 79)",
			},
			{
				iceberg.IsIn(iceberg.Reference("id"), int32(IntMaxValue+6), IntMaxValue+7), false,
				"should not read: id above upper bound (85 > 79, 86 > 79)",
			},
			{
				iceberg.IsIn(iceberg.Reference("all_nulls_missing_nan"), "abc", "def"), false,
				"should skip: in on all nulls column",
			},
			{
				iceberg.IsIn(iceberg.Reference("some_nulls"), "abc", "def"), true,
				"should read: in on some nulls column",
			},
			{
				iceberg.IsIn(iceberg.Reference("no_nulls"), "abc", "def"), true,
				"should read: in on no nulls column",
			},
			{
				iceberg.IsIn(iceberg.Reference("no_nulls_same_value_a"), "a", "b"), true,
				"should read: in on no nulls column",
			},
			{
				iceberg.IsIn(iceberg.Reference("float"), 0, -5.5), true,
				"should read: float equal to lower bound",
			},
			{
				iceberg.IsIn(iceberg.Reference("no_nan_or_null"), 0, -5.5), true,
				"should read: float equal to lower bound",
			},
			{
				iceberg.NotIn(iceberg.Reference("id"), int32(IntMinValue-25), IntMinValue-24), true,
				"should read: id below lower bound (5 < 30, 6 < 30)",
			},
			{
				iceberg.NotIn(iceberg.Reference("id"), int32(IntMinValue-2), IntMinValue-1), true,
				"should read: id below lower bound (28 < 30, 29 < 30)",
			},
			{
				iceberg.NotIn(iceberg.Reference("id"), int32(IntMinValue-1), IntMinValue), true,
				"should read: id equal to lower bound (30 == 30)",
			},
			{
				iceberg.NotIn(iceberg.Reference("id"), int32(IntMaxValue-4), IntMaxValue-3), true,
				"should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)",
			},
			{
				iceberg.NotIn(iceberg.Reference("id"), int32(IntMaxValue), IntMaxValue+1), true,
				"should read: id equal to upper bound (79 == 79)",
			},
			{
				iceberg.NotIn(iceberg.Reference("id"), int32(IntMaxValue+1), IntMaxValue+2), true,
				"should read: id above upper bound (80 > 79, 81 > 79)",
			},
			{
				iceberg.NotIn(iceberg.Reference("id"), int32(IntMaxValue+6), IntMaxValue+7), true,
				"should read: id above upper bound (85 > 79, 86 > 79)",
			},
			{
				iceberg.NotIn(iceberg.Reference("all_nulls_missing_nan"), "abc", "def"), true,
				"should read: notIn on all nulls column",
			},
			{
				iceberg.NotIn(iceberg.Reference("some_nulls"), "abc", "def"), true,
				"should read: notIn on some nulls column",
			},
			{
				iceberg.NotIn(iceberg.Reference("no_nulls"), "abc", "def"), true,
				"should read: notIn on no nulls column",
			},
			{
				iceberg.StartsWith(iceberg.Reference("some_nulls"), "a"), true,
				"should read: range matches",
			},
			{
				iceberg.StartsWith(iceberg.Reference("some_nulls"), "aa"), true,
				"should read: range matches",
			},
			{
				iceberg.StartsWith(iceberg.Reference("some_nulls"), "dddd"), true,
				"should read: range matches",
			},
			{
				iceberg.StartsWith(iceberg.Reference("some_nulls"), "z"), true,
				"should read: range matches",
			},
			{
				iceberg.StartsWith(iceberg.Reference("no_nulls"), "a"), true,
				"should read: range matches",
			},
			{
				iceberg.StartsWith(iceberg.Reference("some_nulls"), "zzzz"), false,
				"should skip: range doesn't match",
			},
			{
				iceberg.StartsWith(iceberg.Reference("some_nulls"), "1"), false,
				"should skip: range doesn't match",
			},
			{
				iceberg.StartsWith(iceberg.Reference("no_nulls_same_value_a"), "a"), true,
				"should read: all values start with the prefix",
			},
			{
				iceberg.NotStartsWith(iceberg.Reference("some_nulls"), "a"), true,
				"should read: range matches",
			},
			{
				iceberg.NotStartsWith(iceberg.Reference("some_nulls"), "aa"), true,
				"should read: range matches",
			},
			{
				iceberg.NotStartsWith(iceberg.Reference("some_nulls"), "dddd"), true,
				"should read: range matches",
			},
			{
				iceberg.NotStartsWith(iceberg.Reference("some_nulls"), "z"), true,
				"should read: range matches",
			},
			{
				iceberg.NotStartsWith(iceberg.Reference("no_nulls"), "a"), true,
				"should read: range matches",
			},
			{
				iceberg.NotStartsWith(iceberg.Reference("some_nulls"), "zzzz"), true,
				"should read: range matches",
			},
			{
				iceberg.NotStartsWith(iceberg.Reference("some_nulls"), "1"), true,
				"should read: range matches",
			},
			{
				iceberg.NotStartsWith(iceberg.Reference("all_same_value_or_null"), "a"), true,
				"should read: range matches",
			},
			{
				iceberg.NotStartsWith(iceberg.Reference("all_same_value_or_null"), "aa"), true,
				"should read: range matches",
			},
			{
				iceberg.NotStartsWith(iceberg.Reference("all_same_value_or_null"), "A"), true,
				"should read: range matches",
			},
			// Iceberg does not implement SQL 3-way boolean logic, so the choice of an
			// all null column matching is by definition in order to surface more values
			// to the query engine to allow it to make its own decision
			{
				iceberg.NotStartsWith(iceberg.Reference("all_nulls_missing_nan"), "A"), true,
				"should read: range matches",
			},
			{
				iceberg.NotStartsWith(iceberg.Reference("no_nulls_same_value_a"), "a"), false,
				"should not read: all values start with the prefix",
			},
		}

		for _, tt := range tests {
			t.Run(tt.expr.String(), func(t *testing.T) {
				eval, err := newManifestEvaluator(spec, testSchema,
					tt.expr, true)
				require.NoError(t, err)

				result, err := eval(manifest)
				require.NoError(t, err)
				assert.Equal(t, tt.expect, result, tt.msg)
			})
		}
	})
}

type ProjectionTestSuite struct {
	suite.Suite
}

func (*ProjectionTestSuite) schema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "event_date", Type: iceberg.PrimitiveTypes.Date},
		iceberg.NestedField{ID: 4, Name: "event_ts", Type: iceberg.PrimitiveTypes.Timestamp},
	)
}

func (*ProjectionTestSuite) emptySpec() iceberg.PartitionSpec {
	return iceberg.NewPartitionSpec()
}

func (*ProjectionTestSuite) idSpec() iceberg.PartitionSpec {
	return iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			SourceID: 1, FieldID: 1000,
			Transform: iceberg.IdentityTransform{}, Name: "id_part",
		},
	)
}

func (*ProjectionTestSuite) bucketSpec() iceberg.PartitionSpec {
	return iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			SourceID: 2, FieldID: 1000,
			Transform: iceberg.BucketTransform{NumBuckets: 16}, Name: "data_bucket",
		},
	)
}

func (*ProjectionTestSuite) daySpec() iceberg.PartitionSpec {
	return iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			SourceID: 4, FieldID: 1000,
			Transform: iceberg.DayTransform{}, Name: "date",
		},
		iceberg.PartitionField{
			SourceID: 3, FieldID: 1001,
			Transform: iceberg.DayTransform{}, Name: "ddate",
		},
	)
}

func (*ProjectionTestSuite) hourSpec() iceberg.PartitionSpec {
	return iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			SourceID: 4, FieldID: 1000,
			Transform: iceberg.HourTransform{}, Name: "hour",
		},
	)
}

func (*ProjectionTestSuite) truncateStrSpec() iceberg.PartitionSpec {
	return iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			SourceID: 2, FieldID: 1000,
			Transform: iceberg.TruncateTransform{Width: 2}, Name: "data_trunc",
		},
	)
}

func (*ProjectionTestSuite) truncateIntSpec() iceberg.PartitionSpec {
	return iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			SourceID: 1, FieldID: 1000,
			Transform: iceberg.TruncateTransform{Width: 10}, Name: "id_trunc",
		},
	)
}

func (*ProjectionTestSuite) idAndBucketSpec() iceberg.PartitionSpec {
	return iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			SourceID: 1, FieldID: 1000,
			Transform: iceberg.IdentityTransform{}, Name: "id_part",
		},
		iceberg.PartitionField{
			SourceID: 2, FieldID: 1001,
			Transform: iceberg.BucketTransform{NumBuckets: 16}, Name: "data_bucket",
		},
	)
}

func (p *ProjectionTestSuite) TestIdentityProjection() {
	schema, spec := p.schema(), p.idSpec()

	idRef, idPartRef := iceberg.Reference("id"), iceberg.Reference("id_part")
	tests := []struct {
		pred     iceberg.BooleanExpression
		expected iceberg.BooleanExpression
	}{
		{iceberg.NotNull(idRef), iceberg.NotNull(idPartRef)},
		{iceberg.IsNull(idRef), iceberg.IsNull(idPartRef)},
		{iceberg.LessThan(idRef, int64(100)), iceberg.LessThan(idPartRef, int64(100))},
		{iceberg.LessThanEqual(idRef, int64(101)), iceberg.LessThanEqual(idPartRef, int64(101))},
		{iceberg.GreaterThan(idRef, int64(102)), iceberg.GreaterThan(idPartRef, int64(102))},
		{iceberg.GreaterThanEqual(idRef, int64(103)), iceberg.GreaterThanEqual(idPartRef, int64(103))},
		{iceberg.EqualTo(idRef, int64(104)), iceberg.EqualTo(idPartRef, int64(104))},
		{iceberg.NotEqualTo(idRef, int64(105)), iceberg.NotEqualTo(idPartRef, int64(105))},
		{iceberg.IsIn(idRef, int64(3), 4, 5), iceberg.IsIn(idPartRef, int64(3), 4, 5)},
		{iceberg.NotIn(idRef, int64(3), 4, 5), iceberg.NotIn(idPartRef, int64(3), 4, 5)},
	}

	project := newInclusiveProjection(schema, spec, true)
	for _, tt := range tests {
		p.Run(tt.pred.String(), func() {
			expr, err := project(tt.pred)
			p.Require().NoError(err)
			p.Truef(tt.expected.Equals(expr), "expected: %s\ngot: %s", tt.expected, expr)
		})
	}
}

func (p *ProjectionTestSuite) TestBucketProjection() {
	schema, spec := p.schema(), p.bucketSpec()

	dataRef, dataBkt := iceberg.Reference("data"), iceberg.Reference("data_bucket")
	tests := []struct {
		pred, expected iceberg.BooleanExpression
	}{
		{iceberg.NotNull(dataRef), iceberg.NotNull(dataBkt)},
		{iceberg.IsNull(dataRef), iceberg.IsNull(dataBkt)},
		{iceberg.LessThan(dataRef, "val"), iceberg.AlwaysTrue{}},
		{iceberg.LessThanEqual(dataRef, "val"), iceberg.AlwaysTrue{}},
		{iceberg.GreaterThan(dataRef, "val"), iceberg.AlwaysTrue{}},
		{iceberg.GreaterThanEqual(dataRef, "val"), iceberg.AlwaysTrue{}},
		{iceberg.EqualTo(dataRef, "val"), iceberg.EqualTo(dataBkt, int32(14))},
		{iceberg.NotEqualTo(dataRef, "val"), iceberg.AlwaysTrue{}},
		{iceberg.IsIn(dataRef, "v1", "v2", "v3"), iceberg.IsIn(dataBkt, int32(1), 3, 13)},
		{iceberg.NotIn(dataRef, "v1", "v2", "v3"), iceberg.AlwaysTrue{}},
	}

	project := newInclusiveProjection(schema, spec, true)
	for _, tt := range tests {
		p.Run(tt.pred.String(), func() {
			expr, err := project(tt.pred)
			p.Require().NoError(err)
			p.Truef(tt.expected.Equals(expr), "expected: %s\ngot: %s", tt.expected, expr)
		})
	}
}

func (p *ProjectionTestSuite) TestHourProjection() {
	schema, spec := p.schema(), p.hourSpec()

	ref, hour := iceberg.Reference("event_ts"), iceberg.Reference("hour")
	tests := []struct {
		pred, expected iceberg.BooleanExpression
	}{
		{iceberg.NotNull(ref), iceberg.NotNull(hour)},
		{iceberg.IsNull(ref), iceberg.IsNull(hour)},
		{iceberg.LessThan(ref, "2022-11-27T10:00:00"), iceberg.LessThanEqual(hour, int32(463761))},
		{iceberg.LessThanEqual(ref, "2022-11-27T10:00:00"), iceberg.LessThanEqual(hour, int32(463762))},
		{iceberg.GreaterThan(ref, "2022-11-27T09:59:59.999999"), iceberg.GreaterThanEqual(hour, int32(463762))},
		{iceberg.GreaterThanEqual(ref, "2022-11-27T09:59:59.999999"), iceberg.GreaterThanEqual(hour, int32(463761))},
		{iceberg.EqualTo(ref, "2022-11-27T10:00:00"), iceberg.EqualTo(hour, int32(463762))},
		{iceberg.NotEqualTo(ref, "2022-11-27T10:00:00"), iceberg.AlwaysTrue{}},
		{iceberg.IsIn(ref, "2022-11-27T10:00:00", "2022-11-27T09:59:59.999999"), iceberg.IsIn(hour, int32(463761), 463762)},
		{iceberg.NotIn(ref, "2022-11-27T10:00:00", "2022-11-27T09:59:59.999999"), iceberg.AlwaysTrue{}},
	}

	project := newInclusiveProjection(schema, spec, true)
	for _, tt := range tests {
		p.Run(tt.pred.String(), func() {
			expr, err := project(tt.pred)
			p.Require().NoError(err)
			p.Truef(tt.expected.Equals(expr), "expected: %s\ngot: %s", tt.expected, expr)
		})
	}
}

func (p *ProjectionTestSuite) TestDayProjection() {
	schema, spec := p.schema(), p.daySpec()

	ref, date := iceberg.Reference("event_ts"), iceberg.Reference("date")
	tests := []struct {
		pred, expected iceberg.BooleanExpression
	}{
		{iceberg.NotNull(ref), iceberg.NotNull(date)},
		{iceberg.IsNull(ref), iceberg.IsNull(date)},
		{iceberg.LessThan(ref, "2022-11-27T00:00:00"), iceberg.LessThanEqual(date, int32(19322))},
		{iceberg.LessThanEqual(ref, "2022-11-27T00:00:00"), iceberg.LessThanEqual(date, int32(19323))},
		{iceberg.GreaterThan(ref, "2022-11-26T23:59:59.999999"), iceberg.GreaterThanEqual(date, int32(19323))},
		{iceberg.GreaterThanEqual(ref, "2022-11-26T23:59:59.999999"), iceberg.GreaterThanEqual(date, int32(19322))},
		{iceberg.EqualTo(ref, "2022-11-27T10:00:00"), iceberg.EqualTo(date, int32(19323))},
		{iceberg.NotEqualTo(ref, "2022-11-27T10:00:00"), iceberg.AlwaysTrue{}},
		{iceberg.IsIn(ref, "2022-11-27T00:00:00", "2022-11-26T23:59:59.999999"), iceberg.IsIn(date, int32(19322), 19323)},
		{iceberg.NotIn(ref, "2022-11-27T00:00:00", "2022-11-26T23:59:59.999999"), iceberg.AlwaysTrue{}},
	}

	project := newInclusiveProjection(schema, spec, true)
	for _, tt := range tests {
		p.Run(tt.pred.String(), func() {
			expr, err := project(tt.pred)
			p.Require().NoError(err)
			p.Truef(tt.expected.Equals(expr), "expected: %s\ngot: %s", tt.expected, expr)
		})
	}
}

func (p *ProjectionTestSuite) TestDateDayProjection() {
	schema, spec := p.schema(), p.daySpec()

	ref, date := iceberg.Reference("event_date"), iceberg.Reference("ddate")
	tests := []struct {
		pred, expected iceberg.BooleanExpression
	}{
		{iceberg.NotNull(ref), iceberg.NotNull(date)},
		{iceberg.IsNull(ref), iceberg.IsNull(date)},
		{iceberg.LessThan(ref, "2022-11-27"), iceberg.LessThanEqual(date, int32(19322))},
		{iceberg.LessThanEqual(ref, "2022-11-27"), iceberg.LessThanEqual(date, int32(19323))},
		{iceberg.GreaterThan(ref, "2022-11-26"), iceberg.GreaterThanEqual(date, int32(19323))},
		{iceberg.GreaterThanEqual(ref, "2022-11-26"), iceberg.GreaterThanEqual(date, int32(19322))},
		{iceberg.EqualTo(ref, "2022-11-27"), iceberg.EqualTo(date, int32(19323))},
		{iceberg.NotEqualTo(ref, "2022-11-27"), iceberg.AlwaysTrue{}},
		{iceberg.IsIn(ref, "2022-11-27", "2022-11-26"), iceberg.IsIn(date, int32(19322), 19323)},
		{iceberg.NotIn(ref, "2022-11-27", "2022-11-26"), iceberg.AlwaysTrue{}},
	}

	project := newInclusiveProjection(schema, spec, true)
	for _, tt := range tests {
		p.Run(tt.pred.String(), func() {
			expr, err := project(tt.pred)
			p.Require().NoError(err)
			p.Truef(tt.expected.Equals(expr), "expected: %s\ngot: %s", tt.expected, expr)
		})
	}
}

func (p *ProjectionTestSuite) TestStringTruncateProjection() {
	schema, spec := p.schema(), p.truncateStrSpec()

	ref, truncStr := iceberg.Reference("data"), iceberg.Reference("data_trunc")
	tests := []struct {
		pred, expected iceberg.BooleanExpression
	}{
		{iceberg.NotNull(ref), iceberg.NotNull(truncStr)},
		{iceberg.IsNull(ref), iceberg.IsNull(truncStr)},
		{iceberg.LessThan(ref, "aaa"), iceberg.LessThanEqual(truncStr, "aa")},
		{iceberg.LessThanEqual(ref, "aaa"), iceberg.LessThanEqual(truncStr, "aa")},
		{iceberg.GreaterThan(ref, "aaa"), iceberg.GreaterThanEqual(truncStr, "aa")},
		{iceberg.GreaterThanEqual(ref, "aaa"), iceberg.GreaterThanEqual(truncStr, "aa")},
		{iceberg.EqualTo(ref, "aaa"), iceberg.EqualTo(truncStr, "aa")},
		{iceberg.NotEqualTo(ref, "aaa"), iceberg.AlwaysTrue{}},
		{iceberg.IsIn(ref, "aaa", "aab"), iceberg.EqualTo(truncStr, "aa")},
		{iceberg.NotIn(ref, "aaa", "aab"), iceberg.AlwaysTrue{}},
	}

	project := newInclusiveProjection(schema, spec, true)
	for _, tt := range tests {
		p.Run(tt.pred.String(), func() {
			expr, err := project(tt.pred)
			p.Require().NoError(err)
			p.Truef(tt.expected.Equals(expr), "expected: %s\ngot: %s", tt.expected, expr)
		})
	}
}

func (p *ProjectionTestSuite) TestIntTruncateProjection() {
	schema, spec := p.schema(), p.truncateIntSpec()

	ref, idTrunc := iceberg.Reference("id"), iceberg.Reference("id_trunc")
	tests := []struct {
		pred, expected iceberg.BooleanExpression
	}{
		{iceberg.NotNull(ref), iceberg.NotNull(idTrunc)},
		{iceberg.IsNull(ref), iceberg.IsNull(idTrunc)},
		{iceberg.LessThan(ref, int32(10)), iceberg.LessThanEqual(idTrunc, int64(0))},
		{iceberg.LessThanEqual(ref, int32(10)), iceberg.LessThanEqual(idTrunc, int64(10))},
		{iceberg.GreaterThan(ref, int32(9)), iceberg.GreaterThanEqual(idTrunc, int64(10))},
		{iceberg.GreaterThanEqual(ref, int32(10)), iceberg.GreaterThanEqual(idTrunc, int64(10))},
		{iceberg.EqualTo(ref, int32(15)), iceberg.EqualTo(idTrunc, int64(10))},
		{iceberg.NotEqualTo(ref, int32(15)), iceberg.AlwaysTrue{}},
		{iceberg.IsIn(ref, int32(15), 16), iceberg.EqualTo(idTrunc, int64(10))},
		{iceberg.NotIn(ref, int32(15), 16), iceberg.AlwaysTrue{}},
	}

	project := newInclusiveProjection(schema, spec, true)
	for _, tt := range tests {
		p.Run(tt.pred.String(), func() {
			expr, err := project(tt.pred)
			p.Require().NoError(err)
			p.Truef(tt.expected.Equals(expr), "expected: %s\ngot: %s", tt.expected, expr)
		})
	}
}

func (p *ProjectionTestSuite) TestProjectionCaseSensitive() {
	schema, spec := p.schema(), p.idSpec()
	project := newInclusiveProjection(schema, spec, true)
	_, err := project(iceberg.NotNull(iceberg.Reference("ID")))
	p.ErrorIs(err, iceberg.ErrInvalidSchema)
	p.ErrorContains(err, "could not bind reference 'ID', caseSensitive=true")
}

func (p *ProjectionTestSuite) TestProjectionCaseInsensitive() {
	schema, spec := p.schema(), p.idSpec()
	project := newInclusiveProjection(schema, spec, false)
	expr, err := project(iceberg.NotNull(iceberg.Reference("ID")))
	p.Require().NoError(err)
	p.True(expr.Equals(iceberg.NotNull(iceberg.Reference("id_part"))))
}

func (p *ProjectionTestSuite) TestProjectEmptySpec() {
	project := newInclusiveProjection(p.schema(), p.emptySpec(), true)
	expr, err := project(iceberg.NewAnd(iceberg.LessThan(iceberg.Reference("id"), int32(5)),
		iceberg.NotNull(iceberg.Reference("data"))))
	p.Require().NoError(err)
	p.Equal(iceberg.AlwaysTrue{}, expr)
}

func (p *ProjectionTestSuite) TestAndProjectionMultipleFields() {
	project := newInclusiveProjection(p.schema(), p.idAndBucketSpec(), true)
	expr, err := project(iceberg.NewAnd(iceberg.LessThan(iceberg.Reference("id"),
		int32(5)), iceberg.IsIn(iceberg.Reference("data"), "a", "b", "c")))
	p.Require().NoError(err)

	p.True(expr.Equals(iceberg.NewAnd(iceberg.LessThan(iceberg.Reference("id_part"), int64(5)),
		iceberg.IsIn(iceberg.Reference("data_bucket"), int32(2), 3, 15))))
}

func (p *ProjectionTestSuite) TestOrProjectionMultipleFields() {
	project := newInclusiveProjection(p.schema(), p.idAndBucketSpec(), true)
	expr, err := project(iceberg.NewOr(iceberg.LessThan(iceberg.Reference("id"), int32(5)),
		iceberg.IsIn(iceberg.Reference("data"), "a", "b", "c")))
	p.Require().NoError(err)

	p.True(expr.Equals(iceberg.NewOr(iceberg.LessThan(iceberg.Reference("id_part"), int64(5)),
		iceberg.IsIn(iceberg.Reference("data_bucket"), int32(2), 3, 15))))
}

func (p *ProjectionTestSuite) TestNotProjectionMultipleFields() {
	project := newInclusiveProjection(p.schema(), p.idAndBucketSpec(), true)
	// not causes In to be rewritten to NotIn, which cannot be projected
	expr, err := project(iceberg.NewNot(iceberg.NewOr(iceberg.LessThan(iceberg.Reference("id"), int64(5)),
		iceberg.IsIn(iceberg.Reference("data"), "a", "b", "c"))))
	p.Require().NoError(err)

	p.True(expr.Equals(iceberg.GreaterThanEqual(iceberg.Reference("id_part"), int64(5))))
}

func (p *ProjectionTestSuite) TestPartialProjectedFields() {
	project := newInclusiveProjection(p.schema(), p.idSpec(), true)
	expr, err := project(iceberg.NewAnd(iceberg.LessThan(iceberg.Reference("id"), int32(5)),
		iceberg.IsIn(iceberg.Reference("data"), "a", "b", "c")))
	p.Require().NoError(err)
	p.True(expr.Equals(iceberg.LessThan(iceberg.Reference("id_part"), int64(5))))
}

type mockDataFile struct {
	path        string
	format      iceberg.FileFormat
	partition   map[int]any
	count       int64
	columnSizes map[int]int64
	filesize    int64
	valueCounts map[int]int64
	nullCounts  map[int]int64
	nanCounts   map[int]int64
	lowerBounds map[int][]byte
	upperBounds map[int][]byte

	specid int32
}

func (*mockDataFile) ContentType() iceberg.ManifestEntryContent { return iceberg.EntryContentData }
func (m *mockDataFile) FilePath() string                        { return m.path }
func (m *mockDataFile) FileFormat() iceberg.FileFormat          { return m.format }
func (m *mockDataFile) Partition() map[int]any                  { return m.partition }
func (m *mockDataFile) Count() int64                            { return m.count }
func (m *mockDataFile) FileSizeBytes() int64                    { return m.filesize }
func (m *mockDataFile) ColumnSizes() map[int]int64              { return m.columnSizes }
func (m *mockDataFile) ValueCounts() map[int]int64              { return m.valueCounts }
func (m *mockDataFile) NullValueCounts() map[int]int64          { return m.nullCounts }
func (m *mockDataFile) NaNValueCounts() map[int]int64           { return m.nanCounts }
func (*mockDataFile) DistinctValueCounts() map[int]int64        { return nil }
func (m *mockDataFile) LowerBoundValues() map[int][]byte        { return m.lowerBounds }
func (m *mockDataFile) UpperBoundValues() map[int][]byte        { return m.upperBounds }
func (*mockDataFile) KeyMetadata() []byte                       { return nil }
func (*mockDataFile) SplitOffsets() []int64                     { return nil }
func (*mockDataFile) EqualityFieldIDs() []int                   { return nil }
func (*mockDataFile) SortOrderID() *int                         { return nil }
func (m *mockDataFile) SpecID() int32                           { return m.specid }

type InclusiveMetricsTestSuite struct {
	suite.Suite

	schemaDataFile *iceberg.Schema
	dataFiles      [4]iceberg.DataFile

	schemaDataFileNan *iceberg.Schema
	dataFileNan       iceberg.DataFile
}

func (suite *InclusiveMetricsTestSuite) SetupSuite() {
	suite.schemaDataFile = iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "no_stats", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		iceberg.NestedField{ID: 3, Name: "required", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 4, Name: "all_nulls", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 5, Name: "some_nulls", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 6, Name: "no_nulls", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 7, Name: "all_nans", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 8, Name: "some_nans", Type: iceberg.PrimitiveTypes.Float32},
		iceberg.NestedField{ID: 9, Name: "no_nans", Type: iceberg.PrimitiveTypes.Float32},
		iceberg.NestedField{ID: 10, Name: "all_nulls_double", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 11, Name: "all_nans_v1_stats", Type: iceberg.PrimitiveTypes.Float32},
		iceberg.NestedField{ID: 12, Name: "nan_and_null_only", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 13, Name: "no_nan_stats", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 14, Name: "some_empty", Type: iceberg.PrimitiveTypes.String},
	)

	var (
		IntMin, _   = iceberg.Int32Literal(IntMinValue).MarshalBinary()
		IntMax, _   = iceberg.Int32Literal(IntMaxValue).MarshalBinary()
		FltNan, _   = iceberg.Float32Literal(float32(math.NaN())).MarshalBinary()
		DblNan, _   = iceberg.Float64Literal(math.NaN()).MarshalBinary()
		FltSeven, _ = iceberg.Float32Literal(7).MarshalBinary()
		DblSeven, _ = iceberg.Float64Literal(7).MarshalBinary()
		FltMax, _   = iceberg.Float32Literal(22).MarshalBinary()
	)

	suite.dataFiles = [4]iceberg.DataFile{
		&mockDataFile{
			path:     "file_1.parquet",
			format:   iceberg.ParquetFile,
			count:    50,
			filesize: 3,
			valueCounts: map[int]int64{
				4: 50, 5: 50, 6: 50, 7: 50, 8: 50, 9: 50,
				10: 50, 11: 50, 12: 50, 13: 50, 14: 50,
			},
			nullCounts: map[int]int64{4: 50, 5: 10, 6: 0, 10: 50, 11: 0, 12: 1, 14: 8},
			nanCounts:  map[int]int64{7: 50, 8: 10, 9: 0},
			lowerBounds: map[int][]byte{
				1:  IntMin,
				11: FltNan,
				12: DblNan,
				14: {},
			},
			upperBounds: map[int][]byte{
				1:  IntMax,
				11: FltNan,
				12: DblNan,
				14: []byte("房东整租霍营小区二层两居室"),
			},
		},
		&mockDataFile{
			path:        "file_2.parquet",
			format:      iceberg.ParquetFile,
			count:       50,
			filesize:    3,
			valueCounts: map[int]int64{3: 20},
			nullCounts:  map[int]int64{3: 2},
			nanCounts:   nil,
			lowerBounds: map[int][]byte{3: {'a', 'a'}},
			upperBounds: map[int][]byte{3: {'d', 'C'}},
		},
		&mockDataFile{
			path:        "file_3.parquet",
			format:      iceberg.ParquetFile,
			count:       50,
			filesize:    3,
			valueCounts: map[int]int64{3: 20},
			nullCounts:  map[int]int64{3: 2},
			nanCounts:   nil,
			lowerBounds: map[int][]byte{3: []byte("1str1")},
			upperBounds: map[int][]byte{3: []byte("3str3")},
		},
		&mockDataFile{
			path:        "file_4.parquet",
			format:      iceberg.ParquetFile,
			count:       50,
			filesize:    3,
			valueCounts: map[int]int64{3: 20},
			nullCounts:  map[int]int64{3: 2},
			nanCounts:   nil,
			lowerBounds: map[int][]byte{3: []byte("abc")},
			upperBounds: map[int][]byte{3: []byte("イロハニホヘト")},
		},
	}

	suite.schemaDataFileNan = iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "all_nan", Type: iceberg.PrimitiveTypes.Float64, Required: true},
		iceberg.NestedField{ID: 2, Name: "max_nan", Type: iceberg.PrimitiveTypes.Float64, Required: true},
		iceberg.NestedField{ID: 3, Name: "min_max_nan", Type: iceberg.PrimitiveTypes.Float32},
		iceberg.NestedField{ID: 4, Name: "all_nan_null_bounds", Type: iceberg.PrimitiveTypes.Float64, Required: true},
		iceberg.NestedField{ID: 5, Name: "some_nan_correct_bounds", Type: iceberg.PrimitiveTypes.Float32},
	)

	suite.dataFileNan = &mockDataFile{
		path:        "file.avro",
		format:      iceberg.AvroFile,
		count:       50,
		filesize:    3,
		columnSizes: map[int]int64{1: 10, 2: 10, 3: 10, 4: 10, 5: 10},
		valueCounts: map[int]int64{1: 10, 2: 10, 3: 10, 4: 10, 5: 10},
		nullCounts:  map[int]int64{1: 0, 2: 0, 3: 0, 4: 0, 5: 0},
		nanCounts:   map[int]int64{1: 10, 4: 10, 5: 5},
		lowerBounds: map[int][]byte{
			1: DblNan,
			2: DblSeven,
			3: FltNan,
			5: FltSeven,
		},
		upperBounds: map[int][]byte{
			1: DblNan,
			2: DblNan,
			3: FltNan,
			5: FltMax,
		},
	}
}

func (suite *InclusiveMetricsTestSuite) TestAllNull() {
	allNull, someNull, noNull := iceberg.Reference("all_nulls"), iceberg.Reference("some_nulls"), iceberg.Reference("no_nulls")

	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.NotNull(allNull), false, "should skip: no non-null value in all null column"},
		{iceberg.LessThan(allNull, "a"), false, "should skip: lessThan on all null column"},
		{iceberg.LessThanEqual(allNull, "a"), false, "should skip: lessThanEqual on all null column"},
		{iceberg.GreaterThan(allNull, "a"), false, "should skip: greaterThan on all null column"},
		{iceberg.GreaterThanEqual(allNull, "a"), false, "should skip: greaterThanEqual on all null column"},
		{iceberg.EqualTo(allNull, "a"), false, "should skip: equal on all null column"},
		{iceberg.NotNull(someNull), true, "should read: column with some nulls contains a non-null value"},
		{iceberg.NotNull(noNull), true, "should read: non-null column contains a non-null value"},
		{iceberg.StartsWith(allNull, "asad"), false, "should skip: starts with on all null column"},
		{iceberg.NotStartsWith(allNull, "asad"), true, "should read: notStartsWith on all null column"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestNoNulls() {
	allNull, someNull, noNull := iceberg.Reference("all_nulls"), iceberg.Reference("some_nulls"), iceberg.Reference("no_nulls")

	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.IsNull(allNull), true, "should read: at least one null value in all null column"},
		{iceberg.IsNull(someNull), true, "should read: column with some nulls contains a null value"},
		{iceberg.IsNull(noNull), false, "should skip: non-null column contains no null values"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestIsNan() {
	allNan, someNan, noNan := iceberg.Reference("all_nans"), iceberg.Reference("some_nans"), iceberg.Reference("no_nans")
	allNullsDbl, noNanStats := iceberg.Reference("all_nulls_double"), iceberg.Reference("no_nan_stats")
	allNansV1, nanNullOnly := iceberg.Reference("all_nans_v1_stats"), iceberg.Reference("nan_and_null_only")

	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.IsNaN(allNan), true, "should read: at least one nan value in all nan column"},
		{iceberg.IsNaN(someNan), true, "should read: at least one nan value in some nan column"},
		{iceberg.IsNaN(noNan), false, "should skip: no-nans column has no nans"},
		{iceberg.IsNaN(allNullsDbl), false, "should skip: all-null column doesn't contain nan values"},
		{iceberg.IsNaN(noNanStats), true, "should read: no guarantee if contains nan without stats"},
		{iceberg.IsNaN(allNansV1), true, "should read: at least one nan value in all nan column"},
		{iceberg.IsNaN(nanNullOnly), true, "should read: at least one nan value in nan and nulls only column"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestNotNaN() {
	allNan, someNan, noNan := iceberg.Reference("all_nans"), iceberg.Reference("some_nans"), iceberg.Reference("no_nans")
	allNullsDbl, noNanStats := iceberg.Reference("all_nulls_double"), iceberg.Reference("no_nan_stats")
	allNansV1, nanNullOnly := iceberg.Reference("all_nans_v1_stats"), iceberg.Reference("nan_and_null_only")

	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.NotNaN(allNan), false, "should skip: column with all nans will not contain non-nan"},
		{iceberg.NotNaN(someNan), true, "should read: at least one non-nan value in some nan column"},
		{iceberg.NotNaN(noNan), true, "should read: at least one non-nan value in no nan column"},
		{iceberg.NotNaN(allNullsDbl), true, "should read: at least one non-nan value in all null column"},
		{iceberg.NotNaN(noNanStats), true, "should read: no guarantee if contains nan without stats"},
		{iceberg.NotNaN(allNansV1), true, "should read: no guarantee"},
		{iceberg.NotNaN(nanNullOnly), true, "should read: at least one null value in nan and nulls only column"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestRequiredColumn() {
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.NotNull(iceberg.Reference("required")), true, "should read: required columns are always non-null"},
		{iceberg.IsNull(iceberg.Reference("required")), false, "should skip: required columns are always non-null"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestMissingColumn() {
	_, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, iceberg.LessThan(iceberg.Reference("missing"), int32(22)), true, true)
	suite.ErrorIs(err, iceberg.ErrInvalidSchema)
}

func (suite *InclusiveMetricsTestSuite) TestMissingStats() {
	noStatsSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 2, Name: "no_stats", Type: iceberg.PrimitiveTypes.Float64})

	noStatsFile := &mockDataFile{
		path:   "file_1.parquet",
		format: iceberg.ParquetFile,
		count:  50,
	}

	ref := iceberg.Reference("no_stats")
	tests := []iceberg.BooleanExpression{
		iceberg.LessThan(ref, int32(5)),
		iceberg.LessThanEqual(ref, int32(30)),
		iceberg.EqualTo(ref, int32(70)),
		iceberg.GreaterThan(ref, int32(78)),
		iceberg.GreaterThanEqual(ref, int32(90)),
		iceberg.NotEqualTo(ref, int32(101)),
		iceberg.IsNull(ref),
		iceberg.NotNull(ref),
		iceberg.IsNaN(ref),
		iceberg.NotNaN(ref),
	}

	for _, tt := range tests {
		suite.Run(tt.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(noStatsSchema, tt, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(noStatsFile)
			suite.Require().NoError(err)
			suite.True(shouldRead, "should read when stats are missing")
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestZeroRecordFileStats() {
	zeroRecordFile := &mockDataFile{
		path:   "file_1.parquet",
		format: iceberg.ParquetFile,
		count:  0,
	}

	ref := iceberg.Reference("no_stats")
	tests := []iceberg.BooleanExpression{
		iceberg.LessThan(ref, int32(5)),
		iceberg.LessThanEqual(ref, int32(30)),
		iceberg.EqualTo(ref, int32(70)),
		iceberg.GreaterThan(ref, int32(78)),
		iceberg.GreaterThanEqual(ref, int32(90)),
		iceberg.NotEqualTo(ref, int32(101)),
		iceberg.IsNull(ref),
		iceberg.NotNull(ref),
		iceberg.IsNaN(ref),
		iceberg.NotNaN(ref),
	}

	for _, tt := range tests {
		suite.Run(tt.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, tt, true, false)
			suite.Require().NoError(err)
			shouldRead, err := eval(zeroRecordFile)
			suite.Require().NoError(err)
			suite.False(shouldRead, "should skip datafile without records")
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestNot() {
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.NewNot(iceberg.LessThan(iceberg.Reference("id"), IntMinValue-25)), true, "should read: not(false)"},
		{iceberg.NewNot(iceberg.GreaterThan(iceberg.Reference("id"), IntMinValue-25)), false, "should skip: not(true)"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestAnd() {
	ref := iceberg.Reference("id")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.NewAnd(
			iceberg.LessThan(ref, IntMinValue-25),
			iceberg.GreaterThanEqual(ref, IntMinValue-30)), false, "should skip: and(false, true)"},
		{iceberg.NewAnd(
			iceberg.LessThan(ref, IntMinValue-25),
			iceberg.GreaterThanEqual(ref, IntMinValue+1)), false, "should skip: and(false, false)"},
		{iceberg.NewAnd(
			iceberg.GreaterThan(ref, IntMinValue-25),
			iceberg.LessThanEqual(ref, IntMinValue)), true, "should read: and(true, true)"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestOr() {
	ref := iceberg.Reference("id")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.NewOr(
			iceberg.LessThan(ref, IntMinValue-25),
			iceberg.GreaterThanEqual(ref, IntMaxValue+1)), false, "should skip: or(false, false)"},
		{iceberg.NewOr(
			iceberg.LessThan(ref, IntMinValue-25),
			iceberg.GreaterThanEqual(ref, IntMaxValue-19)), true, "should read: or(false, true)"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestIntLt() {
	ref := iceberg.Reference("id")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.LessThan(ref, IntMinValue-25), false, "should skip: id range below lower bound (5 < 30)"},
		{iceberg.LessThan(ref, IntMinValue), false, "should skip: id range below lower bound (30 is not < 30)"},
		{iceberg.LessThan(ref, IntMinValue+1), true, "should read: one possible id"},
		{iceberg.LessThan(ref, IntMaxValue), true, "should read: many possible ids"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestIntLtEq() {
	ref := iceberg.Reference("id")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.LessThanEqual(ref, IntMinValue-25), false, "should skip: id range below lower bound (5 < 30)"},
		{iceberg.LessThanEqual(ref, IntMinValue-1), false, "should skip: id range below lower bound (29 is not <= 30)"},
		{iceberg.LessThanEqual(ref, IntMinValue), true, "should read: one possible id"},
		{iceberg.LessThanEqual(ref, IntMaxValue), true, "should read: many possible ids"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestIntGt() {
	ref := iceberg.Reference("id")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.GreaterThan(ref, IntMaxValue+6), false, "should skip: id range above upper bound (85 > 79)"},
		{iceberg.GreaterThan(ref, IntMaxValue), false, "should skip: id range above upper bound (79 is not > 79)"},
		{iceberg.GreaterThan(ref, IntMinValue-1), true, "should read: one possible id"},
		{iceberg.GreaterThan(ref, IntMaxValue-4), true, "should read: many possible ids"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestIntGtEq() {
	ref := iceberg.Reference("id")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.GreaterThanEqual(ref, IntMaxValue+6), false, "should skip: id range above upper bound (85 < 79)"},
		{iceberg.GreaterThanEqual(ref, IntMaxValue+1), false, "should skip: id range above upper bound (80 > 79)"},
		{iceberg.GreaterThanEqual(ref, IntMaxValue), true, "should read: one possible id"},
		{iceberg.GreaterThanEqual(ref, IntMaxValue-4), true, "should read: many possible ids"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestIntEq() {
	ref := iceberg.Reference("id")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.EqualTo(ref, IntMinValue-25), false, "should skip: id range below lower bound"},
		{iceberg.EqualTo(ref, IntMinValue-1), false, "should skip: id range below lower bound"},
		{iceberg.EqualTo(ref, IntMinValue), true, "should read: id equal to lower bound"},
		{iceberg.EqualTo(ref, IntMaxValue-4), true, "should read: id between lower and upper bounds"},
		{iceberg.EqualTo(ref, IntMaxValue), true, "should read: id equal to upper bound"},
		{iceberg.EqualTo(ref, IntMaxValue+1), false, "should skip: id above upper bound"},
		{iceberg.EqualTo(ref, IntMaxValue+6), false, "should skip: id above upper bound"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestIntNeq() {
	ref := iceberg.Reference("id")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.NotEqualTo(ref, IntMinValue-25), true, "should read: id range below lower bound"},
		{iceberg.NotEqualTo(ref, IntMinValue-1), true, "should read: id range below lower bound"},
		{iceberg.NotEqualTo(ref, IntMinValue), true, "should read: id equal to lower bound"},
		{iceberg.NotEqualTo(ref, IntMaxValue-4), true, "should read: id between lower and upper bounds"},
		{iceberg.NotEqualTo(ref, IntMaxValue), true, "should read: id equal to upper bound"},
		{iceberg.NotEqualTo(ref, IntMaxValue+1), true, "should read: id above upper bound"},
		{iceberg.NotEqualTo(ref, IntMaxValue+6), true, "should read: id above upper bound"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestIntNeqRewritten() {
	ref := iceberg.Reference("id")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.EqualTo(ref, IntMinValue-25), true, "should read: id range below lower bound"},
		{iceberg.EqualTo(ref, IntMinValue-1), true, "should read: id range below lower bound"},
		{iceberg.EqualTo(ref, IntMinValue), true, "should read: id equal to lower bound"},
		{iceberg.EqualTo(ref, IntMaxValue-4), true, "should read: id between lower and upper bounds"},
		{iceberg.EqualTo(ref, IntMaxValue), true, "should read: id equal to upper bound"},
		{iceberg.EqualTo(ref, IntMaxValue+1), true, "should read: id above upper bound"},
		{iceberg.EqualTo(ref, IntMaxValue+6), true, "should read: id above upper bound"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, iceberg.NewNot(tt.expr), true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestIntNeqRewrittenCaseInsensitive() {
	ref := iceberg.Reference("ID")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.EqualTo(ref, IntMinValue-25), true, "should read: id range below lower bound"},
		{iceberg.EqualTo(ref, IntMinValue-1), true, "should read: id range below lower bound"},
		{iceberg.EqualTo(ref, IntMinValue), true, "should read: id equal to lower bound"},
		{iceberg.EqualTo(ref, IntMaxValue-4), true, "should read: id between lower and upper bounds"},
		{iceberg.EqualTo(ref, IntMaxValue), true, "should read: id equal to upper bound"},
		{iceberg.EqualTo(ref, IntMaxValue+1), true, "should read: id above upper bound"},
		{iceberg.EqualTo(ref, IntMaxValue+6), true, "should read: id above upper bound"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, iceberg.NewNot(tt.expr), false, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestInMetrics() {
	ref := iceberg.Reference("id")

	ids := make([]int32, 400)
	for i := range ids {
		ids[i] = int32(i)
	}

	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.IsIn(ref, IntMinValue-25, IntMinValue-24), false, "should skip: id below lower bound"},
		{iceberg.IsIn(ref, IntMinValue-2, IntMinValue-1), false, "should skip: id below lower bound"},
		{iceberg.IsIn(ref, IntMinValue-1, IntMinValue), true, "should read: id equal to lower bound"},
		{iceberg.IsIn(ref, IntMaxValue-4, IntMaxValue-3), true, "should read: id between upper and lower bounds"},
		{iceberg.IsIn(ref, IntMaxValue, IntMaxValue+1), true, "should read: id equal to upper bound"},
		{iceberg.IsIn(ref, IntMaxValue+1, IntMaxValue+2), false, "should skip: id above upper bound"},
		{iceberg.IsIn(ref, IntMaxValue+6, IntMaxValue+7), false, "should skip: id above upper bound"},
		{iceberg.IsIn(iceberg.Reference("all_nulls"), "abc", "def"), false, "should skip: in on all nulls column"},
		{iceberg.IsIn(iceberg.Reference("some_nulls"), "abc", "def"), true, "should read: in on some nulls column"},
		{iceberg.IsIn(iceberg.Reference("no_nulls"), "abc", "def"), true, "should read: in on no nulls column"},
		{iceberg.IsIn(ref, ids...), true, "should read: large in expression"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestNotInMetrics() {
	ref := iceberg.Reference("id")

	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.NotIn(ref, IntMinValue-25, IntMinValue-24), true, "should read: id below lower bound"},
		{iceberg.NotIn(ref, IntMinValue-2, IntMinValue-1), true, "should read: id below lower bound"},
		{iceberg.NotIn(ref, IntMinValue-1, IntMinValue), true, "should read: id equal to lower bound"},
		{iceberg.NotIn(ref, IntMaxValue-4, IntMaxValue-3), true, "should read: id between upper and lower bounds"},
		{iceberg.NotIn(ref, IntMaxValue, IntMaxValue+1), true, "should read: id equal to upper bound"},
		{iceberg.NotIn(ref, IntMaxValue+1, IntMaxValue+2), true, "should read: id above upper bound"},
		{iceberg.NotIn(ref, IntMaxValue+6, IntMaxValue+7), true, "should read: id above upper bound"},
		{iceberg.NotIn(iceberg.Reference("all_nulls"), "abc", "def"), true, "should read: in on all nulls column"},
		{iceberg.NotIn(iceberg.Reference("some_nulls"), "abc", "def"), true, "should read: in on some nulls column"},
		{iceberg.NotIn(iceberg.Reference("no_nulls"), "abc", "def"), true, "should read: in on no nulls column"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestLessAndLessEqualNans() {
	type Op func(iceberg.UnboundTerm, int32) iceberg.UnboundPredicate
	for _, operator := range []Op{iceberg.LessThan[int32], iceberg.LessThanEqual[int32]} {
		tests := []struct {
			expr     iceberg.BooleanExpression
			expected bool
			msg      string
		}{
			{operator(iceberg.Reference("all_nan"), int32(1)), false, "should skip: all nan column doesn't contain number"},
			{operator(iceberg.Reference("max_nan"), int32(1)), false, "should skip: 1 is smaller than lower bound"},
			{operator(iceberg.Reference("max_nan"), int32(10)), true, "should read: 10 is larger than lower bound"},
			{operator(iceberg.Reference("min_max_nan"), int32(1)), true, "should read: no visibility"},
			{operator(iceberg.Reference("all_nan_null_bounds"), int32(1)), false, "should skip: all nan column doesn't contain number"},
			{operator(iceberg.Reference("some_nan_correct_bounds"), int32(1)), false, "should skip: 1 is smaller than lower bound"},
			{operator(iceberg.Reference("some_nan_correct_bounds"), int32(10)), true, "should read: 10 is larger than lower bound"},
		}

		for _, tt := range tests {
			suite.Run(tt.expr.String(), func() {
				eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFileNan, tt.expr, true, true)
				suite.Require().NoError(err)
				shouldRead, err := eval(suite.dataFileNan)
				suite.Require().NoError(err)
				suite.Equal(tt.expected, shouldRead, tt.msg)
			})
		}
	}
}

func (suite *InclusiveMetricsTestSuite) TestGreaterAndGreaterEqualNans() {
	type Op func(iceberg.UnboundTerm, int32) iceberg.UnboundPredicate
	for _, operator := range []Op{iceberg.GreaterThan[int32], iceberg.GreaterThanEqual[int32]} {
		tests := []struct {
			expr     iceberg.BooleanExpression
			expected bool
			msg      string
		}{
			{operator(iceberg.Reference("all_nan"), int32(1)), false, "should skip: all nan column doesn't contain number"},
			{operator(iceberg.Reference("max_nan"), int32(1)), true, "should read: upper bound is larger than 1"},
			{operator(iceberg.Reference("max_nan"), int32(10)), true, "should read: 10 is smaller than upper bound"},
			{operator(iceberg.Reference("min_max_nan"), int32(1)), true, "should read: no visibility"},
			{operator(iceberg.Reference("all_nan_null_bounds"), int32(1)), false, "should skip: all nan column doesn't contain number"},
			{operator(iceberg.Reference("some_nan_correct_bounds"), int32(1)), true, "should read: 1 is smaller than upper bound"},
			{operator(iceberg.Reference("some_nan_correct_bounds"), int32(10)), true, "should read: 10 is smaller than upper bound"},
			{operator(iceberg.Reference("all_nan"), int32(30)), false, "should skip: 30 is larger than upper bound"},
		}

		for _, tt := range tests {
			suite.Run(tt.expr.String(), func() {
				eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFileNan, tt.expr, true, true)
				suite.Require().NoError(err)
				shouldRead, err := eval(suite.dataFileNan)
				suite.Require().NoError(err)
				suite.Equal(tt.expected, shouldRead, tt.msg)
			})
		}
	}
}

func (suite *InclusiveMetricsTestSuite) TestEqualsNans() {
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.EqualTo(iceberg.Reference("all_nan"), int32(1)), false, "should skip: all nan column doesn't contain number"},
		{iceberg.EqualTo(iceberg.Reference("max_nan"), int32(1)), false, "should skip: 1 is smaller than lower bound"},
		{iceberg.EqualTo(iceberg.Reference("max_nan"), int32(10)), true, "should read: 10 is within bounds"},
		{iceberg.EqualTo(iceberg.Reference("min_max_nan"), int32(1)), true, "should read: no visibility"},
		{iceberg.EqualTo(iceberg.Reference("all_nan_null_bounds"), int32(1)), false, "should skip: all nan column doesn't contain number"},
		{iceberg.EqualTo(iceberg.Reference("some_nan_correct_bounds"), int32(1)), false, "should skip: 1 is smaller than lower bound"},
		{iceberg.EqualTo(iceberg.Reference("some_nan_correct_bounds"), int32(10)), true, "should read: 10 within bounds"},
		{iceberg.EqualTo(iceberg.Reference("all_nan"), int32(30)), false, "should skip: 30 is larger than upper bound"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFileNan, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFileNan)
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestNotEqualsNans() {
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.NotEqualTo(iceberg.Reference("all_nan"), int32(1)), true, "should read: no visibility"},
		{iceberg.NotEqualTo(iceberg.Reference("max_nan"), int32(1)), true, "should read: no visibility"},
		{iceberg.NotEqualTo(iceberg.Reference("max_nan"), int32(10)), true, "should read: no visibility"},
		{iceberg.NotEqualTo(iceberg.Reference("min_max_nan"), int32(1)), true, "should read: no visibility"},
		{iceberg.NotEqualTo(iceberg.Reference("all_nan_null_bounds"), int32(1)), true, "should read: no visibility"},
		{iceberg.NotEqualTo(iceberg.Reference("some_nan_correct_bounds"), int32(1)), true, "should read: no visibility"},
		{iceberg.NotEqualTo(iceberg.Reference("some_nan_correct_bounds"), int32(10)), true, "should read: no visibility"},
		{iceberg.NotEqualTo(iceberg.Reference("all_nan"), int32(30)), true, "should read: no visibility"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFileNan, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFileNan)
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestInWithNans() {
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.IsIn(iceberg.Reference("all_nan"), int32(1), 10, 30), false, "should skip: all nan column doesn't contain number"},
		{iceberg.IsIn(iceberg.Reference("max_nan"), int32(1), 10, 30), true, "should read: 10 and 30 are greater than lower bound"},
		{iceberg.IsIn(iceberg.Reference("min_max_nan"), int32(1), 10, 30), true, "should read: no visibility"},
		{iceberg.IsIn(iceberg.Reference("all_nan_null_bounds"), int32(1), 10, 30), false, "should skip: all nan column doesn't contain number"},
		{iceberg.IsIn(iceberg.Reference("some_nan_correct_bounds"), int32(1), 10, 30), true, "should read: 10 within bounds"},
		{iceberg.IsIn(iceberg.Reference("some_nan_correct_bounds"), int32(1), 30), false, "should skip: 1 and 30 not within bounds"},
		{iceberg.IsIn(iceberg.Reference("some_nan_correct_bounds"), int32(5), 7), true, "should read: overlap with lower bound"},
		{iceberg.IsIn(iceberg.Reference("some_nan_correct_bounds"), int32(22), 25), true, "should read: overlap with upper bound"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFileNan, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFileNan)
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestNotInWithNans() {
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.NotIn(iceberg.Reference("all_nan"), int32(1), 10, 30), true, "should read: no visibility"},
		{iceberg.NotIn(iceberg.Reference("max_nan"), int32(1), 10, 30), true, "should read: no visibility"},
		{iceberg.NotIn(iceberg.Reference("min_max_nan"), int32(1), 10, 30), true, "should read: no visibility"},
		{iceberg.NotIn(iceberg.Reference("all_nan_null_bounds"), int32(1), 10, 30), true, "should read: no visibility"},
		{iceberg.NotIn(iceberg.Reference("some_nan_correct_bounds"), int32(1), 10, 30), true, "should read: no visibility"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFileNan, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFileNan)
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestStartsWith() {
	ref, refEmpty := iceberg.Reference("required"), iceberg.Reference("some_empty")

	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		dataFile iceberg.DataFile
		msg      string
	}{
		{iceberg.StartsWith(ref, "a"), true, suite.dataFiles[0], "should read: no stats"},
		{iceberg.StartsWith(ref, "a"), true, suite.dataFiles[1], "should read: range matches"},
		{iceberg.StartsWith(ref, "aa"), true, suite.dataFiles[1], "should read: range matches"},
		{iceberg.StartsWith(ref, "aaa"), true, suite.dataFiles[1], "should read: range matches"},
		{iceberg.StartsWith(ref, "1s"), true, suite.dataFiles[2], "should read: range matches"},
		{iceberg.StartsWith(ref, "1str1x"), true, suite.dataFiles[2], "should read: range matches"},
		{iceberg.StartsWith(ref, "ff"), true, suite.dataFiles[3], "should read: range matches"},
		{iceberg.StartsWith(ref, "aB"), false, suite.dataFiles[1], "should skip: range doesn't match"},
		{iceberg.StartsWith(ref, "dWx"), false, suite.dataFiles[1], "should skip: range doesn't match"},
		{iceberg.StartsWith(ref, "5"), false, suite.dataFiles[2], "should skip: range doesn't match"},
		{iceberg.StartsWith(ref, "3str3x"), false, suite.dataFiles[2], "should skip: range doesn't match"},
		{iceberg.StartsWith(refEmpty, "房东整租霍"), true, suite.dataFiles[0], "should read: range matches"},
		{iceberg.StartsWith(iceberg.Reference("all_nulls"), ""), false, suite.dataFiles[0], "should skip: range doesn't match"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(tt.dataFile)
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *InclusiveMetricsTestSuite) TestNotStartsWith() {
	ref, refEmpty := iceberg.Reference("required"), iceberg.Reference("some_empty")

	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		dataFile iceberg.DataFile
		msg      string
	}{
		{iceberg.NotStartsWith(ref, "a"), true, suite.dataFiles[0], "should read: no stats"},
		{iceberg.NotStartsWith(ref, "a"), true, suite.dataFiles[1], "should read: range matches"},
		{iceberg.NotStartsWith(ref, "aa"), true, suite.dataFiles[1], "should read: range matches"},
		{iceberg.NotStartsWith(ref, "aaa"), true, suite.dataFiles[1], "should read: range matches"},
		{iceberg.NotStartsWith(ref, "1s"), true, suite.dataFiles[2], "should read: range matches"},
		{iceberg.NotStartsWith(ref, "1str1x"), true, suite.dataFiles[2], "should read: range matches"},
		{iceberg.NotStartsWith(ref, "ff"), true, suite.dataFiles[3], "should read: range matches"},
		{iceberg.NotStartsWith(ref, "aB"), true, suite.dataFiles[1], "should read: range doesn't match"},
		{iceberg.NotStartsWith(ref, "dWx"), true, suite.dataFiles[1], "should read: range doesn't match"},
		{iceberg.NotStartsWith(ref, "5"), true, suite.dataFiles[2], "should read: range doesn't match"},
		{iceberg.NotStartsWith(ref, "3str3x"), true, suite.dataFiles[2], "should read: range doesn't match"},
		{iceberg.NotStartsWith(refEmpty, "房东整租霍"), true, suite.dataFiles[0], "should read: range matches"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newInclusiveMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(tt.dataFile)
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

type StrictMetricsTestSuite struct {
	suite.Suite

	schemaDataFile *iceberg.Schema
	dataFiles      [3]iceberg.DataFile

	schemaDataFileNan *iceberg.Schema
	dataFileNan       iceberg.DataFile
}

func (suite *StrictMetricsTestSuite) SetupSuite() {
	suite.schemaDataFile = iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "no_stats", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		iceberg.NestedField{ID: 3, Name: "required", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 4, Name: "all_nulls", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 5, Name: "some_nulls", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 6, Name: "no_nulls", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 7, Name: "always_5", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 8, Name: "all_nans", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 9, Name: "some_nans", Type: iceberg.PrimitiveTypes.Float32},
		iceberg.NestedField{ID: 10, Name: "no_nans", Type: iceberg.PrimitiveTypes.Float32},
		iceberg.NestedField{ID: 11, Name: "all_nulls_double", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 12, Name: "all_nans_v1_stats", Type: iceberg.PrimitiveTypes.Float32},
		iceberg.NestedField{ID: 13, Name: "nan_and_null_only", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 14, Name: "no_nan_stats", Type: iceberg.PrimitiveTypes.Float64},
	)

	var (
		IntMin, _   = iceberg.Int32Literal(IntMinValue).MarshalBinary()
		IntMax, _   = iceberg.Int32Literal(IntMaxValue).MarshalBinary()
		IntFive, _  = iceberg.Int32Literal(5).MarshalBinary()
		FltNan, _   = iceberg.Float32Literal(float32(math.NaN())).MarshalBinary()
		DblNan, _   = iceberg.Float64Literal(math.NaN()).MarshalBinary()
		FltSeven, _ = iceberg.Float32Literal(7).MarshalBinary()
		DblSeven, _ = iceberg.Float64Literal(7).MarshalBinary()
		FltMax, _   = iceberg.Float32Literal(22).MarshalBinary()
	)

	suite.dataFiles = [3]iceberg.DataFile{
		&mockDataFile{
			path:     "file_1.parquet",
			format:   iceberg.ParquetFile,
			count:    50,
			filesize: 3,
			valueCounts: map[int]int64{
				4: 50, 5: 50, 6: 50, 8: 50, 9: 50,
				10: 50, 11: 50, 12: 50, 13: 50, 14: 50,
			},
			nullCounts: map[int]int64{4: 50, 5: 10, 6: 0, 11: 50, 12: 0, 13: 1},
			nanCounts:  map[int]int64{8: 50, 9: 10, 10: 0},
			lowerBounds: map[int][]byte{
				1:  IntMin,
				7:  IntFive,
				12: FltNan,
				13: DblNan,
			},
			upperBounds: map[int][]byte{
				1:  IntMax,
				7:  IntFive,
				12: FltNan,
				14: DblNan,
			},
		},
		&mockDataFile{
			path:        "file_2.parquet",
			format:      iceberg.ParquetFile,
			count:       50,
			filesize:    3,
			valueCounts: map[int]int64{4: 50, 5: 50, 6: 50, 8: 50},
			nullCounts:  map[int]int64{4: 50, 5: 10, 6: 0},
			nanCounts:   nil,
			lowerBounds: map[int][]byte{5: {'b', 'b', 'b'}},
			upperBounds: map[int][]byte{5: {'e', 'e', 'e'}},
		},
		&mockDataFile{
			path:        "file_3.parquet",
			format:      iceberg.ParquetFile,
			count:       50,
			filesize:    3,
			valueCounts: map[int]int64{4: 50, 5: 50, 6: 50},
			nullCounts:  map[int]int64{4: 50, 5: 10, 6: 0},
			nanCounts:   nil,
			lowerBounds: map[int][]byte{5: {'b', 'b', 'b'}},
			upperBounds: map[int][]byte{5: {'e', 'e', 'e'}},
		},
	}

	suite.schemaDataFileNan = iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "all_nan", Type: iceberg.PrimitiveTypes.Float64, Required: true},
		iceberg.NestedField{ID: 2, Name: "max_nan", Type: iceberg.PrimitiveTypes.Float64, Required: true},
		iceberg.NestedField{ID: 3, Name: "min_max_nan", Type: iceberg.PrimitiveTypes.Float32},
		iceberg.NestedField{ID: 4, Name: "all_nan_null_bounds", Type: iceberg.PrimitiveTypes.Float64, Required: true},
		iceberg.NestedField{ID: 5, Name: "some_nan_correct_bounds", Type: iceberg.PrimitiveTypes.Float32},
	)

	suite.dataFileNan = &mockDataFile{
		path:        "file.avro",
		format:      iceberg.AvroFile,
		count:       50,
		filesize:    3,
		columnSizes: map[int]int64{1: 10, 2: 10, 3: 10, 4: 10, 5: 10},
		valueCounts: map[int]int64{1: 10, 2: 10, 3: 10, 4: 10, 5: 10},
		nullCounts:  map[int]int64{1: 0, 2: 0, 3: 0, 4: 0, 5: 0},
		nanCounts:   map[int]int64{1: 10, 4: 10, 5: 5},
		lowerBounds: map[int][]byte{
			1: DblNan,
			2: DblSeven,
			3: FltNan,
			5: FltSeven,
		},
		upperBounds: map[int][]byte{
			1: DblNan,
			2: DblNan,
			3: FltNan,
			5: FltMax,
		},
	}
}

func (suite *StrictMetricsTestSuite) TestAllNull() {
	allNull, someNull, noNull := iceberg.Reference("all_nulls"), iceberg.Reference("some_nulls"), iceberg.Reference("no_nulls")

	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.NotNull(allNull), false, "should skip: no non-null value in all null column"},
		{iceberg.NotNull(someNull), false, "should skip: column with some nulls contains a non-null value"},
		{iceberg.NotNull(noNull), true, "should read: non-null column contains no null values"},
		{iceberg.NotEqualTo(allNull, "a"), true, "should read: notEqual on all nulls column"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *StrictMetricsTestSuite) TestNoNulls() {
	allNull, someNull, noNull := iceberg.Reference("all_nulls"), iceberg.Reference("some_nulls"), iceberg.Reference("no_nulls")

	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.IsNull(allNull), true, "should read: all values are null"},
		{iceberg.IsNull(someNull), false, "should skip: not all values are null"},
		{iceberg.IsNull(noNull), false, "should skip: no values are null"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *StrictMetricsTestSuite) TestSomeNulls() {
	someNull := iceberg.Reference("some_nulls")

	tests := []struct {
		expr     iceberg.BooleanExpression
		dataFile int
		expected bool
		msg      string
	}{
		{iceberg.LessThan(someNull, "ggg"), 1, false, "should skip: some values are null"},
		{iceberg.LessThanEqual(someNull, "ggg"), 1, false, "should skip: some values are null"},
		{iceberg.GreaterThan(someNull, "aaa"), 1, false, "should skip: some values are null"},
		{iceberg.GreaterThanEqual(someNull, "bbb"), 1, false, "should skip: some values are null"},
		{iceberg.EqualTo(someNull, "bbb"), 2, false, "should skip: some values are null"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[tt.dataFile])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *StrictMetricsTestSuite) TestIsNan() {
	allNan, someNan, noNan := iceberg.Reference("all_nans"), iceberg.Reference("some_nans"), iceberg.Reference("no_nans")
	allNullsDbl, noNanStats := iceberg.Reference("all_nulls_double"), iceberg.Reference("no_nan_stats")
	allNansV1, nanNullOnly := iceberg.Reference("all_nans_v1_stats"), iceberg.Reference("nan_and_null_only")

	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.IsNaN(allNan), true, "should read: all values are nan"},
		{iceberg.IsNaN(someNan), false, "should skip:  at least one non-nan value in some nan column"},
		{iceberg.IsNaN(noNan), false, "should skip: at least one non-nan value in no nan column"},
		{iceberg.IsNaN(allNullsDbl), false, "should skip: at least one non-nan value in all null column"},
		{iceberg.IsNaN(noNanStats), false, "should skip: cannot determine without nan stats"},
		{iceberg.IsNaN(allNansV1), false, "should skip: cannot determine without nan stats"},
		{iceberg.IsNaN(nanNullOnly), false, "should skip: null values are not nan"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *StrictMetricsTestSuite) TestNotNaN() {
	allNan, someNan, noNan := iceberg.Reference("all_nans"), iceberg.Reference("some_nans"), iceberg.Reference("no_nans")
	allNullsDbl, noNanStats := iceberg.Reference("all_nulls_double"), iceberg.Reference("no_nan_stats")
	allNansV1, nanNullOnly := iceberg.Reference("all_nans_v1_stats"), iceberg.Reference("nan_and_null_only")

	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.NotNaN(allNan), false, "should skip: all values are nan"},
		{iceberg.NotNaN(someNan), false, "should skip: at least one nan value in some nan column"},
		{iceberg.NotNaN(noNan), true, "should read: no value is nan"},
		{iceberg.NotNaN(allNullsDbl), true, "should read: no nan value in all null column"},
		{iceberg.NotNaN(noNanStats), false, "should skip: cannot determine without nan stats"},
		{iceberg.NotNaN(allNansV1), false, "should skip: all values are nan"},
		{iceberg.NotNaN(nanNullOnly), false, "should skip: null values are not nan"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *StrictMetricsTestSuite) TestRequiredColumn() {
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.NotNull(iceberg.Reference("required")), true, "should read: required columns are always non-null"},
		{iceberg.IsNull(iceberg.Reference("required")), false, "should skip: required columns are always non-null"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *StrictMetricsTestSuite) TestMissingColumn() {
	_, err := newStrictMetricsEvaluator(suite.schemaDataFile, iceberg.LessThan(iceberg.Reference("missing"), int32(22)), true, true)
	suite.ErrorIs(err, iceberg.ErrInvalidSchema)
}

func (suite *StrictMetricsTestSuite) TestMissingStats() {
	noStatsSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 2, Name: "no_stats", Type: iceberg.PrimitiveTypes.Float64})

	noStatsFile := &mockDataFile{
		path:   "file_1.parquet",
		format: iceberg.ParquetFile,
		count:  50,
	}

	ref := iceberg.Reference("no_stats")
	tests := []iceberg.BooleanExpression{
		iceberg.LessThan(ref, int32(5)),
		iceberg.LessThanEqual(ref, int32(30)),
		iceberg.EqualTo(ref, int32(70)),
		iceberg.GreaterThan(ref, int32(78)),
		iceberg.GreaterThanEqual(ref, int32(90)),
		iceberg.NotEqualTo(ref, int32(101)),
		iceberg.IsNull(ref),
		iceberg.NotNull(ref),
		iceberg.IsNaN(ref),
		iceberg.NotNaN(ref),
	}

	for _, tt := range tests {
		suite.Run(tt.String(), func() {
			eval, err := newStrictMetricsEvaluator(noStatsSchema, tt, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(noStatsFile)
			suite.Require().NoError(err)
			suite.False(shouldRead, "should not read when stats are missing")
		})
	}
}

func (suite *StrictMetricsTestSuite) TestZeroRecordFileStats() {
	zeroRecordFile := &mockDataFile{
		path:   "file_1.parquet",
		format: iceberg.ParquetFile,
		count:  0,
	}

	ref := iceberg.Reference("no_stats")
	tests := []iceberg.BooleanExpression{
		iceberg.LessThan(ref, int32(5)),
		iceberg.LessThanEqual(ref, int32(30)),
		iceberg.EqualTo(ref, int32(70)),
		iceberg.GreaterThan(ref, int32(78)),
		iceberg.GreaterThanEqual(ref, int32(90)),
		iceberg.NotEqualTo(ref, int32(101)),
		iceberg.IsNull(ref),
		iceberg.NotNull(ref),
		iceberg.IsNaN(ref),
		iceberg.NotNaN(ref),
	}

	for _, tt := range tests {
		suite.Run(tt.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, tt, true, false)
			suite.Require().NoError(err)
			shouldRead, err := eval(zeroRecordFile)
			suite.Require().NoError(err)
			suite.True(shouldRead, "should match datafile without records")
		})
	}
}

func (suite *StrictMetricsTestSuite) TestNot() {
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.NewNot(iceberg.LessThan(iceberg.Reference("id"), IntMinValue-25)), true, "should read: not(false)"},
		{iceberg.NewNot(iceberg.GreaterThan(iceberg.Reference("id"), IntMinValue-25)), false, "should skip: not(true)"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *StrictMetricsTestSuite) TestAnd() {
	ref := iceberg.Reference("id")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.NewAnd(
			iceberg.GreaterThan(ref, IntMinValue-25),
			iceberg.LessThanEqual(ref, IntMinValue)), false, "should skip: range may not overlap data"},
		{iceberg.NewAnd(
			iceberg.LessThan(ref, IntMinValue-25),
			iceberg.GreaterThanEqual(ref, IntMinValue-30)), false, "should skip: range does not overlap data"},
		{iceberg.NewAnd(
			iceberg.LessThan(ref, IntMaxValue+6),
			iceberg.GreaterThanEqual(ref, IntMinValue-30)), true, "should match: range includes all data"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *StrictMetricsTestSuite) TestOr() {
	ref := iceberg.Reference("id")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.NewOr(
			iceberg.LessThan(ref, IntMinValue-25),
			iceberg.GreaterThanEqual(ref, IntMaxValue+1)), false, "should skip: no matching values"},
		{iceberg.NewOr(
			iceberg.LessThan(ref, IntMinValue-25),
			iceberg.GreaterThanEqual(ref, IntMaxValue-19)), false, "should skip: some values do not match"},
		{iceberg.NewOr(
			iceberg.LessThan(ref, IntMinValue-25),
			iceberg.GreaterThanEqual(ref, IntMinValue)), true, "should match: all values match"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *StrictMetricsTestSuite) TestIntLt() {
	ref := iceberg.Reference("id")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.LessThan(ref, IntMinValue), false, "should skip: always false"},
		{iceberg.LessThan(ref, IntMinValue+1), false, "should skip: 32 and greater not in range"},
		{iceberg.LessThan(ref, IntMaxValue), false, "should skip: 79 not in range"},
		{iceberg.LessThan(ref, IntMaxValue+1), true, "should read: all values in range"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *StrictMetricsTestSuite) TestIntLtEq() {
	ref := iceberg.Reference("id")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.LessThanEqual(ref, IntMinValue-1), false, "should skip: always false"},
		{iceberg.LessThanEqual(ref, IntMinValue), false, "should skip: 31 and greater not in range"},
		{iceberg.LessThanEqual(ref, IntMaxValue), true, "should read: all values in range"},
		{iceberg.LessThanEqual(ref, IntMaxValue+1), true, "should read: all values in range"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *StrictMetricsTestSuite) TestIntGt() {
	ref := iceberg.Reference("id")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.GreaterThan(ref, IntMaxValue), false, "should skip: always false"},
		{iceberg.GreaterThan(ref, IntMaxValue-1), false, "should skip: 77 and less not in range"},
		{iceberg.GreaterThan(ref, IntMinValue), false, "should skip: 30 not in range"},
		{iceberg.GreaterThan(ref, IntMinValue-1), true, "should read: all values in range"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *StrictMetricsTestSuite) TestIntGtEq() {
	ref := iceberg.Reference("id")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.GreaterThanEqual(ref, IntMaxValue+1), false, "should skip: no values in range"},
		{iceberg.GreaterThanEqual(ref, IntMaxValue), false, "should skip: 78 and lower are not in range"},
		{iceberg.GreaterThanEqual(ref, IntMinValue+1), false, "should skip: 30 not in range"},
		{iceberg.GreaterThanEqual(ref, IntMinValue), true, "should read: all values in range"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *StrictMetricsTestSuite) TestIntEq() {
	id, alwaysFive := iceberg.Reference("id"), iceberg.Reference("always_5")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.EqualTo(id, IntMinValue-25), false, "should skip: all values != 5"},
		{iceberg.EqualTo(id, IntMinValue), false, "should skip: some values != 30"},
		{iceberg.EqualTo(id, IntMinValue-4), false, "should skip: some values != 75"},
		{iceberg.EqualTo(id, IntMaxValue), false, "should skip: some values != 79"},
		{iceberg.EqualTo(id, IntMaxValue+1), false, "should skip: some values != 80"},
		{iceberg.EqualTo(alwaysFive, IntMinValue-25), true, "should read: all values == 5"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *StrictMetricsTestSuite) TestIntNeq() {
	ref := iceberg.Reference("id")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.NotEqualTo(ref, IntMinValue-25), true, "should read: no values == 5"},
		{iceberg.NotEqualTo(ref, IntMinValue-1), true, "should read: no values == 39"},
		{iceberg.NotEqualTo(ref, IntMinValue), false, "should skip: some value may be == 30"},
		{iceberg.NotEqualTo(ref, IntMaxValue-4), false, "should skip: some value may be == 75"},
		{iceberg.NotEqualTo(ref, IntMaxValue), false, "should skip: some value may be == 79"},
		{iceberg.NotEqualTo(ref, IntMaxValue+1), true, "should read: no values == 80"},
		{iceberg.NotEqualTo(ref, IntMaxValue+6), true, "should read: no values == 85"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *StrictMetricsTestSuite) TestIntNeqRewritten() {
	ref := iceberg.Reference("id")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.EqualTo(ref, IntMinValue-25), true, "should read: no values == 5"},
		{iceberg.EqualTo(ref, IntMinValue-1), true, "should read: no values == 39"},
		{iceberg.EqualTo(ref, IntMinValue), false, "should skip: some value may be == 30"},
		{iceberg.EqualTo(ref, IntMaxValue-4), false, "should skip: some value may be == 75"},
		{iceberg.EqualTo(ref, IntMaxValue), false, "should skip: some value may be == 79"},
		{iceberg.EqualTo(ref, IntMaxValue+1), true, "should read: no values == 80"},
		{iceberg.EqualTo(ref, IntMaxValue+6), true, "should read: no values == 85"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, iceberg.NewNot(tt.expr), true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *StrictMetricsTestSuite) TestIntNeqRewrittenCaseInsensitive() {
	ref := iceberg.Reference("ID")
	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.EqualTo(ref, IntMinValue-25), true, "should read: no values == 5"},
		{iceberg.EqualTo(ref, IntMinValue-1), true, "should read: no values == 39"},
		{iceberg.EqualTo(ref, IntMinValue), false, "should skip: some value may be == 30"},
		{iceberg.EqualTo(ref, IntMaxValue-4), false, "should skip: some value may be == 75"},
		{iceberg.EqualTo(ref, IntMaxValue), false, "should skip: some value may be == 79"},
		{iceberg.EqualTo(ref, IntMaxValue+1), true, "should read: no values == 80"},
		{iceberg.EqualTo(ref, IntMaxValue+6), true, "should read: no values == 85"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, iceberg.NewNot(tt.expr), false, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *StrictMetricsTestSuite) TestInMetrics() {
	ref := iceberg.Reference("id")

	ids := make([]int32, 400)
	for i := range ids {
		ids[i] = int32(i)
	}

	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.IsIn(ref, IntMinValue-25, IntMinValue-24), false, "should skip: all values != 5 and != 6"},
		{iceberg.IsIn(ref, IntMinValue-1, IntMinValue), false, "should skip: some values != 30 and != 31"},
		{iceberg.IsIn(ref, IntMaxValue-4, IntMaxValue-3), false, "should skip: some values != 75 and != 76"},
		{iceberg.IsIn(ref, IntMaxValue, IntMaxValue+1), false, "should skip: some values != 78 and != 79"},
		{iceberg.IsIn(ref, IntMaxValue+1, IntMaxValue+2), false, "should skip: some values != 80 and != 81"},
		{iceberg.IsIn(iceberg.Reference("always_5"), int32(5), int32(6)), true, "should read: all values == 5"},
		{iceberg.IsIn(iceberg.Reference("all_nulls"), "abc", "def"), false, "should skip: in on all nulls column"},
		{iceberg.IsIn(iceberg.Reference("some_nulls"), "abc", "def"), false, "should skip: in on some nulls column"},
		{iceberg.IsIn(iceberg.Reference("no_nulls"), "abc", "def"), false, "should skip: in on no nulls column"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func (suite *StrictMetricsTestSuite) TestNotInMetrics() {
	ref := iceberg.Reference("id")

	tests := []struct {
		expr     iceberg.BooleanExpression
		expected bool
		msg      string
	}{
		{iceberg.NotIn(ref, IntMinValue-1, IntMinValue), false, "should skip: some values may be == 30"},
		{iceberg.NotIn(ref, IntMaxValue-4, IntMaxValue-3), false, "should skip: some value may be == 75 or == 76"},
		{iceberg.NotIn(ref, IntMaxValue, IntMaxValue+1), false, "should skip: some value may be == 79"},
		{iceberg.NotIn(ref, IntMaxValue+1, IntMaxValue+2), true, "should read: no values == 80 or == 81"},
		{iceberg.NotIn(iceberg.Reference("always_5"), int32(5), int32(6)), false, "should skip: all values == 5"},
		{iceberg.NotIn(iceberg.Reference("all_nulls"), "abc", "def"), true, "should read: notIn on all nulls column"},
		{iceberg.NotIn(iceberg.Reference("some_nulls"), "abc", "def"), true, "should read: notIn on some nulls column"},
		{iceberg.NotIn(iceberg.Reference("no_nulls"), "abc", "def"), false, "should read: notIn on no nulls column"},
	}

	for _, tt := range tests {
		suite.Run(tt.expr.String(), func() {
			eval, err := newStrictMetricsEvaluator(suite.schemaDataFile, tt.expr, true, true)
			suite.Require().NoError(err)
			shouldRead, err := eval(suite.dataFiles[0])
			suite.Require().NoError(err)
			suite.Equal(tt.expected, shouldRead, tt.msg)
		})
	}
}

func TestEvaluators(t *testing.T) {
	suite.Run(t, &ProjectionTestSuite{})
	suite.Run(t, &InclusiveMetricsTestSuite{})
	suite.Run(t, &StrictMetricsTestSuite{})
}
