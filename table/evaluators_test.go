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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			iceberg.NestedField{ID: 1, Name: "id",
				Type: iceberg.PrimitiveTypes.Int32, Required: true},
			iceberg.NestedField{ID: 2, Name: "all_nulls_missing_nan",
				Type: iceberg.PrimitiveTypes.String, Required: false},
			iceberg.NestedField{ID: 3, Name: "some_nulls",
				Type: iceberg.PrimitiveTypes.String, Required: false},
			iceberg.NestedField{ID: 4, Name: "no_nulls",
				Type: iceberg.PrimitiveTypes.String, Required: false},
			iceberg.NestedField{ID: 5, Name: "float",
				Type: iceberg.PrimitiveTypes.Float32, Required: false},
			iceberg.NestedField{ID: 6, Name: "all_nulls_double",
				Type: iceberg.PrimitiveTypes.Float64, Required: false},
			iceberg.NestedField{ID: 7, Name: "all_nulls_no_nans",
				Type: iceberg.PrimitiveTypes.Float32, Required: false},
			iceberg.NestedField{ID: 8, Name: "all_nans",
				Type: iceberg.PrimitiveTypes.Float64, Required: false},
			iceberg.NestedField{ID: 9, Name: "both_nan_and_null",
				Type: iceberg.PrimitiveTypes.Float32, Required: false},
			iceberg.NestedField{ID: 10, Name: "no_nan_or_null",
				Type: iceberg.PrimitiveTypes.Float64, Required: false},
			iceberg.NestedField{ID: 11, Name: "all_nulls_missing_nan_float",
				Type: iceberg.PrimitiveTypes.Float32, Required: false},
			iceberg.NestedField{ID: 12, Name: "all_same_value_or_null",
				Type: iceberg.PrimitiveTypes.String, Required: false},
			iceberg.NestedField{ID: 13, Name: "no_nulls_same_value_a",
				Type: iceberg.PrimitiveTypes.Binary, Required: false},
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
	manifestNoStats := iceberg.NewManifestV1Builder("", 0, 0, 0).Build()
	manifest := iceberg.NewManifestV1Builder("", 0, 0, 0).Partitions(
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
			{iceberg.NewNot(iceberg.LessThan(iceberg.Reference("id"), int32(IntMinValue-25))),
				true, "should read: not(false)"},
			{iceberg.NewNot(iceberg.GreaterThan(iceberg.Reference("id"), int32(IntMinValue-25))),
				false, "should skip: not(true)"},
			{iceberg.NewAnd(
				iceberg.LessThan(iceberg.Reference("id"), int32(IntMinValue-25)),
				iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(IntMinValue-30))),
				false, "should skip: and(false, true)"},
			{iceberg.NewAnd(
				iceberg.LessThan(iceberg.Reference("id"), int32(IntMinValue-25)),
				iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(IntMaxValue+1))),
				false, "should skip: and(false, false)"},
			{iceberg.NewAnd(
				iceberg.GreaterThan(iceberg.Reference("id"), int32(IntMinValue-25)),
				iceberg.LessThanEqual(iceberg.Reference("id"), int32(IntMinValue))),
				true, "should read: and(true, true)"},
			{iceberg.NewOr(
				iceberg.LessThan(iceberg.Reference("id"), int32(IntMinValue-25)),
				iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(IntMaxValue+1))),
				false, "should skip: or(false, false)"},
			{iceberg.NewOr(
				iceberg.LessThan(iceberg.Reference("id"), int32(IntMinValue-25)),
				iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(IntMaxValue-19))),
				true, "should read: or(false, true)"},
			{iceberg.LessThan(iceberg.Reference("some_nulls"), "1"), false,
				"should not read: id range below lower bound"},
			{iceberg.LessThan(iceberg.Reference("some_nulls"), "b"), true,
				"should read: lower bound in range"},
			{iceberg.LessThan(iceberg.Reference("float"), 15.50), true,
				"should read: lower bound in range"},
			{iceberg.LessThan(iceberg.Reference("no_nan_or_null"), 15.50), true,
				"should read: lower bound in range"},
			{iceberg.LessThanEqual(iceberg.Reference("no_nulls_same_value_a"), "a"), true,
				"should read: lower bound in range"},
			{iceberg.LessThan(iceberg.Reference("id"), int32(IntMinValue-25)), false,
				"should not read: id range below lower bound (5 < 30)"},
			{iceberg.LessThan(iceberg.Reference("id"), int32(IntMinValue)), false,
				"should not read: id range below lower bound (30 is not < 30)"},
			{iceberg.LessThan(iceberg.Reference("id"), int32(IntMinValue+1)), true,
				"should read: one possible id"},
			{iceberg.LessThan(iceberg.Reference("id"), int32(IntMaxValue)), true,
				"should read: many possible ids"},
			{iceberg.LessThanEqual(iceberg.Reference("id"), int32(IntMinValue-25)), false,
				"should not read: id range below lower bound (5 < 30)"},
			{iceberg.LessThanEqual(iceberg.Reference("id"), int32(IntMinValue-1)), false,
				"should not read: id range below lower bound 29 < 30"},
			{iceberg.LessThanEqual(iceberg.Reference("id"), int32(IntMinValue)), true,
				"should read: one possible id"},
			{iceberg.LessThanEqual(iceberg.Reference("id"), int32(IntMaxValue)), true,
				"should read: many possible ids"},
			{iceberg.GreaterThan(iceberg.Reference("id"), int32(IntMaxValue+6)), false,
				"should not read: id range above upper bound (85 < 79)"},
			{iceberg.GreaterThan(iceberg.Reference("id"), int32(IntMaxValue)), false,
				"should not read: id range above upper bound (79 is not > 79)"},
			{iceberg.GreaterThan(iceberg.Reference("id"), int32(IntMaxValue-1)), true,
				"should read: one possible id"},
			{iceberg.GreaterThan(iceberg.Reference("id"), int32(IntMaxValue-4)), true,
				"should read: many possible ids"},
			{iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(IntMaxValue+6)), false,
				"should not read: id range is above upper bound (85 < 79)"},
			{iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(IntMaxValue+1)), false,
				"should not read: id range above upper bound (80 > 79)"},
			{iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(IntMaxValue)), true,
				"should read: one possible id"},
			{iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(IntMaxValue)), true,
				"should read: many possible ids"},
			{iceberg.EqualTo(iceberg.Reference("id"), int32(IntMinValue-25)), false,
				"should not read: id below lower bound"},
			{iceberg.EqualTo(iceberg.Reference("id"), int32(IntMinValue-1)), false,
				"should not read: id below lower bound"},
			{iceberg.EqualTo(iceberg.Reference("id"), int32(IntMinValue)), true,
				"should read: id equal to lower bound"},
			{iceberg.EqualTo(iceberg.Reference("id"), int32(IntMaxValue-4)), true,
				"should read: id between lower and upper bounds"},
			{iceberg.EqualTo(iceberg.Reference("id"), int32(IntMaxValue)), true,
				"should read: id equal to upper bound"},
			{iceberg.EqualTo(iceberg.Reference("id"), int32(IntMaxValue+1)), false,
				"should not read: id above upper bound"},
			{iceberg.EqualTo(iceberg.Reference("id"), int32(IntMaxValue+6)), false,
				"should not read: id above upper bound"},
			{iceberg.NotEqualTo(iceberg.Reference("id"), int32(IntMinValue-25)), true,
				"should read: id below lower bound"},
			{iceberg.NotEqualTo(iceberg.Reference("id"), int32(IntMinValue-1)), true,
				"should read: id below lower bound"},
			{iceberg.NotEqualTo(iceberg.Reference("id"), int32(IntMinValue)), true,
				"should read: id equal to lower bound"},
			{iceberg.NotEqualTo(iceberg.Reference("id"), int32(IntMaxValue-4)), true,
				"should read: id between lower and upper bounds"},
			{iceberg.NotEqualTo(iceberg.Reference("id"), int32(IntMaxValue)), true,
				"should read: id equal to upper bound"},
			{iceberg.NotEqualTo(iceberg.Reference("id"), int32(IntMaxValue+1)), true,
				"should read: id above upper bound"},
			{iceberg.NotEqualTo(iceberg.Reference("id"), int32(IntMaxValue+6)), true,
				"should read: id above upper bound"},
			{iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("id"), int32(IntMinValue-25))), true,
				"should read: id below lower bound"},
			{iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("id"), int32(IntMinValue-1))), true,
				"should read: id below lower bound"},
			{iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("id"), int32(IntMinValue))), true,
				"should read: id equal to lower bound"},
			{iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("id"), int32(IntMaxValue-4))), true,
				"should read: id between lower and upper bounds"},
			{iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("id"), int32(IntMaxValue))), true,
				"should read: id equal to upper bound"},
			{iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("id"), int32(IntMaxValue+1))), true,
				"should read: id above upper bound"},
			{iceberg.NewNot(iceberg.EqualTo(iceberg.Reference("id"), int32(IntMaxValue+6))), true,
				"should read: id above upper bound"},
			{iceberg.IsIn(iceberg.Reference("id"), int32(IntMinValue-25), IntMinValue-24), false,
				"should not read: id below lower bound (5 < 30, 6 < 30)"},
			{iceberg.IsIn(iceberg.Reference("id"), int32(IntMinValue-2), IntMinValue-1), false,
				"should not read: id below lower bound (28 < 30, 29 < 30)"},
			{iceberg.IsIn(iceberg.Reference("id"), int32(IntMinValue-1), IntMinValue), true,
				"should read: id equal to lower bound (30 == 30)"},
			{iceberg.IsIn(iceberg.Reference("id"), int32(IntMaxValue-4), IntMaxValue-3), true,
				"should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)"},
			{iceberg.IsIn(iceberg.Reference("id"), int32(IntMaxValue), IntMaxValue+1), true,
				"should read: id equal to upper bound (79 == 79)"},
			{iceberg.IsIn(iceberg.Reference("id"), int32(IntMaxValue+1), IntMaxValue+2), false,
				"should not read: id above upper bound (80 > 79, 81 > 79)"},
			{iceberg.IsIn(iceberg.Reference("id"), int32(IntMaxValue+6), IntMaxValue+7), false,
				"should not read: id above upper bound (85 > 79, 86 > 79)"},
			{iceberg.IsIn(iceberg.Reference("all_nulls_missing_nan"), "abc", "def"), false,
				"should skip: in on all nulls column"},
			{iceberg.IsIn(iceberg.Reference("some_nulls"), "abc", "def"), true,
				"should read: in on some nulls column"},
			{iceberg.IsIn(iceberg.Reference("no_nulls"), "abc", "def"), true,
				"should read: in on no nulls column"},
			{iceberg.IsIn(iceberg.Reference("no_nulls_same_value_a"), "a", "b"), true,
				"should read: in on no nulls column"},
			{iceberg.IsIn(iceberg.Reference("float"), 0, -5.5), true,
				"should read: float equal to lower bound"},
			{iceberg.IsIn(iceberg.Reference("no_nan_or_null"), 0, -5.5), true,
				"should read: float equal to lower bound"},
			{iceberg.NotIn(iceberg.Reference("id"), int32(IntMinValue-25), IntMinValue-24), true,
				"should read: id below lower bound (5 < 30, 6 < 30)"},
			{iceberg.NotIn(iceberg.Reference("id"), int32(IntMinValue-2), IntMinValue-1), true,
				"should read: id below lower bound (28 < 30, 29 < 30)"},
			{iceberg.NotIn(iceberg.Reference("id"), int32(IntMinValue-1), IntMinValue), true,
				"should read: id equal to lower bound (30 == 30)"},
			{iceberg.NotIn(iceberg.Reference("id"), int32(IntMaxValue-4), IntMaxValue-3), true,
				"should read: id between lower and upper bounds (30 < 75 < 79, 30 < 76 < 79)"},
			{iceberg.NotIn(iceberg.Reference("id"), int32(IntMaxValue), IntMaxValue+1), true,
				"should read: id equal to upper bound (79 == 79)"},
			{iceberg.NotIn(iceberg.Reference("id"), int32(IntMaxValue+1), IntMaxValue+2), true,
				"should read: id above upper bound (80 > 79, 81 > 79)"},
			{iceberg.NotIn(iceberg.Reference("id"), int32(IntMaxValue+6), IntMaxValue+7), true,
				"should read: id above upper bound (85 > 79, 86 > 79)"},
			{iceberg.NotIn(iceberg.Reference("all_nulls_missing_nan"), "abc", "def"), true,
				"should read: notIn on all nulls column"},
			{iceberg.NotIn(iceberg.Reference("some_nulls"), "abc", "def"), true,
				"should read: notIn on some nulls column"},
			{iceberg.NotIn(iceberg.Reference("no_nulls"), "abc", "def"), true,
				"should read: notIn on no nulls column"},
			{iceberg.StartsWith(iceberg.Reference("some_nulls"), "a"), true,
				"should read: range matches"},
			{iceberg.StartsWith(iceberg.Reference("some_nulls"), "aa"), true,
				"should read: range matches"},
			{iceberg.StartsWith(iceberg.Reference("some_nulls"), "dddd"), true,
				"should read: range matches"},
			{iceberg.StartsWith(iceberg.Reference("some_nulls"), "z"), true,
				"should read: range matches"},
			{iceberg.StartsWith(iceberg.Reference("no_nulls"), "a"), true,
				"should read: range matches"},
			{iceberg.StartsWith(iceberg.Reference("some_nulls"), "zzzz"), false,
				"should skip: range doesn't match"},
			{iceberg.StartsWith(iceberg.Reference("some_nulls"), "1"), false,
				"should skip: range doesn't match"},
			{iceberg.StartsWith(iceberg.Reference("no_nulls_same_value_a"), "a"), true,
				"should read: all values start with the prefix"},
			{iceberg.NotStartsWith(iceberg.Reference("some_nulls"), "a"), true,
				"should read: range matches"},
			{iceberg.NotStartsWith(iceberg.Reference("some_nulls"), "aa"), true,
				"should read: range matches"},
			{iceberg.NotStartsWith(iceberg.Reference("some_nulls"), "dddd"), true,
				"should read: range matches"},
			{iceberg.NotStartsWith(iceberg.Reference("some_nulls"), "z"), true,
				"should read: range matches"},
			{iceberg.NotStartsWith(iceberg.Reference("no_nulls"), "a"), true,
				"should read: range matches"},
			{iceberg.NotStartsWith(iceberg.Reference("some_nulls"), "zzzz"), true,
				"should read: range matches"},
			{iceberg.NotStartsWith(iceberg.Reference("some_nulls"), "1"), true,
				"should read: range matches"},
			{iceberg.NotStartsWith(iceberg.Reference("all_same_value_or_null"), "a"), true,
				"should read: range matches"},
			{iceberg.NotStartsWith(iceberg.Reference("all_same_value_or_null"), "aa"), true,
				"should read: range matches"},
			{iceberg.NotStartsWith(iceberg.Reference("all_same_value_or_null"), "A"), true,
				"should read: range matches"},
			// Iceberg does not implement SQL 3-way boolean logic, so the choice of an
			// all null column matching is by definition in order to surface more values
			// to the query engine to allow it to make its own decision
			{iceberg.NotStartsWith(iceberg.Reference("all_nulls_missing_nan"), "A"), true,
				"should read: range matches"},
			{iceberg.NotStartsWith(iceberg.Reference("no_nulls_same_value_a"), "a"), false,
				"should not read: all values start with the prefix"},
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
