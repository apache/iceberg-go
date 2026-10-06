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

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManifestEvaluatorInPredicateExtrema(t *testing.T) {
	decimal := func(value int64) iceberg.Decimal {
		return iceberg.Decimal{Val: decimal128.FromI64(value), Scale: 2}
	}

	type testCase struct {
		name       string
		typ        iceberg.Type
		expr       iceberg.BooleanExpression
		lower      iceberg.Literal
		upper      iceberg.Literal
		expectRead bool
	}

	tests := []testCase{
		{
			name:       "decimal below lower bound",
			typ:        iceberg.DecimalTypeOf(12, 2),
			expr:       iceberg.IsIn(iceberg.Reference("value"), decimal(100), decimal(200), decimal(300)),
			lower:      iceberg.NewLiteral(decimal(400)),
			upper:      iceberg.NewLiteral(decimal(500)),
			expectRead: false,
		},
		{
			name:       "decimal above upper bound",
			typ:        iceberg.DecimalTypeOf(12, 2),
			expr:       iceberg.IsIn(iceberg.Reference("value"), decimal(100), decimal(200), decimal(300)),
			lower:      iceberg.NewLiteral(decimal(-100)),
			upper:      iceberg.NewLiteral(decimal(0)),
			expectRead: false,
		},
		{
			name:       "decimal overlaps bound",
			typ:        iceberg.DecimalTypeOf(12, 2),
			expr:       iceberg.IsIn(iceberg.Reference("value"), decimal(100), decimal(200), decimal(300)),
			lower:      iceberg.NewLiteral(decimal(200)),
			upper:      iceberg.NewLiteral(decimal(250)),
			expectRead: true,
		},
		{
			name:       "timestamp nanos below lower bound",
			typ:        iceberg.PrimitiveTypes.TimestampNs,
			expr:       iceberg.IsIn(iceberg.Reference("value"), iceberg.TimestampNano(100), iceberg.TimestampNano(200), iceberg.TimestampNano(300)),
			lower:      iceberg.NewLiteral(iceberg.TimestampNano(400)),
			upper:      iceberg.NewLiteral(iceberg.TimestampNano(500)),
			expectRead: false,
		},
		{
			name:       "timestamp nanos above upper bound",
			typ:        iceberg.PrimitiveTypes.TimestampNs,
			expr:       iceberg.IsIn(iceberg.Reference("value"), iceberg.TimestampNano(100), iceberg.TimestampNano(200), iceberg.TimestampNano(300)),
			lower:      iceberg.NewLiteral(iceberg.TimestampNano(-100)),
			upper:      iceberg.NewLiteral(iceberg.TimestampNano(99)),
			expectRead: false,
		},
		{
			name:       "timestamp nanos overlaps bound",
			typ:        iceberg.PrimitiveTypes.TimestampNs,
			expr:       iceberg.IsIn(iceberg.Reference("value"), iceberg.TimestampNano(100), iceberg.TimestampNano(200), iceberg.TimestampNano(300)),
			lower:      iceberg.NewLiteral(iceberg.TimestampNano(200)),
			upper:      iceberg.NewLiteral(iceberg.TimestampNano(250)),
			expectRead: true,
		},
	}

	largeValues := make([]int32, inPredicateLimit+1)
	for i := range largeValues {
		largeValues[i] = int32(i)
	}
	largeIn := iceberg.IsIn(iceberg.Reference("value"), largeValues...)
	tests = append(tests,
		testCase{
			name:       "large IN below lower bound",
			typ:        iceberg.PrimitiveTypes.Int32,
			expr:       largeIn,
			lower:      iceberg.NewLiteral(int32(inPredicateLimit + 1)),
			upper:      iceberg.NewLiteral(int32(inPredicateLimit + 2)),
			expectRead: false,
		},
		testCase{
			name:       "large IN includes lower bound",
			typ:        iceberg.PrimitiveTypes.Int32,
			expr:       largeIn,
			lower:      iceberg.NewLiteral(int32(inPredicateLimit)),
			upper:      iceberg.NewLiteral(int32(inPredicateLimit)),
			expectRead: true,
		},
	)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "value", Type: tt.typ})
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "value", Transform: iceberg.IdentityTransform{},
			})
			eval, err := newManifestEvaluator(spec, schema, tt.expr, true)
			require.NoError(t, err)

			lower, err := tt.lower.MarshalBinary()
			require.NoError(t, err)
			upper, err := tt.upper.MarshalBinary()
			require.NoError(t, err)
			manifest := iceberg.NewManifestFile(2, "manifest.avro", 1, 0, 1).Partitions(
				[]iceberg.FieldSummary{{LowerBound: &lower, UpperBound: &upper}},
			).Build()

			result, err := eval(manifest)
			require.NoError(t, err)
			assert.Equal(t, tt.expectRead, result)
		})
	}
}

func TestInclusiveMetricsEvaluatorInPredicateExtrema(t *testing.T) {
	encode := func(value int32) []byte {
		encoded, err := iceberg.NewLiteral(value).MarshalBinary()
		require.NoError(t, err)

		return encoded
	}

	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Int32})
	expr := iceberg.IsIn(iceberg.Reference("value"), int32(1), int32(100))
	eval, err := newInclusiveMetricsEvaluator(schema, expr, true, true)
	require.NoError(t, err)

	tests := []struct {
		name       string
		lowerBound []byte
		upperBound []byte
		want       bool
	}{
		{name: "lower bound above set", lowerBound: encode(101), upperBound: encode(200), want: false},
		{name: "upper bound below set", lowerBound: encode(-10), upperBound: encode(0), want: false},
		{name: "sparse set has no member in range", lowerBound: encode(40), upperBound: encode(60), want: false},
		{name: "overlapping range contains member", lowerBound: encode(50), upperBound: encode(100), want: true},
		{name: "upper bound equals minimum literal", upperBound: encode(1), want: true},
		{name: "lower bound equals maximum literal", lowerBound: encode(100), want: true},
		{name: "missing bounds", want: true},
		{name: "upper bound below set without lower bound", upperBound: encode(0), want: false},
		{name: "lower bound above set without upper bound", lowerBound: encode(101), want: false},
		{name: "disjoint lower bound avoids malformed upper bound", lowerBound: encode(101), upperBound: []byte{1}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := inclusiveMetricsInTestFile(t, tt.lowerBound, tt.upperBound)
			got, err := eval(file)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("NaN bounds fail open", func(t *testing.T) {
		encodeFloat := func(value float64) []byte {
			encoded, err := iceberg.NewLiteral(value).MarshalBinary()
			require.NoError(t, err)

			return encoded
		}
		nan := encodeFloat(math.NaN())
		finiteLower := encodeFloat(0)
		finiteUpper := encodeFloat(3)
		floatSchema := iceberg.NewSchema(1, iceberg.NestedField{
			ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Float64,
		})
		floatEval, err := newInclusiveMetricsEvaluator(
			floatSchema, iceberg.IsIn(iceberg.Reference("value"), float64(1), float64(2)), true, true,
		)
		require.NoError(t, err)

		for _, bounds := range []struct {
			name         string
			lower, upper []byte
		}{
			{name: "both NaN", lower: nan, upper: nan},
			{name: "NaN lower finite upper", lower: nan, upper: finiteUpper},
			{name: "finite lower NaN upper", lower: finiteLower, upper: nan},
		} {
			t.Run(bounds.name, func(t *testing.T) {
				got, err := floatEval(inclusiveMetricsInTestFile(t, bounds.lower, bounds.upper))
				require.NoError(t, err)
				assert.True(t, got)
			})
		}
	})

	t.Run("oversized set still uses extrema before member scan limit", func(t *testing.T) {
		values := make([]int32, inPredicateLimit+1)
		for i := range values {
			values[i] = int32(i)
		}
		largeEval, err := newInclusiveMetricsEvaluator(
			schema, iceberg.IsIn(iceberg.Reference("value"), values...), true, true,
		)
		require.NoError(t, err)

		disjointLower := int32(inPredicateLimit + 1000)
		got, err := largeEval(inclusiveMetricsInTestFile(
			t, encode(disjointLower), encode(disjointLower+1),
		))
		require.NoError(t, err)
		assert.False(t, got)

		got, err = largeEval(inclusiveMetricsInTestFile(t, encode(50), encode(60)))
		require.NoError(t, err)
		assert.True(t, got)
	})
}

func TestInclusiveMetricsEvaluatorInPredicateExtremaFastPathTypes(t *testing.T) {
	decimal := func(value int64) iceberg.Decimal {
		return iceberg.Decimal{Val: decimal128.FromI64(value), Scale: 2}
	}

	tests := []struct {
		name           string
		typ            iceberg.Type
		expr           iceberg.BooleanExpression
		minLit, maxLit iceberg.Literal
		lower, upper   iceberg.Literal
		want           bool
	}{
		{
			name:   "string lower disjoint",
			typ:    iceberg.PrimitiveTypes.String,
			expr:   iceberg.IsIn(iceberg.Reference("value"), "a", "b"),
			minLit: iceberg.NewLiteral("a"), maxLit: iceberg.NewLiteral("b"),
			lower: iceberg.NewLiteral("m"), upper: iceberg.NewLiteral("z"),
		},
		{
			name:   "string overlaps",
			typ:    iceberg.PrimitiveTypes.String,
			expr:   iceberg.IsIn(iceberg.Reference("value"), "a", "b"),
			minLit: iceberg.NewLiteral("a"), maxLit: iceberg.NewLiteral("b"),
			lower: iceberg.NewLiteral("a"), upper: iceberg.NewLiteral("a"),
			want: true,
		},
		{
			name:   "decimal upper disjoint",
			typ:    iceberg.DecimalTypeOf(12, 2),
			expr:   iceberg.IsIn(iceberg.Reference("value"), decimal(100), decimal(200)),
			minLit: iceberg.NewLiteral(decimal(100)), maxLit: iceberg.NewLiteral(decimal(200)),
			lower: iceberg.NewLiteral(decimal(-200)), upper: iceberg.NewLiteral(decimal(0)),
		},
		{
			name:   "decimal overlaps",
			typ:    iceberg.DecimalTypeOf(12, 2),
			expr:   iceberg.IsIn(iceberg.Reference("value"), decimal(100), decimal(200)),
			minLit: iceberg.NewLiteral(decimal(100)), maxLit: iceberg.NewLiteral(decimal(200)),
			lower: iceberg.NewLiteral(decimal(100)), upper: iceberg.NewLiteral(decimal(150)),
			want: true,
		},
		{
			name:   "binary lower disjoint",
			typ:    iceberg.PrimitiveTypes.Binary,
			expr:   iceberg.IsIn(iceberg.Reference("value"), []byte{1}, []byte{2}),
			minLit: iceberg.NewLiteral([]byte{1}), maxLit: iceberg.NewLiteral([]byte{2}),
			lower: iceberg.NewLiteral([]byte{10}), upper: iceberg.NewLiteral([]byte{20}),
		},
		{
			name:   "binary overlaps",
			typ:    iceberg.PrimitiveTypes.Binary,
			expr:   iceberg.IsIn(iceberg.Reference("value"), []byte{1}, []byte{2}),
			minLit: iceberg.NewLiteral([]byte{1}), maxLit: iceberg.NewLiteral([]byte{2}),
			lower: iceberg.NewLiteral([]byte{1}), upper: iceberg.NewLiteral([]byte{1}),
			want: true,
		},
		{
			name:   "date upper disjoint",
			typ:    iceberg.PrimitiveTypes.Date,
			expr:   iceberg.IsIn(iceberg.Reference("value"), iceberg.Date(10), iceberg.Date(20)),
			minLit: iceberg.NewLiteral(iceberg.Date(10)), maxLit: iceberg.NewLiteral(iceberg.Date(20)),
			lower: iceberg.NewLiteral(iceberg.Date(-10)), upper: iceberg.NewLiteral(iceberg.Date(0)),
		},
		{
			name:   "date overlaps",
			typ:    iceberg.PrimitiveTypes.Date,
			expr:   iceberg.IsIn(iceberg.Reference("value"), iceberg.Date(10), iceberg.Date(20)),
			minLit: iceberg.NewLiteral(iceberg.Date(10)), maxLit: iceberg.NewLiteral(iceberg.Date(20)),
			lower: iceberg.NewLiteral(iceberg.Date(10)), upper: iceberg.NewLiteral(iceberg.Date(15)),
			want: true,
		},
		{
			name:   "timestamp lower disjoint",
			typ:    iceberg.PrimitiveTypes.Timestamp,
			expr:   iceberg.IsIn(iceberg.Reference("value"), iceberg.Timestamp(10), iceberg.Timestamp(20)),
			minLit: iceberg.NewLiteral(iceberg.Timestamp(10)), maxLit: iceberg.NewLiteral(iceberg.Timestamp(20)),
			lower: iceberg.NewLiteral(iceberg.Timestamp(30)), upper: iceberg.NewLiteral(iceberg.Timestamp(40)),
		},
		{
			name:   "timestamp overlaps",
			typ:    iceberg.PrimitiveTypes.Timestamp,
			expr:   iceberg.IsIn(iceberg.Reference("value"), iceberg.Timestamp(10), iceberg.Timestamp(20)),
			minLit: iceberg.NewLiteral(iceberg.Timestamp(10)), maxLit: iceberg.NewLiteral(iceberg.Timestamp(20)),
			lower: iceberg.NewLiteral(iceberg.Timestamp(10)), upper: iceberg.NewLiteral(iceberg.Timestamp(15)),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "value", Type: tt.typ})
			bound, err := iceberg.BindExpr(schema, tt.expr, true)
			require.NoError(t, err)
			pred, ok := bound.(iceberg.BoundSetPredicate)
			require.True(t, ok)

			lower, err := tt.lower.MarshalBinary()
			require.NoError(t, err)
			upper, err := tt.upper.MarshalBinary()
			require.NoError(t, err)
			newVisitor := func() *inclusiveMetricsEval {
				return &inclusiveMetricsEval{metricsEvaluator: metricsEvaluator{
					valueCounts: map[int]int64{1: 10},
					nullCounts:  map[int]int64{1: 0},
					nanCounts:   map[int]int64{1: 0},
					lowerBounds: map[int][]byte{1: lower},
					upperBounds: map[int][]byte{1: upper},
				}}
			}

			slow := newVisitor().VisitIn(pred.Term(), pred.Literals())
			fast := newVisitor().VisitInWithExtrema(pred.Term(), pred.Literals(), tt.minLit, tt.maxLit)
			require.Equal(t, slow, fast)
			require.Equal(t, tt.want, fast)
		})
	}
}

func TestInclusiveMetricsEvaluatorInPredicateExtremaPartialNilUsesSlowPath(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Int32})
	bound, err := iceberg.BindExpr(
		schema, iceberg.IsIn(iceberg.Reference("value"), int32(1), int32(100)), true,
	)
	require.NoError(t, err)
	pred := bound.(iceberg.BoundSetPredicate)
	lower, err := iceberg.NewLiteral(int32(40)).MarshalBinary()
	require.NoError(t, err)
	upper, err := iceberg.NewLiteral(int32(60)).MarshalBinary()
	require.NoError(t, err)

	newVisitor := func() *inclusiveMetricsEval {
		return &inclusiveMetricsEval{metricsEvaluator: metricsEvaluator{
			valueCounts: map[int]int64{1: 10},
			nullCounts:  map[int]int64{1: 0},
			nanCounts:   map[int]int64{1: 0},
			lowerBounds: map[int][]byte{1: lower},
			upperBounds: map[int][]byte{1: upper},
		}}
	}
	slow := newVisitor().VisitIn(pred.Term(), pred.Literals())
	minLit := iceberg.NewLiteral(int32(1))
	maxLit := iceberg.NewLiteral(int32(100))
	require.Equal(t, slow, newVisitor().VisitInWithExtrema(pred.Term(), pred.Literals(), minLit, nil))
	require.Equal(t, slow, newVisitor().VisitInWithExtrema(pred.Term(), pred.Literals(), nil, maxLit))
}

func TestInclusiveMetricsEvaluatorInPredicateExtremaVariantExtract(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "payload", Type: iceberg.VariantType{},
	})
	expr := iceberg.IsIn(
		iceberg.Extract("payload", "$.a", iceberg.PrimitiveTypes.Int64),
		int64(1), int64(2),
	)
	bound, err := iceberg.BindExpr(schema, expr, true)
	require.NoError(t, err)
	pred, ok := bound.(iceberg.BoundSetPredicate)
	require.True(t, ok)

	newVisitor := func() *inclusiveMetricsEval {
		return &inclusiveMetricsEval{metricsEvaluator: metricsEvaluator{
			valueCounts: map[int]int64{1: 10},
			nullCounts:  map[int]int64{1: 0},
			nanCounts:   map[int]int64{1: 0},
			lowerBounds: map[int][]byte{1: variantMetricBoundInt64(t, "$['a']", 10)},
			upperBounds: map[int][]byte{1: variantMetricBoundInt64(t, "$['a']", 20)},
		}}
	}

	slow := newVisitor().VisitIn(pred.Term(), pred.Literals())
	fast := newVisitor().VisitInWithExtrema(
		pred.Term(), pred.Literals(), iceberg.NewLiteral(int64(1)), iceberg.NewLiteral(int64(2)),
	)
	require.False(t, slow)
	require.Equal(t, slow, fast)
}

func variantMetricBoundInt64(t *testing.T, path string, value int64) []byte {
	t.Helper()

	var builder variant.Builder
	start := builder.Offset()
	entries := []variant.FieldEntry{builder.NextField(start, path)}
	require.NoError(t, builder.AppendInt(value))
	require.NoError(t, builder.FinishObject(start, entries))
	v, err := builder.Build()
	require.NoError(t, err)

	return append(append([]byte{}, v.Metadata().Bytes()...), v.Bytes()...)
}

func inclusiveMetricsInTestFile(t *testing.T, lowerBound, upperBound []byte) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData, "file.parquet", iceberg.ParquetFile,
		nil, nil, nil, 10, 100,
	)
	require.NoError(t, err)

	lowerBounds := map[int][]byte{}
	if lowerBound != nil {
		lowerBounds[1] = lowerBound
	}
	upperBounds := map[int][]byte{}
	if upperBound != nil {
		upperBounds[1] = upperBound
	}

	return builder.
		ValueCounts(map[int]int64{1: 10}).
		NullValueCounts(map[int]int64{1: 0}).
		NaNValueCounts(map[int]int64{1: 0}).
		LowerBoundValues(lowerBounds).
		UpperBoundValues(upperBounds).
		Build()
}

func TestRemoveBoundCheckSupportsTimestampNanos(t *testing.T) {
	bound := iceberg.NewLiteral(iceberg.TimestampNano(2))
	values := []iceberg.Literal{
		iceberg.NewLiteral(iceberg.TimestampNano(1)),
		iceberg.NewLiteral(iceberg.TimestampNano(2)),
		iceberg.NewLiteral(iceberg.TimestampNano(3)),
	}

	assert.Equal(t, []iceberg.Literal{
		iceberg.NewLiteral(iceberg.TimestampNano(2)),
		iceberg.NewLiteral(iceberg.TimestampNano(3)),
	}, removeBoundCheck(bound, values, 1))
}
