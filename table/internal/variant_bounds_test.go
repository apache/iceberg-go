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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
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
	assert.Equal(t, "payload.typed_value.a.value", got["$['a']"].valuePath)
	assert.Equal(t, iceberg.PrimitiveTypes.Int64, got["$['a']"].icebergType)
	assert.Equal(t, iceberg.PrimitiveTypes.Int32, got["$['n']"].icebergType)
	assert.Equal(t, "payload.typed_value.location.typed_value.latitude.typed_value", got["$['location']['latitude']"].typedPath)
	assert.Equal(t, iceberg.PrimitiveTypes.Float64, got["$['location']['latitude']"].icebergType)
}

func TestEnumerateVariantLeavesRootScalar(t *testing.T) {
	vt := extensions.NewShreddedVariantType(arrow.PrimitiveTypes.Int64)
	leaves := enumerateVariantLeaves([]string{"payload"}, vt.TypedValue())
	require.Len(t, leaves, 1)
	assert.Equal(t, "$", leaves[0].jsonPath)
	assert.Equal(t, "payload.typed_value", leaves[0].typedPath)
	assert.Equal(t, "payload.value", leaves[0].valuePath)
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
