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

package iceberg_test

import (
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMarshalExpressionJSON checks that expressions serialize to the exact JSON
// that Java's ExpressionParser produces.
func TestMarshalExpressionJSON(t *testing.T) {
	tests := []struct {
		name string
		expr iceberg.BooleanExpression
		want string
	}{
		{
			name: "always true",
			expr: iceberg.AlwaysTrue{},
			want: `true`,
		},
		{
			name: "always false",
			expr: iceberg.AlwaysFalse{},
			want: `false`,
		},
		{
			name: "is null",
			expr: iceberg.IsNull(iceberg.Reference("a")),
			want: `{"type":"is-null","term":"a"}`,
		},
		{
			name: "not null",
			expr: iceberg.NotNull(iceberg.Reference("a")),
			want: `{"type":"not-null","term":"a"}`,
		},
		{
			name: "is nan",
			expr: iceberg.IsNaN(iceberg.Reference("f")),
			want: `{"type":"is-nan","term":"f"}`,
		},
		{
			name: "equal int",
			expr: iceberg.EqualTo(iceberg.Reference("name"), int32(25)),
			want: `{"type":"eq","term":"name","value":25}`,
		},
		{
			name: "less than or equal long",
			expr: iceberg.LessThanEqual(iceberg.Reference("c"), int64(50)),
			want: `{"type":"lt-eq","term":"c","value":50}`,
		},
		{
			name: "greater than double",
			expr: iceberg.GreaterThan(iceberg.Reference("d"), float64(3.5)),
			want: `{"type":"gt","term":"d","value":3.5}`,
		},
		{
			name: "not equal string",
			expr: iceberg.NotEqualTo(iceberg.Reference("s"), "abc"),
			want: `{"type":"not-eq","term":"s","value":"abc"}`,
		},
		{
			name: "starts with",
			expr: iceberg.StartsWith(iceberg.Reference("s"), "ab"),
			want: `{"type":"starts-with","term":"s","value":"ab"}`,
		},
		{
			name: "not starts with",
			expr: iceberg.NotStartsWith(iceberg.Reference("s"), "ab"),
			want: `{"type":"not-starts-with","term":"s","value":"ab"}`,
		},
		{
			name: "equal bool",
			expr: iceberg.EqualTo(iceberg.Reference("b"), true),
			want: `{"type":"eq","term":"b","value":true}`,
		},
		{
			name: "in ints (sorted)",
			expr: iceberg.IsIn(iceberg.Reference("c"), int32(52), int32(50), int32(51)),
			want: `{"type":"in","term":"c","values":[50,51,52]}`,
		},
		{
			name: "not in strings (sorted)",
			expr: iceberg.NotIn(iceberg.Reference("c"), "two", "one"),
			want: `{"type":"not-in","term":"c","values":["one","two"]}`,
		},
		{
			name: "not",
			expr: iceberg.NewNot(iceberg.GreaterThanEqual(iceberg.Reference("c"), int32(50))),
			want: `{"type":"not","child":{"type":"gt-eq","term":"c","value":50}}`,
		},
		{
			name: "and",
			expr: iceberg.NewAnd(
				iceberg.GreaterThanEqual(iceberg.Reference("c1"), int32(50)),
				iceberg.IsIn(iceberg.Reference("c2"), "one", "two"),
			),
			want: `{"type":"and","left":{"type":"gt-eq","term":"c1","value":50},"right":{"type":"in","term":"c2","values":["one","two"]}}`,
		},
		{
			name: "or with nested and",
			expr: iceberg.NewOr(
				iceberg.NewAnd(
					iceberg.IsIn(iceberg.Reference("c1"), int32(50)),
					iceberg.EqualTo(iceberg.Reference("c2"), "test"),
				),
				iceberg.IsNaN(iceberg.Reference("c3")),
			),
			// c1 IN (50) folds to c1 == 50
			want: `{"type":"or","left":{"type":"and","left":{"type":"eq","term":"c1","value":50},"right":{"type":"eq","term":"c2","value":"test"}},"right":{"type":"is-nan","term":"c3"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.expr)
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, string(got))
			// JSONEq is order-insensitive; also assert exact bytes to lock field order.
			assert.Equal(t, tt.want, string(got))
		})
	}
}

// TestMarshalExpressionTypedLiterals checks the wire encoding of the trickier
// literal types against SingleValueParser's format.
func TestMarshalExpressionTypedLiterals(t *testing.T) {
	tests := []struct {
		name string
		lit  any
		want string
	}{
		{"date", mustLit(t, "2022-08-14", iceberg.PrimitiveTypes.Date), `{"type":"eq","term":"d","value":"2022-08-14"}`},
		{"timestamp (no fraction)", mustLit(t, "2022-08-14T10:00:00", iceberg.PrimitiveTypes.Timestamp), `{"type":"eq","term":"d","value":"2022-08-14T10:00:00"}`},
		{"timestamp (micros)", mustLit(t, "2022-08-14T10:00:00.123456", iceberg.PrimitiveTypes.Timestamp), `{"type":"eq","term":"d","value":"2022-08-14T10:00:00.123456"}`},
		{"timestamp (trailing zeros trimmed)", mustLit(t, "2022-08-14T10:00:00.500000", iceberg.PrimitiveTypes.Timestamp), `{"type":"eq","term":"d","value":"2022-08-14T10:00:00.5"}`},
		{"uuid", iceberg.UUIDLiteral(uuid.MustParse("f79c3e09-677c-4bbd-a479-3f349cb785e7")), `{"type":"eq","term":"d","value":"f79c3e09-677c-4bbd-a479-3f349cb785e7"}`},
		{"binary", iceberg.BinaryLiteral([]byte{0x01, 0x02, 0x03}), `{"type":"eq","term":"d","value":"010203"}`},
		{"decimal", mustLit(t, "3.14", iceberg.DecimalTypeOf(9, 2)), `{"type":"eq","term":"d","value":"3.14"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lit, ok := tt.lit.(iceberg.Literal)
			require.True(t, ok)
			expr := iceberg.LiteralPredicate(iceberg.OpEQ, iceberg.Reference("d"), lit)
			got, err := json.Marshal(expr)
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

func mustLit(t *testing.T, s string, typ iceberg.Type) iceberg.Literal {
	t.Helper()
	lit, err := iceberg.NewLiteral(s).To(typ)
	require.NoError(t, err)

	return lit
}

// TestExpressionRoundTripSchemaless verifies that parsing without a schema
// normalizes literals to their base JSON kind, as the reference does.
func TestExpressionRoundTripSchemaless(t *testing.T) {
	tests := []struct {
		name string
		json string
		want iceberg.BooleanExpression
	}{
		{"true", `true`, iceberg.AlwaysTrue{}},
		{"false", `false`, iceberg.AlwaysFalse{}},
		{"literal true", `{"type":"literal","value":true}`, iceberg.AlwaysTrue{}},
		{"is null", `{"type":"is-null","term":"a"}`, iceberg.IsNull(iceberg.Reference("a"))},
		{"eq long", `{"type":"eq","term":"name","value":25}`, iceberg.EqualTo(iceberg.Reference("name"), int64(25))},
		{"eq double", `{"type":"eq","term":"d","value":3.5}`, iceberg.EqualTo(iceberg.Reference("d"), float64(3.5))},
		{"eq string", `{"type":"not-eq","term":"s","value":"abc"}`, iceberg.NotEqualTo(iceberg.Reference("s"), "abc")},
		{"eq bool", `{"type":"eq","term":"b","value":true}`, iceberg.EqualTo(iceberg.Reference("b"), true)},
		{"in longs", `{"type":"in","term":"c","values":[50,51]}`, iceberg.IsIn(iceberg.Reference("c"), int64(50), int64(51))},
		{
			"and",
			`{"type":"and","left":{"type":"gt-eq","term":"c1","value":50},"right":{"type":"is-nan","term":"c2"}}`,
			iceberg.NewAnd(
				iceberg.GreaterThanEqual(iceberg.Reference("c1"), int64(50)),
				iceberg.IsNaN(iceberg.Reference("c2")),
			),
		},
		{
			"reference object term",
			`{"type":"eq","term":{"type":"reference","term":"name"},"value":25}`,
			iceberg.EqualTo(iceberg.Reference("name"), int64(25)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := iceberg.ParseExpr([]byte(tt.json), nil)
			require.NoError(t, err)
			assert.Truef(t, tt.want.Equals(got), "want %s, got %s", tt.want, got)
		})
	}
}

// TestExpressionRoundTripWithSchema verifies that with a schema, literals are
// parsed into the referenced field's type, giving an identity round-trip.
func TestExpressionRoundTripWithSchema(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "i", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "l", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 3, Name: "s", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 4, Name: "b", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 5, Name: "f", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 6, Name: "d", Type: iceberg.PrimitiveTypes.Date},
		iceberg.NestedField{ID: 7, Name: "u", Type: iceberg.PrimitiveTypes.UUID},
		iceberg.NestedField{ID: 8, Name: "bin", Type: iceberg.PrimitiveTypes.Binary},
	)

	exprs := []iceberg.BooleanExpression{
		iceberg.EqualTo(iceberg.Reference("i"), int32(25)),
		iceberg.LessThan(iceberg.Reference("l"), int64(100)),
		iceberg.NotEqualTo(iceberg.Reference("s"), "abc"),
		iceberg.EqualTo(iceberg.Reference("b"), true),
		iceberg.GreaterThanEqual(iceberg.Reference("f"), float64(1.5)),
		iceberg.LiteralPredicate(iceberg.OpEQ, iceberg.Reference("d"), mustLit(t, "2022-08-14", iceberg.PrimitiveTypes.Date)),
		iceberg.LiteralPredicate(iceberg.OpEQ, iceberg.Reference("u"), iceberg.UUIDLiteral(uuid.MustParse("f79c3e09-677c-4bbd-a479-3f349cb785e7"))),
		iceberg.LiteralPredicate(iceberg.OpEQ, iceberg.Reference("bin"), iceberg.BinaryLiteral([]byte{0x01, 0x02, 0x03})),
		iceberg.IsIn(iceberg.Reference("i"), int32(1), int32(2), int32(3)),
		iceberg.NewAnd(
			iceberg.EqualTo(iceberg.Reference("i"), int32(1)),
			iceberg.NewNot(iceberg.IsNull(iceberg.Reference("s"))),
		),
	}

	for _, want := range exprs {
		t.Run(want.String(), func(t *testing.T) {
			data, err := json.Marshal(want)
			require.NoError(t, err)

			got, err := iceberg.ParseExpr(data, schema)
			require.NoError(t, err)
			assert.Truef(t, want.Equals(got), "want %s, got %s", want, got)
		})
	}
}

func TestUnmarshalExpressionErrors(t *testing.T) {
	tests := []struct {
		name string
		json string
	}{
		{"empty", ``},
		{"unknown type", `{"type":"bogus","term":"a"}`},
		{"unary with value", `{"type":"is-null","term":"a","value":1}`},
		{"literal missing value", `{"type":"eq","term":"a"}`},
		{"in missing values", `{"type":"in","term":"a"}`},
		{"missing term", `{"type":"eq","value":1}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := iceberg.ParseExpr([]byte(tt.json), nil)
			require.Error(t, err)
		})
	}
}

func TestUnmarshalExpressionTransformTermUnsupported(t *testing.T) {
	_, err := iceberg.ParseExpr(
		[]byte(`{"type":"eq","term":{"type":"transform","transform":"bucket[16]","term":"id"},"value":1}`), nil)
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)
}
