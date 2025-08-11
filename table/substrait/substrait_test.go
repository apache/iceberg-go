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

package substrait_test

import (
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table/substrait"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/substrait-io/substrait-go/v4/types"
)

func TestRefTypes(t *testing.T) {
	sc := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "a", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 2, Name: "b", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 3, Name: "c", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 4, Name: "d", Type: iceberg.PrimitiveTypes.Float32},
		iceberg.NestedField{ID: 5, Name: "e", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 6, Name: "f", Type: iceberg.PrimitiveTypes.Date},
		iceberg.NestedField{ID: 7, Name: "g", Type: iceberg.PrimitiveTypes.Time},
		iceberg.NestedField{ID: 8, Name: "h", Type: iceberg.PrimitiveTypes.Timestamp},
		iceberg.NestedField{ID: 9, Name: "i", Type: iceberg.DecimalTypeOf(9, 2)},
		iceberg.NestedField{ID: 10, Name: "j", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 11, Name: "k", Type: iceberg.PrimitiveTypes.Binary},
		iceberg.NestedField{ID: 12, Name: "l", Type: iceberg.PrimitiveTypes.UUID},
		iceberg.NestedField{ID: 13, Name: "m", Type: iceberg.FixedTypeOf(5)})

	tests := []struct {
		name string
		exp  types.Type
	}{
		{"a", &types.BooleanType{}},
		{"b", &types.Int32Type{}},
		{"c", &types.Int64Type{}},
		{"d", &types.Float32Type{}},
		{"e", &types.Float64Type{}},
		{"f", &types.DateType{}},
		{"g", &types.TimeType{}},
		{"h", &types.TimestampType{}},
		{"i", &types.DecimalType{Scale: 2, Precision: 9}},
		{"j", &types.StringType{}},
		{"k", &types.BinaryType{}},
		{"l", &types.UUIDType{}},
		{"m", &types.FixedBinaryType{Length: 5}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref, err := iceberg.IsNull(iceberg.Reference(tt.name)).Bind(sc, true)
			require.NoError(t, err)

			idx := ref.(iceberg.BoundPredicate).Term().Ref().Pos()

			_, converted, err := substrait.ConvertExpr(sc, ref, true)
			require.NoError(t, err)

			assert.Equal(t, fmt.Sprintf("is_null(.field(%d) => %s) => boolean", idx,
				tt.exp.WithNullability(types.NullabilityNullable)), converted.String())
		})
	}
}

var (
	tableSchemaSimple = iceberg.NewSchemaWithIdentifiers(1,
		[]int{2},
		iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool},
	)

	doubleSchema = iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.Float64})
)

func TestExprs(t *testing.T) {
	ref := iceberg.Reference("foo")

	tests := []struct {
		e        iceberg.BooleanExpression
		schema   *iceberg.Schema
		expected string
	}{
		{iceberg.IsNull(ref), tableSchemaSimple, "is_null(.field(0) => string?) => boolean"},
		{iceberg.NotNull(ref), tableSchemaSimple, "is_not_null(.field(0) => string?) => boolean"},
		{iceberg.IsNaN(ref), doubleSchema, "is_nan(.field(0) => fp64?) => boolean?"},
		{iceberg.NotNaN(ref), doubleSchema, "not(is_nan(.field(0) => fp64?) => boolean?) => boolean?"},
		{iceberg.EqualTo(ref, "hello"), tableSchemaSimple, "equal(.field(0) => string?, string(hello)) => boolean?"},
		{iceberg.NotEqualTo(ref, "hello"), tableSchemaSimple, "not_equal(.field(0) => string?, string(hello)) => boolean?"},
		{iceberg.GreaterThanEqual(ref, "hello"), tableSchemaSimple, "gte(.field(0) => string?, string(hello)) => boolean?"},
		{iceberg.GreaterThan(ref, "hello"), tableSchemaSimple, "gt(.field(0) => string?, string(hello)) => boolean?"},
		{iceberg.LessThanEqual(ref, "hello"), tableSchemaSimple, "lte(.field(0) => string?, string(hello)) => boolean?"},
		{iceberg.LessThan(ref, "hello"), tableSchemaSimple, "lt(.field(0) => string?, string(hello)) => boolean?"},
		{iceberg.StartsWith(ref, "he"), tableSchemaSimple, "starts_with(.field(0) => string?, string(he)) => boolean?"},
		{iceberg.NotStartsWith(ref, "he"), tableSchemaSimple, "not(starts_with(.field(0) => string?, string(he)) => boolean?) => boolean?"},
		{
			iceberg.NewAnd(iceberg.EqualTo(ref, "hello"), iceberg.IsNull(ref)), tableSchemaSimple,
			"and(equal(.field(0) => string?, string(hello)) => boolean?, is_null(.field(0) => string?) => boolean) => boolean?",
		},
		{
			iceberg.NewOr(iceberg.EqualTo(ref, "hello"), iceberg.IsNull(ref)), tableSchemaSimple,
			"or(equal(.field(0) => string?, string(hello)) => boolean?, is_null(.field(0) => string?) => boolean) => boolean?",
		},
		{
			iceberg.NewNot(iceberg.EqualTo(ref, "hello")), tableSchemaSimple,
			"not(equal(.field(0) => string?, string(hello)) => boolean?) => boolean?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.e.String(), func(t *testing.T) {
			bound, err := iceberg.BindExpr(tt.schema, tt.e, false)
			require.NoError(t, err)

			_, result, err := substrait.ConvertExpr(tt.schema, bound, true)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result.String())
		})
	}
}
