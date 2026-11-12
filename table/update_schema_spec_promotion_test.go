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
	"github.com/stretchr/testify/require"
)

func TestUpdateSchemaRejectsReadOnlyTypePromotions(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "str", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "bytes", Type: iceberg.PrimitiveTypes.Binary},
		iceberg.NestedField{ID: 3, Name: "fixed", Type: iceberg.MustFixedTypeOf(16)},
		iceberg.NestedField{ID: 4, Name: "int", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 5, Name: "float", Type: iceberg.PrimitiveTypes.Float32},
		iceberg.NestedField{ID: 6, Name: "decimal", Type: iceberg.MustDecimalTypeOf(9, 2)},
	)
	meta, err := NewMetadata(schema, nil, UnsortedSortOrder, "", nil)
	require.NoError(t, err)
	tbl := New([]string{"test"}, meta, "", nil, nil)

	for _, tt := range []struct {
		name   string
		field  string
		target iceberg.Type
	}{
		{name: "string to binary", field: "str", target: iceberg.PrimitiveTypes.Binary},
		{name: "binary to string", field: "bytes", target: iceberg.PrimitiveTypes.String},
		{name: "fixed to uuid", field: "fixed", target: iceberg.PrimitiveTypes.UUID},
	} {
		t.Run(tt.name, func(t *testing.T) {
			txn := tbl.NewTransaction()
			_, err := NewUpdateSchema(txn, true, false).
				UpdateColumn([]string{tt.field}, ColumnUpdate{
					FieldType: iceberg.Optional[iceberg.Type]{Valid: true, Val: tt.target},
				}).Apply()
			require.ErrorIs(t, err, iceberg.ErrResolve)
		})
	}

	for _, tt := range []struct {
		name   string
		field  string
		target iceberg.Type
	}{
		{name: "int to long", field: "int", target: iceberg.PrimitiveTypes.Int64},
		{name: "float to double", field: "float", target: iceberg.PrimitiveTypes.Float64},
		{name: "widen decimal precision", field: "decimal", target: iceberg.MustDecimalTypeOf(12, 2)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			txn := tbl.NewTransaction()
			next, err := NewUpdateSchema(txn, true, false).
				UpdateColumn([]string{tt.field}, ColumnUpdate{
					FieldType: iceberg.Optional[iceberg.Type]{Valid: true, Val: tt.target},
				}).Apply()
			require.NoError(t, err)
			field, ok := next.FindFieldByName(tt.field)
			require.True(t, ok)
			require.True(t, field.Type.Equals(tt.target))
		})
	}

	t.Run("explicit incompatible change", func(t *testing.T) {
		txn := tbl.NewTransaction()
		next, err := NewUpdateSchema(txn, true, true).
			UpdateColumn([]string{"str"}, ColumnUpdate{
				FieldType: iceberg.Optional[iceberg.Type]{Valid: true, Val: iceberg.PrimitiveTypes.Binary},
			}).Apply()
		require.NoError(t, err)
		field, ok := next.FindFieldByName("str")
		require.True(t, ok)
		require.True(t, field.Type.Equals(iceberg.PrimitiveTypes.Binary))
	})
}

func TestUpdateSchemaPromotesV3UnknownType(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Unknown},
	)
	meta, err := NewMetadata(schema, nil, UnsortedSortOrder, "", iceberg.Properties{
		PropertyFormatVersion: "3",
	})
	require.NoError(t, err)
	tbl := New([]string{"test"}, meta, "", nil, nil)

	next, err := NewUpdateSchema(tbl.NewTransaction(), true, false).
		UpdateColumn([]string{"value"}, ColumnUpdate{
			FieldType: iceberg.Optional[iceberg.Type]{Valid: true, Val: iceberg.PrimitiveTypes.String},
		}).Apply()
	require.NoError(t, err)
	field, ok := next.FindFieldByName("value")
	require.True(t, ok)
	require.True(t, field.Type.Equals(iceberg.PrimitiveTypes.String))
}
