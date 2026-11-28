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

package fieldids_test

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal/fieldids"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckUnique(t *testing.T) {
	typ := iceberg.PrimitiveTypes.Int32

	tests := []struct {
		name    string
		fields  []iceberg.NestedField
		wantErr string // empty means no error expected
	}{
		{
			name: "unique positive ids",
			fields: []iceberg.NestedField{
				{ID: 1, Name: "a", Type: typ},
				{ID: 2, Name: "b", Type: typ},
			},
		},
		{
			name: "single zero id",
			fields: []iceberg.NestedField{
				{ID: 0, Name: "a", Type: typ},
				{ID: 1, Name: "b", Type: typ},
			},
		},
		{
			name: "duplicate zero ids",
			fields: []iceberg.NestedField{
				{ID: 0, Name: "created_at", Type: iceberg.PrimitiveTypes.TimestampTz},
				{ID: 0, Name: "event", Type: iceberg.PrimitiveTypes.String},
			},
			wantErr: "multiple fields for id 0: created_at and event",
		},
		{
			name: "duplicate negative ids",
			fields: []iceberg.NestedField{
				{ID: -1, Name: "a", Type: typ},
				{ID: -1, Name: "b", Type: typ},
			},
			wantErr: "multiple fields for id -1: a and b",
		},
		{
			name: "duplicate zero inside a struct",
			fields: []iceberg.NestedField{
				{ID: 1, Name: "s", Type: iceberg.NewStructType(
					iceberg.NestedField{ID: 0, Name: "x", Type: typ},
					iceberg.NestedField{ID: 0, Name: "y", Type: typ},
				)},
			},
			wantErr: "multiple fields for id 0: x and y",
		},
		{
			name: "list element shares an id with a field",
			fields: []iceberg.NestedField{
				{ID: 0, Name: "a", Type: typ},
				{ID: 1, Name: "items", Type: iceberg.NewListType(0, typ, false)},
			},
			wantErr: "multiple fields for id 0: a and element",
		},
		{
			name: "map key shares an id with a field",
			fields: []iceberg.NestedField{
				{ID: 0, Name: "a", Type: typ},
				{ID: 1, Name: "m", Type: iceberg.NewMapType(0, iceberg.PrimitiveTypes.String, 2, typ, false)},
			},
			wantErr: "multiple fields for id 0: a and key",
		},
		{
			name: "map value shares an id with a field",
			fields: []iceberg.NestedField{
				{ID: 0, Name: "a", Type: typ},
				{ID: 1, Name: "m", Type: iceberg.NewMapType(2, iceberg.PrimitiveTypes.String, 0, typ, false)},
			},
			wantErr: "multiple fields for id 0: a and value",
		},
		{
			name: "empty field list",
		},
		{
			name: "duplicate positive ids",
			fields: []iceberg.NestedField{
				{ID: 1, Name: "a", Type: typ},
				{ID: 1, Name: "b", Type: typ},
			},
			wantErr: "multiple fields for id 1: a and b",
		},
		{
			// The seen IDs are shared across the whole schema, not per struct.
			name: "children of two different structs share an id",
			fields: []iceberg.NestedField{
				{ID: 1, Name: "s1", Type: iceberg.NewStructType(
					iceberg.NestedField{ID: 5, Name: "x", Type: typ},
				)},
				{ID: 2, Name: "s2", Type: iceberg.NewStructType(
					iceberg.NestedField{ID: 5, Name: "y", Type: typ},
				)},
			},
			wantErr: "multiple fields for id 5: x and y",
		},
		{
			name: "map key and value share an id",
			fields: []iceberg.NestedField{
				{ID: 1, Name: "m", Type: iceberg.NewMapType(3, iceberg.PrimitiveTypes.String, 3, typ, false)},
			},
			wantErr: "multiple fields for id 3: key and value",
		},
		{
			name: "duplicate inside a struct inside a list",
			fields: []iceberg.NestedField{
				{ID: 1, Name: "items", Type: iceberg.NewListType(2, iceberg.NewStructType(
					iceberg.NestedField{ID: 3, Name: "x", Type: typ},
					iceberg.NestedField{ID: 3, Name: "y", Type: typ},
				), false)},
			},
			wantErr: "multiple fields for id 3: x and y",
		},
		{
			name: "duplicate inside a struct as a map value",
			fields: []iceberg.NestedField{
				{ID: 1, Name: "m", Type: iceberg.NewMapType(2, iceberg.PrimitiveTypes.String, 3, iceberg.NewStructType(
					iceberg.NestedField{ID: 4, Name: "p", Type: typ},
					iceberg.NestedField{ID: 4, Name: "q", Type: typ},
				), false)},
			},
			wantErr: "multiple fields for id 4: p and q",
		},
		{
			name: "nil struct, list and map types",
			fields: []iceberg.NestedField{
				{ID: 1, Name: "s", Type: (*iceberg.StructType)(nil)},
				{ID: 2, Name: "l", Type: (*iceberg.ListType)(nil)},
				{ID: 3, Name: "m", Type: (*iceberg.MapType)(nil)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := fieldids.CheckUnique(tt.fields)
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}
			require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}
