// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

func TestNewMetadata_DuplicateValues(t *testing.T) {
	names := []string{"foo", "bar", "foo"}

	fields := make([]iceberg.NestedField, len(names))
	for i, name := range names {
		fields[i] = iceberg.NestedField{
			ID:   i + 1,
			Name: name,
			Type: iceberg.PrimitiveTypes.String,
		}
	}
	icebergSchema := iceberg.NewSchema(1, fields...)

	_, err := NewMetadata(
		icebergSchema,
		&iceberg.PartitionSpec{},
		UnsortedSortOrder,
		"",
		iceberg.Properties{},
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	assert.Contains(t, err.Error(), "multiple fields for name foo")
}

func TestUnknownTypeValidatorRetainsErrorBehavior(t *testing.T) {
	validator := &unknownTypeValidator{}
	tests := []struct {
		name     string
		validate func() error
		want     string
	}{
		{
			name: "struct field requires optional unknown",
			validate: func() error {
				return validator.Field(iceberg.NestedField{
					ID: 1, Name: "field", Type: iceberg.UnknownType{}, Required: true,
				}, nil)
			},
			want: "unknown type field 'field' (id: 1) must be optional, but was marked as required",
		},
		{
			name: "list element requires optional unknown",
			validate: func() error {
				return validator.List(*iceberg.NewListType(2, iceberg.UnknownType{}, true), nil)
			},
			want: "unknown type field 'element' (id: 2) must be optional, but was marked required",
		},
		{
			name: "map key requires optional unknown",
			validate: func() error {
				return validator.Map(*iceberg.NewMapType(3, iceberg.UnknownType{}, 4, iceberg.StringType{}, false), nil, nil)
			},
			want: "unknown type field 'key' (id: 3) must be optional, but was marked required",
		},
		{
			name: "map value requires optional unknown",
			validate: func() error {
				return validator.Map(*iceberg.NewMapType(3, iceberg.StringType{}, 4, iceberg.UnknownType{}, true), nil, nil)
			},
			want: "unknown type field 'value' (id: 4) must be optional, but was marked required",
		},
		{
			name: "unknown initial default is rejected",
			validate: func() error {
				return validator.Field(iceberg.NestedField{
					ID: 5, Name: "field", Type: iceberg.UnknownType{}, InitialDefault: "wrong",
				}, nil)
			},
			want: "unknown type field 'field' (id: 5) must have null initial-default, but got: wrong",
		},
		{
			name: "unknown write default is rejected",
			validate: func() error {
				return validator.Field(iceberg.NestedField{
					ID: 5, Name: "field", Type: iceberg.UnknownType{}, WriteDefault: "wrong",
				}, nil)
			},
			want: "unknown type field 'field' (id: 5) must have null write-default, but got: wrong",
		},
		{
			name: "variant initial default is rejected",
			validate: func() error {
				return validator.Field(iceberg.NestedField{
					ID: 5, Name: "field", Type: iceberg.VariantType{}, InitialDefault: "wrong",
				}, nil)
			},
			want: "variant type field 'field' (id: 5) must have null initial-default, but got: wrong",
		},
		{
			name: "variant write default is rejected",
			validate: func() error {
				return validator.Field(iceberg.NestedField{
					ID: 5, Name: "field", Type: iceberg.VariantType{}, WriteDefault: "wrong",
				}, nil)
			},
			want: "variant type field 'field' (id: 5) must have null write-default, but got: wrong",
		},
		{
			name: "required variant is allowed",
			validate: func() error {
				return validator.Field(iceberg.NestedField{
					ID: 5, Name: "field", Type: iceberg.VariantType{}, Required: true,
				}, nil)
			},
		},
		{
			name: "regular types retain non-null defaults",
			validate: func() error {
				return validator.Field(iceberg.NestedField{
					ID: 5, Name: "field", Type: iceberg.StringType{},
					InitialDefault: "initial", WriteDefault: "written",
				}, nil)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.validate()
			if tt.want == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.want)
			}
		})
	}
}

func TestUnknownTypeValidatorPreservesChildErrorPriority(t *testing.T) {
	validator := &unknownTypeValidator{}
	childErr := iceberg.ErrInvalidSchema
	require.ErrorIs(t, validator.Field(iceberg.NestedField{
		ID: 1, Name: "field", Type: iceberg.UnknownType{}, Required: true,
	}, childErr), childErr)
	require.ErrorIs(t, validator.List(*iceberg.NewListType(2, iceberg.UnknownType{}, true), childErr), childErr)

	mapType := *iceberg.NewMapType(3, iceberg.UnknownType{}, 4, iceberg.UnknownType{}, true)
	require.ErrorIs(t, validator.Map(mapType, childErr, nil), childErr)
	require.ErrorIs(t, validator.Map(mapType, nil, childErr), childErr)
}

func TestValidateComplexDefaultRetainsShapeRules(t *testing.T) {
	tests := []struct {
		name    string
		typ     iceberg.Type
		initial any
		write   any
		want    string
	}{
		{
			name: "struct initial default must be object",
			typ:  iceberg.NewStructType(), initial: "wrong",
			want: "struct type field 'field' (id: 5) must have null or empty JSON object initial-default, but got: wrong",
		},
		{
			name: "struct write default must be object",
			typ:  iceberg.NewStructType(), write: "wrong",
			want: "struct type field 'field' (id: 5) must have null or empty JSON object write-default, but got: wrong",
		},
		{
			name: "list initial default must be array",
			typ:  iceberg.NewListType(0, nil, false), initial: "wrong",
			want: "list type field 'field' (id: 5) must have null or JSON array initial-default, but got: wrong",
		},
		{
			name: "list write default must be array",
			typ:  iceberg.NewListType(0, nil, false), write: "wrong",
			want: "list type field 'field' (id: 5) must have null or JSON array write-default, but got: wrong",
		},
		{
			name: "map initial default must be object",
			typ:  iceberg.NewMapType(0, nil, 0, nil, false), initial: "wrong",
			want: "map type field 'field' (id: 5) must have null or JSON object initial-default, but got: wrong",
		},
		{
			name: "map write default must be object",
			typ:  iceberg.NewMapType(0, nil, 0, nil, false), write: "wrong",
			want: "map type field 'field' (id: 5) must have null or JSON object write-default, but got: wrong",
		},
		{
			name: "initial default error takes priority",
			typ:  iceberg.NewListType(0, nil, false), initial: "initial", write: "write",
			want: "list type field 'field' (id: 5) must have null or JSON array initial-default, but got: initial",
		},
		{
			name:    "struct accepts empty object defaults",
			typ:     iceberg.NewStructType(),
			initial: map[string]any{}, write: map[string]any{},
		},
		{
			name: "struct initial default cannot contain fields",
			typ:  iceberg.NewStructType(), initial: map[string]any{"x": 1},
			want: "struct type field 'field' (id: 5) must have null or empty JSON object initial-default, but got: map[x:1]",
		},
		{
			name: "struct write default cannot contain fields",
			typ:  iceberg.NewStructType(), write: map[string]any{"x": 1},
			want: "struct type field 'field' (id: 5) must have null or empty JSON object write-default, but got: map[x:1]",
		},
		{
			name:    "list accepts array defaults",
			typ:     iceberg.NewListType(0, nil, false),
			initial: []any{"a"}, write: []any(nil),
		},
		{
			name:    "map accepts object defaults",
			typ:     iceberg.NewMapType(0, nil, 0, nil, false),
			initial: map[string]any{"a": 1}, write: map[string]any{"b": 2},
		},
		{
			name: "primitive defaults unchanged",
			typ:  iceberg.StringType{}, initial: "initial", write: "write",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := iceberg.NestedField{
				ID: 5, Name: "field", Type: tt.typ,
				InitialDefault: tt.initial, WriteDefault: tt.write,
			}
			err := validateComplexDefault(field)
			if tt.want == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.want)
			}
		})
	}
}

func TestCheckSchemaCompatibilityRejectsPopulatedStructDefaults(t *testing.T) {
	pointType := iceberg.NewStructType(iceberg.NestedField{
		ID: 2, Name: "x", Type: iceberg.PrimitiveTypes.Int32,
	})
	for _, tt := range []struct {
		name    string
		initial any
		write   any
	}{
		{name: "initial", initial: map[string]any{"x": 1}},
		{name: "write", write: map[string]any{"x": 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			schema := iceberg.NewSchema(0, iceberg.NestedField{
				ID: 1, Name: "point", Type: pointType,
				InitialDefault: tt.initial, WriteDefault: tt.write,
			})
			err := checkSchemaCompatibility(schema, 3)
			require.ErrorContains(t, err, "must have null or empty JSON object "+tt.name+"-default")
		})
	}
}

func TestValidateUniqueFieldIDs(t *testing.T) {
	typ := iceberg.PrimitiveTypes.Int32

	tests := []struct {
		name    string
		fields  []iceberg.NestedField
		wantErr string
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
				{Name: "created_at", Type: iceberg.PrimitiveTypes.TimestampTz},
				{Name: "event", Type: iceberg.PrimitiveTypes.String},
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
					iceberg.NestedField{Name: "x", Type: typ},
					iceberg.NestedField{Name: "y", Type: typ},
				)},
			},
			wantErr: "multiple fields for id 0: x and y",
		},
		{
			name: "list element shares zero with a field",
			fields: []iceberg.NestedField{
				{Name: "a", Type: typ},
				{ID: 1, Name: "items", Type: iceberg.NewListType(0, typ, false)},
			},
			wantErr: "multiple fields for id 0: a and element",
		},
		{
			name: "map value shares zero with a field",
			fields: []iceberg.NestedField{
				{Name: "a", Type: typ},
				{ID: 1, Name: "m", Type: iceberg.NewMapType(2, iceberg.PrimitiveTypes.String, 0, typ, false)},
			},
			wantErr: "multiple fields for id 0: a and value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateUniqueFieldIDs(iceberg.NewSchema(0, tt.fields...))
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}
			require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}
