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
				return validator.List(iceberg.ListType{
					ElementID: 2, Element: iceberg.UnknownType{}, ElementRequired: true,
				}, nil)
			},
			want: "unknown type field 'element' (id: 2) must be optional, but was marked required",
		},
		{
			name: "map key requires optional unknown",
			validate: func() error {
				return validator.Map(iceberg.MapType{
					KeyID: 3, KeyType: iceberg.UnknownType{},
					ValueID: 4, ValueType: iceberg.StringType{},
				}, nil, nil)
			},
			want: "unknown type field 'key' (id: 3) must be optional, but was marked required",
		},
		{
			name: "map value requires optional unknown",
			validate: func() error {
				return validator.Map(iceberg.MapType{
					KeyID: 3, KeyType: iceberg.StringType{},
					ValueID: 4, ValueType: iceberg.UnknownType{}, ValueRequired: true,
				}, nil, nil)
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
	require.ErrorIs(t, validator.List(iceberg.ListType{
		ElementID: 2, Element: iceberg.UnknownType{}, ElementRequired: true,
	}, childErr), childErr)

	mapType := iceberg.MapType{
		KeyID: 3, KeyType: iceberg.UnknownType{},
		ValueID: 4, ValueType: iceberg.UnknownType{}, ValueRequired: true,
	}
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
			typ: &iceberg.StructType{}, initial: "wrong",
			want: "struct type field 'field' (id: 5) must have null or JSON object initial-default, but got: wrong",
		},
		{
			name: "struct write default must be object",
			typ: &iceberg.StructType{}, write: "wrong",
			want: "struct type field 'field' (id: 5) must have null or JSON object write-default, but got: wrong",
		},
		{
			name: "list initial default must be array",
			typ: &iceberg.ListType{}, initial: "wrong",
			want: "list type field 'field' (id: 5) must have null or JSON array initial-default, but got: wrong",
		},
		{
			name: "list write default must be array",
			typ: &iceberg.ListType{}, write: "wrong",
			want: "list type field 'field' (id: 5) must have null or JSON array write-default, but got: wrong",
		},
		{
			name: "map initial default must be object",
			typ: &iceberg.MapType{}, initial: "wrong",
			want: "map type field 'field' (id: 5) must have null or JSON object initial-default, but got: wrong",
		},
		{
			name: "map write default must be object",
			typ: &iceberg.MapType{}, write: "wrong",
			want: "map type field 'field' (id: 5) must have null or JSON object write-default, but got: wrong",
		},
		{
			name: "initial default error takes priority",
			typ: &iceberg.ListType{}, initial: "initial", write: "write",
			want: "list type field 'field' (id: 5) must have null or JSON array initial-default, but got: initial",
		},
		{
			name: "struct accepts object defaults",
			typ: &iceberg.StructType{},
			initial: map[string]any{"a": 1}, write: map[string]any(nil),
		},
		{
			name: "list accepts array defaults",
			typ: &iceberg.ListType{},
			initial: []any{"a"}, write: []any(nil),
		},
		{
			name: "map accepts object defaults",
			typ: &iceberg.MapType{},
			initial: map[string]any{"a": 1}, write: map[string]any{"b": 2},
		},
		{
			name: "primitive defaults unchanged",
			typ: iceberg.StringType{}, initial: "initial", write: "write",
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
