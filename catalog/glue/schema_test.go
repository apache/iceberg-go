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

package glue

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
)

func TestIcebergTypeToGlueType(t *testing.T) {
	tests := []struct {
		name     string
		input    iceberg.Type
		expected string
	}{
		{
			name:     "boolean type",
			input:    iceberg.BooleanType{},
			expected: "boolean",
		},
		{
			name:     "int32 type",
			input:    iceberg.Int32Type{},
			expected: "int",
		},
		{
			name:     "int64 type",
			input:    iceberg.Int64Type{},
			expected: "bigint",
		},
		{
			name:     "float type",
			input:    iceberg.Float32Type{},
			expected: "float",
		},
		{
			name:     "double type",
			input:    iceberg.Float64Type{},
			expected: "double",
		},
		{
			name:     "date type",
			input:    iceberg.DateType{},
			expected: "date",
		},
		{
			name:     "time type",
			input:    iceberg.TimeType{},
			expected: "string",
		},
		{
			name:     "timestamp type",
			input:    iceberg.TimestampType{},
			expected: "timestamp",
		},
		{
			name:     "timestamptz type",
			input:    iceberg.TimestampTzType{},
			expected: "timestamp",
		},
		{
			name:     "string type",
			input:    iceberg.StringType{},
			expected: "string",
		},
		{
			name:     "uuid type",
			input:    iceberg.UUIDType{},
			expected: "string",
		},
		{
			name:     "binary type",
			input:    iceberg.BinaryType{},
			expected: "binary",
		},
		{
			name:     "decimal type",
			input:    iceberg.DecimalTypeOf(10, 2),
			expected: "decimal(10,2)",
		},
		{
			name:     "fixed type",
			input:    iceberg.FixedTypeOf(16),
			expected: "binary(16)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := icebergTypeToGlueType(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFieldToGlueColumn(t *testing.T) {
	tests := []struct {
		name     string
		field    iceberg.NestedField
		expected types.Column
	}{
		{
			name: "simple field",
			field: iceberg.NestedField{
				ID:       1,
				Name:     "simple_field",
				Type:     iceberg.StringType{},
				Required: true,
				Doc:      "A simple string field",
			},
			expected: types.Column{
				Name:    aws.String("simple_field"),
				Type:    aws.String("string"),
				Comment: aws.String("A simple string field"),
				Parameters: map[string]string{
					icebergFieldIDKey:       "1",
					icebergFieldOptionalKey: "false",
					icebergFieldCurrentKey:  "true",
				},
			},
		},
		{
			name: "decimal field",
			field: iceberg.NestedField{
				ID:       2,
				Name:     "price",
				Type:     iceberg.DecimalTypeOf(10, 2),
				Required: false,
				Doc:      "Price with 2 decimal places",
			},
			expected: types.Column{
				Name:    aws.String("price"),
				Type:    aws.String("decimal(10,2)"),
				Comment: aws.String("Price with 2 decimal places"),
				Parameters: map[string]string{
					icebergFieldIDKey:       "2",
					icebergFieldOptionalKey: "true",
					icebergFieldCurrentKey:  "true",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fieldToGlueColumn(tt.field, true)
			assert.Equal(t, tt.expected.Name, result.Name)
			assert.Equal(t, tt.expected.Type, result.Type)
			assert.Equal(t, tt.expected.Comment, result.Comment)
			assert.Equal(t, tt.expected.Parameters, result.Parameters)
		})
	}
}

func TestNestedStructType(t *testing.T) {
	addressStruct := &iceberg.StructType{
		FieldList: []iceberg.NestedField{
			{
				ID:       1,
				Name:     "street",
				Type:     iceberg.StringType{},
				Required: true,
			},
			{
				ID:       2,
				Name:     "city",
				Type:     iceberg.StringType{},
				Required: true,
			},
			{
				ID:       3,
				Name:     "zip",
				Type:     iceberg.Int32Type{},
				Required: true,
			},
		},
	}
	structType := icebergTypeToGlueType(addressStruct)
	expected := "struct<street:string,city:string,zip:int>"
	assert.Equal(t, expected, structType)
	field := iceberg.NestedField{
		ID:       4,
		Name:     "address",
		Type:     addressStruct,
		Required: true,
		Doc:      "User address",
	}
	column := fieldToGlueColumn(field, true)
	assert.Equal(t, "address", aws.ToString(column.Name))
	assert.Equal(t, expected, aws.ToString(column.Type))
	assert.Equal(t, "User address", aws.ToString(column.Comment))
}

func TestNestedListType(t *testing.T) {
	stringList := &iceberg.ListType{
		ElementID:       1,
		Element:         iceberg.StringType{},
		ElementRequired: true,
	}
	listType := icebergTypeToGlueType(stringList)
	assert.Equal(t, "array<string>", listType)
	field := iceberg.NestedField{
		ID:       2,
		Name:     "tags",
		Type:     stringList,
		Required: false,
		Doc:      "User tags",
	}
	column := fieldToGlueColumn(field, false)
	assert.Equal(t, "tags", aws.ToString(column.Name))
	assert.Equal(t, "array<string>", aws.ToString(column.Type))
	assert.Equal(t, "User tags", aws.ToString(column.Comment))
}

func TestNestedMapType(t *testing.T) {
	stringIntMap := &iceberg.MapType{
		KeyID:         1,
		KeyType:       iceberg.StringType{},
		ValueID:       2,
		ValueType:     iceberg.Int32Type{},
		ValueRequired: true,
	}
	mapType := icebergTypeToGlueType(stringIntMap)
	assert.Equal(t, "map<string,int>", mapType)
	field := iceberg.NestedField{
		ID:       3,
		Name:     "properties",
		Type:     stringIntMap,
		Required: true,
		Doc:      "User properties",
	}
	column := fieldToGlueColumn(field, true)
	assert.Equal(t, "properties", aws.ToString(column.Name))
	assert.Equal(t, "map<string,int>", aws.ToString(column.Type))
	assert.Equal(t, "User properties", aws.ToString(column.Comment))
}

func TestComplexNestedTypes(t *testing.T) {
	stringIntMap := &iceberg.MapType{
		KeyID:         1,
		KeyType:       iceberg.StringType{},
		ValueID:       2,
		ValueType:     iceberg.Int32Type{},
		ValueRequired: true,
	}
	mapList := &iceberg.ListType{
		ElementID:       3,
		Element:         stringIntMap,
		ElementRequired: true,
	}
	complexStruct := &iceberg.StructType{
		FieldList: []iceberg.NestedField{
			{
				ID:       4,
				Name:     "name",
				Type:     iceberg.StringType{},
				Required: true,
			},
			{
				ID:       5,
				Name:     "attributes",
				Type:     mapList,
				Required: false,
				Doc:      "List of attribute maps",
			},
		},
	}
	complexType := icebergTypeToGlueType(complexStruct)
	expected := "struct<name:string,attributes:array<map<string,int>>>"
	assert.Equal(t, expected, complexType)
	field := iceberg.NestedField{
		ID:       6,
		Name:     "complex_field",
		Type:     complexStruct,
		Required: true,
		Doc:      "A complex nested field",
	}
	column := fieldToGlueColumn(field, true)
	assert.Equal(t, "complex_field", aws.ToString(column.Name))
	assert.Equal(t, expected, aws.ToString(column.Type))
	assert.Equal(t, "A complex nested field", aws.ToString(column.Comment))
}

func TestSchemaToGlueColumns(t *testing.T) {
	addressStruct := &iceberg.StructType{
		FieldList: []iceberg.NestedField{
			{
				ID:       7,
				Name:     "street",
				Type:     iceberg.StringType{},
				Required: true,
			},
			{
				ID:       6,
				Name:     "city",
				Type:     iceberg.StringType{},
				Required: true,
			},
		},
	}
	stringList := &iceberg.ListType{
		ElementID:       5,
		Element:         iceberg.StringType{},
		ElementRequired: true,
	}
	fields := []iceberg.NestedField{
		{
			ID:       1,
			Name:     "id",
			Type:     iceberg.Int64Type{},
			Required: true,
		},
		{
			ID:       2,
			Name:     "name",
			Type:     iceberg.StringType{},
			Required: true,
		},
		{
			ID:       3,
			Name:     "address",
			Type:     addressStruct,
			Required: false,
			Doc:      "User address",
		},
		{
			ID:       4,
			Name:     "tags",
			Type:     stringList,
			Required: false,
			Doc:      "User tags",
		},
	}
	schema := iceberg.NewSchema(1, fields...)
	columns := schemaToGlueColumns(schema, true)
	assert.Equal(t, 4, len(columns))
	assert.Equal(t, "id", aws.ToString(columns[0].Name))
	assert.Equal(t, "bigint", aws.ToString(columns[0].Type))
	assert.Equal(t, "name", aws.ToString(columns[1].Name))
	assert.Equal(t, "string", aws.ToString(columns[1].Type))
	assert.Equal(t, "address", aws.ToString(columns[2].Name))
	assert.Equal(t, "struct<street:string,city:string>", aws.ToString(columns[2].Type))
	assert.Equal(t, "User address", aws.ToString(columns[2].Comment))
	assert.Equal(t, "tags", aws.ToString(columns[3].Name))
	assert.Equal(t, "array<string>", aws.ToString(columns[3].Type))
	assert.Equal(t, "User tags", aws.ToString(columns[3].Comment))
}

func TestSchemasToGlueColumns(t *testing.T) {
	schemas := []*iceberg.Schema{
		iceberg.NewSchema(0,
			iceberg.NestedField{
				ID:       1,
				Name:     "id",
				Type:     iceberg.Int64Type{},
				Required: true,
			},
			iceberg.NestedField{
				ID:       2,
				Name:     "name",
				Type:     iceberg.StringType{},
				Required: true,
			},
			iceberg.NestedField{
				ID:       3,
				Name:     "address",
				Type:     iceberg.StringType{},
				Required: false,
			},
		),
		iceberg.NewSchema(1,
			iceberg.NestedField{
				ID:       1,
				Name:     "id",
				Type:     iceberg.Int64Type{},
				Required: true,
			},
			iceberg.NestedField{
				ID:       2,
				Name:     "name",
				Type:     iceberg.StringType{},
				Required: true,
			},
		),
	}

	expectedColumns := []types.Column{
		{
			Name:    aws.String("id"),
			Type:    aws.String("bigint"),
			Comment: aws.String(""),
			Parameters: map[string]string{
				icebergFieldIDKey:       "1",
				icebergFieldOptionalKey: "false",
				icebergFieldCurrentKey:  "true",
			},
		},
		{
			Name:    aws.String("name"),
			Type:    aws.String("string"),
			Comment: aws.String(""),
			Parameters: map[string]string{
				icebergFieldIDKey:       "2",
				icebergFieldOptionalKey: "false",
				icebergFieldCurrentKey:  "true",
			},
		},
		{
			Name:    aws.String("address"),
			Type:    aws.String("string"),
			Comment: aws.String(""),
			Parameters: map[string]string{
				icebergFieldIDKey:       "3",
				icebergFieldOptionalKey: "true",
				icebergFieldCurrentKey:  "false",
			},
		},
	}
	metadata, err := table.NewMetadata(schemas[0], nil, table.SortOrder{}, "s3://example/path", nil)
	assert.NoError(t, err)

	mb, err := table.MetadataBuilderFromBase(metadata)
	assert.NoError(t, err)

	mb, err = mb.AddSchema(schemas[1])
	assert.NoError(t, err)
	mb, err = mb.SetCurrentSchemaID(1)
	assert.NoError(t, err)

	metadata, err = mb.Build()
	assert.NoError(t, err)

	columns := schemasToGlueColumns(metadata)
	assert.Equal(t, expectedColumns, columns)
}
