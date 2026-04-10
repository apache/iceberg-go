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

package iceberg

import (
	"fmt"
	"strings"
	"testing"

	"github.com/apache/iceberg-go/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/avro"
)

// partitionTypeToAvroSchemaNonNullable builds a partition schema where fields are NOT wrapped
// in a nullable union. Used in tests to verify that nil values cause encoding failures.
func partitionTypeToAvroSchemaNonNullable(t *StructType) (*avro.Schema, error) {
	if len(t.FieldList) == 0 {
		return avro.Parse(`{"type":"record","name":"r102","fields":[]}`)
	}

	definedNames := make(map[string]bool)

	fieldJSONs := make([]string, 0, len(t.FieldList))
	for _, f := range t.FieldList {
		typeJSON, err := nonNullablePartitionFieldTypeJSON(f.Type, definedNames)
		if err != nil {
			return nil, err
		}
		fieldJSONs = append(fieldJSONs, fmt.Sprintf(`{"name":%q,"type":%s,"field-id":%d}`,
			f.Name, typeJSON, f.ID))
	}

	schemaJSON := fmt.Sprintf(`{"type":"record","name":"r102","fields":[%s]}`,
		strings.Join(fieldJSONs, ","))

	return avro.Parse(schemaJSON)
}

func nonNullablePartitionFieldTypeJSON(typ Type, definedNames map[string]bool) (string, error) {
	switch t := typ.(type) {
	case Int32Type:
		return `"int"`, nil
	case Int64Type:
		return `"long"`, nil
	case Float32Type:
		return `"float"`, nil
	case Float64Type:
		return `"double"`, nil
	case StringType:
		return `"string"`, nil
	case DateType:
		return `{"type":"int","logicalType":"date"}`, nil
	case TimeType:
		return `{"type":"long","logicalType":"time-micros"}`, nil
	case TimestampType:
		return `{"type":"long","logicalType":"timestamp-micros","adjust-to-utc":false}`, nil
	case TimestampTzType:
		return `{"type":"long","logicalType":"timestamp-micros","adjust-to-utc":true}`, nil
	case UUIDType:
		if !definedNames["uuid"] {
			definedNames["uuid"] = true
			return `{"type":"fixed","name":"uuid","size":16,"logicalType":"uuid"}`, nil
		}

		return `"uuid"`, nil
	case BooleanType:
		return `"boolean"`, nil
	case BinaryType:
		return `"bytes"`, nil
	case FixedType:
		fixedName := fmt.Sprintf("fixed_%d", t.len)
		if !definedNames[fixedName] {
			definedNames[fixedName] = true
			return fmt.Sprintf(`{"type":"fixed","name":"%s","size":%d}`, fixedName, t.len), nil
		}

		return fmt.Sprintf(`"%s"`, fixedName), nil
	case DecimalType:
		size := internal.DecimalRequiredBytes(t.precision)
		decName := fmt.Sprintf("fixed_%d_%d", t.precision, t.scale)
		if !definedNames[decName] {
			definedNames[decName] = true
			return fmt.Sprintf(`{"type":"fixed","name":"%s","size":%d,"logicalType":"decimal","precision":%d,"scale":%d}`,
				decName, size, t.precision, t.scale), nil
		}

		return fmt.Sprintf(`"%s"`, decName), nil
	default:
		return "", fmt.Errorf("unsupported partition type: %s", typ.String())
	}
}

func TestPartitionTypeToAvroSchemaNullableAndNonNullable(t *testing.T) {
	partitionType := &StructType{
		FieldList: []NestedField{
			{ID: 1, Name: "int32_col", Type: Int32Type{}, Required: false},
			{ID: 2, Name: "int64_col", Type: Int64Type{}, Required: false},
			{ID: 3, Name: "float32_col", Type: Float32Type{}, Required: false},
			{ID: 4, Name: "float64_col", Type: Float64Type{}, Required: false},
			{ID: 5, Name: "string_col", Type: StringType{}, Required: false},
			{ID: 6, Name: "date_col", Type: DateType{}, Required: false},
			{ID: 7, Name: "time_col", Type: TimeType{}, Required: false},
			{ID: 8, Name: "timestamp_col", Type: TimestampType{}, Required: false},
			{ID: 9, Name: "timestamptz_col", Type: TimestampTzType{}, Required: false},
			{ID: 10, Name: "uuid_col", Type: UUIDType{}, Required: false},
			{ID: 11, Name: "bool_col", Type: BooleanType{}, Required: false},
			{ID: 12, Name: "binary_col", Type: BinaryType{}, Required: false},
			{ID: 13, Name: "fixed_col", Type: FixedType{len: 16}, Required: false},
			{ID: 14, Name: "decimal_col", Type: DecimalType{precision: 10, scale: 2}, Required: false},
		},
	}

	partitionData := map[string]any{
		"int32_col":       nil,
		"int64_col":       nil,
		"float32_col":     nil,
		"float64_col":     nil,
		"string_col":      nil,
		"date_col":        nil,
		"time_col":        nil,
		"timestamp_col":   nil,
		"timestamptz_col": nil,
		"uuid_col":        nil,
		"bool_col":        nil,
		"binary_col":      nil,
		"fixed_col":       nil,
		"decimal_col":     nil,
	}

	t.Run("nullable schema accepts nil", func(t *testing.T) {
		schemaNullable, err := partitionTypeToAvroSchema(partitionType)
		require.NoError(t, err)
		require.NotNil(t, schemaNullable)

		encoded, err := schemaNullable.Encode(partitionData)
		require.NoError(t, err)
		require.NotEmpty(t, encoded)

		var decoded map[string]any
		_, err = schemaNullable.Decode(encoded, &decoded)
		require.NoError(t, err)

		assert.Nil(t, decoded["int32_col"])
		assert.Nil(t, decoded["int64_col"])
		assert.Nil(t, decoded["float32_col"])
		assert.Nil(t, decoded["float64_col"])
		assert.Nil(t, decoded["string_col"])
		assert.Nil(t, decoded["date_col"])
		assert.Nil(t, decoded["time_col"])
		assert.Nil(t, decoded["timestamp_col"])
		assert.Nil(t, decoded["timestamptz_col"])
		assert.Nil(t, decoded["uuid_col"])
		assert.Nil(t, decoded["bool_col"])
		assert.Nil(t, decoded["binary_col"])
		assert.Nil(t, decoded["fixed_col"])
		assert.Nil(t, decoded["decimal_col"])
	})

	t.Run("non-nullable schema rejects nil", func(t *testing.T) {
		schemaNonNullable, err := partitionTypeToAvroSchemaNonNullable(partitionType)
		require.NoError(t, err)
		require.NotNil(t, schemaNonNullable)

		encoded, err := schemaNonNullable.Encode(partitionData)
		require.Error(t, err, "expected marshal to fail when values are nil for non-nullable schema")
		assert.Empty(t, encoded)
	})
}
