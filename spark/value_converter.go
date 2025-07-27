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

package spark

import (
	"errors"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

// SparkValueConverter provides conversion utilities between Iceberg and Spark data types
type SparkValueConverter struct{}

// NewSparkValueConverter creates a new SparkValueConverter instance
func NewSparkValueConverter() *SparkValueConverter {
	return &SparkValueConverter{}
}

// ConvertIcebergToSparkValue converts an Iceberg value to a Spark-compatible value
func (c *SparkValueConverter) ConvertIcebergToSparkValue(value interface{}, icebergType iceberg.Type) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch t := icebergType.(type) {
	case *iceberg.BooleanType, iceberg.BooleanType:
		if v, ok := value.(bool); ok {
			return v, nil
		}

		return nil, fmt.Errorf("expected bool, got %T", value)

	case *iceberg.Int32Type, iceberg.Int32Type:
		if v, ok := value.(int32); ok {
			return v, nil
		}
		if v, ok := value.(int); ok {
			return int32(v), nil
		}

		return nil, fmt.Errorf("expected int32, got %T", value)

	case *iceberg.Int64Type, iceberg.Int64Type:
		if v, ok := value.(int64); ok {
			return v, nil
		}
		if v, ok := value.(int); ok {
			return int64(v), nil
		}

		return nil, fmt.Errorf("expected int64, got %T", value)

	case *iceberg.Float32Type, iceberg.Float32Type:
		if v, ok := value.(float32); ok {
			return v, nil
		}
		if v, ok := value.(float64); ok {
			return float32(v), nil
		}

		return nil, fmt.Errorf("expected float32, got %T", value)

	case *iceberg.Float64Type, iceberg.Float64Type:
		if v, ok := value.(float64); ok {
			return v, nil
		}
		if v, ok := value.(float32); ok {
			return float64(v), nil
		}

		return nil, fmt.Errorf("expected float64, got %T", value)

	case *iceberg.DecimalType:
		// Decimal handling - assuming string representation for now
		if v, ok := value.(string); ok {
			return v, nil
		}

		return fmt.Sprintf("%v", value), nil

	case *iceberg.DateType:
		if v, ok := value.(time.Time); ok {
			// Convert to days since epoch (1970-01-01)
			epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

			return int32(v.Sub(epoch).Hours() / 24), nil
		}
		if v, ok := value.(int32); ok {
			return v, nil
		}

		return nil, fmt.Errorf("expected time.Time or int32 for date, got %T", value)

	case *iceberg.TimeType:
		if v, ok := value.(time.Time); ok {
			// Convert to microseconds since midnight
			midnight := time.Date(v.Year(), v.Month(), v.Day(), 0, 0, 0, 0, v.Location())

			return v.Sub(midnight).Microseconds(), nil
		}
		if v, ok := value.(int64); ok {
			return v, nil
		}

		return nil, fmt.Errorf("expected time.Time or int64 for time, got %T", value)

	case *iceberg.TimestampType:
		if v, ok := value.(time.Time); ok {
			// Simplified - always use UTC microseconds
			return v.UTC().UnixMicro(), nil
		}
		if v, ok := value.(int64); ok {
			return v, nil
		}

		return nil, fmt.Errorf("expected time.Time or int64 for timestamp, got %T", value)

	case *iceberg.StringType, iceberg.StringType:
		if v, ok := value.(string); ok {
			return v, nil
		}

		return fmt.Sprintf("%v", value), nil

	case *iceberg.UUIDType:
		if v, ok := value.(uuid.UUID); ok {
			return v.String(), nil
		}
		if v, ok := value.(string); ok {
			return v, nil
		}

		return nil, fmt.Errorf("expected uuid.UUID or string for UUID, got %T", value)

	case *iceberg.FixedType:
		if v, ok := value.([]byte); ok {
			if len(v) != int(t.Len()) {
				return nil, fmt.Errorf("fixed type length mismatch: expected %d, got %d", t.Len(), len(v))
			}

			return v, nil
		}

		return nil, fmt.Errorf("expected []byte for fixed type, got %T", value)

	case *iceberg.BinaryType:
		if v, ok := value.([]byte); ok {
			return v, nil
		}

		return nil, fmt.Errorf("expected []byte for binary type, got %T", value)

	case *iceberg.ListType:
		if v, ok := value.([]interface{}); ok {
			var converted []interface{}
			for i, elem := range v {
				// Simplified - use string type for elements
				convertedElem, err := c.ConvertIcebergToSparkValue(elem, iceberg.PrimitiveTypes.String)
				if err != nil {
					return nil, fmt.Errorf("failed to convert list element %d: %w", i, err)
				}
				converted = append(converted, convertedElem)
			}

			return converted, nil
		}

		return nil, fmt.Errorf("expected []interface{} for list type, got %T", value)

	case *iceberg.MapType:
		if v, ok := value.(map[interface{}]interface{}); ok {
			converted := make(map[interface{}]interface{})
			for key, val := range v {
				// Simplified - use string type for keys and values
				convertedKey, err := c.ConvertIcebergToSparkValue(key, iceberg.PrimitiveTypes.String)
				if err != nil {
					return nil, fmt.Errorf("failed to convert map key: %w", err)
				}
				convertedVal, err := c.ConvertIcebergToSparkValue(val, iceberg.PrimitiveTypes.String)
				if err != nil {
					return nil, fmt.Errorf("failed to convert map value: %w", err)
				}
				converted[convertedKey] = convertedVal
			}

			return converted, nil
		}

		return nil, fmt.Errorf("expected map[interface{}]interface{} for map type, got %T", value)

	case *iceberg.StructType:
		if v, ok := value.(map[string]interface{}); ok {
			converted := make(map[string]interface{})
			for _, field := range t.FieldList {
				fieldValue, exists := v[field.Name]
				if !exists && field.Required {
					return nil, fmt.Errorf("required field %s not found in struct", field.Name)
				}
				if exists {
					convertedField, err := c.ConvertIcebergToSparkValue(fieldValue, field.Type)
					if err != nil {
						return nil, fmt.Errorf("failed to convert struct field %s: %w", field.Name, err)
					}
					converted[field.Name] = convertedField
				}
			}

			return converted, nil
		}

		return nil, fmt.Errorf("expected map[string]interface{} for struct type, got %T", value)

	default:
		return nil, fmt.Errorf("unsupported Iceberg type for conversion: %T", icebergType)
	}
}

// ConvertSparkToIcebergValue converts a Spark value to an Iceberg-compatible value
func (c *SparkValueConverter) ConvertSparkToIcebergValue(value interface{}, icebergType iceberg.Type) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	// For most basic types, the conversion is symmetric
	return c.ConvertIcebergToSparkValue(value, icebergType)
}

// ConvertArrowRecordToSparkRow converts an Arrow record to Spark-compatible row data
func (c *SparkValueConverter) ConvertArrowRecordToSparkRow(record arrow.Record, schema *iceberg.Schema) ([]interface{}, error) {
	numRows := int(record.NumRows())
	rows := make([]interface{}, numRows)

	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		row := make(map[string]interface{})

		fields := schema.Fields()
		for colIdx := 0; colIdx < len(fields); colIdx++ {
			field := fields[colIdx]
			column := record.Column(colIdx)

			var value interface{}
			if column.IsNull(rowIdx) {
				value = nil
			} else {
				var err error
				value, err = c.extractArrowValue(column, rowIdx, field.Type)
				if err != nil {
					return nil, fmt.Errorf("failed to extract value for field %s at row %d: %w", field.Name, rowIdx, err)
				}
			}

			row[field.Name] = value
		}

		rows[rowIdx] = row
	}

	return rows, nil
}

// extractArrowValue extracts a value from an Arrow array at a specific index
func (c *SparkValueConverter) extractArrowValue(arr arrow.Array, index int, icebergType iceberg.Type) (interface{}, error) {
	switch t := icebergType.(type) {
	case *iceberg.BooleanType:
		if boolArr, ok := arr.(*array.Boolean); ok {
			return boolArr.Value(index), nil
		}
	case *iceberg.Int32Type:
		if int32Arr, ok := arr.(*array.Int32); ok {
			return int32Arr.Value(index), nil
		}
	case *iceberg.Int64Type:
		if int64Arr, ok := arr.(*array.Int64); ok {
			return int64Arr.Value(index), nil
		}
	case *iceberg.Float32Type:
		if float32Arr, ok := arr.(*array.Float32); ok {
			return float32Arr.Value(index), nil
		}
	case *iceberg.Float64Type:
		if float64Arr, ok := arr.(*array.Float64); ok {
			return float64Arr.Value(index), nil
		}
	case *iceberg.StringType:
		if stringArr, ok := arr.(*array.String); ok {
			return stringArr.Value(index), nil
		}
	case *iceberg.BinaryType:
		if binaryArr, ok := arr.(*array.Binary); ok {
			return binaryArr.Value(index), nil
		}
	case *iceberg.DateType:
		if date32Arr, ok := arr.(*array.Date32); ok {
			return date32Arr.Value(index), nil
		}
	case *iceberg.TimestampType:
		if timestampArr, ok := arr.(*array.Timestamp); ok {
			return timestampArr.Value(index), nil
		}
	case *iceberg.ListType:
		if listArr, ok := arr.(*array.List); ok {
			start, end := listArr.ValueOffsets(index)
			var elements []interface{}
			valueArr := listArr.ListValues()
			for i := start; i < end; i++ {
				// Simplified - use string type for element type
				elem, err := c.extractArrowValue(valueArr, int(i), iceberg.PrimitiveTypes.String)
				if err != nil {
					return nil, err
				}
				elements = append(elements, elem)
			}

			return elements, nil
		}
	case *iceberg.StructType:
		if structArr, ok := arr.(*array.Struct); ok {
			result := make(map[string]interface{})
			for i, field := range t.FieldList {
				fieldArr := structArr.Field(i)
				value, err := c.extractArrowValue(fieldArr, index, field.Type)
				if err != nil {
					return nil, err
				}
				result[field.Name] = value
			}

			return result, nil
		}
	}

	return nil, fmt.Errorf("unsupported Arrow array type %T for Iceberg type %T", arr, icebergType)
}

// ConvertSparkRowToArrowRecord converts Spark row data to Arrow record
func (c *SparkValueConverter) ConvertSparkRowToArrowRecord(rows []interface{}, schema *iceberg.Schema) (arrow.Record, error) {
	// This would require building Arrow arrays from the row data
	// Implementation would be complex and depends on the specific Arrow builder pattern
	// For now, return an error indicating this needs implementation
	return nil, errors.New("conversion from Spark rows to Arrow records not yet implemented")
}
