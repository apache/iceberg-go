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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package internal

import (
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/iceberg-go"
	avro "github.com/twmb/avro"
)

// avroArrowSchema converts the OCF writer schema without passing through
// arrow/avro. Iceberg field IDs remain attached to their corresponding Arrow
// fields, including elements and map key/value fields.
func avroArrowSchema(schemaJSON string) (*arrow.Schema, error) {
	schema, err := avro.Parse(schemaJSON)
	if err != nil {
		return nil, fmt.Errorf("%w: parse Avro writer schema: %v", iceberg.ErrInvalidSchema, err)
	}
	root := schema.Root().ExpandReferences()
	if root.Type != "record" {
		return nil, fmt.Errorf("%w: Avro data must have a root record", iceberg.ErrInvalidSchema)
	}
	fields := make([]arrow.Field, len(root.Fields))
	used := make(map[int]struct{})
	for i, child := range root.Fields {
		fields[i], err = avroArrowField(child.Name, child.Type, child.Props, "field-id", used)
		if err != nil {
			return nil, fmt.Errorf("avro field %q: %w", child.Name, err)
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

func avroArrowField(name string, node avro.SchemaNode, props map[string]any, idProperty string, used map[int]struct{}) (arrow.Field, error) {
	typ, nullable, err := avroArrowType(node, used)
	if err != nil {
		return arrow.Field{}, err
	}

	return addAvroFieldID(arrow.Field{Name: name, Type: typ, Nullable: nullable}, props, idProperty, used)
}

func avroArrowType(node avro.SchemaNode, used map[int]struct{}) (arrow.DataType, bool, error) {
	nullable := false
	if node.Type == "union" {
		if len(node.Branches) != 2 {
			return nil, false, fmt.Errorf("%w: Avro unions must contain null and one value type", iceberg.ErrNotImplemented)
		}
		switch {
		case node.Branches[0].Type == "null" && node.Branches[1].Type != "null":
			node = node.Branches[1]
		case node.Branches[1].Type == "null" && node.Branches[0].Type != "null":
			node = node.Branches[0]
		default:
			return nil, false, fmt.Errorf("%w: invalid Avro nullable union", iceberg.ErrNotImplemented)
		}
		nullable = true
	}

	// Iceberg timestamp and other logical annotations must match their
	// Avro physical types. Do not silently reinterpret a different type.
	switch node.LogicalType {
	case "timestamp-micros", "timestamp-nanos", "local-timestamp-micros",
		"local-timestamp-nanos", "time-micros":
		if node.Type != "long" {
			return nil, false, fmt.Errorf("%w: Avro %s requires long, got %s", iceberg.ErrInvalidSchema, node.LogicalType, node.Type)
		}
	case "date", "time-millis":
		if node.Type != "int" {
			return nil, false, fmt.Errorf("%w: Avro %s requires int, got %s", iceberg.ErrInvalidSchema, node.LogicalType, node.Type)
		}
	case "decimal":
		if node.Type != "bytes" && node.Type != "fixed" {
			return nil, false, fmt.Errorf("%w: Avro decimal requires bytes or fixed, got %s", iceberg.ErrInvalidSchema, node.Type)
		}
	}

	switch node.Type {
	case "record":
		fields := make([]arrow.Field, len(node.Fields))
		for i, child := range node.Fields {
			field, err := avroArrowField(child.Name, child.Type, child.Props, "field-id", used)
			if err != nil {
				return nil, false, fmt.Errorf("record field %q: %w", child.Name, err)
			}
			fields[i] = field
		}

		return arrow.StructOf(fields...), nullable, nil

	case "array":
		if node.Items == nil {
			return nil, false, fmt.Errorf("%w: Avro array has no items", iceberg.ErrInvalidSchema)
		}
		if node.LogicalType == "map" {
			// Iceberg encodes non-string map keys as array<record<key,value>>.
			entry := *node.Items
			if entry.Type != "record" || len(entry.Fields) != 2 ||
				entry.Fields[0].Name != "key" || entry.Fields[1].Name != "value" {
				return nil, false, fmt.Errorf("%w: invalid Avro array-backed map", iceberg.ErrInvalidSchema)
			}
			keyProps := entry.Fields[0].Props
			valueProps := entry.Fields[1].Props
			if _, ok := keyProps["field-id"]; !ok {
				if id, exists := node.Props["key-id"]; exists {
					keyProps = map[string]any{"field-id": id}
				}
			}
			if _, ok := valueProps["field-id"]; !ok {
				if id, exists := node.Props["value-id"]; exists {
					valueProps = map[string]any{"field-id": id}
				}
			}
			key, err := avroArrowField("key", entry.Fields[0].Type, keyProps, "field-id", used)
			if err != nil {
				return nil, false, err
			}
			if key.Nullable {
				return nil, false, fmt.Errorf("%w: Avro map key must not be null", iceberg.ErrInvalidSchema)
			}
			value, err := avroArrowField("value", entry.Fields[1].Type, valueProps, "field-id", used)
			if err != nil {
				return nil, false, err
			}

			return arrow.MapOfFields(key, value), nullable, nil
		}
		element, err := avroArrowField("element", *node.Items, node.Props, "element-id", used)
		if err != nil {
			return nil, false, err
		}

		return arrow.ListOfField(element), nullable, nil

	case "map":
		if node.Values == nil {
			return nil, false, fmt.Errorf("%w: Avro map has no values", iceberg.ErrInvalidSchema)
		}
		key, err := addAvroFieldID(arrow.Field{Name: "key", Type: arrow.BinaryTypes.String}, node.Props, "key-id", used)
		if err != nil {
			return nil, false, err
		}
		value, err := avroArrowField("value", *node.Values, node.Props, "value-id", used)
		if err != nil {
			return nil, false, err
		}

		return arrow.MapOfFields(key, value), nullable, nil

	case "boolean":
		return arrow.FixedWidthTypes.Boolean, nullable, nil
	case "int":
		switch node.LogicalType {
		case "date":
			return arrow.FixedWidthTypes.Date32, nullable, nil
		case "time-millis":
			return &arrow.Time32Type{Unit: arrow.Millisecond}, nullable, nil
		default:
			return arrow.PrimitiveTypes.Int32, nullable, nil
		}
	case "long":
		switch node.LogicalType {
		case "timestamp-micros", "timestamp-nanos", "local-timestamp-micros", "local-timestamp-nanos":
			unit := arrow.Microsecond
			if strings.HasSuffix(node.LogicalType, "nanos") {
				unit = arrow.Nanosecond
			}
			tz := ""
			if v, exists := node.Props["adjust-to-utc"]; exists {
				utc, ok := v.(bool)
				if !ok {
					return nil, false, fmt.Errorf("%w: Avro adjust-to-utc must be a boolean", iceberg.ErrInvalidSchema)
				}
				if utc {
					tz = "UTC"
				}
			}

			return &arrow.TimestampType{Unit: unit, TimeZone: tz}, nullable, nil
		case "time-micros":
			return &arrow.Time64Type{Unit: arrow.Microsecond}, nullable, nil
		default:
			return arrow.PrimitiveTypes.Int64, nullable, nil
		}
	case "float":
		return arrow.PrimitiveTypes.Float32, nullable, nil
	case "double":
		return arrow.PrimitiveTypes.Float64, nullable, nil
	case "enum", "string":
		return arrow.BinaryTypes.String, nullable, nil
	case "bytes", "fixed":
		if node.LogicalType == "uuid" {
			if node.Type != "fixed" || node.Size != 16 {
				return nil, false, fmt.Errorf("%w: Avro UUID must use fixed(16)", iceberg.ErrInvalidSchema)
			}

			return extensions.NewUUIDType(), nullable, nil
		}
		if node.LogicalType == "decimal" {
			if node.Precision < 1 || node.Precision > 38 || node.Scale < 0 || node.Scale > node.Precision {
				return nil, false, fmt.Errorf("%w: invalid or unsupported Avro decimal(%d,%d)", iceberg.ErrNotImplemented, node.Precision, node.Scale)
			}

			return &arrow.Decimal128Type{Precision: int32(node.Precision), Scale: int32(node.Scale)}, nullable, nil
		}
		if node.Type == "fixed" {
			if node.Size <= 0 {
				return nil, false, fmt.Errorf("%w: invalid Avro fixed size", iceberg.ErrInvalidSchema)
			}

			return &arrow.FixedSizeBinaryType{ByteWidth: node.Size}, nullable, nil
		}

		return arrow.BinaryTypes.Binary, nullable, nil
	default:
		return nil, false, fmt.Errorf("%w: unsupported Avro type %q", iceberg.ErrNotImplemented, node.Type)
	}
}

// appendAvroValue writes the native Go values returned by twmb/avro directly
// to Arrow builders. Every nested field is handled recursively.
func appendAvroValue(b array.Builder, value any) error {
	if value == nil {
		b.AppendNull()

		return nil
	}
	switch builder := b.(type) {
	case *array.BooleanBuilder:
		if v, ok := value.(bool); ok {
			builder.Append(v)

			return nil
		}
	case *array.Int32Builder:
		if v, ok := value.(int32); ok {
			builder.Append(v)

			return nil
		}
	case *array.Int64Builder:
		if v, ok := value.(int64); ok {
			builder.Append(v)

			return nil
		}
	case *array.Float32Builder:
		if v, ok := value.(float32); ok {
			builder.Append(v)

			return nil
		}
	case *array.Float64Builder:
		if v, ok := value.(float64); ok {
			builder.Append(v)
			return nil
		}
	case *array.StringBuilder:
		if v, ok := value.(string); ok {
			builder.Append(v)
			return nil
		}
	case *array.BinaryBuilder:
		if v, ok := value.([]byte); ok {
			builder.Append(v)
			return nil
		}
	case *extensions.UUIDBuilder:
		v, ok := avroFixedBytes(value)
		if ok && len(v) == 16 {
			var id [16]byte
			copy(id[:], v)
			builder.AppendBytes(id)

			return nil
		}
	case *array.FixedSizeBinaryBuilder:
		v, ok := avroFixedBytes(value)
		if ok && len(v) == builder.Type().(*arrow.FixedSizeBinaryType).ByteWidth {
			builder.Append(v)
			return nil
		}
	case *array.Decimal128Builder:
		if v, ok := value.(*big.Rat); ok {
			typ := builder.Type().(*arrow.Decimal128Type)
			scaled := new(big.Rat).Mul(v, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(typ.Scale)), nil)))
			if scaled.IsInt() {
				n := scaled.Num()
				if n.BitLen() <= 127 && len(strings.TrimPrefix(n.String(), "-")) <= int(typ.Precision) {
					builder.Append(decimal128.FromBigInt(n))
					return nil
				}
			}
			return fmt.Errorf("%w: Avro decimal exceeds declared precision or scale", iceberg.ErrInvalidSchema)
		}
	case *array.TimestampBuilder:
		typ := builder.Type().(*arrow.TimestampType)
		switch v := value.(type) {
		case int64:
			builder.Append(arrow.Timestamp(v))
			return nil
		case time.Time:
			if typ.Unit == arrow.Nanosecond {
				builder.Append(arrow.Timestamp(v.UnixNano()))
			} else {
				builder.Append(arrow.Timestamp(v.UnixMicro()))
			}
			return nil
		}
	case *array.Date32Builder:
		switch v := value.(type) {
		case int32:
			builder.Append(arrow.Date32(v))
			return nil
		case time.Time:
			builder.Append(arrow.Date32(v.Unix() / 86400))
			return nil
		}
	case *array.Time32Builder:
		switch v := value.(type) {
		case int32:
			builder.Append(arrow.Time32(v))
			return nil
		case time.Duration:
			builder.Append(arrow.Time32(v / time.Millisecond))
			return nil
		}
	case *array.Time64Builder:
		switch v := value.(type) {
		case int64:
			builder.Append(arrow.Time64(v))
			return nil
		case time.Duration:
			builder.Append(arrow.Time64(v / time.Microsecond))
			return nil
		}
	case *array.StructBuilder:
		fields, ok := value.(map[string]any)
		if !ok {
			break
		}
		typ := builder.Type().(*arrow.StructType)
		builder.Append(true)
		for i, field := range typ.Fields() {
			v, present := fields[field.Name]
			if (!present || v == nil) && !field.Nullable {
				return fmt.Errorf("%w: required struct field %q is null", iceberg.ErrInvalidSchema, field.Name)
			}
			if err := appendAvroValue(builder.FieldBuilder(i), v); err != nil {
				return fmt.Errorf("struct field %q: %w", field.Name, err)
			}
		}
		return nil
	case *array.ListBuilder:
		values, ok := value.([]any)
		if !ok {
			break
		}
		element := builder.Type().(*arrow.ListType).ElemField()
		builder.Append(true)
		for _, v := range values {
			if v == nil && !element.Nullable {
				return fmt.Errorf("%w: null Avro list element", iceberg.ErrInvalidSchema)
			}
			if err := appendAvroValue(builder.ValueBuilder(), v); err != nil {
				return err
			}
		}
		return nil
	case *array.MapBuilder:
		builder.Append(true)
		switch values := value.(type) {
		case map[string]any:
			for key, v := range values {
				if v == nil && !builder.Type().(*arrow.MapType).ItemField().Nullable {
					return fmt.Errorf("%w: null Avro map value", iceberg.ErrInvalidSchema)
				}
				if err := appendAvroValue(builder.KeyBuilder(), key); err != nil {
					return err
				}
				if err := appendAvroValue(builder.ItemBuilder(), v); err != nil {
					return err
				}
			}
			return nil
		case []any:
			for _, item := range values {
				pair, ok := item.(map[string]any)
				if !ok || pair["key"] == nil {
					return fmt.Errorf("%w: invalid Avro array-backed map entry", iceberg.ErrInvalidSchema)
				}
				if pair["value"] == nil && !builder.Type().(*arrow.MapType).ItemField().Nullable {
					return fmt.Errorf("%w: null Avro map value", iceberg.ErrInvalidSchema)
				}
				if err := appendAvroValue(builder.KeyBuilder(), pair["key"]); err != nil {
					return err
				}
				if err := appendAvroValue(builder.ItemBuilder(), pair["value"]); err != nil {
					return err
				}
			}
			return nil
		}
	}

	return fmt.Errorf("%w: Avro value %T cannot be decoded into %s", iceberg.ErrInvalidSchema, value, b.Type())
}

func avroFixedBytes(value any) ([]byte, bool) {
	if b, ok := value.([]byte); ok {
		return b, true
	}
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Array || v.Type().Elem().Kind() != reflect.Uint8 {
		return nil, false
	}
	b := make([]byte, v.Len())
	for i := range b {
		b[i] = byte(v.Index(i).Uint())
	}
	return b, true
}
