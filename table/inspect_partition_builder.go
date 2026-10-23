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
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

type inspectPartitionBuilder struct {
	builder *array.StructBuilder
	fields  []inspectPartitionFieldBuilder
}

type inspectPartitionFieldBuilder struct {
	id          int
	name        string
	typ         iceberg.Type
	arrowType   arrow.DataType
	builder     array.Builder
	appendValue func(any) error
}

func newInspectPartitionBuilder(
	builder *array.StructBuilder,
	partitionType *iceberg.StructType,
) (*inspectPartitionBuilder, error) {
	arrowType, ok := builder.Type().(*arrow.StructType)
	if !ok {
		return nil, fmt.Errorf("partition builder has type %T, want struct", builder.Type())
	}
	lookup, err := newInspectBuilderLookup("partition", arrowType.Fields(), builder.FieldBuilder)
	if err != nil {
		return nil, err
	}
	expected := make(map[int]struct{}, len(partitionType.FieldList))
	for _, field := range partitionType.FieldList {
		expected[field.ID] = struct{}{}
	}
	if err := validateInspectBuilderLookup("partition", lookup, expected); err != nil {
		return nil, err
	}

	fields := make([]inspectPartitionFieldBuilder, 0, len(partitionType.FieldList))
	for _, field := range partitionType.FieldList {
		fieldBuilder := lookup[field.ID]
		partitionField := inspectPartitionFieldBuilder{
			id:        field.ID,
			name:      field.Name,
			typ:       field.Type,
			arrowType: fieldBuilder.Type(),
			builder:   fieldBuilder,
		}
		partitionField.appendValue = newInspectPartitionFieldAppender(partitionField)
		fields = append(fields, partitionField)
	}

	return &inspectPartitionBuilder{builder: builder, fields: fields}, nil
}

func (b *inspectPartitionBuilder) append(values map[int]any) error {
	b.builder.Append(true)
	for _, field := range b.fields {
		if err := field.appendValue(values[field.id]); err != nil {
			return err
		}
	}

	return nil
}

func newInspectPartitionFieldAppender(field inspectPartitionFieldBuilder) func(any) error {
	appendScalar := func(value any) error {
		sc, err := inspectValueScalar(value, field.typ, field.arrowType)
		if err != nil {
			return fmt.Errorf("partition field %q: %w", field.name, err)
		}

		return scalar.Append(field.builder, sc)
	}
	appendNull := func() {
		field.builder.AppendNull()
	}

	switch field.typ.(type) {
	case iceberg.BooleanType:
		if builder, ok := field.builder.(*array.BooleanBuilder); ok {
			return newInspectTypedPartitionAppender(builder.Append, builder.AppendNull, appendScalar)
		}
	case iceberg.Int32Type:
		if builder, ok := field.builder.(*array.Int32Builder); ok {
			return newInspectTypedPartitionAppender(builder.Append, builder.AppendNull, appendScalar)
		}
	case iceberg.Int64Type:
		if builder, ok := field.builder.(*array.Int64Builder); ok {
			return newInspectTypedPartitionAppender(builder.Append, builder.AppendNull, appendScalar)
		}
	case iceberg.Float32Type:
		if builder, ok := field.builder.(*array.Float32Builder); ok {
			return newInspectTypedPartitionAppender(builder.Append, builder.AppendNull, appendScalar)
		}
	case iceberg.Float64Type:
		if builder, ok := field.builder.(*array.Float64Builder); ok {
			return newInspectTypedPartitionAppender(builder.Append, builder.AppendNull, appendScalar)
		}
	case iceberg.StringType:
		if builder, ok := field.builder.(*array.StringBuilder); ok {
			return newInspectTypedPartitionAppender(builder.Append, builder.AppendNull, appendScalar)
		}
	case iceberg.BinaryType:
		if builder, ok := field.builder.(*array.BinaryBuilder); ok {
			return newInspectTypedPartitionAppender(builder.Append, builder.AppendNull, appendScalar)
		}
	}

	return func(value any) error {
		if value == nil {
			appendNull()

			return nil
		}

		return appendScalar(value)
	}
}

func newInspectTypedPartitionAppender[T any](
	appendValue func(T),
	appendNull func(),
	appendScalar func(any) error,
) func(any) error {
	return func(value any) error {
		if value == nil {
			appendNull()

			return nil
		}
		typed, ok := value.(T)
		if !ok {
			return appendScalar(value)
		}
		appendValue(typed)

		return nil
	}
}

func inspectValueScalar(value any, typ iceberg.Type, arrowType arrow.DataType) (scalar.Scalar, error) {
	switch typ.(type) {
	case iceberg.DateType:
		switch value := value.(type) {
		case iceberg.Date:
			return scalar.NewDate32Scalar(arrow.Date32(value)), nil
		case int32:
			return scalar.NewDate32Scalar(arrow.Date32(value)), nil
		default:
			return nil, fmt.Errorf("unsupported date partition value %T", value)
		}
	case iceberg.TimeType:
		if value, ok := value.(iceberg.Time); ok {
			return scalar.NewTime64Scalar(arrow.Time64(value), arrowType), nil
		}

		return nil, fmt.Errorf("unsupported time partition value %T", value)
	case iceberg.TimestampType, iceberg.TimestampTzType:
		if value, ok := value.(iceberg.Timestamp); ok {
			return scalar.NewTimestampScalar(arrow.Timestamp(value), arrowType), nil
		}

		return nil, fmt.Errorf("unsupported timestamp partition value %T", value)
	case iceberg.TimestampNsType, iceberg.TimestampTzNsType:
		if value, ok := value.(iceberg.TimestampNano); ok {
			return scalar.NewTimestampScalar(arrow.Timestamp(value), arrowType), nil
		}

		return nil, fmt.Errorf("unsupported nanosecond timestamp partition value %T", value)
	case iceberg.UUIDType:
		if value, ok := value.(uuid.UUID); ok {
			return scalar.MakeScalarParam(value[:], arrowType)
		}

		return nil, fmt.Errorf("unsupported UUID partition value %T", value)
	case iceberg.DecimalType:
		switch value := value.(type) {
		case iceberg.DecimalLiteral:
			return scalar.NewDecimal128Scalar(value.Val, arrowType), nil
		case iceberg.Decimal:
			return scalar.NewDecimal128Scalar(value.Val, arrowType), nil
		default:
			return nil, fmt.Errorf("unsupported decimal partition value %T", value)
		}
	}

	return scalar.MakeScalarParam(value, arrowType)
}
