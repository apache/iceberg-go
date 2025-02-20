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
	"reflect"
	"slices"
	"strconv"

	"github.com/hamba/avro/v2"
)

func AvroSchemaToType(sch avro.Schema) (Type, error) {
	switch sch.Type() {
	case avro.Boolean:
		return &BooleanType{}, nil

	case avro.Fixed:
		var fixedSchema = sch.(*avro.FixedSchema)

		// For an unknwon reason, with uuid, fixedSchema.Logical() returns nil
		// even if logicalType prop is filled.
		if fixedSchema.Prop("logicalType") == nil {
			return FixedTypeOf(fixedSchema.Size()), nil
		}

		s, err := tryCast[string](fixedSchema.Prop("logicalType"))

		if err != nil {
			return nil, err
		}

		switch avro.LogicalType(s) {
		case avro.UUID:
			if fixedSchema.Size() != 16 {
				return nil, fmt.Errorf("cannot convert Avro schema: %s", sch.String())
			}

			return UUIDType{}, nil
		default:
			return nil, fmt.Errorf("cannot convert Avro schema: %s", sch.String())
		}

	case avro.Int:
		var primSchema = sch.(*avro.PrimitiveSchema)

		if primSchema.Logical() == nil {
			return &Int32Type{}, nil
		}

		switch primSchema.Logical().Type() {
		case avro.Date:
			return &DateType{}, nil
		default:
			return nil, fmt.Errorf("cannot convert Avro schema: %s", sch.String())
		}

	case avro.Long:
		var primSchema = sch.(*avro.PrimitiveSchema)

		if primSchema.Logical() == nil {
			return &Int64Type{}, nil
		}

		switch primSchema.Logical().Type() {
		case avro.TimeMicros:
			return &TimeType{}, nil
		case avro.TimestampMicros:
			b, err := getTypedProp[bool](sch.(*avro.PrimitiveSchema), "adjust-to-utc")

			if err != nil {
				return nil, err
			}

			if b {
				return &TimestampTzType{}, nil
			} else {
				return &TimestampType{}, nil
			}

		default:
			return nil, fmt.Errorf("cannot convert Avro schema: %s", sch.String())
		}

	case avro.String:
		return &StringType{}, nil
	case avro.Bytes:
		return &BinaryType{}, nil
	case avro.Float:
		return &Float32Type{}, nil
	case avro.Double:
		return &Float64Type{}, nil

	case avro.Record:
		var (
			recordSch = sch.(*avro.RecordSchema)
			st        StructType
		)

		for _, field := range recordSch.Fields() {
			nestedField, err := AvroFieldToNestedField(field)

			if err != nil {
				return nil, err
			}

			st.FieldList = append(st.FieldList, nestedField)
		}

		return &st, nil

	case avro.Array:
		var (
			listSch = sch.(*avro.ArraySchema)
			lt      ListType
		)

		required, schema, err := unwrapAvroSchema(listSch.Items())

		if err != nil {
			return nil, err
		}

		lt.ElementRequired = required

		elemT, err := AvroSchemaToType(schema)

		if err != nil {
			return nil, err
		}

		lt.Element = elemT
		lt.ElementID, err = getTypedProp[int](listSch, "element-id")

		if err != nil {
			return nil, err
		}

		return &lt, nil

	default:
		return nil, fmt.Errorf("cannot convert Avro schema: %s", sch.String())
	}
}

func AvroFieldToNestedField(field *avro.Field) (NestedField, error) {
	var (
		nestedField NestedField
		err         error
	)

	nestedField.ID, err = getTypedProp[int](field, "field-id")

	if err != nil {
		return nestedField, err
	}

	nestedField.Name = field.Name()
	nestedField.Doc = field.Doc()

	if v := field.Default(); v != nil {
		nestedField.WriteDefault = v
	}

	required, schema, err := unwrapAvroSchema(field.Type())

	if err != nil {
		return nestedField, err
	}

	nestedField.Required = required
	nestedField.Type, err = AvroSchemaToType(schema)

	if err != nil {
		return nestedField, err
	}

	return nestedField, nil
}

func NestedFieldToAvroField(f NestedField) (*avro.Field, error) {
	sch, err := TypeToAvroSchema(strconv.FormatInt(int64(f.ID), 10), f.Type)

	if err != nil {
		return nil, err
	}

	var opts = []avro.SchemaOption{
		avro.WithProps(map[string]any{"field-id": f.ID}),
	}

	if !f.Required {
		sch, err = avro.NewUnionSchema([]avro.Schema{
			avro.NewNullSchema(),
			sch,
		})

		if err != nil {
			return nil, err
		}
	}

	if len(f.Doc) > 0 {
		opts = append(opts, avro.WithDoc(f.Doc))
	}

	if f.WriteDefault != nil {
		opts = append(opts, avro.WithDefault(f.WriteDefault))
	}

	return avro.NewField(
		f.Name,
		sch,
		opts...,
	)
}

func TypeToAvroSchema(recordName string, t Type, opts ...avro.SchemaOption) (avro.Schema, error) {
	var (
		sch avro.Schema
		err error
	)

	switch t := t.(type) {
	case *StringType, StringType:
		sch = avro.NewPrimitiveSchema(avro.String, nil, opts...)
	case *Int32Type, Int32Type:
		sch = avro.NewPrimitiveSchema(avro.Int, nil, opts...)
	case *Int64Type, Int64Type:
		sch = avro.NewPrimitiveSchema(avro.Long, nil, opts...)
	case *BinaryType, BinaryType:
		sch = avro.NewPrimitiveSchema(avro.Bytes, nil, opts...)
	case *BooleanType, BooleanType:
		sch = avro.NewPrimitiveSchema(avro.Boolean, nil, opts...)
	case *Float32Type, Float32Type:
		sch = avro.NewPrimitiveSchema(avro.Float, nil, opts...)
	case *Float64Type, Float64Type:
		sch = avro.NewPrimitiveSchema(avro.Double, nil, opts...)
	case *DateType, DateType:
		sch = avro.NewPrimitiveSchema(avro.Int, avro.NewPrimitiveLogicalSchema(avro.Date), opts...)
	case *TimeType, TimeType:
		sch = avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimeMicros), opts...)
	case *TimestampType, TimestampType:
		sch = avro.NewPrimitiveSchema(
			avro.Long,
			avro.NewPrimitiveLogicalSchema(avro.TimeMicros),
			slices.Concat(
				[]avro.SchemaOption{
					avro.WithProps(map[string]any{"adjust-to-utc": false}),
				},
				opts,
			)...,
		)
	case *TimestampTzType, TimestampTzType:
		sch = avro.NewPrimitiveSchema(
			avro.Long,
			avro.NewPrimitiveLogicalSchema(avro.TimeMicros),
			slices.Concat(
				[]avro.SchemaOption{
					avro.WithProps(map[string]any{"adjust-to-utc": true}),
				},
				opts,
			)...,
		)

	case *UUIDType, UUIDType:
		sch, err = avro.NewFixedSchema("uuid_fixed", "", 16, avro.NewPrimitiveLogicalSchema(avro.UUID))

	case *ListType:
		elem, err := TypeToAvroSchema(
			fmt.Sprintf("r%d", t.ElementID),
			t.Element,
		)

		if err != nil {
			return nil, err
		}

		sch = avro.NewArraySchema(
			elem,
			slices.Concat(
				[]avro.SchemaOption{
					avro.WithProps(map[string]any{"element-id": t.ElementID}),
				},
				opts,
			)...,
		)

	case *StructType:
		var aFields = make([]*avro.Field, 0)

		for _, field := range t.Fields() {
			aField, err := NestedFieldToAvroField(field)

			if err != nil {
				return nil, err
			}

			aFields = append(aFields, aField)
		}

		return avro.NewRecordSchema(recordName, "", aFields)

	default:
		return nil, fmt.Errorf("cannot convert Iceberg type: %s", t.String())
	}

	if err != nil {
		return nil, err
	}

	return sch, nil
}

func unwrapAvroSchema(sch avro.Schema) (bool, avro.Schema, error) {
	var u, ok = sch.(*avro.UnionSchema)

	if !ok {
		return true, sch, nil
	}

	if len(u.Type()) == 0 || len(u.Types()) > 2 {
		return false, nil, fmt.Errorf("union must have exactly 2 components: %s", u.String())
	}

	if u.Types()[0].Type() != avro.Null {
		return false, nil, fmt.Errorf("union first component must be null: %s", u.String())
	}

	return false, u.Types()[1], nil
}

func getTypedProp[T any](p interface {
	Prop(string) any
	String() string
}, propName string) (T, error) {
	var (
		zero T
		v    = p.Prop(propName)
	)

	if v == nil {
		return zero, fmt.Errorf("cannot find `%s` property in %s", propName, p.String())
	}

	return tryCast[T](v)
}

func tryCast[T any](i any) (T, error) {
	var (
		zero T
		t    = reflect.TypeFor[T]()
		v    = reflect.ValueOf(i)
	)

	if !v.CanConvert(t) {
		return zero, fmt.Errorf("cannot convert %v to %v", v, t)
	}

	return v.Convert(t).Interface().(T), nil
}
