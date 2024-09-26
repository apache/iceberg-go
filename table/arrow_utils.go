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
	"slices"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
)

const (
	ArrowFieldDocKey       = "doc"
	ArrowParquetFieldIDKey = "PARQUET:field_id"
)

type ArrowSchemaVisitor[T any] interface {
	Schema(*arrow.Schema, T) T
	Struct(*arrow.StructType, []T) T
	Field(arrow.Field, T) T
	List(arrow.ListLikeType, T) T
	Map(mt *arrow.MapType, keyResult T, valueResult T) T
	Primitive(arrow.DataType) T
}

func recoverError(err *error) {
	if r := recover(); r != nil {
		switch e := r.(type) {
		case string:
			*err = fmt.Errorf("error encountered during arrow schema visitor: %s", e)
		case error:
			*err = fmt.Errorf("error encountered during arrow schema visitor: %w", e)
		}
	}
}

func VisitArrowSchema[T any](sc *arrow.Schema, visitor ArrowSchemaVisitor[T]) (res T, err error) {
	if sc == nil {
		err = fmt.Errorf("%w: cannot visit nil arrow schema", iceberg.ErrInvalidArgument)
		return
	}

	defer recoverError(&err)

	return visitor.Schema(sc, visitArrowStruct(arrow.StructOf(sc.Fields()...), visitor)), err
}

func visitArrowField[T any](f arrow.Field, visitor ArrowSchemaVisitor[T]) T {
	switch typ := f.Type.(type) {
	case *arrow.StructType:
		return visitArrowStruct(typ, visitor)
	case *arrow.MapType:
		return visitArrowMap(typ, visitor)
	case arrow.ListLikeType:
		return visitArrowList(typ, visitor)
	default:
		return visitor.Primitive(typ)
	}
}

func visitArrowStruct[T any](dt *arrow.StructType, visitor ArrowSchemaVisitor[T]) T {
	type (
		beforeField interface {
			BeforeField(arrow.Field)
		}
		afterField interface {
			AfterField(arrow.Field)
		}
	)

	results := make([]T, dt.NumFields())
	bf, _ := visitor.(beforeField)
	af, _ := visitor.(afterField)

	for i, f := range dt.Fields() {
		if bf != nil {
			bf.BeforeField(f)
		}

		res := visitArrowField(f, visitor)

		if af != nil {
			af.AfterField(f)
		}

		results[i] = visitor.Field(f, res)
	}

	return visitor.Struct(dt, results)
}

func visitArrowMap[T any](dt *arrow.MapType, visitor ArrowSchemaVisitor[T]) T {
	type (
		beforeMapKey interface {
			BeforeMapKey(arrow.Field)
		}
		beforeMapValue interface {
			BeforeMapValue(arrow.Field)
		}
		afterMapKey interface {
			AfterMapKey(arrow.Field)
		}
		afterMapValue interface {
			AfterMapValue(arrow.Field)
		}
	)

	key, val := dt.KeyField(), dt.ItemField()

	if bmk, ok := visitor.(beforeMapKey); ok {
		bmk.BeforeMapKey(key)
	}

	keyResult := visitArrowField(key, visitor)

	if amk, ok := visitor.(afterMapKey); ok {
		amk.AfterMapKey(key)
	}

	if bmv, ok := visitor.(beforeMapValue); ok {
		bmv.BeforeMapValue(val)
	}

	valueResult := visitArrowField(val, visitor)

	if amv, ok := visitor.(afterMapValue); ok {
		amv.AfterMapValue(val)
	}

	return visitor.Map(dt, keyResult, valueResult)
}

func visitArrowList[T any](dt arrow.ListLikeType, visitor ArrowSchemaVisitor[T]) T {
	type (
		beforeListElem interface {
			BeforeListElement(arrow.Field)
		}
		afterListElem interface {
			AfterListElement(arrow.Field)
		}
	)

	elemField := dt.ElemField()

	if bl, ok := visitor.(beforeListElem); ok {
		bl.BeforeListElement(elemField)
	}

	res := visitArrowField(elemField, visitor)

	if al, ok := visitor.(afterListElem); ok {
		al.AfterListElement(elemField)
	}

	return visitor.List(dt, res)
}

func getFieldID(f arrow.Field) *int {
	if !f.HasMetadata() {
		return nil
	}

	fieldIDStr, ok := f.Metadata.GetValue(ArrowParquetFieldIDKey)
	if !ok {
		return nil
	}

	id, err := strconv.Atoi(fieldIDStr)
	if err != nil {
		return nil
	}

	return &id
}

type hasIDs struct{}

func (hasIDs) Schema(sc *arrow.Schema, result bool) bool {
	return result
}

func (hasIDs) Struct(st *arrow.StructType, results []bool) bool {
	return !slices.Contains(results, false)
}

func (hasIDs) Field(f arrow.Field, result bool) bool {
	return getFieldID(f) != nil
}

func (hasIDs) List(dt arrow.ListLikeType, elem bool) bool {
	elemField := dt.ElemField()
	return elem && getFieldID(elemField) != nil
}

func (hasIDs) Map(m *arrow.MapType, key, val bool) bool {
	return key && val &&
		getFieldID(m.KeyField()) != nil && getFieldID(m.ItemField()) != nil
}

func (hasIDs) Primitive(arrow.DataType) bool { return true }

type convertToIceberg struct {
	downcastTimestamp bool

	fieldID func(arrow.Field) int
}

func (convertToIceberg) Schema(_ *arrow.Schema, result iceberg.NestedField) iceberg.NestedField {
	return result
}

func (convertToIceberg) Struct(_ *arrow.StructType, results []iceberg.NestedField) iceberg.NestedField {
	return iceberg.NestedField{
		Type: &iceberg.StructType{FieldList: results},
	}
}

func (c convertToIceberg) Field(field arrow.Field, result iceberg.NestedField) iceberg.NestedField {
	result.ID = c.fieldID(field)
	if field.HasMetadata() {
		if doc, ok := field.Metadata.GetValue(ArrowFieldDocKey); ok {
			result.Doc = doc
		}
	}

	result.Required = !field.Nullable
	result.Name = field.Name
	return result
}

func (c convertToIceberg) List(dt arrow.ListLikeType, elemResult iceberg.NestedField) iceberg.NestedField {
	elemField := dt.ElemField()
	elemID := c.fieldID(elemField)

	return iceberg.NestedField{
		Type: &iceberg.ListType{
			ElementID:       elemID,
			Element:         elemResult.Type,
			ElementRequired: !elemField.Nullable,
		},
	}
}

func (c convertToIceberg) Map(m *arrow.MapType, keyResult, valueResult iceberg.NestedField) iceberg.NestedField {
	keyField, valField := m.KeyField(), m.ItemField()
	keyID, valID := c.fieldID(keyField), c.fieldID(valField)

	return iceberg.NestedField{
		Type: &iceberg.MapType{
			KeyID:         keyID,
			KeyType:       keyResult.Type,
			ValueID:       valID,
			ValueType:     valueResult.Type,
			ValueRequired: !valField.Nullable,
		},
	}
}

var (
	utcAliases = []string{"UTC", "+00:00", "Etc/UTC", "Z"}
)

func (c convertToIceberg) Primitive(dt arrow.DataType) (result iceberg.NestedField) {
	switch dt := dt.(type) {
	case *arrow.DictionaryType:
		if _, ok := dt.ValueType.(arrow.NestedType); ok {
			panic(fmt.Errorf("%w: unsupported arrow type for conversion - %s", iceberg.ErrInvalidSchema, dt))
		}
		return c.Primitive(dt.ValueType)
	case *arrow.RunEndEncodedType:
		if _, ok := dt.Encoded().(arrow.NestedType); ok {
			panic(fmt.Errorf("%w: unsupported arrow type for conversion - %s", iceberg.ErrInvalidSchema, dt))
		}
		return c.Primitive(dt.Encoded())
	case *arrow.BooleanType:
		result.Type = iceberg.PrimitiveTypes.Bool
	case *arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type,
		*arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type:
		result.Type = iceberg.PrimitiveTypes.Int32
	case *arrow.Uint64Type, *arrow.Int64Type:
		result.Type = iceberg.PrimitiveTypes.Int64
	case *arrow.Float16Type, *arrow.Float32Type:
		result.Type = iceberg.PrimitiveTypes.Float32
	case *arrow.Float64Type:
		result.Type = iceberg.PrimitiveTypes.Float64
	case *arrow.Decimal32Type, *arrow.Decimal64Type, *arrow.Decimal128Type:
		dec := dt.(arrow.DecimalType)
		result.Type = iceberg.DecimalTypeOf(int(dec.GetPrecision()), int(dec.GetScale()))
	case *arrow.StringType, *arrow.LargeStringType:
		result.Type = iceberg.PrimitiveTypes.String
	case *arrow.BinaryType, *arrow.LargeBinaryType:
		result.Type = iceberg.PrimitiveTypes.Binary
	case *arrow.Date32Type:
		result.Type = iceberg.PrimitiveTypes.Date
	case *arrow.Time64Type:
		if dt.Unit == arrow.Microsecond {
			result.Type = iceberg.PrimitiveTypes.Time
		} else {
			panic(fmt.Errorf("%w: unsupported arrow type for conversion - %s", iceberg.ErrInvalidSchema, dt))
		}
	case *arrow.TimestampType:
		if dt.Unit == arrow.Nanosecond {
			if !c.downcastTimestamp {
				panic(fmt.Errorf("%w: 'ns' timestamp precision not supported", iceberg.ErrType))
			}
			// TODO: log something
		}

		if slices.Contains(utcAliases, dt.TimeZone) {
			result.Type = iceberg.PrimitiveTypes.TimestampTz
		} else if dt.TimeZone == "" {
			result.Type = iceberg.PrimitiveTypes.Timestamp
		} else {
			panic(fmt.Errorf("%w: unsupported arrow type for conversion - %s", iceberg.ErrInvalidSchema, dt))
		}
	case *arrow.FixedSizeBinaryType:
		result.Type = iceberg.FixedTypeOf(dt.ByteWidth)
	case arrow.ExtensionType:
		if dt.ExtensionName() == "arrow.uuid" {
			result.Type = iceberg.PrimitiveTypes.UUID
		} else {
			panic(fmt.Errorf("%w: unsupported arrow type for conversion - %s", iceberg.ErrInvalidSchema, dt))
		}
	default:
		panic(fmt.Errorf("%w: unsupported arrow type for conversion - %s", iceberg.ErrInvalidSchema, dt))
	}

	return
}

func ArrowTypeToIceberg(dt arrow.DataType, downcastNsTimestamp bool) (iceberg.Type, error) {
	sc := arrow.NewSchema([]arrow.Field{{Type: dt,
		Metadata: arrow.NewMetadata([]string{ArrowParquetFieldIDKey}, []string{"1"})}}, nil)

	out, err := VisitArrowSchema(sc, convertToIceberg{
		downcastTimestamp: downcastNsTimestamp,
		fieldID: func(field arrow.Field) int {
			if id := getFieldID(field); id != nil {
				return *id
			}

			panic(fmt.Errorf("%w: cannot convert %s to Iceberg field, missing field_id",
				iceberg.ErrInvalidSchema, field))
		},
	})
	if err != nil {
		return nil, err
	}

	return out.Type.(*iceberg.StructType).FieldList[0].Type, nil
}

func ArrowSchemaToIceberg(sc *arrow.Schema, downcastNsTimestamp bool, nameMapping NameMapping) (*iceberg.Schema, error) {
	hasIDs, _ := VisitArrowSchema(sc, hasIDs{})

	switch {
	case hasIDs:
		out, err := VisitArrowSchema(sc, convertToIceberg{
			downcastTimestamp: downcastNsTimestamp,
			fieldID: func(field arrow.Field) int {
				if id := getFieldID(field); id != nil {
					return *id
				}

				panic(fmt.Errorf("%w: cannot convert %s to Iceberg field, missing field_id",
					iceberg.ErrInvalidSchema, field))
			},
		})
		if err != nil {
			return nil, err
		}

		return iceberg.NewSchema(0, out.Type.(*iceberg.StructType).FieldList...), nil
	case nameMapping != nil:
		withoutIDs, err := VisitArrowSchema(sc, convertToIceberg{
			downcastTimestamp: downcastNsTimestamp,
			fieldID:           func(_ arrow.Field) int { return -1 },
		})
		if err != nil {
			return nil, err
		}

		schemaWithoutIDs := iceberg.NewSchema(0, withoutIDs.Type.(*iceberg.StructType).FieldList...)
		return ApplyNameMapping(schemaWithoutIDs, nameMapping)
	default:
		return nil, fmt.Errorf("%w: arrow schema does not have field-ids and no name mapping provided",
			iceberg.ErrInvalidSchema)
	}
}
