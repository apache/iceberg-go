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
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"iter"
	"log/slog"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/config"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
	"github.com/pterm/pterm"
	"golang.org/x/sync/errgroup"
)

// constants to look for as Keys in Arrow field metadata
const (
	ArrowFieldDocKey = "doc"
	// Arrow schemas that are generated from the Parquet library will utilize
	// this key to identify the field id of the source Parquet field.
	// We use this when converting to Iceberg to provide field IDs
	ArrowParquetFieldIDKey = "PARQUET:field_id"

	defaultBinPackLookback = 20
)

// ArrowSchemaVisitor is an interface that can be implemented and used to
// call VisitArrowSchema for iterating
type ArrowSchemaVisitor[T any] interface {
	Schema(*arrow.Schema, T) T
	Struct(*arrow.StructType, []T) T
	Field(arrow.Field, T) T
	List(arrow.ListLikeType, T) T
	Map(mt *arrow.MapType, keyResult T, valueResult T) T
	Primitive(arrow.DataType) T
}

func VisitArrowSchema[T any](sc *arrow.Schema, visitor ArrowSchemaVisitor[T]) (res T, err error) {
	if sc == nil {
		err = fmt.Errorf("%w: cannot visit nil arrow schema", iceberg.ErrInvalidArgument)

		return res, err
	}

	defer internal.RecoverError(&err)

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

	if id > 0 {
		return &id
	}

	return nil
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

var utcAliases = []string{"UTC", "+00:00", "Etc/UTC", "Z"}

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
	case *arrow.NullType:
		result.Type = iceberg.PrimitiveTypes.Unknown
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
		if dt.Unit == arrow.Nanosecond && !c.downcastTimestamp {
			if slices.Contains(utcAliases, dt.TimeZone) {
				result.Type = iceberg.PrimitiveTypes.TimestampTzNs
			} else if dt.TimeZone == "" {
				result.Type = iceberg.PrimitiveTypes.TimestampNs
			} else {
				panic(fmt.Errorf("%w: unsupported arrow type for conversion - %s", iceberg.ErrInvalidSchema, dt))
			}

			return result
		}

		if dt.Unit == arrow.Nanosecond {
			slog.Warn("downcasting nanosecond timestamp to microsecond, precision loss may occur")
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

	return result
}

func ArrowTypeToIceberg(dt arrow.DataType, downcastNsTimestamp bool) (iceberg.Type, error) {
	sc := arrow.NewSchema([]arrow.Field{{
		Type:     dt,
		Metadata: arrow.NewMetadata([]string{ArrowParquetFieldIDKey}, []string{"1"}),
	}}, nil)

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

func ArrowSchemaToIceberg(sc *arrow.Schema, downcastNsTimestamp bool, nameMapping iceberg.NameMapping) (*iceberg.Schema, error) {
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
		schemaWithoutIDs, err := arrowToSchemaWithoutIDs(sc, downcastNsTimestamp)
		if err != nil {
			return nil, err
		}

		return iceberg.ApplyNameMapping(schemaWithoutIDs, nameMapping)
	default:
		return nil, fmt.Errorf("%w: arrow schema does not have field-ids and no name mapping provided",
			iceberg.ErrInvalidSchema)
	}
}

func ArrowSchemaToIcebergWithFreshIDs(sc *arrow.Schema, downcastNsTimestamp bool) (*iceberg.Schema, error) {
	schemaWithoutIDs, err := arrowToSchemaWithoutIDs(sc, downcastNsTimestamp)
	if err != nil {
		return nil, err
	}

	return iceberg.AssignFreshSchemaIDs(schemaWithoutIDs, nil)
}

func arrowToSchemaWithoutIDs(sc *arrow.Schema, downcastNsTimestamp bool) (*iceberg.Schema, error) {
	withoutIDs, err := VisitArrowSchema(sc, convertToIceberg{
		downcastTimestamp: downcastNsTimestamp,
		fieldID:           func(_ arrow.Field) int { return -1 },
	})
	if err != nil {
		return nil, err
	}

	schemaWithoutIDs := iceberg.NewSchema(0, withoutIDs.Type.(*iceberg.StructType).FieldList...)

	return schemaWithoutIDs, nil
}

type convertToSmallTypes struct{}

func (convertToSmallTypes) Schema(_ *arrow.Schema, structResult arrow.Field) arrow.Field {
	return structResult
}

func (convertToSmallTypes) Struct(_ *arrow.StructType, results []arrow.Field) arrow.Field {
	return arrow.Field{Type: arrow.StructOf(results...)}
}

func (convertToSmallTypes) Field(field arrow.Field, fieldResult arrow.Field) arrow.Field {
	field.Type = fieldResult.Type

	return field
}

func (convertToSmallTypes) List(_ arrow.ListLikeType, elemResult arrow.Field) arrow.Field {
	return arrow.Field{Type: arrow.ListOfField(elemResult)}
}

func (convertToSmallTypes) Map(_ *arrow.MapType, keyResult, valueResult arrow.Field) arrow.Field {
	return arrow.Field{
		Type: arrow.MapOfWithMetadata(keyResult.Type, keyResult.Metadata,
			valueResult.Type, valueResult.Metadata),
	}
}

func (convertToSmallTypes) Primitive(dt arrow.DataType) arrow.Field {
	switch dt.ID() {
	case arrow.LARGE_STRING:
		dt = arrow.BinaryTypes.String
	case arrow.LARGE_BINARY:
		dt = arrow.BinaryTypes.Binary
	}

	return arrow.Field{Type: dt}
}

func ensureSmallArrowTypes(dt arrow.DataType) (arrow.DataType, error) {
	top, err := VisitArrowSchema(arrow.NewSchema([]arrow.Field{{Type: dt}}, nil), convertToSmallTypes{})
	if err != nil {
		return nil, err
	}

	return top.Type.(*arrow.StructType).Field(0).Type, nil
}

type convertToArrow struct {
	metadata        map[string]string
	includeFieldIDs bool
	useLargeTypes   bool
}

func (c convertToArrow) Schema(_ *iceberg.Schema, result arrow.Field) arrow.Field {
	result.Metadata = arrow.MetadataFrom(c.metadata)

	return result
}

func (c convertToArrow) Struct(_ iceberg.StructType, results []arrow.Field) arrow.Field {
	return arrow.Field{Type: arrow.StructOf(results...)}
}

func (c convertToArrow) Field(field iceberg.NestedField, result arrow.Field) arrow.Field {
	meta := map[string]string{}
	if len(field.Doc) > 0 {
		meta[ArrowFieldDocKey] = field.Doc
	}

	if c.includeFieldIDs {
		meta[ArrowParquetFieldIDKey] = strconv.Itoa(field.ID)
	}

	if len(meta) > 0 {
		result.Metadata = arrow.MetadataFrom(meta)
	}

	result.Name, result.Nullable = field.Name, !field.Required

	return result
}

func (c convertToArrow) List(list iceberg.ListType, elemResult arrow.Field) arrow.Field {
	elemField := c.Field(list.ElementField(), elemResult)
	if c.useLargeTypes {
		return arrow.Field{Type: arrow.LargeListOfField(elemField)}
	}

	return arrow.Field{Type: arrow.ListOfField(elemField)}
}

func (c convertToArrow) Map(m iceberg.MapType, keyResult, valResult arrow.Field) arrow.Field {
	keyField := c.Field(m.KeyField(), keyResult)
	valField := c.Field(m.ValueField(), valResult)

	return arrow.Field{Type: arrow.MapOfFields(keyField, valField)}
}

func (c convertToArrow) Primitive(iceberg.PrimitiveType) arrow.Field { panic("shouldn't be called") }

func (c convertToArrow) VisitFixed(f iceberg.FixedType) arrow.Field {
	return arrow.Field{Type: &arrow.FixedSizeBinaryType{ByteWidth: f.Len()}}
}

func (c convertToArrow) VisitDecimal(d iceberg.DecimalType) arrow.Field {
	return arrow.Field{Type: &arrow.Decimal128Type{
		Precision: int32(d.Precision()), Scale: int32(d.Scale()),
	}}
}

func (c convertToArrow) VisitBoolean() arrow.Field {
	return arrow.Field{Type: arrow.FixedWidthTypes.Boolean}
}

func (c convertToArrow) VisitInt32() arrow.Field {
	return arrow.Field{Type: arrow.PrimitiveTypes.Int32}
}

func (c convertToArrow) VisitInt64() arrow.Field {
	return arrow.Field{Type: arrow.PrimitiveTypes.Int64}
}

func (c convertToArrow) VisitFloat32() arrow.Field {
	return arrow.Field{Type: arrow.PrimitiveTypes.Float32}
}

func (c convertToArrow) VisitFloat64() arrow.Field {
	return arrow.Field{Type: arrow.PrimitiveTypes.Float64}
}

func (c convertToArrow) VisitDate() arrow.Field {
	return arrow.Field{Type: arrow.FixedWidthTypes.Date32}
}

func (c convertToArrow) VisitTime() arrow.Field {
	return arrow.Field{Type: arrow.FixedWidthTypes.Time64us}
}

func (c convertToArrow) VisitTimestampTz() arrow.Field {
	return arrow.Field{Type: arrow.FixedWidthTypes.Timestamp_us}
}

func (c convertToArrow) VisitTimestamp() arrow.Field {
	return arrow.Field{Type: &arrow.TimestampType{Unit: arrow.Microsecond}}
}

func (c convertToArrow) VisitTimestampNs() arrow.Field {
	return arrow.Field{Type: &arrow.TimestampType{Unit: arrow.Nanosecond}}
}

func (c convertToArrow) VisitTimestampNsTz() arrow.Field {
	return arrow.Field{Type: arrow.FixedWidthTypes.Timestamp_ns}
}

func (c convertToArrow) VisitString() arrow.Field {
	if c.useLargeTypes {
		return arrow.Field{Type: arrow.BinaryTypes.LargeString}
	}

	return arrow.Field{Type: arrow.BinaryTypes.String}
}

func (c convertToArrow) VisitBinary() arrow.Field {
	if c.useLargeTypes {
		return arrow.Field{Type: arrow.BinaryTypes.LargeBinary}
	}

	return arrow.Field{Type: arrow.BinaryTypes.Binary}
}

func (c convertToArrow) VisitUUID() arrow.Field {
	return arrow.Field{Type: extensions.NewUUIDType()}
}

func (c convertToArrow) VisitUnknown() arrow.Field {
	return arrow.Field{Type: arrow.Null}
}

var _ iceberg.SchemaVisitorPerPrimitiveType[arrow.Field] = convertToArrow{}

// SchemaToArrowSchema converts an Iceberg schema to an Arrow schema. If the metadata parameter
// is non-nil, it will be included as the top-level metadata in the schema. If includeFieldIDs
// is true, then each field of the schema will contain a metadata key PARQUET:field_id set to
// the field id from the iceberg schema.
func SchemaToArrowSchema(sc *iceberg.Schema, metadata map[string]string, includeFieldIDs, useLargeTypes bool) (*arrow.Schema, error) {
	top, err := iceberg.Visit(sc, convertToArrow{
		metadata:        metadata,
		includeFieldIDs: includeFieldIDs, useLargeTypes: useLargeTypes,
	})
	if err != nil {
		return nil, err
	}

	return arrow.NewSchema(top.Type.(*arrow.StructType).Fields(), &top.Metadata), nil
}

// TypeToArrowType converts a given iceberg type, into the equivalent Arrow data type.
// For dealing with nested fields (List, Struct, Map) if includeFieldIDs is true, then
// the child fields will contain a metadata key PARQUET:field_id set to the field id.
func TypeToArrowType(t iceberg.Type, includeFieldIDs bool, useLargeTypes bool) (arrow.DataType, error) {
	top, err := iceberg.Visit(iceberg.NewSchema(0, iceberg.NestedField{Type: t}),
		convertToArrow{includeFieldIDs: includeFieldIDs, useLargeTypes: useLargeTypes})
	if err != nil {
		return nil, err
	}

	return top.Type.(*arrow.StructType).Field(0).Type, nil
}

type arrowAccessor struct {
	fileSchema *iceberg.Schema
}

func (a arrowAccessor) SchemaPartner(partner arrow.Array) arrow.Array {
	return partner
}

func (a arrowAccessor) FieldPartner(partnerStruct arrow.Array, fieldID int, _ string) arrow.Array {
	if partnerStruct == nil {
		return nil
	}

	field, ok := a.fileSchema.FindFieldByID(fieldID)
	if !ok {
		return nil
	}

	if st, ok := partnerStruct.(*array.Struct); ok {
		if idx, ok := st.DataType().(*arrow.StructType).FieldIdx(field.Name); ok {
			return st.Field(idx)
		}
	}

	panic(fmt.Errorf("cannot find %s in expected partner_struct type %s",
		field.Name, partnerStruct.DataType()))
}

func (a arrowAccessor) ListElementPartner(partnerList arrow.Array) arrow.Array {
	if l, ok := partnerList.(array.ListLike); ok {
		return l.ListValues()
	}

	return nil
}

func (a arrowAccessor) MapKeyPartner(partnerMap arrow.Array) arrow.Array {
	if m, ok := partnerMap.(*array.Map); ok {
		return m.Keys()
	}

	return nil
}

func (a arrowAccessor) MapValuePartner(partnerMap arrow.Array) arrow.Array {
	if m, ok := partnerMap.(*array.Map); ok {
		return m.Items()
	}

	return nil
}

func retOrPanic[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}

	return v
}

// numericDefault converts v to T, accepting the typed iceberg form, the
// float64 that encoding/json produces when deserializing into any, or the
// json.Number that a decoder configured with UseNumber() produces.
func numericDefault[T ~int32 | ~int64 | ~float32 | ~float64](v any) T {
	switch val := v.(type) {
	case T:
		return val
	case float64:
		return T(val)
	case json.Number:
		f, err := val.Float64()
		if err != nil {
			panic(fmt.Errorf("unsupported json.Number %q for numeric iceberg type: %w", val, err))
		}

		return T(f)
	}
	panic(fmt.Errorf("unsupported write-default value type %T for numeric iceberg type", v))
}

// defaultToScalar converts an Iceberg default value to an Arrow scalar.
func defaultToScalar(v any, t iceberg.Type, dt arrow.DataType) scalar.Scalar {
	switch typ := t.(type) {
	case iceberg.Float32Type:
		s, err := scalar.MakeScalarParam(numericDefault[float32](v), dt)
		if err != nil {
			panic(fmt.Errorf("write-default float32 (iceberg type %s, value %v %T): %w", t, v, v, err))
		}

		return s
	case iceberg.DateType:
		return scalar.NewDate32Scalar(arrow.Date32(numericDefault[iceberg.Date](v)))
	case iceberg.TimeType:
		return scalar.NewTime64Scalar(arrow.Time64(numericDefault[iceberg.Time](v)), dt)
	case iceberg.TimestampType, iceberg.TimestampTzType:
		return scalar.NewTimestampScalar(arrow.Timestamp(numericDefault[iceberg.Timestamp](v)), dt)
	case iceberg.TimestampNsType, iceberg.TimestampTzNsType:
		return scalar.NewTimestampScalar(arrow.Timestamp(numericDefault[iceberg.TimestampNano](v)), dt)
	case iceberg.UUIDType:
		switch val := v.(type) {
		case uuid.UUID:
			s, err := scalar.MakeScalarParam(val[:], &arrow.FixedSizeBinaryType{ByteWidth: 16})
			if err != nil {
				panic(fmt.Errorf("write-default uuid (value %v): %w", val, err))
			}

			return s
		case string:
			u, err := uuid.Parse(val)
			if err != nil {
				panic(fmt.Errorf("write-default uuid: cannot parse string %q: %w", val, err))
			}
			s, err := scalar.MakeScalarParam(u[:], &arrow.FixedSizeBinaryType{ByteWidth: 16})
			if err != nil {
				panic(fmt.Errorf("write-default uuid (value %v): %w", val, err))
			}

			return s
		}
		panic(fmt.Errorf("write-default uuid: unsupported value type %T (%v)", v, v))
	case iceberg.DecimalType:
		switch val := v.(type) {
		case iceberg.Decimal:
			return scalar.NewDecimal128Scalar(val.Val, dt)
		case string:
			n, err := decimal128.FromString(val, int32(typ.Precision()), int32(typ.Scale()))
			if err != nil {
				panic(fmt.Errorf("write-default decimal(p=%d, s=%d): cannot parse string %q: %w", typ.Precision(), typ.Scale(), val, err))
			}

			return scalar.NewDecimal128Scalar(n, dt)
		}
		panic(fmt.Errorf("write-default decimal: unsupported value type %T (%v)", v, v))
	case iceberg.BinaryType, iceberg.FixedType:
		switch val := v.(type) {
		case []byte:
			s, err := scalar.MakeScalarParam(val, dt)
			if err != nil {
				panic(fmt.Errorf("write-default binary/fixed (iceberg type %s, value %v): %w", t, val, err))
			}

			return s
		case string:
			b, err := base64.StdEncoding.DecodeString(val)
			if err != nil {
				panic(fmt.Errorf("write-default binary/fixed (iceberg type %s): cannot base64-decode string %q: %w", t, val, err))
			}
			s, err := scalar.MakeScalarParam(b, dt)
			if err != nil {
				panic(fmt.Errorf("write-default binary/fixed (iceberg type %s, value %v): %w", t, b, err))
			}

			return s
		}
		panic(fmt.Errorf("write-default binary/fixed: unsupported value type %T (%v)", v, v))
	// Float64, Bool, and String cast normally.
	// Int32 and Int64 arrive as float64 from JSON and are handled by MakeScalarParam.
	default:
		s, err := scalar.MakeScalarParam(v, dt)
		if err != nil {
			panic(fmt.Errorf("write-default (iceberg type %s, value %v %T): %w", t, v, v, err))
		}

		return s
	}
}

// defaultToArray creates an Arrow array of length n filled with the given default value v.
func defaultToArray(v any, t iceberg.Type, dt arrow.DataType, n int, alloc memory.Allocator) arrow.Array {
	sc := defaultToScalar(v, t, dt)
	out, err := scalar.MakeArrayFromScalar(sc, n, alloc)
	if err != nil {
		panic(fmt.Errorf("write-default (iceberg type %s, value %v %T): failed to create array: %w", t, v, v, err))
	}
	if _, ok := dt.(*extensions.UUIDType); ok {
		defer out.Release()

		data := array.NewData(dt, out.Len(), out.Data().Buffers(), nil, out.NullN(), 0)
		defer data.Release()

		return array.MakeFromData(data)
	}

	return out
}

type arrowProjectionVisitor struct {
	ctx                 context.Context
	fileSchema          *iceberg.Schema
	includeFieldIDs     bool
	downcastNsTimestamp bool
	useLargeTypes       bool
	useWriteDefault     bool
}

func (a *arrowProjectionVisitor) castIfNeeded(field iceberg.NestedField, vals arrow.Array) arrow.Array {
	fileField, ok := a.fileSchema.FindFieldByID(field.ID)
	if !ok {
		panic(fmt.Errorf("could not find field id %d in schema", field.ID))
	}

	typ, ok := fileField.Type.(iceberg.PrimitiveType)
	if !ok {
		vals.Retain()

		return vals
	}

	if !field.Type.Equals(typ) {
		promoted := retOrPanic(iceberg.PromoteType(fileField.Type, field.Type))
		targetType := retOrPanic(TypeToArrowType(promoted, a.includeFieldIDs, a.useLargeTypes))
		if !a.useLargeTypes {
			targetType = retOrPanic(ensureSmallArrowTypes(targetType))
		}

		return retOrPanic(compute.CastArray(a.ctx, vals,
			compute.SafeCastOptions(targetType)))
	}

	targetType := retOrPanic(TypeToArrowType(field.Type, a.includeFieldIDs, a.useLargeTypes))
	if !arrow.TypeEqual(targetType, vals.DataType()) {
		switch field.Type.(type) {
		case iceberg.TimestampType:
			tt, tgtok := targetType.(*arrow.TimestampType)
			vt, valok := vals.DataType().(*arrow.TimestampType)

			if tgtok && valok && tt.TimeZone == "" && vt.TimeZone == "" && tt.Unit == arrow.Microsecond {
				if vt.Unit == arrow.Nanosecond && a.downcastNsTimestamp {
					return retOrPanic(compute.CastArray(a.ctx, vals, compute.UnsafeCastOptions(tt)))
				} else if vt.Unit == arrow.Second || vt.Unit == arrow.Millisecond {
					return retOrPanic(compute.CastArray(a.ctx, vals, compute.SafeCastOptions(tt)))
				}
			}

			panic(fmt.Errorf("unsupported schema projection from %s to %s",
				vals.DataType(), targetType))
		case iceberg.TimestampTzType:
			tt, tgtok := targetType.(*arrow.TimestampType)
			vt, valok := vals.DataType().(*arrow.TimestampType)

			if tgtok && valok && tt.TimeZone == "UTC" &&
				slices.Contains(utcAliases, vt.TimeZone) && tt.Unit == arrow.Microsecond {
				if vt.Unit == arrow.Nanosecond && a.downcastNsTimestamp {
					return retOrPanic(compute.CastArray(a.ctx, vals, compute.UnsafeCastOptions(tt)))
				} else if vt.Unit != arrow.Nanosecond {
					return retOrPanic(compute.CastArray(a.ctx, vals, compute.SafeCastOptions(tt)))
				}
			}

			panic(fmt.Errorf("unsupported schema projection from %s to %s",
				vals.DataType(), targetType))
		default:
			return retOrPanic(compute.CastArray(a.ctx, vals,
				compute.SafeCastOptions(targetType)))
		}
	}
	vals.Retain()

	return vals
}

func (a *arrowProjectionVisitor) constructField(field iceberg.NestedField, arrowType arrow.DataType) arrow.Field {
	metadata := map[string]string{}
	if field.Doc != "" {
		metadata[ArrowFieldDocKey] = field.Doc
	}

	if a.includeFieldIDs {
		metadata[ArrowParquetFieldIDKey] = strconv.Itoa(field.ID)
	}

	return arrow.Field{
		Name:     field.Name,
		Type:     arrowType,
		Nullable: !field.Required,
		Metadata: arrow.MetadataFrom(metadata),
	}
}

func (a *arrowProjectionVisitor) Schema(_ *iceberg.Schema, _ arrow.Array, result arrow.Array) arrow.Array {
	return result
}

func (a *arrowProjectionVisitor) Struct(st iceberg.StructType, structArr arrow.Array, fieldResults []arrow.Array) arrow.Array {
	if structArr == nil {
		return nil
	}

	fieldArrs := make([]arrow.Array, len(st.FieldList))
	fields := make([]arrow.Field, len(st.FieldList))
	for i, field := range st.FieldList {
		arr := fieldResults[i]
		if arr != nil {
			if _, ok := arr.DataType().(arrow.NestedType); ok {
				defer arr.Release()
			}

			arr = a.castIfNeeded(field, arr)
			defer arr.Release()
			fieldArrs[i] = arr
			fields[i] = a.constructField(field, arr.DataType())
		} else {
			dt := retOrPanic(TypeToArrowType(field.Type, false, a.useLargeTypes))
			alloc := compute.GetAllocator(a.ctx)

			switch {
			case field.WriteDefault != nil && a.useWriteDefault:
				arr = defaultToArray(field.WriteDefault, field.Type, dt, structArr.Len(), alloc)
			case field.InitialDefault != nil && !a.useWriteDefault:
				arr = defaultToArray(field.InitialDefault, field.Type, dt, structArr.Len(), alloc)
			case !field.Required:
				arr = array.MakeArrayOfNull(alloc, dt, structArr.Len())
			default:
				panic(fmt.Errorf("%w: required field is missing and has no default value: %s",
					iceberg.ErrInvalidSchema, field))
			}

			defer arr.Release()
			fieldArrs[i] = arr
			fields[i] = a.constructField(field, arr.DataType())
		}
	}

	var nullBitmap *memory.Buffer
	if structArr.NullN() > 0 {
		if structArr.Data().Offset() > 0 {
			// the children already accounted for any offset because we used the `Field` method
			// on the struct array in the FieldPartner accessor. So we just need to adjust the
			// bitmap to account for the offset.
			nullBitmap = memory.NewResizableBuffer(compute.GetAllocator(a.ctx))
			defer nullBitmap.Release()
			nullBitmap.Resize(int(bitutil.BytesForBits(int64(structArr.Len()))))

			bitutil.CopyBitmap(structArr.NullBitmapBytes(), structArr.Data().Offset(), structArr.Len(),
				nullBitmap.Bytes(), 0)

		} else {
			nullBitmap = structArr.Data().Buffers()[0]
		}
	}

	return retOrPanic(array.NewStructArrayWithFieldsAndNulls(fieldArrs, fields,
		nullBitmap, structArr.NullN(), 0))
}

func (a *arrowProjectionVisitor) Field(_ iceberg.NestedField, _ arrow.Array, fieldArr arrow.Array) arrow.Array {
	return fieldArr
}

func (a *arrowProjectionVisitor) List(listType iceberg.ListType, listArr arrow.Array, valArr arrow.Array) arrow.Array {
	arr, ok := listArr.(array.ListLike)
	if !ok || valArr == nil {
		return nil
	}

	valArr = a.castIfNeeded(listType.ElementField(), valArr)
	defer valArr.Release()

	var outType arrow.ListLikeType
	elemField := a.constructField(listType.ElementField(), valArr.DataType())
	switch arr.DataType().ID() {
	case arrow.LIST:
		outType = arrow.ListOfField(elemField)
	case arrow.LARGE_LIST:
		outType = arrow.LargeListOfField(elemField)
	case arrow.LIST_VIEW:
		outType = arrow.LargeListViewOfField(elemField)
	}

	data := array.NewData(outType, arr.Len(), arr.Data().Buffers(),
		[]arrow.ArrayData{valArr.Data()}, arr.NullN(), arr.Data().Offset())
	defer data.Release()

	return array.MakeFromData(data)
}

func (a *arrowProjectionVisitor) Map(m iceberg.MapType, mapArray, keyResult, valResult arrow.Array) arrow.Array {
	if keyResult == nil || valResult == nil {
		return nil
	}

	arr, ok := mapArray.(*array.Map)
	if !ok {
		return nil
	}

	keys := a.castIfNeeded(m.KeyField(), keyResult)
	defer keys.Release()
	vals := a.castIfNeeded(m.ValueField(), valResult)
	defer vals.Release()

	keyField := a.constructField(m.KeyField(), keys.DataType())
	valField := a.constructField(m.ValueField(), vals.DataType())

	mapType := arrow.MapOfWithMetadata(keyField.Type, keyField.Metadata, valField.Type, valField.Metadata)
	childData := array.NewData(mapType.Elem(), arr.Data().Children()[0].Len(), []*memory.Buffer{nil},
		[]arrow.ArrayData{keys.Data(), vals.Data()}, 0, 0)
	defer childData.Release()
	newData := array.NewData(mapType, arr.Len(), arr.Data().Buffers(),
		[]arrow.ArrayData{childData}, arr.NullN(), arr.Offset())
	defer newData.Release()

	return array.NewMapData(newData)
}

func (a *arrowProjectionVisitor) Primitive(_ iceberg.PrimitiveType, arr arrow.Array) arrow.Array {
	return arr
}

// SchemaOptions controls the behaviour of ToRequestedSchema.
type SchemaOptions struct {
	DowncastTimestamp bool
	IncludeFieldIDs   bool
	UseLargeTypes     bool
	UseWriteDefault   bool
}

// ToRequestedSchema will construct a new record batch matching the requested iceberg schema
// casting columns if necessary as appropriate.
func ToRequestedSchema(ctx context.Context, requested, fileSchema *iceberg.Schema, batch arrow.RecordBatch, opts SchemaOptions) (arrow.RecordBatch, error) {
	st := array.RecordToStructArray(batch)
	defer st.Release()

	result, err := iceberg.VisitSchemaWithPartner[arrow.Array, arrow.Array](requested, st,
		&arrowProjectionVisitor{
			ctx:                 ctx,
			fileSchema:          fileSchema,
			includeFieldIDs:     opts.IncludeFieldIDs,
			downcastNsTimestamp: opts.DowncastTimestamp,
			useLargeTypes:       opts.UseLargeTypes,
			useWriteDefault:     opts.UseWriteDefault,
		}, arrowAccessor{fileSchema: fileSchema})
	if err != nil {
		return nil, err
	}
	out := array.RecordFromStructArray(result.(*array.Struct), nil)
	result.Release()

	return out, nil
}

type schemaCompatVisitor struct {
	provided *iceberg.Schema

	errorData pterm.TableData
}

func checkSchemaCompat(requested, provided *iceberg.Schema) error {
	sc := &schemaCompatVisitor{
		provided:  provided,
		errorData: pterm.TableData{{"", "Table Field", "Requested Field"}},
	}

	_, compat := iceberg.PreOrderVisit(requested, sc)

	return compat
}

func checkArrowSchemaCompat(requested *iceberg.Schema, provided *arrow.Schema, downcastNanoToMicro bool) error {
	mapping := requested.NameMapping()
	providedSchema, err := ArrowSchemaToIceberg(provided, downcastNanoToMicro, mapping)
	if err != nil {
		return err
	}

	return checkSchemaCompat(requested, providedSchema)
}

func (sc *schemaCompatVisitor) isFieldCompat(lhs iceberg.NestedField) bool {
	rhs, ok := sc.provided.FindFieldByID(lhs.ID)
	if !ok {
		if lhs.Required && lhs.WriteDefault == nil && lhs.InitialDefault == nil {
			sc.errorData = append(sc.errorData,
				[]string{"❌", lhs.String(), "missing"})

			return false
		}
		sc.errorData = append(sc.errorData,
			[]string{"✅", lhs.String(), "missing"})

		return true
	}

	if lhs.Required && !rhs.Required {
		sc.errorData = append(sc.errorData,
			[]string{"❌", lhs.String(), rhs.String()})

		return false
	}

	if lhs.Type.Equals(rhs.Type) {
		sc.errorData = append(sc.errorData,
			[]string{"✅", lhs.String(), rhs.String()})

		return true
	}

	// we only check that parent node is also of the same type
	// we check the type of the child nodes as we traverse them later
	switch lhs.Type.(type) {
	case *iceberg.StructType:
		if rhs, ok := rhs.Type.(*iceberg.StructType); ok {
			sc.errorData = append(sc.errorData,
				[]string{"✅", lhs.String(), rhs.String()})

			return true
		}
	case *iceberg.ListType:
		if rhs, ok := rhs.Type.(*iceberg.ListType); ok {
			sc.errorData = append(sc.errorData,
				[]string{"✅", lhs.String(), rhs.String()})

			return true
		}
	case *iceberg.MapType:
		if rhs, ok := rhs.Type.(*iceberg.MapType); ok {
			sc.errorData = append(sc.errorData,
				[]string{"✅", lhs.String(), rhs.String()})

			return true
		}
	}

	if _, err := iceberg.PromoteType(rhs.Type, lhs.Type); err != nil {
		sc.errorData = append(sc.errorData,
			[]string{"❌", lhs.String(), rhs.String()})

		return false
	}

	sc.errorData = append(sc.errorData,
		[]string{"✅", lhs.String(), rhs.String()})

	return true
}

func (sc *schemaCompatVisitor) Schema(s *iceberg.Schema, v func() bool) bool {
	if !v() {
		pterm.DisableColor()
		tbl := pterm.DefaultTable.WithHasHeader(true).WithData(sc.errorData)
		tbl.Render()
		txt, _ := tbl.Srender()
		pterm.EnableColor()
		panic("mismatch in fields:\n" + txt)
	}

	return true
}

func (sc *schemaCompatVisitor) Struct(st iceberg.StructType, v []func() bool) bool {
	out := true
	for _, res := range v {
		out = res() && out
	}

	return out
}

func (sc *schemaCompatVisitor) Field(n iceberg.NestedField, v func() bool) bool {
	return sc.isFieldCompat(n) && v()
}

func (sc *schemaCompatVisitor) List(l iceberg.ListType, v func() bool) bool {
	return sc.isFieldCompat(l.ElementField()) && v()
}

func (sc *schemaCompatVisitor) Map(m iceberg.MapType, vk, vv func() bool) bool {
	return sc.isFieldCompat(m.KeyField()) && sc.isFieldCompat(m.ValueField()) && vk() && vv()
}

func (sc *schemaCompatVisitor) Primitive(p iceberg.PrimitiveType) bool {
	return true
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}

	return v
}

type arrowStatsCollector struct {
	fieldID     int
	schema      *iceberg.Schema
	props       iceberg.Properties
	defaultMode string
}

func (a *arrowStatsCollector) Schema(_ *iceberg.Schema, results func() []tblutils.StatisticsCollector) []tblutils.StatisticsCollector {
	return results()
}

func (a *arrowStatsCollector) Struct(_ iceberg.StructType, results []func() []tblutils.StatisticsCollector) []tblutils.StatisticsCollector {
	result := make([]tblutils.StatisticsCollector, 0, len(results))
	for _, res := range results {
		result = append(result, res()...)
	}

	return result
}

func (a *arrowStatsCollector) Field(field iceberg.NestedField, fieldRes func() []tblutils.StatisticsCollector) []tblutils.StatisticsCollector {
	a.fieldID = field.ID

	return fieldRes()
}

func (a *arrowStatsCollector) List(list iceberg.ListType, elemResult func() []tblutils.StatisticsCollector) []tblutils.StatisticsCollector {
	a.fieldID = list.ElementID

	return elemResult()
}

func (a *arrowStatsCollector) Map(m iceberg.MapType, keyResult, valResult func() []tblutils.StatisticsCollector) []tblutils.StatisticsCollector {
	a.fieldID = m.KeyID
	keyRes := keyResult()

	a.fieldID = m.ValueID
	valRes := valResult()

	return append(keyRes, valRes...)
}

func (a *arrowStatsCollector) Primitive(dt iceberg.PrimitiveType) []tblutils.StatisticsCollector {
	colName, ok := a.schema.FindColumnName(a.fieldID)
	if !ok {
		return []tblutils.StatisticsCollector{}
	}

	metMode, err := tblutils.MatchMetricsMode(a.defaultMode)
	if err != nil {
		panic(err)
	}

	colMode, ok := a.props[MetricsModeColumnConfPrefix+"."+colName]
	if ok {
		metMode, err = tblutils.MatchMetricsMode(colMode)
		if err != nil {
			panic(err)
		}
	}

	switch dt.(type) {
	case iceberg.StringType:
	case iceberg.BinaryType:
	default:
		if metMode.Typ == tblutils.MetricModeTruncate {
			metMode = tblutils.MetricsMode{Typ: tblutils.MetricModeFull, Len: 0}
		}
	}

	isNested := strings.Contains(colName, ".")
	if isNested && (metMode.Typ == tblutils.MetricModeTruncate || metMode.Typ == tblutils.MetricModeFull) {
		metMode = tblutils.MetricsMode{Typ: tblutils.MetricModeCounts}
	}

	return []tblutils.StatisticsCollector{{
		FieldID:    a.fieldID,
		IcebergTyp: dt,
		ColName:    colName,
		Mode:       metMode,
	}}
}

func computeStatsPlan(sc *iceberg.Schema, props iceberg.Properties) (map[int]tblutils.StatisticsCollector, error) {
	result := make(map[int]tblutils.StatisticsCollector)

	visitor := &arrowStatsCollector{
		schema: sc, props: props,
		defaultMode: props.Get(DefaultWriteMetricsModeKey, DefaultWriteMetricsModeDefault),
	}

	collectors, err := iceberg.PreOrderVisit(sc, visitor)
	if err != nil {
		return nil, err
	}

	for _, entry := range collectors {
		result[entry.FieldID] = entry
	}

	return result, nil
}

func filesToDataFiles(ctx context.Context, fileIO iceio.IO, meta *MetadataBuilder, filePaths []string, concurrency int) (_ []iceberg.DataFile, err error) {
	partitionSpec, err := meta.CurrentSpec()
	if err != nil || partitionSpec == nil {
		return nil, fmt.Errorf("%w: cannot add files without a current spec", err)
	}

	currentSchema, currentSpec := meta.CurrentSchema(), *partitionSpec

	dataFiles := make([]iceberg.DataFile, len(filePaths))
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(concurrency)
	for i, filePath := range filePaths {
		eg.Go(func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					switch e := r.(type) {
					case error:
						err = fmt.Errorf("error encountered during file conversion: %w", e)
					default:
						err = fmt.Errorf("error encountered during file conversion: %v", e)
					}
				}
			}()

			dataFiles[i] = fileToDataFile(ctx, fileIO, filePath, currentSchema, currentSpec, meta.defaultSortOrderID, meta.props)

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return dataFiles, nil
}

func fileToDataFile(ctx context.Context, fileIO iceio.IO, filePath string, currentSchema *iceberg.Schema, currentSpec iceberg.PartitionSpec, sortOrderID int, props iceberg.Properties) iceberg.DataFile {
	format := tblutils.FormatFromFileName(filePath)
	rdr := must(format.Open(ctx, fileIO, filePath))
	defer rdr.Close()

	arrSchema := must(rdr.Schema())
	if err := checkArrowSchemaCompat(currentSchema, arrSchema, false); err != nil {
		panic(err)
	}

	pathToIDSchema := currentSchema
	if fileHasIDs := must(VisitArrowSchema(arrSchema, hasIDs{})); fileHasIDs {
		pathToIDSchema = must(ArrowSchemaToIceberg(arrSchema, false, nil))
	}
	statistics := format.DataFileStatsFromMeta(
		rdr.Metadata(),
		must(computeStatsPlan(currentSchema, props)),
		must(format.PathToIDMapping(pathToIDSchema)),
	)

	partitionValues := make(map[int]any)
	if !currentSpec.Equals(*iceberg.UnpartitionedSpec) {
		for _, field := range currentSpec.Fields() {
			if !field.Transform.PreservesOrder() {
				panic(fmt.Errorf("cannot infer partition value from parquet metadata for a non-linear partition field: %s with transform %s", field.Name, field.Transform))
			}

			partitionVal := statistics.PartitionValue(field, currentSchema)
			if partitionVal != nil {
				partitionValues[field.FieldID] = partitionVal
			}
		}
	}

	return statistics.ToDataFile(tblutils.DataFileOpts{
		Schema:          currentSchema,
		Spec:            currentSpec,
		Path:            filePath,
		Format:          iceberg.ParquetFile,
		Content:         iceberg.EntryContentData,
		FileSize:        rdr.SourceFileSize(),
		PartitionValues: partitionValues,
		SortOrderID:     sortOrderID,
	})
}

func recordNBytes(rec arrow.RecordBatch) (total int64) {
	for _, c := range rec.Columns() {
		total += int64(c.Data().SizeInBytes())
	}

	return total
}

func binPackRecords(itr iter.Seq2[arrow.RecordBatch, error], recordLookback int, targetFileSize int64) iter.Seq[[]arrow.RecordBatch] {
	return internal.PackingIterator(func(yield func(arrow.RecordBatch) bool) {
		for rec, err := range itr {
			if err != nil {
				panic(err)
			}

			rec.Retain()
			if !yield(rec) {
				return
			}
		}
	}, targetFileSize, recordLookback, recordNBytes, false)
}

type recordWritingArgs struct {
	sc              *arrow.Schema
	itr             iter.Seq2[arrow.RecordBatch, error]
	fs              iceio.WriteFileIO
	writeUUID       *uuid.UUID
	counter         iter.Seq[int]
	maxWriteWorkers int
	clustered       bool
}

func recordsToDataFiles(ctx context.Context, rootLocation string, meta *MetadataBuilder, args recordWritingArgs) (ret iter.Seq2[iceberg.DataFile, error]) {
	if args.counter == nil {
		args.counter = internal.Counter(0)
	}

	defer func() {
		if r := recover(); r != nil {
			var err error
			switch e := r.(type) {
			case error:
				err = fmt.Errorf("error encountered during file writing: %w", e)
			default:
				err = fmt.Errorf("error encountered during position delete file writing: %v", e)
			}
			ret = func(yield func(iceberg.DataFile, error) bool) {
				yield(nil, err)
			}
		}
	}()

	if args.writeUUID == nil {
		u := uuid.Must(uuid.NewRandom())
		args.writeUUID = &u
	}

	targetFileSize := int64(meta.props.GetInt(WriteTargetFileSizeBytesKey,
		WriteTargetFileSizeBytesDefault))

	nameMapping := meta.CurrentSchema().NameMapping()
	taskSchema, err := ArrowSchemaToIceberg(args.sc, false, nameMapping)
	if err != nil {
		return func(yield func(iceberg.DataFile, error) bool) {
			yield(nil, err)
		}
	}

	factory, err := newWriterFactory(rootLocation, args, meta, taskSchema, targetFileSize)
	if err != nil {
		panic(err)
	}

	if factory.currentSpec.IsUnpartitioned() {
		return unpartitionedWrite(ctx, factory, args.itr)
	}

	if args.clustered {
		return clusteredPartitionedWrite(ctx, factory.currentSpec, meta.CurrentSchema(), factory, args.itr)
	}

	cw := newConcurrentDataFileWriter(func(rootLocation string, fs iceio.WriteFileIO, meta *MetadataBuilder, props iceberg.Properties, opts ...dataFileWriterOption) (dataFileWriter, error) {
		return newDataFileWriter(rootLocation, fs, meta, props, opts...)
	})

	partitionWriter := newPartitionedFanoutWriter(factory.currentSpec, cw, meta.CurrentSchema(), args.itr, factory)
	workers := config.EnvConfig.MaxWorkers
	if args.maxWriteWorkers > 0 {
		workers = args.maxWriteWorkers
	}

	return partitionWriter.Write(ctx, workers)
}

func unpartitionedWrite(ctx context.Context, factory *writerFactory, records iter.Seq2[arrow.RecordBatch, error]) iter.Seq2[iceberg.DataFile, error] {
	outputCh := make(chan iceberg.DataFile, 1)
	errCh := make(chan error, 1)

	go func() {
		defer close(outputCh)
		defer factory.stopCount()

		writer := factory.newRollingDataWriter(ctx, nil, "", nil, outputCh)
		for rec, err := range records {
			if err != nil {
				errCh <- err
				close(errCh)
				writer.close()
				writer.wg.Wait()

				return
			}
			if err := writer.Add(rec); err != nil {
				errCh <- err
				close(errCh)
				writer.close()
				writer.wg.Wait()

				return
			}
		}
		close(writer.recordCh)
		writer.wg.Wait()
		if err := <-writer.errorCh; err != nil {
			errCh <- err
		}
		close(errCh)
	}()

	return func(yield func(iceberg.DataFile, error) bool) {
		defer func() {
			for range outputCh {
			}
		}()
		for df := range outputCh {
			if !yield(df, nil) {
				return
			}
		}
		if err := <-errCh; err != nil {
			yield(nil, err)
		}
	}
}

type partitionContext struct {
	partitionData map[int]any
	specID        int32
}

func positionDeleteRecordsToDataFiles(ctx context.Context, rootLocation string, meta *MetadataBuilder, partitionContextByFilePath map[string]partitionContext, args recordWritingArgs) (ret iter.Seq2[iceberg.DataFile, error]) {
	if args.counter == nil {
		args.counter = internal.Counter(0)
	}

	defer func() {
		if r := recover(); r != nil {
			var err error
			switch e := r.(type) {
			case error:
				err = fmt.Errorf("error encountered during position delete file writing: %w", e)
			default:
				err = fmt.Errorf("error encountered during position delete file writing: %v", e)
			}
			ret = func(yield func(iceberg.DataFile, error) bool) {
				yield(nil, err)
			}
		}
	}()

	latestMetadata, err := meta.Build()
	if err != nil {
		return func(yield func(iceberg.DataFile, error) bool) {
			yield(nil, err)
		}
	}

	// V3 and later prefer deletion vectors over Parquet position-delete files;
	// warn so users migrate when DV-write support lands. The check is `>= 3`
	// rather than `== 3` so the warning carries forward to v4+ without churn.
	// See apache/iceberg#12048.
	if latestMetadata.Version() >= 3 {
		slog.Warn("writing Parquet position-delete file on a v3 table; prefer deletion vectors",
			"table_location", latestMetadata.Location())
	}

	if args.writeUUID == nil {
		u := uuid.Must(uuid.NewRandom())
		args.writeUUID = &u
	}

	targetFileSize := int64(meta.props.GetInt(WriteTargetFileSizeBytesKey,
		WriteTargetFileSizeBytesDefault))

	cw := newConcurrentDataFileWriter(func(rootLocation string, fs iceio.WriteFileIO, meta *MetadataBuilder, props iceberg.Properties, opts ...dataFileWriterOption) (dataFileWriter, error) {
		return newPositionDeleteWriter(rootLocation, fs, meta, props, opts...)
	}, withSchemaSanitization(false))
	if latestMetadata.PartitionSpec().IsUnpartitioned() {
		nextCount, stopCount := iter.Pull(args.counter)
		tasks := func(yield func(WriteTask) bool) {
			defer stopCount()

			fileCount := 0
			for batch := range binPackRecords(args.itr, defaultBinPackLookback, targetFileSize) {
				cnt, _ := nextCount()
				fileCount++
				t := WriteTask{
					Uuid:        *args.writeUUID,
					ID:          cnt,
					PartitionID: iceberg.UnpartitionedSpec.ID(),
					FileCount:   fileCount,
					Schema:      iceberg.PositionalDeleteSchema,
					Batches:     batch,
					SortOrderID: meta.defaultSortOrderID,
				}
				if !yield(t) {
					return
				}
			}
		}

		return cw.writeFiles(ctx, rootLocation, args.fs, meta, meta.props, nil, tasks)
	}
	factory, err := newWriterFactory(rootLocation, args, meta, iceberg.PositionalDeleteSchema, targetFileSize,
		withContentType(iceberg.EntryContentPosDeletes),
		withFactoryFileSchema(iceberg.PositionalDeleteSchema))
	if err != nil {
		panic(err)
	}

	partitionWriter := newPositionDeletePartitionedFanoutWriter(latestMetadata, cw, partitionContextByFilePath, args.itr, factory)
	workers := config.EnvConfig.MaxWorkers

	return partitionWriter.Write(ctx, workers)
}
