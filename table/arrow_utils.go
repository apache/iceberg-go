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
	"errors"
	"fmt"
	"iter"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
	"github.com/pterm/pterm"
)

// constants to look for as Keys in Arrow field metadata
const (
	ArrowFieldDocKey = "doc"
	// Arrow schemas that are generated from the Parquet library will utilize
	// this key to identify the field id of the source Parquet field.
	// We use this when converting to Iceberg to provide field IDs
	ArrowParquetFieldIDKey = "PARQUET:field_id"
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

		return
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

type arrowProjectionVisitor struct {
	ctx                 context.Context
	fileSchema          *iceberg.Schema
	includeFieldIDs     bool
	downcastNsTimestamp bool
	useLargeTypes       bool
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
		} else if !field.Required {
			dt := retOrPanic(TypeToArrowType(field.Type, false, a.useLargeTypes))

			arr = array.MakeArrayOfNull(compute.GetAllocator(a.ctx), dt, structArr.Len())
			defer arr.Release()
			fieldArrs[i] = arr
			fields[i] = a.constructField(field, arr.DataType())
		} else {
			panic(fmt.Errorf("%w: field is required, but could not be found in file: %s",
				iceberg.ErrInvalidSchema, field))
		}
	}

	return retOrPanic(array.NewStructArrayWithFields(fieldArrs, fields))
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
	childData := array.NewData(mapType.Elem(), arr.Len(), []*memory.Buffer{nil},
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

// ToRequestedSchema will construct a new record batch matching the requested iceberg schema
// casting columns if necessary as appropriate.
func ToRequestedSchema(ctx context.Context, requested, fileSchema *iceberg.Schema, batch arrow.Record, downcastTimestamp, includeFieldIDs, useLargeTypes bool) (arrow.Record, error) {
	st := array.RecordToStructArray(batch)
	defer st.Release()

	result, err := iceberg.VisitSchemaWithPartner[arrow.Array, arrow.Array](requested, st,
		&arrowProjectionVisitor{
			ctx:                 ctx,
			fileSchema:          fileSchema,
			includeFieldIDs:     includeFieldIDs,
			downcastNsTimestamp: downcastTimestamp,
			useLargeTypes:       useLargeTypes,
		}, arrowAccessor{fileSchema: fileSchema})
	if err != nil {
		return nil, err
	}
	st.Release()
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
		if lhs.Required {
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

func primitiveToPhysicalType(typ iceberg.Type) string {
	switch t := typ.(type) {
	case iceberg.BooleanType:
		return "BOOLEAN"
	case iceberg.Int32Type:
		return "INT32"
	case iceberg.Int64Type:
		return "INT64"
	case iceberg.Float32Type:
		return "FLOAT"
	case iceberg.Float64Type:
		return "DOUBLE"
	case iceberg.DateType:
		return "INT32"
	case iceberg.TimeType:
		return "INT64"
	case iceberg.TimestampType:
		return "INT64"
	case iceberg.TimestampTzType:
		return "INT64"
	case iceberg.StringType:
		return "BYTE_ARRAY"
	case iceberg.UUIDType:
		return "FIXED_LEN_BYTE_ARRAY"
	case iceberg.FixedType:
		return "FIXED_LEN_BYTE_ARRAY"
	case iceberg.BinaryType:
		return "BYTE_ARRAY"
	case iceberg.DecimalType:
		switch {
		case t.Precision() <= 9:
			return "INT32"
		case t.Precision() <= 18:
			return "INT64"
		default:
			return "FIXED_LEN_BYTE_ARRAY"
		}
	default:
		panic(fmt.Errorf("expected primitive type, got: %s", typ))
	}
}

type typedStat[T iceberg.LiteralType] interface {
	Min() T
	Max() T
}

type wrappedBinaryStats struct {
	*metadata.ByteArrayStatistics
}

func (w wrappedBinaryStats) Min() []byte {
	return w.ByteArrayStatistics.Min()
}

func (w wrappedBinaryStats) Max() []byte {
	return w.ByteArrayStatistics.Max()
}

type wrappedStringStats struct {
	*metadata.ByteArrayStatistics
}

func (w wrappedStringStats) Min() string {
	data := w.ByteArrayStatistics.Min()

	return unsafe.String(unsafe.SliceData(data), len(data))
}

func (w wrappedStringStats) Max() string {
	data := w.ByteArrayStatistics.Max()

	return unsafe.String(unsafe.SliceData(data), len(data))
}

type wrappedUUIDStats struct {
	*metadata.FixedLenByteArrayStatistics
}

func (w wrappedUUIDStats) Min() uuid.UUID {
	uid, err := uuid.FromBytes(w.FixedLenByteArrayStatistics.Min())
	if err != nil {
		panic(err)
	}

	return uid
}

func (w wrappedUUIDStats) Max() uuid.UUID {
	uid, err := uuid.FromBytes(w.FixedLenByteArrayStatistics.Max())
	if err != nil {
		panic(err)
	}

	return uid
}

type wrappedFLBAStats struct {
	*metadata.FixedLenByteArrayStatistics
}

func (w wrappedFLBAStats) Min() []byte {
	return w.FixedLenByteArrayStatistics.Min()
}

func (w wrappedFLBAStats) Max() []byte {
	return w.FixedLenByteArrayStatistics.Max()
}

type wrappedDecStats struct {
	*metadata.FixedLenByteArrayStatistics
	scale int
}

func (w wrappedDecStats) Min() iceberg.Decimal {
	dec, err := tblutils.BigEndianToDecimal(w.FixedLenByteArrayStatistics.Min())
	if err != nil {
		panic(err)
	}

	return iceberg.Decimal{Val: dec, Scale: w.scale}
}

func (w wrappedDecStats) Max() iceberg.Decimal {
	dec, err := tblutils.BigEndianToDecimal(w.FixedLenByteArrayStatistics.Max())
	if err != nil {
		panic(err)
	}

	return iceberg.Decimal{Val: dec, Scale: w.scale}
}

type statsAgg interface {
	min() iceberg.Literal
	max() iceberg.Literal
	update(stats metadata.TypedStatistics)
	minAsBytes() ([]byte, error)
	maxAsBytes() ([]byte, error)
}

type statsAggregator[T iceberg.LiteralType] struct {
	curMin iceberg.TypedLiteral[T]
	curMax iceberg.TypedLiteral[T]

	cmp           iceberg.Comparator[T]
	primitiveType iceberg.PrimitiveType
	truncLen      int
}

func newStatAgg[T iceberg.LiteralType](typ iceberg.PrimitiveType, trunc int) statsAgg {
	var z T

	return &statsAggregator[T]{
		primitiveType: typ,
		truncLen:      trunc,
		cmp:           iceberg.NewLiteral(z).(iceberg.TypedLiteral[T]).Comparator(),
	}
}

func createStatsAgg(typ iceberg.PrimitiveType, physicalTypeStr string, truncLen int) (statsAgg, error) {
	expectedPhysical := primitiveToPhysicalType(typ)
	if physicalTypeStr != expectedPhysical {
		switch {
		case physicalTypeStr == "INT32" && expectedPhysical == "INT64":
		case physicalTypeStr == "FLOAT" && expectedPhysical == "DOUBLE":
		default:
			return nil, fmt.Errorf("unexpected physical type %s for %s, expected %s",
				physicalTypeStr, typ, expectedPhysical)
		}
	}

	switch physicalTypeStr {
	case "BOOLEAN":
		return newStatAgg[bool](typ, truncLen), nil
	case "INT32":
		switch typ.(type) {
		case iceberg.DecimalType:
			return &decAsIntAgg[int32]{
				newStatAgg[int32](typ, truncLen).(*statsAggregator[int32]),
			}, nil
		}

		return newStatAgg[int32](typ, truncLen), nil
	case "INT64":
		switch typ.(type) {
		case iceberg.DecimalType:
			return &decAsIntAgg[int64]{
				newStatAgg[int64](typ, truncLen).(*statsAggregator[int64]),
			}, nil
		}

		return newStatAgg[int64](typ, truncLen), nil
	case "FLOAT":
		return newStatAgg[float32](typ, truncLen), nil
	case "DOUBLE":
		return newStatAgg[float64](typ, truncLen), nil
	case "FIXED_LEN_BYTE_ARRAY":
		switch typ.(type) {
		case iceberg.UUIDType:
			return newStatAgg[uuid.UUID](typ, truncLen), nil
		case iceberg.DecimalType:
			return newStatAgg[iceberg.Decimal](typ, truncLen), nil
		default:
			return newStatAgg[[]byte](typ, truncLen), nil
		}
	case "BYTE_ARRAY":
		if typ.Equals(iceberg.PrimitiveTypes.String) {
			return newStatAgg[string](typ, truncLen), nil
		}

		return newStatAgg[[]byte](typ, truncLen), nil
	default:
		return nil, fmt.Errorf("unsupported physical type: %s", physicalTypeStr)
	}
}

func (s *statsAggregator[T]) min() iceberg.Literal { return s.curMin }
func (s *statsAggregator[T]) max() iceberg.Literal { return s.curMax }

func (s *statsAggregator[T]) update(stats metadata.TypedStatistics) {
	st := stats.(typedStat[T])
	s.updateMin(st.Min())
	s.updateMax(st.Max())
}

func (s *statsAggregator[T]) toBytes(val iceberg.Literal) ([]byte, error) {
	v, err := val.To(s.primitiveType)
	if err != nil {
		return nil, err
	}

	return v.MarshalBinary()
}

func (s *statsAggregator[T]) updateMin(val T) {
	if s.curMin == nil {
		s.curMin = iceberg.NewLiteral(val).(iceberg.TypedLiteral[T])
	} else {
		if s.cmp(val, s.curMin.Value()) < 0 {
			s.curMin = iceberg.NewLiteral(val).(iceberg.TypedLiteral[T])
		}
	}
}

func (s *statsAggregator[T]) updateMax(val T) {
	if s.curMax == nil {
		s.curMax = iceberg.NewLiteral(val).(iceberg.TypedLiteral[T])
	} else {
		if s.cmp(val, s.curMax.Value()) > 0 {
			s.curMax = iceberg.NewLiteral(val).(iceberg.TypedLiteral[T])
		}
	}
}

func (s *statsAggregator[T]) minAsBytes() ([]byte, error) {
	if s.curMin == nil {
		return nil, nil
	}

	if s.truncLen > 0 {
		return s.toBytes((&iceberg.TruncateTransform{Width: s.truncLen}).
			Apply(iceberg.Optional[iceberg.Literal]{Valid: true, Val: s.curMin}).Val)
	}

	return s.toBytes(s.curMin)
}

func (s *statsAggregator[T]) maxAsBytes() ([]byte, error) {
	if s.curMax == nil {
		return nil, nil
	}

	if s.truncLen <= 0 {
		return s.toBytes(s.curMax)
	}

	switch s.primitiveType.(type) {
	case iceberg.StringType:
		if !s.curMax.Type().Equals(s.primitiveType) {
			return nil, errors.New("expected current max to be a string")
		}

		curMax := any(s.curMax.Value()).(string)
		result := truncateUpperBoundText(curMax, s.truncLen)
		if result != "" {
			return s.toBytes(iceberg.StringLiteral(result))
		}

		return nil, nil
	case iceberg.BinaryType:
		if !s.curMax.Type().Equals(s.primitiveType) {
			return nil, errors.New("expected current max to be a binary")
		}

		curMax := any(s.curMax.Value()).([]byte)
		result := truncateUpperBoundBinary(curMax, s.truncLen)
		if len(result) > 0 {
			return s.toBytes(iceberg.BinaryLiteral(result))
		}

		return nil, nil
	default:
		return nil, fmt.Errorf("%s cannot be truncated for upper bound", s.primitiveType)
	}
}

type decAsIntAgg[T int32 | int64] struct {
	*statsAggregator[T]
}

func (s *decAsIntAgg[T]) minAsBytes() ([]byte, error) {
	if s.curMin == nil {
		return nil, nil
	}

	lit := iceberg.DecimalLiteral(iceberg.Decimal{
		Val:   decimal128.FromI64(int64(s.curMin.Value())),
		Scale: s.primitiveType.(iceberg.DecimalType).Scale(),
	})
	if s.truncLen > 0 {
		return s.toBytes((&iceberg.TruncateTransform{Width: s.truncLen}).
			Apply(iceberg.Optional[iceberg.Literal]{Valid: true, Val: lit}).Val)
	}

	return s.toBytes(lit)
}

func (s *decAsIntAgg[T]) maxAsBytes() ([]byte, error) {
	if s.curMax == nil {
		return nil, nil
	}

	lit := iceberg.DecimalLiteral(iceberg.Decimal{
		Val:   decimal128.FromI64(int64(s.curMax.Value())),
		Scale: s.primitiveType.(iceberg.DecimalType).Scale(),
	})
	if s.truncLen <= 0 {
		return s.toBytes(lit)
	}

	return nil, fmt.Errorf("%s cannot be truncated for upper bound", s.primitiveType)
}

type metricModeType string

const (
	metricModeTruncate metricModeType = "truncate"
	metricModeNone     metricModeType = "none"
	metricModeCounts   metricModeType = "counts"
	metricModeFull     metricModeType = "full"
)

type metricsMode struct {
	typ metricModeType
	len int
}

var truncationExpr = regexp.MustCompile(`^truncate\((\d+)\)$`)

func matchMetricsMode(mode string) (metricsMode, error) {
	sanitized := strings.ToLower(strings.TrimSpace(mode))
	if strings.HasPrefix(sanitized, string(metricModeTruncate)) {
		m := truncationExpr.FindStringSubmatch(sanitized)
		if len(m) < 2 {
			return metricsMode{}, fmt.Errorf("malformed truncate metrics mode: %s", mode)
		}

		truncLen, err := strconv.Atoi(m[1])
		if err != nil {
			return metricsMode{}, fmt.Errorf("malformed truncate metrics mode: %s", mode)
		}

		if truncLen <= 0 {
			return metricsMode{}, fmt.Errorf("invalid truncate length: %d", truncLen)
		}

		return metricsMode{typ: metricModeTruncate, len: truncLen}, nil
	}

	switch sanitized {
	case string(metricModeNone):
		return metricsMode{typ: metricModeNone}, nil
	case string(metricModeCounts):
		return metricsMode{typ: metricModeCounts}, nil
	case string(metricModeFull):
		return metricsMode{typ: metricModeFull}, nil
	default:
		return metricsMode{}, fmt.Errorf("unsupported metrics mode: %s", mode)
	}
}

type statisticsCollector struct {
	fieldID    int
	icebergTyp iceberg.PrimitiveType
	mode       metricsMode
	colName    string
}

type arrowStatsCollector struct {
	fieldID     int
	schema      *iceberg.Schema
	props       iceberg.Properties
	defaultMode string
}

func (a *arrowStatsCollector) Schema(_ *iceberg.Schema, results func() []statisticsCollector) []statisticsCollector {
	return results()
}

func (a *arrowStatsCollector) Struct(_ iceberg.StructType, results []func() []statisticsCollector) []statisticsCollector {
	result := make([]statisticsCollector, 0, len(results))
	for _, res := range results {
		result = append(result, res()...)
	}

	return result
}

func (a *arrowStatsCollector) Field(field iceberg.NestedField, fieldRes func() []statisticsCollector) []statisticsCollector {
	a.fieldID = field.ID

	return fieldRes()
}

func (a *arrowStatsCollector) List(list iceberg.ListType, elemResult func() []statisticsCollector) []statisticsCollector {
	a.fieldID = list.ElementID

	return elemResult()
}

func (a *arrowStatsCollector) Map(m iceberg.MapType, keyResult func() []statisticsCollector, valResult func() []statisticsCollector) []statisticsCollector {
	a.fieldID = m.KeyID
	keyRes := keyResult()

	a.fieldID = m.ValueID
	valRes := valResult()

	return append(keyRes, valRes...)
}

func (a *arrowStatsCollector) Primitive(dt iceberg.PrimitiveType) []statisticsCollector {
	colName, ok := a.schema.FindColumnName(a.fieldID)
	if !ok {
		return []statisticsCollector{}
	}

	metMode, err := matchMetricsMode(a.defaultMode)
	if err != nil {
		panic(err)
	}

	colMode, ok := a.props[MetricsModeColumnConfPrefix+"."+colName]
	if ok {
		metMode, err = matchMetricsMode(colMode)
		if err != nil {
			panic(err)
		}
	}

	switch dt.(type) {
	case iceberg.StringType:
	case iceberg.BinaryType:
	default:
		if metMode.typ == metricModeTruncate {
			metMode = metricsMode{typ: metricModeFull, len: 0}
		}
	}

	isNested := strings.Contains(colName, ".")
	if isNested && (metMode.typ == metricModeTruncate || metMode.typ == metricModeFull) {
		metMode = metricsMode{typ: metricModeCounts}
	}

	return []statisticsCollector{{
		fieldID:    a.fieldID,
		icebergTyp: dt,
		colName:    colName,
		mode:       metMode,
	}}
}

func computeStatsPlan(sc *iceberg.Schema, props iceberg.Properties) (map[int]statisticsCollector, error) {
	result := make(map[int]statisticsCollector)

	visitor := &arrowStatsCollector{
		schema: sc, props: props,
		defaultMode: props.Get(DefaultWriteMetricsModeKey, DefaultWriteMetricsModeDefault),
	}

	collectors, err := iceberg.PreOrderVisit(sc, visitor)
	if err != nil {
		return nil, err
	}

	for _, entry := range collectors {
		result[entry.fieldID] = entry
	}

	return result, nil
}

type id2ParquetPath struct {
	fieldID int
	path    string
}

type id2ParquetPathVisitor struct {
	fieldID int
	path    []string
}

func (v *id2ParquetPathVisitor) Schema(_ *iceberg.Schema, res func() []id2ParquetPath) []id2ParquetPath {
	return res()
}

func (v *id2ParquetPathVisitor) Struct(_ iceberg.StructType, results []func() []id2ParquetPath) []id2ParquetPath {
	result := make([]id2ParquetPath, 0, len(results))
	for _, res := range results {
		result = append(result, res()...)
	}

	return result
}

func (v *id2ParquetPathVisitor) Field(field iceberg.NestedField, res func() []id2ParquetPath) []id2ParquetPath {
	v.fieldID = field.ID
	v.path = append(v.path, field.Name)
	result := res()
	v.path = v.path[:len(v.path)-1]

	return result
}

func (v *id2ParquetPathVisitor) List(listType iceberg.ListType, elemResult func() []id2ParquetPath) []id2ParquetPath {
	v.fieldID = listType.ElementID
	v.path = append(v.path, "list")
	result := elemResult()
	v.path = v.path[:len(v.path)-1]

	return result
}

func (v *id2ParquetPathVisitor) Map(m iceberg.MapType, keyResult func() []id2ParquetPath, valResult func() []id2ParquetPath) []id2ParquetPath {
	v.fieldID = m.KeyID
	v.path = append(v.path, "key_value")
	keyRes := keyResult()
	v.path = v.path[:len(v.path)-1]

	v.fieldID = m.ValueID
	v.path = append(v.path, "key_value")
	valRes := valResult()
	v.path = v.path[:len(v.path)-1]

	return append(keyRes, valRes...)
}

func (v *id2ParquetPathVisitor) Primitive(iceberg.PrimitiveType) []id2ParquetPath {
	return []id2ParquetPath{{fieldID: v.fieldID, path: strings.Join(v.path, ".")}}
}

func parquetPathToIDMapping(sc *iceberg.Schema) (map[string]int, error) {
	result := make(map[string]int)

	paths, err := iceberg.PreOrderVisit(sc, &id2ParquetPathVisitor{})
	if err != nil {
		return nil, err
	}

	for _, entry := range paths {
		result[entry.path] = entry.fieldID
	}

	return result, nil
}

type dataFileStatistics struct {
	recordCount     int64
	colSizes        map[int]int64
	valueCounts     map[int]int64
	nullValueCounts map[int]int64
	nanValueCounts  map[int]int64
	colAggs         map[int]statsAgg
	splitOffsets    []int64
}

func (d *dataFileStatistics) partitionValue(field iceberg.PartitionField, sc *iceberg.Schema) any {
	agg, ok := d.colAggs[field.SourceID]
	if !ok {
		return nil
	}

	if !field.Transform.PreservesOrder() {
		panic(fmt.Errorf("cannot infer partition value from parquet metadata for a non-linear partition field: %s with transform %s",
			field.Name, field.Transform))
	}

	lowerVal, upperVal := agg.min(), agg.max()
	if !lowerVal.Equals(upperVal) {
		panic(fmt.Errorf("cannot infer partition value from parquet metadata as there is more than one value for partition field: %s. (low: %s, high: %s)",
			field.Name, lowerVal, upperVal))
	}

	val := field.Transform.Apply(must(partitionRecordValue(field, lowerVal, sc)))
	if !val.Valid {
		return nil
	}

	return val.Val.Any()
}

func (d *dataFileStatistics) toDataFile(schema *iceberg.Schema, spec iceberg.PartitionSpec, path string, format iceberg.FileFormat, filesize int64) iceberg.DataFile {
	var partitionData map[string]any
	if !spec.Equals(*iceberg.UnpartitionedSpec) {
		partitionData = make(map[string]any)
		for field := range spec.Fields() {
			val := d.partitionValue(field, schema)
			if val != nil {
				partitionData[field.Name] = val
			}
		}
	}

	bldr, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData,
		path, format, partitionData, d.recordCount, filesize)
	if err != nil {
		panic(err)
	}

	lowerBounds := make(map[int][]byte)
	upperBounds := make(map[int][]byte)

	for fieldID, agg := range d.colAggs {
		min := must(agg.minAsBytes())
		max := must(agg.maxAsBytes())
		if len(min) > 0 {
			lowerBounds[fieldID] = min
		}
		if len(max) > 0 {
			upperBounds[fieldID] = max
		}
	}

	if len(lowerBounds) > 0 {
		bldr.LowerBoundValues(lowerBounds)
	}
	if len(upperBounds) > 0 {
		bldr.UpperBoundValues(upperBounds)
	}

	bldr.ColumnSizes(d.colSizes)
	bldr.ValueCounts(d.valueCounts)
	bldr.NullValueCounts(d.nullValueCounts)
	bldr.NaNValueCounts(d.nanValueCounts)
	bldr.SplitOffsets(d.splitOffsets)

	return bldr.Build()
}

func dataFileStatsFromParquetMetadata(pqmeta *metadata.FileMetaData, statsCols map[int]statisticsCollector, colMapping map[string]int) *dataFileStatistics {
	var (
		colSizes        = make(map[int]int64)
		valueCounts     = make(map[int]int64)
		splitOffsets    = make([]int64, 0)
		nullValueCounts = make(map[int]int64)
		nanValueCounts  = make(map[int]int64)
		invalidateCol   = make(map[int]struct{})
		colAggs         = make(map[int]statsAgg)
	)

	for rg := range pqmeta.NumRowGroups() {
		// reference: https://github.com/apache/iceberg-python/blob/main/pyiceberg/io/pyarrow.py#L2285
		rowGroup := pqmeta.RowGroup(rg)
		colChunk, err := rowGroup.ColumnChunk(0)
		if err != nil {
			panic(err)
		}

		dataOffset, dictOffset := colChunk.DataPageOffset(), colChunk.DictionaryPageOffset()
		if colChunk.HasDictionaryPage() && dictOffset < dataOffset {
			splitOffsets = append(splitOffsets, dictOffset)
		} else {
			splitOffsets = append(splitOffsets, dataOffset)
		}

		for pos := range rowGroup.NumColumns() {
			colChunk, err = rowGroup.ColumnChunk(pos)
			if err != nil {
				panic(err)
			}

			fieldID := colMapping[colChunk.PathInSchema().String()]
			statsCol := statsCols[fieldID]
			if statsCol.mode.typ == metricModeNone {
				continue
			}

			colSizes[fieldID] += colChunk.TotalCompressedSize()
			valueCounts[fieldID] += colChunk.NumValues()
			set, err := colChunk.StatsSet()
			if err != nil {
				panic(err)
			}

			if !set {
				invalidateCol[fieldID] = struct{}{}

				continue
			}

			stats, err := colChunk.Statistics()
			if err != nil {
				invalidateCol[fieldID] = struct{}{}

				continue
			}

			if stats.HasNullCount() {
				nullValueCounts[fieldID] += stats.NullCount()
			}

			if statsCol.mode.typ == metricModeCounts || !stats.HasMinMax() {
				continue
			}

			agg, ok := colAggs[fieldID]
			if !ok {
				agg, err = createStatsAgg(statsCol.icebergTyp, stats.Type().String(), statsCol.mode.len)
				if err != nil {
					panic(err)
				}

				colAggs[fieldID] = agg
			}

			switch typ := statsCol.icebergTyp.(type) {
			case iceberg.BinaryType:
				stats = wrappedBinaryStats{stats.(*metadata.ByteArrayStatistics)}
			case iceberg.UUIDType:
				stats = wrappedUUIDStats{stats.(*metadata.FixedLenByteArrayStatistics)}
			case iceberg.StringType:
				stats = wrappedStringStats{stats.(*metadata.ByteArrayStatistics)}
			case iceberg.FixedType:
				stats = wrappedFLBAStats{stats.(*metadata.FixedLenByteArrayStatistics)}
			case iceberg.DecimalType:
				if typ.Precision() > 18 {
					stats = wrappedDecStats{stats.(*metadata.FixedLenByteArrayStatistics), typ.Scale()}
				}
			}

			agg.update(stats)
		}

	}

	slices.Sort(splitOffsets)
	maps.DeleteFunc(nullValueCounts, func(fieldID int, _ int64) bool {
		_, ok := invalidateCol[fieldID]

		return ok
	})
	maps.DeleteFunc(colAggs, func(fieldID int, _ statsAgg) bool {
		_, ok := invalidateCol[fieldID]

		return ok
	})

	return &dataFileStatistics{
		recordCount:     pqmeta.GetNumRows(),
		colSizes:        colSizes,
		valueCounts:     valueCounts,
		nullValueCounts: nullValueCounts,
		nanValueCounts:  nanValueCounts,
		splitOffsets:    splitOffsets,
		colAggs:         colAggs,
	}
}

func parquetFilesToDataFiles(fileIO iceio.IO, meta *MetadataBuilder, paths iter.Seq[string]) iter.Seq2[iceberg.DataFile, error] {
	return func(yield func(iceberg.DataFile, error) bool) {
		defer func() {
			if r := recover(); r != nil {
				switch e := r.(type) {
				case string:
					yield(nil, fmt.Errorf("error encountered during parquet file conversion: %s", e))
				case error:
					yield(nil, fmt.Errorf("error encountered during parquet file conversion: %w", e))
				}
			}
		}()

		currentSchema, currentSpec := meta.CurrentSchema(), meta.CurrentSpec()

		for filePath := range paths {
			inputFile := must(fileIO.Open(filePath))
			defer inputFile.Close()

			rdr := must(file.NewParquetReader(inputFile))
			defer rdr.Close()

			arrRdr := must(pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator))
			arrSchema := must(arrRdr.Schema())

			if hasIDs := must(VisitArrowSchema(arrSchema, hasIDs{})); hasIDs {
				yield(nil, fmt.Errorf("%w: cannot add file %s because it has field-ids. add-files only supports the addition of files without field_ids",
					iceberg.ErrNotImplemented, filePath))

				return
			}

			if err := checkArrowSchemaCompat(currentSchema, arrSchema, false); err != nil {
				panic(err)
			}

			statistics := dataFileStatsFromParquetMetadata(rdr.MetaData(),
				must(computeStatsPlan(currentSchema, meta.props)),
				must(parquetPathToIDMapping(currentSchema)))

			df := statistics.toDataFile(currentSchema, currentSpec, filePath, iceberg.ParquetFile, rdr.MetaData().GetSourceFileSize())
			if !yield(df, nil) {
				return
			}
		}
	}
}
