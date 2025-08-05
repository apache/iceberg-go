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
	"fmt"
	"iter"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

func filesToDataFiles(ctx context.Context, fileIO iceio.IO, meta *MetadataBuilder, paths iter.Seq[string]) iter.Seq2[iceberg.DataFile, error] {
	return func(yield func(iceberg.DataFile, error) bool) {
		defer func() {
			if r := recover(); r != nil {
				switch e := r.(type) {
				case string:
					yield(nil, fmt.Errorf("error encountered during file conversion: %s", e))
				case error:
					yield(nil, fmt.Errorf("error encountered during file conversion: %w", e))
				}
			}
		}()

		partitionSpec, err := meta.CurrentSpec()
		if err != nil || partitionSpec == nil {
			yield(nil, fmt.Errorf("%w: cannot add files without a current spec", err))

			return
		}

		currentSchema, currentSpec := meta.CurrentSchema(), *partitionSpec

		for filePath := range paths {
			format := tblutils.FormatFromFileName(filePath)
			rdr := must(format.Open(ctx, fileIO, filePath))
			defer rdr.Close()

			arrSchema := must(rdr.Schema())

			if hasIDs := must(VisitArrowSchema(arrSchema, hasIDs{})); hasIDs {
				yield(nil, fmt.Errorf("%w: cannot add file %s because it has field-ids. add-files only supports the addition of files without field_ids",
					iceberg.ErrNotImplemented, filePath))

				return
			}

			if err := checkArrowSchemaCompat(currentSchema, arrSchema, false); err != nil {
				yield(nil, err)

				return
			}

			statistics := format.DataFileStatsFromMeta(rdr.Metadata(), must(computeStatsPlan(currentSchema, meta.props)),
				must(format.PathToIDMapping(currentSchema)))

			df := statistics.ToDataFile(currentSchema, currentSpec, filePath, iceberg.ParquetFile, rdr.SourceFileSize())
			if !yield(df, nil) {
				return
			}
		}
	}
}

func recordNBytes(rec arrow.Record) (total int64) {
	for _, c := range rec.Columns() {
		total += int64(c.Data().SizeInBytes())
	}

	return total
}

func binPackRecords(itr iter.Seq2[arrow.Record, error], recordLookback int, targetFileSize int64) iter.Seq[[]arrow.Record] {
	return internal.PackingIterator(func(yield func(arrow.Record) bool) {
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
	sc        *arrow.Schema
	itr       iter.Seq2[arrow.Record, error]
	fs        iceio.WriteFileIO
	writeUUID *uuid.UUID
	counter   iter.Seq[int]
}

func recordsToDataFiles(ctx context.Context, rootLocation string, meta *MetadataBuilder, args recordWritingArgs) (ret iter.Seq2[iceberg.DataFile, error]) {
	if args.counter == nil {
		args.counter = internal.Counter(0)
	}

	defer func() {
		if r := recover(); r != nil {
			var err error
			switch e := r.(type) {
			case string:
				err = fmt.Errorf("error encountered during file writing %s", e)
			case error:
				err = fmt.Errorf("error encountered during file writing: %w", e)
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
		panic(err)
	}
	currentSpec, err := meta.CurrentSpec()
	if err != nil || currentSpec == nil {
		panic(fmt.Errorf("%w: cannot write files without a current spec", err))
	}
	nextCount, stopCount := iter.Pull(args.counter)
	if currentSpec.IsUnpartitioned() {
		tasks := func(yield func(WriteTask) bool) {
			defer stopCount()

			for batch := range binPackRecords(args.itr, 20, targetFileSize) {
				cnt, _ := nextCount()
				t := WriteTask{
					Uuid:    *args.writeUUID,
					ID:      cnt,
					Schema:  taskSchema,
					Batches: batch,
				}
				if !yield(t) {
					return
				}
			}
		}

		return writeFiles(ctx, rootLocation, args.fs, meta, tasks)
	}

	panic(fmt.Errorf("%w: write stream with partitions", iceberg.ErrNotImplemented))
}
