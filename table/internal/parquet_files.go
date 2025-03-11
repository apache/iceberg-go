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

package internal

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

type ParquetFileSource struct {
	mem  memory.Allocator
	fs   iceio.IO
	file iceberg.DataFile
}

type wrapPqArrowReader struct {
	*pqarrow.FileReader
}

func (w wrapPqArrowReader) Close() error {
	return w.ParquetReader().Close()
}

func (w wrapPqArrowReader) PrunedSchema(projectedIDs map[int]struct{}, mapping iceberg.NameMapping) (*arrow.Schema, []int, error) {
	return pruneParquetColumns(w.Manifest, projectedIDs, false, mapping)
}

func (w wrapPqArrowReader) GetRecords(ctx context.Context, cols []int, tester any) (array.RecordReader, error) {
	var (
		testRg func(*metadata.RowGroupMetaData, []int) (bool, error)
		ok     bool
	)

	if tester != nil {
		testRg, ok = tester.(func(*metadata.RowGroupMetaData, []int) (bool, error))
		if !ok {
			return nil, fmt.Errorf("%w: invalid tester function", iceberg.ErrInvalidArgument)
		}
	}

	var rgList []int
	if testRg != nil {
		rgList = make([]int, 0)
		fileMeta, numRg := w.ParquetReader().MetaData(), w.ParquetReader().NumRowGroups()
		for rg := 0; rg < numRg; rg++ {
			rgMeta := fileMeta.RowGroup(rg)
			use, err := testRg(rgMeta, cols)
			if err != nil {
				return nil, err
			}

			if use {
				rgList = append(rgList, rg)
			}
		}
	}

	return w.GetRecordReader(ctx, cols, rgList)
}

func (pfs *ParquetFileSource) GetReader(ctx context.Context) (FileReader, error) {
	pf, err := pfs.fs.Open(pfs.file.FilePath())
	if err != nil {
		return nil, err
	}

	rdr, err := file.NewParquetReader(pf,
		file.WithReadProps(parquet.NewReaderProperties(pfs.mem)))
	if err != nil {
		return nil, err
	}

	// TODO: grab these from the context
	arrProps := pqarrow.ArrowReadProperties{
		Parallel:  true,
		BatchSize: 1 << 17,
	}

	if pfs.file.ContentType() == iceberg.EntryContentPosDeletes {
		// for dictionary for filepath col
		arrProps.SetReadDict(0, true)
	}

	fr, err := pqarrow.NewFileReader(rdr, arrProps, pfs.mem)
	if err != nil {
		return nil, err
	}

	return wrapPqArrowReader{fr}, nil
}

type manifestVisitor[T any] interface {
	Manifest(*pqarrow.SchemaManifest, []T, *iceberg.MappedField) T
	Field(pqarrow.SchemaField, T, *iceberg.MappedField) T
	Struct(pqarrow.SchemaField, []T, *iceberg.MappedField) T
	List(pqarrow.SchemaField, T, *iceberg.MappedField) T
	Map(pqarrow.SchemaField, T, T, *iceberg.MappedField) T
	Primitive(pqarrow.SchemaField, *iceberg.MappedField) T
}

func visitParquetManifest[T any](manifest *pqarrow.SchemaManifest, visitor manifestVisitor[T], mapping *iceberg.MappedField) (res T, err error) {
	if manifest == nil {
		err = fmt.Errorf("%w: cannot visit nil manifest", iceberg.ErrInvalidArgument)

		return
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%s", r)
		}
	}()

	var fieldMap *iceberg.MappedField

	results := make([]T, len(manifest.Fields))
	for i, f := range manifest.Fields {
		if mapping != nil {
			fieldMap = mapping.GetField(f.Field.Name)
		}
		res := visitManifestField(f, visitor, fieldMap)
		results[i] = visitor.Field(f, res, fieldMap)
	}

	return visitor.Manifest(manifest, results, mapping), nil
}

func visitParquetManifestStruct[T any](field pqarrow.SchemaField, visitor manifestVisitor[T], mapping *iceberg.MappedField) T {
	results := make([]T, len(field.Children))
	var fieldMap *iceberg.MappedField
	for i, f := range field.Children {
		if mapping != nil {
			fieldMap = mapping.GetField(f.Field.Name)
		}
		res := visitManifestField(f, visitor, fieldMap)
		results[i] = visitor.Field(f, res, fieldMap)
	}

	return visitor.Struct(field, results, mapping)
}

func visitManifestList[T any](field pqarrow.SchemaField, visitor manifestVisitor[T], mapping *iceberg.MappedField) T {
	elemField := field.Children[0]
	var elemMapping *iceberg.MappedField
	if mapping != nil {
		elemMapping = mapping.GetField("element")
	}
	res := visitManifestField(elemField, visitor, elemMapping)

	return visitor.List(field, res, mapping)
}

func visitManifestMap[T any](field pqarrow.SchemaField, visitor manifestVisitor[T], mapping *iceberg.MappedField) T {
	kvfield := field.Children[0]
	keyField, valField := kvfield.Children[0], kvfield.Children[1]
	var keyMapping, valMapping *iceberg.MappedField
	if mapping != nil {
		keyMapping = mapping.GetField("key")
		valMapping = mapping.GetField("value")
	}

	return visitor.Map(field, visitManifestField(keyField, visitor, keyMapping), visitManifestField(valField, visitor, valMapping), mapping)
}

func visitManifestField[T any](field pqarrow.SchemaField, visitor manifestVisitor[T], mapping *iceberg.MappedField) T {
	switch field.Field.Type.(type) {
	case *arrow.StructType:
		return visitParquetManifestStruct(field, visitor, mapping)
	case *arrow.MapType:
		return visitManifestMap(field, visitor, mapping)
	case arrow.ListLikeType:
		return visitManifestList(field, visitor, mapping)
	default:
		return visitor.Primitive(field, mapping)
	}
}

func pruneParquetColumns(manifest *pqarrow.SchemaManifest, selected map[int]struct{}, selectFullTypes bool, mapping iceberg.NameMapping) (*arrow.Schema, []int, error) {
	visitor := &pruneParquetSchema{
		selected:  selected,
		manifest:  manifest,
		fullTypes: selectFullTypes,
		indices:   []int{},
	}

	result, err := visitParquetManifest(manifest, visitor, &iceberg.MappedField{Fields: mapping})
	if err != nil {
		return nil, nil, err
	}

	return arrow.NewSchema(result.Type.(*arrow.StructType).Fields(), &result.Metadata),
		visitor.indices, nil
}

func getFieldID(f arrow.Field) *int {
	if !f.HasMetadata() {
		return nil
	}

	fieldIDStr, ok := f.Metadata.GetValue("PARQUET:field_id")
	if !ok {
		return nil
	}

	id, err := strconv.Atoi(fieldIDStr)
	if err != nil {
		return nil
	}

	return &id
}

type pruneParquetSchema struct {
	selected  map[int]struct{}
	fullTypes bool
	manifest  *pqarrow.SchemaManifest

	indices []int
}

func (p *pruneParquetSchema) fieldID(field arrow.Field, mapping *iceberg.MappedField) int {
	if mapping != nil {
		if mapping.FieldID != nil {
			return *mapping.FieldID
		}
	}

	if id := getFieldID(field); id != nil {
		return *id
	}

	panic(fmt.Errorf("%w: cannot convert %s to Iceberg field, missing field_id",
		iceberg.ErrInvalidSchema, field))
}

func (p *pruneParquetSchema) Manifest(manifest *pqarrow.SchemaManifest, fields []arrow.Field, _ *iceberg.MappedField) arrow.Field {
	finalFields := slices.DeleteFunc(fields, func(f arrow.Field) bool { return f.Type == nil })
	result := arrow.Field{
		Type: arrow.StructOf(finalFields...),
	}
	if manifest.SchemaMeta != nil {
		result.Metadata = *manifest.SchemaMeta
	}

	return result
}

func (p *pruneParquetSchema) Struct(field pqarrow.SchemaField, children []arrow.Field, _ *iceberg.MappedField) arrow.Field {
	selected, fields := []arrow.Field{}, field.Children
	sameType := true

	for i, t := range children {
		field := fields[i]
		if arrow.TypeEqual(field.Field.Type, t.Type) {
			selected = append(selected, *field.Field)
		} else if t.Type == nil {
			sameType = false
			// type has changed, create a new field with the projected type
			selected = append(selected, arrow.Field{
				Name:     field.Field.Name,
				Type:     field.Field.Type,
				Nullable: field.Field.Nullable,
				Metadata: field.Field.Metadata,
			})
		}
	}

	if len(selected) > 0 {
		if len(selected) == len(fields) && sameType {
			// nothing changed, return the original
			return *field.Field
		} else {
			result := *field.Field
			result.Type = arrow.StructOf(selected...)

			return result
		}
	}

	return arrow.Field{}
}

func (p *pruneParquetSchema) Field(field pqarrow.SchemaField, result arrow.Field, mapping *iceberg.MappedField) arrow.Field {
	_, ok := p.selected[p.fieldID(*field.Field, mapping)]
	if !ok {
		if result.Type != nil {
			return result
		}

		return arrow.Field{}
	}

	if p.fullTypes {
		return *field.Field
	}

	if _, ok := field.Field.Type.(*arrow.StructType); ok {
		result := *field.Field
		result.Type = p.projectSelectedStruct(result.Type)

		return result
	}

	if !field.IsLeaf() {
		panic(errors.New("cannot explicitly project list or map types"))
	}

	p.indices = append(p.indices, field.ColIndex)

	return *field.Field
}

func (p *pruneParquetSchema) List(field pqarrow.SchemaField, elemResult arrow.Field, mapping *iceberg.MappedField) arrow.Field {
	var elemMapping *iceberg.MappedField
	if mapping != nil {
		elemMapping = mapping.GetField("element")
	}

	_, ok := p.selected[p.fieldID(*field.Children[0].Field, elemMapping)]
	if !ok {
		if elemResult.Type != nil {
			result := *field.Field
			result.Type = p.projectList(field.Field.Type.(arrow.ListLikeType), elemResult.Type)

			return result
		}

		return arrow.Field{}
	}

	if p.fullTypes {
		return *field.Field
	}

	_, ok = field.Children[0].Field.Type.(*arrow.StructType)
	if field.Children[0].Field.Type != nil && ok {
		result := *field.Field
		projected := p.projectSelectedStruct(elemResult.Type)
		result.Type = p.projectList(field.Field.Type.(arrow.ListLikeType), projected)

		return result
	}

	if !field.Children[0].IsLeaf() {
		panic(errors.New("cannot explicitly project list or map types"))
	}

	p.indices = append(p.indices, field.Children[0].ColIndex)

	return *field.Field
}

func (p *pruneParquetSchema) Map(field pqarrow.SchemaField, keyResult, valResult arrow.Field, mapping *iceberg.MappedField) arrow.Field {
	var valMapping *iceberg.MappedField
	if mapping != nil {
		valMapping = mapping.GetField("value")
	}

	_, ok := p.selected[p.fieldID(*field.Children[0].Children[1].Field, valMapping)]
	if !ok {
		if valResult.Type != nil {
			result := *field.Field
			result.Type = p.projectMap(field.Field.Type.(*arrow.MapType), valResult.Type)

			return result
		}

		return arrow.Field{}
	}

	if p.fullTypes {
		return *field.Field
	}

	_, ok = field.Children[0].Children[1].Field.Type.(*arrow.StructType)
	if ok {
		result := *field.Field
		projected := p.projectSelectedStruct(valResult.Type)
		result.Type = p.projectMap(field.Field.Type.(*arrow.MapType), projected)

		return result
	}

	if !field.Children[0].Children[1].IsLeaf() {
		panic("cannot explicitly project list or map types")
	}

	p.indices = append(p.indices, field.Children[0].Children[0].ColIndex)
	p.indices = append(p.indices, field.Children[0].Children[1].ColIndex)

	return *field.Field
}

func (p *pruneParquetSchema) Primitive(_ pqarrow.SchemaField, _ *iceberg.MappedField) arrow.Field {
	return arrow.Field{}
}

func (p *pruneParquetSchema) projectSelectedStruct(projected arrow.DataType) *arrow.StructType {
	if projected == nil {
		return &arrow.StructType{}
	}

	if ty, ok := projected.(*arrow.StructType); ok {
		return ty
	}

	panic("expected a struct")
}

func (p *pruneParquetSchema) projectList(listType arrow.ListLikeType, elemResult arrow.DataType) arrow.ListLikeType {
	if arrow.TypeEqual(listType.Elem(), elemResult) {
		return listType
	}

	origField := listType.ElemField()
	origField.Type = elemResult

	switch listType.(type) {
	case *arrow.ListType:
		return arrow.ListOfField(origField)
	case *arrow.LargeListType:
		return arrow.LargeListOfField(origField)
	case *arrow.ListViewType:
		return arrow.ListViewOfField(origField)
	}

	n := listType.(*arrow.FixedSizeListType).Len()

	return arrow.FixedSizeListOfField(n, origField)
}

func (p *pruneParquetSchema) projectMap(m *arrow.MapType, valResult arrow.DataType) *arrow.MapType {
	if arrow.TypeEqual(m.ItemType(), valResult) {
		return m
	}

	return arrow.MapOf(m.KeyType(), valResult)
}
