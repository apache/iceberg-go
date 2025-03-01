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
	"slices"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go/internal"
)

type MappedField struct {
	Names []string `json:"names"`
	// iceberg spec says this is optional, but I don't see any examples
	// of this being left empty. Does pyiceberg need to be updated or should
	// the spec not say field-id is optional?
	FieldID *int          `json:"field-id,omitempty"`
	Fields  []MappedField `json:"fields,omitempty"`
}

func (m *MappedField) ID() int {
	if m.FieldID == nil {
		return -1
	}

	return *m.FieldID
}

func (m *MappedField) GetField(field string) *MappedField {
	for _, f := range m.Fields {
		if slices.Contains(f.Names, field) {
			return &f
		}
	}

	return nil
}

func (m *MappedField) Len() int { return len(m.Fields) }

func (m *MappedField) String() string {
	var bldr strings.Builder
	bldr.WriteString("([")
	bldr.WriteString(strings.Join(m.Names, ", "))
	bldr.WriteString("] -> ")

	if m.FieldID != nil {
		bldr.WriteString(strconv.Itoa(*m.FieldID))
	} else {
		bldr.WriteByte('?')
	}

	if len(m.Fields) > 0 {
		bldr.WriteByte(' ')
		for i, f := range m.Fields {
			if i != 0 {
				bldr.WriteString(", ")
			}
			bldr.WriteString(f.String())
		}
	}

	bldr.WriteByte(')')

	return bldr.String()
}

type NameMapping []MappedField

func (nm NameMapping) String() string {
	var bldr strings.Builder
	bldr.WriteString("[\n")
	for _, f := range nm {
		bldr.WriteByte('\t')
		bldr.WriteString(f.String())
		bldr.WriteByte('\n')
	}
	bldr.WriteByte(']')

	return bldr.String()
}

type NameMappingVisitor[S, T any] interface {
	Mapping(nm NameMapping, fieldResults S) S
	Fields(st []MappedField, fieldResults []T) S
	Field(field MappedField, fieldResult S) T
}

func VisitNameMapping[S, T any](obj NameMapping, visitor NameMappingVisitor[S, T]) (res S, err error) {
	if obj == nil {
		err = fmt.Errorf("%w: cannot visit nil NameMapping", ErrInvalidArgument)

		return
	}

	defer internal.RecoverError(&err)

	return visitor.Mapping(obj, visitMappedFields([]MappedField(obj), visitor)), err
}

func VisitMappedFields[S, T any](fields []MappedField, visitor NameMappingVisitor[S, T]) (res S, err error) {
	defer internal.RecoverError(&err)

	return visitMappedFields(fields, visitor), err
}

func visitMappedFields[S, T any](fields []MappedField, visitor NameMappingVisitor[S, T]) S {
	results := make([]T, len(fields))
	for i, f := range fields {
		results[i] = visitor.Field(f, visitMappedFields(f.Fields, visitor))
	}

	return visitor.Fields(fields, results)
}

type NameMappingAccessor struct{}

func (NameMappingAccessor) SchemaPartner(partner *MappedField) *MappedField {
	return partner
}

func (n NameMappingAccessor) FieldPartner(partnerStruct *MappedField, _ int, fieldName string) *MappedField {
	if partnerStruct == nil {
		return nil
	}

	return partnerStruct.GetField(fieldName)
}

func (n NameMappingAccessor) ListElementPartner(partnerList *MappedField) *MappedField {
	if partnerList == nil {
		return nil
	}

	return partnerList.GetField("element")
}

func (n NameMappingAccessor) MapKeyPartner(partnerMap *MappedField) *MappedField {
	if partnerMap == nil {
		return nil
	}

	return partnerMap.GetField("key")
}

func (n NameMappingAccessor) MapValuePartner(partnerMap *MappedField) *MappedField {
	if partnerMap == nil {
		return nil
	}

	return partnerMap.GetField("value")
}

type nameMapProjectVisitor struct {
	currentPath []string
}

func (n *nameMapProjectVisitor) popPath() {
	n.currentPath = n.currentPath[:len(n.currentPath)-1]
}

func (n *nameMapProjectVisitor) BeforeField(f NestedField, _ *MappedField) {
	n.currentPath = append(n.currentPath, f.Name)
}

func (n *nameMapProjectVisitor) AfterField(NestedField, *MappedField) {
	n.popPath()
}

func (n *nameMapProjectVisitor) BeforeListElement(NestedField, *MappedField) {
	n.currentPath = append(n.currentPath, "element")
}

func (n *nameMapProjectVisitor) AfterListElement(NestedField, *MappedField) {
	n.popPath()
}

func (n *nameMapProjectVisitor) BeforeMapKey(NestedField, *MappedField) {
	n.currentPath = append(n.currentPath, "key")
}

func (n *nameMapProjectVisitor) AfterMapKey(NestedField, *MappedField) {
	n.popPath()
}

func (n *nameMapProjectVisitor) BeforeMapValue(NestedField, *MappedField) {
	n.currentPath = append(n.currentPath, "value")
}

func (n *nameMapProjectVisitor) AfterMapValue(NestedField, *MappedField) {
	n.popPath()
}

func (n *nameMapProjectVisitor) Schema(_ *Schema, _ *MappedField, structResult NestedField) NestedField {
	return structResult
}

func (n *nameMapProjectVisitor) Struct(_ StructType, _ *MappedField, fieldResults []NestedField) NestedField {
	return NestedField{
		Type: &StructType{FieldList: fieldResults},
	}
}

func (n *nameMapProjectVisitor) Field(field NestedField, fieldPartner *MappedField, fieldResult NestedField) NestedField {
	if fieldPartner == nil {
		panic(fmt.Errorf("%w: field missing from name mapping: %s",
			ErrInvalidArgument, strings.Join(n.currentPath, ".")))
	}

	return NestedField{
		ID:             *fieldPartner.FieldID,
		Name:           field.Name,
		Type:           fieldResult.Type,
		Required:       field.Required,
		Doc:            field.Doc,
		InitialDefault: field.InitialDefault,
		WriteDefault:   field.WriteDefault,
	}
}

func (nameMapProjectVisitor) mappedFieldID(mapped *MappedField, name string) int {
	for _, f := range mapped.Fields {
		if slices.Contains(f.Names, name) {
			if f.FieldID != nil {
				return *f.FieldID
			}

			return -1
		}
	}

	return -1
}

func (n *nameMapProjectVisitor) List(lt ListType, listPartner *MappedField, elemResult NestedField) NestedField {
	if listPartner == nil {
		panic(fmt.Errorf("%w: field missing from name mapping: %s",
			ErrInvalidArgument, strings.Join(n.currentPath, ".")))
	}

	elementID := n.mappedFieldID(listPartner, "element")

	return NestedField{
		Type: &ListType{
			ElementID:       elementID,
			Element:         elemResult.Type,
			ElementRequired: lt.ElementRequired,
		},
	}
}

func (n *nameMapProjectVisitor) Map(m MapType, mapPartner *MappedField, keyResult, valResult NestedField) NestedField {
	if mapPartner == nil {
		panic(fmt.Errorf("%w: field missing from name mapping: %s",
			ErrInvalidArgument, strings.Join(n.currentPath, ".")))
	}

	keyID := n.mappedFieldID(mapPartner, "key")
	valID := n.mappedFieldID(mapPartner, "value")

	return NestedField{
		Type: &MapType{
			KeyID:         keyID,
			KeyType:       keyResult.Type,
			ValueID:       valID,
			ValueType:     valResult.Type,
			ValueRequired: m.ValueRequired,
		},
	}
}

func (n *nameMapProjectVisitor) Primitive(p PrimitiveType, primitivePartner *MappedField) NestedField {
	if primitivePartner == nil {
		panic(fmt.Errorf("%w: field missing from name mapping: %s",
			ErrInvalidArgument, strings.Join(n.currentPath, ".")))
	}

	return NestedField{Type: p}
}

func ApplyNameMapping(schemaWithoutIDs *Schema, nameMapping NameMapping) (*Schema, error) {
	top, err := VisitSchemaWithPartner(schemaWithoutIDs,
		&MappedField{Fields: nameMapping},
		&nameMapProjectVisitor{currentPath: make([]string, 0, 1)},
		NameMappingAccessor{})
	if err != nil {
		return nil, err
	}

	return NewSchema(schemaWithoutIDs.ID,
		top.Type.(*StructType).FieldList...), nil
}

type createMapping struct{}

func (createMapping) Schema(_ *Schema, result []MappedField) []MappedField {
	return result
}

func (createMapping) Struct(st StructType, result [][]MappedField) []MappedField {
	output := make([]MappedField, len(st.FieldList))
	for i, field := range st.FieldList {
		output[i] = MappedField{
			Names:   []string{field.Name},
			FieldID: &field.ID,
			Fields:  result[i],
		}
	}

	return output
}

func (createMapping) Field(_ NestedField, result []MappedField) []MappedField {
	return result
}

func (createMapping) List(listType ListType, elemResult []MappedField) []MappedField {
	return []MappedField{{
		Names:   []string{"element"},
		FieldID: &listType.ElementID,
		Fields:  elemResult,
	}}
}

func (createMapping) Map(mapType MapType, keyResult, valResult []MappedField) []MappedField {
	return []MappedField{
		{
			Names:   []string{"key"},
			FieldID: &mapType.KeyID,
			Fields:  keyResult,
		},
		{
			Names:   []string{"value"},
			FieldID: &mapType.ValueID,
			Fields:  valResult,
		},
	}
}

func (createMapping) Primitive(_ PrimitiveType) []MappedField {
	return []MappedField{}
}

func createMappingFromSchema(schema *Schema) NameMapping {
	result, _ := Visit(schema, createMapping{})

	return NameMapping(result)
}
