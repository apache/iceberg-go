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

		return res, err
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

// UpdateNameMapping performs incremental updates to an existing NameMapping, preserving
// backward compatibility by maintaining existing field name mappings while adding new ones.
// This is different from createMappingFromSchema which creates a completely new mapping
// and loses all historical field name mappings.
//
// For example, when updating a field name:
//
//	Original: {FieldID: 1, Names: ["foo"]}
//	After update: {FieldID: 1, Names: ["foo", "foo_update"]}
//
// This preserves compatibility with existing data files that reference the old field names.
func UpdateNameMapping(nameMapping NameMapping, updates map[int]NestedField, adds map[int][]NestedField) (result NameMapping, err error) {
	if nameMapping == nil {
		return nil, fmt.Errorf("%w: cannot visit nil NameMapping", ErrInvalidArgument)
	}

	defer internal.RecoverError(&err)

	updater := updateNameMappingVisitor{updates: updates, adds: adds}

	return NameMapping(updater.addNewFields(updater.updatedFields(nameMapping), -1)), nil
}

type updateNameMappingVisitor struct {
	updates map[int]NestedField
	adds    map[int][]NestedField
}

// updatedFields builds independent copies while handling renames at each sibling
// level. It avoids allocating an intermediate visitor-result slice per level.
func (u *updateNameMappingVisitor) updatedFields(fields []MappedField) []MappedField {
	if len(fields) == 0 {
		return nil
	}

	// A rename may take an alias from a sibling, so find the new owners before
	// building the output. Most levels have no updates and need no map.
	var reassignments map[string]int
	if len(u.updates) != 0 {
		for _, field := range fields {
			if field.FieldID == nil {
				continue
			}
			if update, exists := u.updates[*field.FieldID]; exists {
				if reassignments == nil {
					reassignments = make(map[string]int)
				}
				reassignments[update.Name] = update.ID
			}
		}
	}

	result := make([]MappedField, 0, len(fields))
	// Keep the copied IDs together rather than allocating one int per field.
	// Every returned FieldID points into this independent backing slice.
	var copiedIDs []int
	for _, field := range fields {
		children := u.updatedFields(field.Fields)
		updated := MappedField{
			Names:  slices.Clone(field.Names),
			Fields: children,
		}
		if field.FieldID != nil {
			if copiedIDs == nil {
				copiedIDs = make([]int, len(fields))
			}
			id := *field.FieldID
			copiedIDs[len(result)] = id
			updated.FieldID = &copiedIDs[len(result)]

			if rename, exists := u.updates[id]; exists && !slices.Contains(updated.Names, rename.Name) {
				updated.Names = append(updated.Names, rename.Name)
			}
			updated.Fields = u.addNewFields(children, id)
		}

		if retained := u.removeReassignedNames(updated, reassignments); retained != nil {
			result = append(result, *retained)
		}
	}

	// The original visitor returned nil when every alias was discarded.
	if len(result) == 0 {
		return nil
	}

	return result
}

func (u *updateNameMappingVisitor) removeReassignedNames(field MappedField, assignments map[string]int) *MappedField {
	remainingNames := field.Names[:0]
	for _, name := range field.Names {
		assignedID, exists := assignments[name]
		if !exists || (field.FieldID != nil && assignedID == *field.FieldID) {
			remainingNames = append(remainingNames, name)
		}
	}
	clear(field.Names[len(remainingNames):])

	if len(remainingNames) == 0 {
		return nil
	}
	field.Names = remainingNames

	return &field
}

func (u *updateNameMappingVisitor) addNewFields(mappedFields []MappedField, parentID int) []MappedField {
	fieldsToAdd, exists := u.adds[parentID]
	if !exists {
		return mappedFields
	}

	reassignments := make(map[string]int, len(fieldsToAdd))
	for _, add := range fieldsToAdd {
		reassignments[add.Name] = add.ID
	}

	// mappedFields was freshly allocated by updatedFields, so compacting it
	// here cannot change the caller's original mapping.
	fields := mappedFields[:0]
	if fields == nil {
		fields = make([]MappedField, 0, len(fieldsToAdd))
	}
	for _, field := range mappedFields {
		if retained := u.removeReassignedNames(field, reassignments); retained != nil {
			fields = append(fields, *retained)
		}
	}

	for _, add := range fieldsToAdd {
		children := visitField(add, createMapping{})
		if len(children) == 0 {
			children = nil
		}
		fields = append(fields, MappedField{
			FieldID: &add.ID,
			Names:   []string{add.Name},
			Fields:  children,
		})
	}

	return fields
}

type NameMappingAccessor struct{}

func (NameMappingAccessor) SchemaPartner(partner *MappedField) *MappedField {
	return partner
}

func (NameMappingAccessor) FieldPartner(partnerStruct *MappedField, _ int, fieldName string) *MappedField {
	if partnerStruct == nil {
		return nil
	}

	return partnerStruct.GetField(fieldName)
}

func (NameMappingAccessor) ListElementPartner(partnerList *MappedField) *MappedField {
	if partnerList == nil {
		return nil
	}

	return partnerList.GetField("element")
}

func (NameMappingAccessor) MapKeyPartner(partnerMap *MappedField) *MappedField {
	if partnerMap == nil {
		return nil
	}

	return partnerMap.GetField("key")
}

func (NameMappingAccessor) MapValuePartner(partnerMap *MappedField) *MappedField {
	if partnerMap == nil {
		return nil
	}

	return partnerMap.GetField("value")
}

type indexedNameMappingAccessor struct {
	fieldsByName map[*MappedField]map[string]*MappedField
}

func newIndexedNameMappingAccessor(root *MappedField) indexedNameMappingAccessor {
	accessor := indexedNameMappingAccessor{
		fieldsByName: make(map[*MappedField]map[string]*MappedField),
	}
	accessor.indexFields(root)

	return accessor
}

func (n indexedNameMappingAccessor) indexFields(field *MappedField) {
	if field == nil || len(field.Fields) == 0 {
		return
	}

	fieldsByName := make(map[string]*MappedField, len(field.Fields))
	n.fieldsByName[field] = fieldsByName

	for i := range field.Fields {
		mappedField := &field.Fields[i]
		for _, name := range mappedField.Names {
			if _, exists := fieldsByName[name]; !exists {
				fieldsByName[name] = mappedField
			}
		}

		n.indexFields(mappedField)
	}
}

func (n indexedNameMappingAccessor) getField(partnerStruct *MappedField, fieldName string) *MappedField {
	if partnerStruct == nil {
		return nil
	}

	if fieldsByName, ok := n.fieldsByName[partnerStruct]; ok {
		return fieldsByName[fieldName]
	}

	return partnerStruct.GetField(fieldName)
}

func (n indexedNameMappingAccessor) SchemaPartner(partner *MappedField) *MappedField {
	return partner
}

func (n indexedNameMappingAccessor) FieldPartner(partnerStruct *MappedField, _ int, fieldName string) *MappedField {
	return n.getField(partnerStruct, fieldName)
}

func (n indexedNameMappingAccessor) ListElementPartner(partnerList *MappedField) *MappedField {
	return n.getField(partnerList, "element")
}

func (n indexedNameMappingAccessor) MapKeyPartner(partnerMap *MappedField) *MappedField {
	return n.getField(partnerMap, "key")
}

func (n indexedNameMappingAccessor) MapValuePartner(partnerMap *MappedField) *MappedField {
	return n.getField(partnerMap, "value")
}

type nameMapProjectVisitor struct {
	currentPath []string
	accessor    indexedNameMappingAccessor
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

func (n nameMapProjectVisitor) mappedFieldID(mapped *MappedField, name string) int {
	field := n.accessor.getField(mapped, name)
	if field != nil && field.FieldID != nil {
		return *field.FieldID
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

func (n *nameMapProjectVisitor) Variant(v VariantType, variantPartner *MappedField) NestedField {
	if variantPartner == nil {
		panic(fmt.Errorf("%w: field missing from name mapping: %s",
			ErrInvalidArgument, strings.Join(n.currentPath, ".")))
	}

	return NestedField{Type: v}
}

func ApplyNameMapping(schemaWithoutIDs *Schema, nameMapping NameMapping) (*Schema, error) {
	partner := &MappedField{Fields: nameMapping}
	accessor := newIndexedNameMappingAccessor(partner)
	top, err := VisitSchemaWithPartner(schemaWithoutIDs,
		partner,
		&nameMapProjectVisitor{
			currentPath: make([]string, 0, 1),
			accessor:    accessor,
		},
		accessor)
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

func (createMapping) Variant(_ VariantType) []MappedField {
	return []MappedField{}
}

func createMappingFromSchema(schema *Schema) NameMapping {
	result, _ := Visit(schema, createMapping{})

	return NameMapping(result)
}
