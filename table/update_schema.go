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
	"strings"

	"github.com/apache/iceberg-go"
)

type UpdateSchema struct {
	schema       *iceberg.Schema
	lastColumnID int

	deletes map[int]struct{}
	updates map[int]*iceberg.NestedField
	adds    map[int][]*iceberg.NestedField

	allowIncompatibleChanges bool
	identifierFields         map[string]struct{}
	caseSensitive            bool
}

func NewUpdateSchema(base Metadata, s *iceberg.Schema, lastColumnID int) *UpdateSchema {

	return &UpdateSchema{
		schema:                   s,
		deletes:                  make(map[int]struct{}),
		updates:                  make(map[int]*iceberg.NestedField),
		adds:                     make(map[int][]*iceberg.NestedField),
		lastColumnID:             lastColumnID,
		allowIncompatibleChanges: false,
		identifierFields:         make(map[string]struct{}),
		caseSensitive:            true,
	}
}

// AllowIncompatibleChanges permits incompatible schema changes.
func (us *UpdateSchema) AllowIncompatibleChanges() *UpdateSchema {
	us.allowIncompatibleChanges = true

	return us
}

func (us *UpdateSchema) AddColumn(path []string, required bool, dataType iceberg.Type, doc string, initialDefault any) (*UpdateSchema, error) {

	if len(path) == 0 {
		return nil, fmt.Errorf("AddColumn: path must contain at least the new column name")
	}

	colName := path[len(path)-1]
	parentID := -1

	var parentField *iceberg.NestedField
	parentPath := strings.Join(path[:len(path)-1], ".")

	if parentPath != "" {

		pf := us.findField(path[:len(path)-1])
		if pf == nil {
			return nil, fmt.Errorf("cannot find parent struct: %s", parentPath)
		}

		// Only Struct / List(element) / Map(value) can accept children.
		switch nt := pf.Type.(type) {
		case *iceberg.StructType:
			parentField = pf
		case *iceberg.MapType:
			vf := nt.ValueField()
			parentField = &vf
		case *iceberg.ListType:
			ef := nt.ElementField()
			parentField = &ef
		default:
			return nil, fmt.Errorf("parent is not a nested type: %s", parentPath)
		}

		parentID = parentField.ID

	}

	if existing := us.findField(path); existing != nil && !us.isDeleted(existing.ID) {
		return nil, fmt.Errorf("cannot add column; name already exists: %s", strings.Join(path, "."))
	}

	// checking if a required column has a default value
	if required && initialDefault == nil && !us.allowIncompatibleChanges {
		return nil, fmt.Errorf("cannot add required column without default value: %s", strings.Join(path, "."))
	}

	newID := us.assignNewColumnID()
	nf := &iceberg.NestedField{
		Name:           colName,
		ID:             newID,
		Required:       required,
		Type:           dataType,
		Doc:            doc,
		InitialDefault: initialDefault,
	}

	us.adds[parentID] = append(us.adds[parentID], nf)

	return us, nil
}

type ColumnUpdate struct {
	Type     iceberg.Type // nil means no change
	Doc      *string      // nil means no change
	Default  *any         // nil means no change
	Required *bool        // nil means no change
}

func (us *UpdateSchema) UpdateColumn(path []string, updates ColumnUpdate) (*UpdateSchema, error) {
	field := us.findForUpdate(path)
	if field == nil {
		return nil, fmt.Errorf("Cannot update missing column: %s", strings.Join(path, "."))
	}

	if us.isDeleted(field.ID) {
		return nil, fmt.Errorf("Cannot update a column that will be deleted: %s", strings.Join(path, "."))
	}

	// Track if any changes were made
	hasChanges := false

	// Update type if provided
	if updates.Type != nil && !field.Type.Equals(updates.Type) {
		if !allowedPromotion(field.Type, updates.Type) {
			return nil, fmt.Errorf("Cannot update type of column: %s: %s -> %s",
				strings.Join(path, "."), field.Type.String(), updates.Type.String())
		}
		field.Type = updates.Type
		hasChanges = true
	}

	// Update documentation if provided
	if updates.Doc != nil && field.Doc != *updates.Doc {
		field.Doc = *updates.Doc
		hasChanges = true
	}

	// Update default value if provided
	if updates.Default != nil && field.InitialDefault != *updates.Default {
		field.InitialDefault = *updates.Default
		hasChanges = true
	}

	// Update required flag if provided
	if updates.Required != nil && field.Required != *updates.Required {
		isRequired := *updates.Required

		// Apply the same logic as internalUpdateColumnRequirement
		if isRequired == field.Required {
			// No change needed
		} else {
			isDefaultedAdd := us.isAdded(field.ID) && field.InitialDefault != nil

			if isRequired && !isDefaultedAdd && !us.allowIncompatibleChanges {
				return nil, fmt.Errorf("Cannot change column nullability: %s: optional -> required",
					strings.Join(path, "."))
			}

			field.Required = isRequired
			hasChanges = true
		}
	}

	// Only update the map if changes were made
	if hasChanges {
		us.updates[field.ID] = field
	}

	return us, nil
}

func stringPtr(s string) *string {
	return &s
}

func boolPtr(b bool) *bool {
	return &b
}

func anyPtr(a any) *any {
	return &a
}

// DeleteColumn removes a column from the schema.
func (us *UpdateSchema) DeleteColumn(path []string) (*UpdateSchema, error) {

	field := us.findField(path)
	if field == nil {
		return nil, fmt.Errorf("Cannot delete missing column: %s", strings.Join(path, "."))
	}

	if _, ok := us.adds[field.ID]; ok {
		return nil, fmt.Errorf("Cannot delete a column that has additions: %s", strings.Join(path, "."))
	}

	if _, ok := us.updates[field.ID]; ok {
		return nil, fmt.Errorf("Cannot delete a column that has updates: %s", strings.Join(path, "."))
	}

	us.deletes[field.ID] = struct{}{}

	return us, nil
}

func (us *UpdateSchema) isDeleted(id int) bool {
	_, ok := us.deletes[id]
	return ok
}

func (us *UpdateSchema) findForUpdate(path []string) *iceberg.NestedField {

	existing := us.findField(path)
	if existing != nil {
		if update, ok := us.updates[existing.ID]; ok {
			return update
		}

		// adding to updates
		us.updates[existing.ID] = existing

		return existing
	}

	// Check in added fields if no existing field found
	for _, addedFields := range us.adds {
		for _, add := range addedFields {
			if add.Name == path[len(path)-1] {
				return add
			}
		}
	}

	return nil
}

func (us *UpdateSchema) Apply() *iceberg.Schema {

	return us.applyChanges()
}

func (us *UpdateSchema) isAdded(id int) bool {
	_, ok := us.adds[id]

	return ok
}

func (us *UpdateSchema) assignNewColumnID() int {
	next := us.lastColumnID + 1
	us.lastColumnID = next

	return next
}

func (us *UpdateSchema) findField(path []string) *iceberg.NestedField {
	name := strings.Join(path, ".")

	var field iceberg.NestedField
	var ok bool

	if us.caseSensitive {
		field, ok = us.schema.FindFieldByName(name)
	} else {
		field, ok = us.schema.FindFieldByNameCaseInsensitive(name)
	}

	if !ok {
		return nil
	}

	return &field
}

func allowedPromotion(oldType, newType iceberg.Type) bool {

	switch old := oldType.(type) {

	case iceberg.PrimitiveType:
		switch old.Type() {
		case "int":
			return newType.Type() == "long"
		case "float":
			return newType.Type() == "double"
		case "decimal":
			o := oldType.(iceberg.DecimalType)
			n, ok := newType.(iceberg.DecimalType)
			return ok && o.Scale() == n.Scale() && n.Precision() > o.Precision()
		case "date":
			return newType.Type() == "timestamp" || newType.Type() == "timestamp_ns"
		}
	}

	return false
}

func (u *UpdateSchema) applyChanges() *iceberg.Schema {

	newFields := rebuild(u.schema.AsStruct().FieldList, -1, u)

	idList := u.schema.IdentifierFieldIDs
	newID := u.schema.ID + 1

	return iceberg.NewSchemaWithIdentifiers(newID, idList, newFields...)
}

func rebuild(fields []iceberg.NestedField, parentID int, u *UpdateSchema) []iceberg.NestedField {
	var out []iceberg.NestedField

	for _, f := range fields {
		if _, gone := u.deletes[f.ID]; gone {
			continue
		}
		if upd, ok := u.updates[f.ID]; ok {
			f = *upd
		}
		switch t := f.Type.(type) {
		case *iceberg.StructType:
			f.Type = &iceberg.StructType{FieldList: rebuild(t.Fields(), f.ID, u)}
		case *iceberg.ListType:
			el := t.ElementField()
			elSlice := rebuild([]iceberg.NestedField{el}, el.ID, u)
			el = elSlice[0]
			f.Type = &iceberg.ListType{
				ElementID:       el.ID,
				Element:         el.Type,
				ElementRequired: el.Required,
			}
		case *iceberg.MapType:
			val := t.ValueField()
			valSlice := rebuild([]iceberg.NestedField{val}, val.ID, u)
			val = valSlice[0]
			f.Type = &iceberg.MapType{
				KeyID:         t.KeyField().ID,
				KeyType:       t.KeyField().Type,
				ValueID:       val.ID,
				ValueType:     val.Type,
				ValueRequired: val.Required,
			}
		}
		out = append(out, f)
	}

	// append new children for this parent
	for _, nf := range u.adds[parentID] {
		out = append(out, *nf)
	}
	return out
}

// func (us *UpdateSchema) Commit() (Metadata, error) {
// 	newSchema := us.applyChanges()

// 	for _, existingSchema := range (*us.base).Schemas() {
// 		if newSchema.Equals(existingSchema) {
// 			if existingSchema.ID != (*us.base).CurrentSchema().ID {
// 				builder, err := MetadataBuilderFromBase(*us.base)
// 				if err != nil {
// 					return nil, err
// 				}

// 				_, err = builder.SetCurrentSchemaID(existingSchema.ID)
// 				if err != nil {
// 					return nil, err
// 				}

// 				return builder.Build()
// 			}

// 			return *us.base, nil
// 		}
// 	}

// 	builder, err := MetadataBuilderFromBase(*us.base)
// 	if err != nil {
// 		return nil, err
// 	}

// 	_, err = builder.AddSchema(newSchema, us.lastColumnID, false)
// 	if err != nil {
// 		return nil, err
// 	}

// 	_, err = builder.SetCurrentSchemaID(newSchema.ID)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return builder.Build()
// }
