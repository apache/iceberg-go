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
	"reflect"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

type UpdateSchema struct {
	txn          *Transaction
	schema       *iceberg.Schema
	lastColumnID int

	deletes map[int]struct{}
	updates map[int]*iceberg.NestedField
	adds    map[int][]*iceberg.NestedField
	moves   map[int][]moveReq // ← NEW (key = parent-field-id, −1 for root)

	allowIncompatibleChanges bool
	identifierFields         map[string]struct{}
	caseSensitive            bool
}

type moveOp string

const (
	OpFirst  moveOp = "first"
	OpBefore moveOp = "before"
	OpAfter  moveOp = "after"
)

type moveReq struct {
	fieldID      int
	otherFieldID int // 0 when opFirst
	op           moveOp
}

func NewUpdateSchema(txn *Transaction, s *iceberg.Schema, lastColumnID int) *UpdateSchema {

	return &UpdateSchema{
		txn:                      txn,
		schema:                   s,
		deletes:                  make(map[int]struct{}),
		updates:                  make(map[int]*iceberg.NestedField),
		adds:                     make(map[int][]*iceberg.NestedField),
		moves:                    make(map[int][]moveReq),
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

// AddColumn adds a new column to the schema.
// usage:
// a is a struct/list/map b is a sub field of a
//
// us.AddColumn([]string{"a","b"}, true, iceberg.StringType{}, "doc", "default")
// us.AddColumn([]string{"a","b"}, true, iceberg.StringType{}, "doc", "default")
func (us *UpdateSchema) AddColumn(path []string, required bool, dataType iceberg.Type, doc string, initialDefault any) (*UpdateSchema, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("AddColumn: path must contain at least the new column name")
	}

	colName := path[len(path)-1]
	parentID := -1

	if len(path) > 1 {
		parentPath := path[:len(path)-1]
		pf := us.findField(parentPath)
		if pf == nil {
			return nil, fmt.Errorf("cannot find parent struct: %s", strings.Join(parentPath, "."))
		}

		switch nt := pf.Type.(type) {
		case *iceberg.StructType:
			parentID = pf.ID
		case *iceberg.MapType:
			vf := nt.ValueField()
			parentID = vf.ID
		case *iceberg.ListType:
			ef := nt.ElementField()
			parentID = ef.ID
		default:
			return nil, fmt.Errorf("parent is not a nested type: %s", strings.Join(parentPath, "."))
		}
	}

	if existing := us.findField(path); existing != nil && !us.isDeleted(existing.ID) {
		return nil, fmt.Errorf("cannot add column; name already exists: %s", strings.Join(path, "."))
	}

	if required && initialDefault == nil && !us.allowIncompatibleChanges {
		return nil, fmt.Errorf("cannot add required column without default value: %s", strings.Join(path, "."))
	}

	if initialDefault != nil {
		if err := validateDefaultValue(dataType, initialDefault); err != nil {
			return nil, err
		}
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

// UpdateColumn updates a column in the schema.
// usage:
// a is a struct/list/map b is a sub field of a
//
// us.UpdateColumn([]string{"a","b"}, ColumnUpdate{Type: iceberg.StringType{}})
// us.UpdateColumn([]string{"a","b"}, ColumnUpdate{...})
func (us *UpdateSchema) UpdateColumn(path []string, updates ColumnUpdate) (*UpdateSchema, error) {
	field := us.findForUpdate(path)
	if field == nil {
		return nil, fmt.Errorf("Cannot update missing column: %s", strings.Join(path, "."))
	}

	if us.isDeleted(field.ID) {
		return nil, fmt.Errorf("Cannot update a column that will be deleted: %s", strings.Join(path, "."))
	}

	hasChanges := false

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

		if isRequired == field.Required {
			// No change needed
			return nil, fmt.Errorf("Cannot change column nullability: %s: %t -> %t", strings.Join(path, "."), field.Required, isRequired)
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

// DeleteColumn removes a column from the schema.
// usage:
// us.DeleteColumn([]string{"a","b"})
// us.DeleteColumn([]string{"a","b","c"})
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

// Move re-orders a column relative to its siblings.
//
//	op = OpFirst             → column becomes first in its struct
//	op = OpBefore / OpAfter  → place column before / after anotherColumn
//
// Paths use the same slice notation as Add/Update/Delete (["a","b"]) which means "a.b".
// usage:
//
//	us.Move([]string{"a","b"}, []string{"c"}, OpBefore)
//	us.Move([]string{"a","b"}, []string{"c"}, OpAfter)
//	us.Move([]string{"a","b"}, []string{"c"}, OpFirst)
//
//	us.Move([]string{"a","b"}, []string{"c"}, OpBefore)
func (us *UpdateSchema) Move(columnToMove, referenceColumn []string, op moveOp) (*UpdateSchema, error) {

	colField := us.findFieldIncludingAdded(columnToMove)
	if colField == nil {
		return nil, fmt.Errorf("cannot move missing column: %s", strings.Join(columnToMove, "."))
	}

	parentID := us.parentIDForPath(columnToMove)

	var otherID int
	if op == OpBefore || op == OpAfter {
		other := us.findFieldIncludingAdded(referenceColumn)
		if other == nil {
			return nil, fmt.Errorf("reference column for move not found: %s", strings.Join(referenceColumn, "."))
		}
		if us.parentIDForPath(referenceColumn) != parentID {
			return nil, fmt.Errorf("cannot move column across different parent structs")
		}
		otherID = other.ID
		if otherID == colField.ID {
			return nil, fmt.Errorf("cannot move column relative to itself")
		}
	}

	us.moves[parentID] = append(us.moves[parentID], moveReq{
		fieldID:      colField.ID,
		otherFieldID: otherID,
		op:           op,
	})
	return us, nil
}

// parentIDForPath returns the field-id of the direct parent struct
// (-1 means root level).
func (us *UpdateSchema) parentIDForPath(path []string) int {

	if len(path) == 1 {
		return -1
	}

	if f := us.findFieldIncludingAdded(path[:len(path)-1]); f != nil {
		return f.ID
	}

	return -1
}

func (us *UpdateSchema) findForUpdate(path []string) *iceberg.NestedField {
	existing := us.findFieldIncludingAdded(path)
	if existing != nil {
		if update, ok := us.updates[existing.ID]; ok {
			return update
		}

		// adding to updates
		us.updates[existing.ID] = existing

		return existing
	}

	return nil
}

// findFieldIncludingAdded finds a field by path, including both existing fields and newly added fields
func (us *UpdateSchema) findFieldIncludingAdded(path []string) *iceberg.NestedField {
	// First try to find in existing schema
	if field := us.findField(path); field != nil {
		return field
	}

	// If not found in existing schema, search in added fields
	// For now, we only support top-level added fields in move operations
	if len(path) == 1 {
		colName := path[0]
		for _, addedFields := range us.adds {
			for _, add := range addedFields {
				if (us.caseSensitive && add.Name == colName) ||
					(!us.caseSensitive && strings.EqualFold(add.Name, colName)) {
					return add
				}
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
	newFields, err := rebuild(u.schema.AsStruct().FieldList, -1, u)
	if err != nil {
		panic(err)
	}

	idList := u.schema.IdentifierFieldIDs
	newID := u.schema.ID + 1

	return iceberg.NewSchemaWithIdentifiers(newID, idList, newFields...)
}

func rebuild(fields []iceberg.NestedField, parentID int, us *UpdateSchema) ([]iceberg.NestedField, error) {
	var out []iceberg.NestedField

	//iterate over the current fields to apply updates and deletes and if struct, list or map, we call rebuild for the fields in them recursively
	for _, f := range fields {
		if _, gone := us.deletes[f.ID]; gone {
			continue
		}
		if upd, ok := us.updates[f.ID]; ok {
			f = *upd
		}
		switch t := f.Type.(type) {

		case *iceberg.StructType:
			fields, err := rebuild(t.Fields(), f.ID, us)
			if err != nil {
				return nil, fmt.Errorf("error rebuilding struct type: %w", err)
			}
			f.Type = &iceberg.StructType{FieldList: fields}

		case *iceberg.ListType:
			el := t.ElementField()
			fields, err := rebuild([]iceberg.NestedField{el}, el.ID, us)
			if err != nil {
				return nil, fmt.Errorf("error rebuilding list type: %w", err)
			}
			el = fields[0]
			f.Type = &iceberg.ListType{
				ElementID:       el.ID,
				Element:         el.Type,
				ElementRequired: el.Required,
			}

		case *iceberg.MapType:
			val := t.ValueField()
			fields, err := rebuild([]iceberg.NestedField{val}, val.ID, us)
			if err != nil {
				return nil, fmt.Errorf("error rebuilding list type: %w", err)
			}
			val = fields[0]
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

	// append new children for this parent id (-1 means root)
	for _, nf := range us.adds[parentID] {
		out = append(out, *nf)
	}

	//check if there are any moves for this parent id (-1 means root)
	if reqs := us.moves[parentID]; len(reqs) > 0 {
		var err error
		out, err = reorder(out, reqs)
		if err != nil {
			return nil, fmt.Errorf("error reordering fields: %w", err)
		}
	}

	return out, nil
}

func reorder(fields []iceberg.NestedField, reqs []moveReq) ([]iceberg.NestedField, error) {
	//find the index of a field by its id
	indexOf := func(id int) int {
		for i, f := range fields {
			if f.ID == id {
				return i
			}
		}
		return -1
	}

	for _, m := range reqs {
		pos := indexOf(m.fieldID)
		if pos == -1 {
			continue // field might have been deleted
		}

		f := fields[pos]
		fields = append(fields[:pos], fields[pos+1:]...)

		switch m.op {
		case OpFirst:
			fields = append([]iceberg.NestedField{f}, fields...)
		case OpBefore:
			idx := indexOf(m.otherFieldID)
			if idx == -1 {
				return nil, fmt.Errorf("move-before target not found at commit time")
			}
			fields = append(fields[:idx],
				append([]iceberg.NestedField{f}, fields[idx:]...)...)
		case OpAfter:
			idx := indexOf(m.otherFieldID)
			if idx == -1 {
				return nil, fmt.Errorf("move-after target not found at commit time")
			}
			fields = append(fields[:idx+1],
				append([]iceberg.NestedField{f}, fields[idx+1:]...)...)
		default:
			return nil, fmt.Errorf("unknown move op: %s", m.op)
		}
	}

	return fields, nil
}

func validateDefaultValue(typ iceberg.Type, val any) error {
	//Defaults are only allowed on primitive columns
	prim, ok := typ.(iceberg.PrimitiveType)
	if !ok {
		return fmt.Errorf("defaults are only allowed on primitive columns, got %s", typ.Type())
	}

	lit, err := literalFromAny(val)
	if err != nil {
		return err
	}

	litType := lit.Type()

	// Exact match?
	if litType.Equals(prim) {
		return nil
	}

	return fmt.Errorf("default literal of type %s is not assignable to of type %s", litType.String(), prim.Type())
}

func literalFromAny(v any) (iceberg.Literal, error) {
	switch x := v.(type) {
	case bool:
		return iceberg.NewLiteral(x), nil
	case int32:
		return iceberg.NewLiteral(x), nil
	case int64, int:
		return iceberg.NewLiteral(int64(reflect.ValueOf(x).Int())), nil
	case float32:
		return iceberg.NewLiteral(x), nil
	case float64:
		return iceberg.NewLiteral(x), nil
	case string:
		return iceberg.NewLiteral(x), nil
	case []byte:
		return iceberg.NewLiteral(x), nil
	case uuid.UUID:
		return iceberg.NewLiteral(x), nil
	case iceberg.Date:
		return iceberg.NewLiteral(x), nil
	case iceberg.Time:
		return iceberg.NewLiteral(x), nil
	case iceberg.Timestamp:
		return iceberg.NewLiteral(x), nil
	case iceberg.Decimal:
		return iceberg.NewLiteral(x), nil
	default:
		return nil, fmt.Errorf("unsupported literal type %T", v)
	}
}

func (us *UpdateSchema) isDeleted(id int) bool {
	_, ok := us.deletes[id]
	return ok
}
