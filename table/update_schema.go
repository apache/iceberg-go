package table

import (
	"fmt"

	"github.com/apache/iceberg-go"
)

type UpdateSchema struct {
	ops                      Operation
	base                     *Metadata
	schema                   *iceberg.Schema
	idToParent               map[int]int
	deletes                  []int
	updates                  map[int]*iceberg.NestedField
	parentToAddedIDs         map[int][]int
	addedNameToID            map[string]int
	lastColumnID             int
	allowIncompatibleChanges bool
	identifierFields         map[string]struct{}
	caseSensitive            bool
}

func allowedPromotion(oldType, newType iceberg.Type) bool {

	//allow promotion between primitive types
	//Only three numeric widenings are permitted
	// 1. int → long (a.k.a. bigint)
	// 2. float → double
	// 3. decimal(P,S) → decimal(P′, S) where P′ > P and S is unchanged

	if oldType.Type() == "int" && newType.Type() == "long" {
		return true
	}

	if oldType.Type() == "float" && newType.Type() == "double" {
		return true
	}

	if oldType.Type() == "decimal" && newType.Type() == "decimal" {
		oldDecimal := oldType.(iceberg.DecimalType)
		newDecimal := newType.(iceberg.DecimalType)
		if oldDecimal.Precision() < newDecimal.Precision() {
			return true
		}
	}

	return false
}

func NewUpdateSchema(ops Operation, base *Metadata, s *iceberg.Schema, lastColumnID int) *UpdateSchema {
	identifierFields := make(map[string]struct{})

	return &UpdateSchema{
		ops:                      ops,
		base:                     base,
		schema:                   s,
		idToParent:               nil,
		deletes:                  make([]int, 0),
		updates:                  make(map[int]*iceberg.NestedField),
		parentToAddedIDs:         make(map[int][]int),
		addedNameToID:            make(map[string]int),
		lastColumnID:             lastColumnID,
		allowIncompatibleChanges: false,
		identifierFields:         identifierFields,
		caseSensitive:            true,
	}
}

// AllowIncompatibleChanges permits incompatible schema changes.
func (us *UpdateSchema) AllowIncompatibleChanges() *UpdateSchema {
	us.allowIncompatibleChanges = true
	return us
}

func (us *UpdateSchema) assignNewColumnID() int {
	next := us.lastColumnID + 1
	us.lastColumnID = next
	return next
}

func (us *UpdateSchema) findField(name string) *iceberg.NestedField {
	if us.caseSensitive {
		field, ok := us.schema.FindFieldByName(name)
		if !ok {
			return nil
		}
		return &field
	}
	field, ok := us.schema.FindFieldByNameCaseInsensitive(name)
	if !ok {
		return nil
	}

	return &field
}

func (us *UpdateSchema) AddColumn(parent, name string, id int, required bool, dataType iceberg.Type, doc string, initialDefaultValue any) *UpdateSchema {

	//ToDO: handle Nested columns

	field := us.findField(name)
	if field != nil {
		foundInDeletes := false
		for _, delete := range us.deletes {
			if delete == field.ID {
				foundInDeletes = true
			}
		}
		if !foundInDeletes {
			panic(fmt.Sprintf("Cannot add column, name already exists: %s", name))
		}

	}

	// cannot add a required column without a default value
	if initialDefaultValue == nil && !required && !us.allowIncompatibleChanges {
		panic(fmt.Sprintf("incompatible change: cannot add required column without a default value: %s", name))
	}

	newID := us.assignNewColumnID()

	nestedField := &iceberg.NestedField{
		Name:     name,
		ID:       newID,
		Required: required,
		Type:     dataType,
		Doc:      doc,
	}

	if initialDefaultValue != nil {
		nestedField.InitialDefault = initialDefaultValue
	}

	us.updates[newID] = nestedField

	return us

}

// DeleteColumn removes a column from the schema.
func (us *UpdateSchema) DeleteColumn(name string) *UpdateSchema {

	field := us.findField(name)
	if field == nil {
		panic(fmt.Sprintf("Cannot delete missing column: %s", name))
	}

	if _, ok := us.parentToAddedIDs[field.ID]; ok {
		panic(fmt.Sprintf("Cannot delete a column that has additions: %s", name))
	}

	if _, ok := us.updates[field.ID]; ok {
		panic(fmt.Sprintf("Cannot delete a column that has updates: %s", name))
	}

	us.deletes = append(us.deletes, field.ID)

	return us
}

func (us *UpdateSchema) UpdateColumnType(name string, newType iceberg.Type) *UpdateSchema {
	field := us.findField(name)
	if field == nil {
		panic(fmt.Sprintf("Cannot update type of missing column: %s", name))
	}

	for _, id := range us.deletes {
		if id == field.ID {
			panic(fmt.Sprintf("Cannot update a column that will be deleted: %s", field.Name))
		}
	}

	if field.Type.Equals(newType) {
		return us
	}

	//get what type is the new type
	newType.Type()

	us.updates[field.ID].Type = newType
	return us
}

func (us *UpdateSchema) UpdateColumnDoc(name string, doc string) *UpdateSchema {
	field := us.findField(name)
	if field == nil {
		panic(fmt.Sprintf("Cannot update type of missing column: %s", name))
	}

	for _, id := range us.deletes {
		if id == field.ID {
			panic(fmt.Sprintf("Cannot update a column that will be deleted: %s", field.Name))
		}
	}

	if field.Doc == doc {
		return us
	}

	us.updates[field.ID].Doc = doc
	return us
}

func (us *UpdateSchema) UpdateColumnDefault(name string, defaultValue any) *UpdateSchema {
	field := us.findField(name)
	if field == nil {
		panic(fmt.Sprintf("Cannot update default value of missing column: %s", name))
	}

	for _, id := range us.deletes {
		if id == field.ID {
			panic(fmt.Sprintf("Cannot update a column that will be deleted: %s", field.Name))
		}
	}

	if field.InitialDefault == defaultValue {
		return us
	}

	us.updates[field.ID].InitialDefault = defaultValue
	return us
}

// RequireColumn changes an optional column to required.
func (us *UpdateSchema) RequireColumn(name string) *UpdateSchema {
	us.internalUpdateColumnRequirement(name, true)
	return us
}

// MakeColumnOptional changes a required column to optional.
func (us *UpdateSchema) MakeColumnOptional(name string) *UpdateSchema {
	us.internalUpdateColumnRequirement(name, false)
	return us
}

func (us *UpdateSchema) findForUpdate(name string) *iceberg.NestedField {
	existing := us.findField(name)
	if existing != nil {
		if update, ok := us.updates[existing.ID]; ok {
			return update
		}
		return existing
	}

	addedID, ok := us.addedNameToID[name]
	if ok {
		return us.updates[addedID]
	}

	return nil
}

func (su *UpdateSchema) isAdded(name string) bool {
	_, ok := su.addedNameToID[name]
	return ok
}

func (su *UpdateSchema) internalUpdateColumnRequirement(name string, isRequired bool) {
	field := su.findForUpdate(name)
	if field == nil {
		panic(fmt.Sprintf("Cannot update missing column: %s", name))
	}

	if (!isRequired && !field.Required) || (isRequired && field.Required) {
		// if the change is a noop, allow it even if allowIncompatibleChanges is false
		return
	}

	isDefaultedAdd := su.isAdded(name) && field.InitialDefault != nil

	if !isRequired && !isDefaultedAdd && !su.allowIncompatibleChanges {
		panic(fmt.Sprintf("Cannot change column nullability: %s: optional -> required", name))
	}

	for _, id := range su.deletes {
		if id == field.ID {
			panic(fmt.Sprintf("Cannot update a column that will be deleted: %s", field.Name))
		}
	}

	field.Required = isRequired

	su.updates[field.ID] = field
}
