package table

import (
	"fmt"

	"github.com/apache/iceberg-go"
)

type UpdateSchema struct {
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

func NewUpdateSchema(base *Metadata, s *iceberg.Schema, lastColumnID int) *UpdateSchema {
	identifierFields := make(map[string]struct{})

	return &UpdateSchema{
		base:                     base,
		schema:                   s,
		idToParent:               make(map[int]int),
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

func (us *UpdateSchema) AddColumn(parent, name string, new_id int, required bool, dataType iceberg.Type, doc string, initialDefaultValue any) *UpdateSchema {
	parentID := -1
	fullName := ""

	if parent != "" {
		parentField := us.findField(parent)
		if parentField == nil {
			panic(fmt.Sprintf("Cannot find parent struct: %s", parent))
		}

		// Get the parent struct's ID
		parentID = parentField.ID

		// Store the parent-child relationship
		us.idToParent[new_id] = parentID // Add this line
		if _, ok := us.parentToAddedIDs[parentID]; !ok {
			us.parentToAddedIDs[parentID] = make([]int, 0)
		}
		us.parentToAddedIDs[parentID] = append(us.parentToAddedIDs[parentID], new_id)

		// Use full qualified name
		fullName = parent + "." + name
		us.addedNameToID[fullName] = new_id

		parentType := parentField.Type

		//ToDo: handle Nested columns
		if nestedType, ok := parentType.(iceberg.NestedType); ok {
			if mapType, ok := nestedType.(*iceberg.MapType); ok {
				// fields are added to the map value type
				vf := mapType.ValueField()
				parentField = &vf
			} else if listType, ok := nestedType.(*iceberg.ListType); ok {
				// fields are added to the element type
				ef := listType.ElementField()
				parentField = &ef
			} else if structType, ok := nestedType.(*iceberg.StructType); ok {
				// fields are added to the struct
				fields := structType.Fields()
				parentField = &fields[len(fields)-1]
			}
		} else {
			panic(fmt.Sprintf("Cannot add to non-nested DataType: %s: %s", parent, parentField.Type))
		}

		parentID = parentField.ID
		currentField := us.findField(parent + "." + name)

		for _, id := range us.deletes {
			if id == parentID {
				panic(fmt.Sprintf("Cannot add to a column that will be deleted: %s", parent))
			}
		}

		if currentField != nil {
			foundInDeletes := false
			for _, id := range us.deletes {
				if id == currentField.ID {
					foundInDeletes = true
					break
				}
			}
			if !foundInDeletes {
				panic(fmt.Sprintf("Cannot add column, name already exists: %s.%s", parent, name))
			}
		}
		_, present := us.schema.FindColumnName(parentID)
		if present {
			fullName = parent + "." + name
		} else {
			fullName = name
		}

		nestedField := &iceberg.NestedField{
			Name:     name,
			ID:       new_id,
			Required: required,
			Type:     dataType,
			Doc:      doc,
		}

		us.updates[new_id] = nestedField
		us.parentToAddedIDs[parentID] = append(us.parentToAddedIDs[parentID], new_id)
		us.addedNameToID[fullName] = new_id

	} else {

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

		nestedField := &iceberg.NestedField{
			Name:     name,
			ID:       new_id,
			Required: required,
			Type:     dataType,
			Doc:      doc,
		}

		if initialDefaultValue != nil {
			nestedField.InitialDefault = initialDefaultValue
		}

		us.updates[new_id] = nestedField
		us.parentToAddedIDs[-1] = append(us.parentToAddedIDs[-1], new_id)
		us.addedNameToID[name] = new_id
	}

	return us

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
	field := us.findForUpdate(name)
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

	//check promotion
	if !allowedPromotion(field.Type, newType) {
		panic(fmt.Sprintf("Cannot update type of column: %s: %s", field.Name, newType))
	}

	us.updates[field.ID].Type = newType
	return us
}

func (us *UpdateSchema) UpdateColumnDoc(name string, doc string) *UpdateSchema {
	field := us.findForUpdate(name)
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
	field := us.findForUpdate(name)
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

		// adding to updates
		us.updates[existing.ID] = existing
		return existing
	}

	addedID, ok := us.addedNameToID[name]
	if ok {
		return us.updates[addedID]
	}

	return nil
}

func (us *UpdateSchema) Apply() *iceberg.Schema {
	return us.applyChanges()
}

func (su *UpdateSchema) isAdded(name string) bool {
	_, ok := su.addedNameToID[name]
	return ok
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

func allowedPromotion(oldType, newType iceberg.Type) bool {
	switch old := oldType.(type) {
	case iceberg.PrimitiveType:
		switch old.Type() {
		case "int":
			return newType.Type() == "long"
		case "float":
			return newType.Type() == "double"
		case "string":
			return newType.Type() == "binary" // widening
		case "fixed":
			return newType.Type() == "binary"
		case "time":
			return newType.Type() == "timestamp"
		case "decimal":
			o := oldType.(iceberg.DecimalType)
			n := newType.(iceberg.DecimalType)
			return o.Scale() == n.Scale() && n.Precision() > o.Precision()
		}
	}
	return false
}

func (u *UpdateSchema) applyChanges() *iceberg.Schema {

	// Helper to check if a field ID is marked for deletion
	isDeleted := func(id int) bool {
		for _, d := range u.deletes {
			if d == id {
				return true
			}
		}
		return false
	}

	var visit func(fields []iceberg.NestedField, parentID int) []iceberg.NestedField

	visit = func(fields []iceberg.NestedField, parentID int) []iceberg.NestedField {

		var newFields []iceberg.NestedField
		// 1) Apply deletes and updates
		for _, f := range fields {

			if isDeleted(f.ID) {
				continue
			}

			// apply update if present
			if upd, ok := u.updates[f.ID]; ok {
				f = *upd
			}

			// 2) Recurse into nested types
			switch t := f.Type.(type) {
			case *iceberg.StructType:
				rebuilt := visit(t.Fields(), f.ID)
				f.Type = &iceberg.StructType{FieldList: rebuilt}

			case *iceberg.ListType:
				elem := t.ElementField()
				rebuiltElem := visit([]iceberg.NestedField{elem}, elem.ID)[0]
				f.Type = &iceberg.ListType{
					ElementID:       t.ElementID,
					Element:         rebuiltElem.Type,
					ElementRequired: rebuiltElem.Required,
				}

			case *iceberg.MapType:
				keyField := t.KeyField()
				valField := t.ValueField()
				rebuiltVal := visit([]iceberg.NestedField{valField}, valField.ID)[0]
				f.Type = &iceberg.MapType{
					KeyID:         keyField.ID,
					ValueID:       rebuiltVal.ID,
					KeyType:       keyField.Type,
					ValueType:     rebuiltVal.Type,
					ValueRequired: rebuiltVal.Required,
				}

			}

			newFields = append(newFields, f)
		}

		// 3) Add new fields for this parent
		for _, addID := range u.parentToAddedIDs[parentID] {
			if added, ok := u.updates[addID]; ok {
				newFields = append(newFields, *added)
			}
		}

		return newFields
	}

	nested := u.schema.AsStruct().FieldList
	rebuiltFields := visit(nested, -1)

	// Build the new schema, preserving identifier field IDs
	idList := u.schema.IdentifierFieldIDs
	new_id := u.schema.ID + 1

	return iceberg.NewSchemaWithIdentifiers(new_id, idList, rebuiltFields...)
}

func (us *UpdateSchema) Commit() (Metadata, error) {

	newSchema := us.applyChanges()

	for _, existingSchema := range (*us.base).Schemas() {

		if newSchema.Equals(existingSchema) {
			if existingSchema.ID != (*us.base).CurrentSchema().ID {
				builder, err := MetadataBuilderFromBase(*us.base)
				if err != nil {
					return nil, err
				}

				_, err = builder.SetCurrentSchemaID(existingSchema.ID)
				if err != nil {
					return nil, err
				}

				return builder.Build()
			}

			return *us.base, nil
		}
	}

	builder, err := MetadataBuilderFromBase(*us.base)
	if err != nil {
		return nil, err
	}

	_, err = builder.AddSchema(newSchema, us.lastColumnID, false)
	if err != nil {
		return nil, err
	}

	_, err = builder.SetCurrentSchemaID(newSchema.ID)
	if err != nil {
		return nil, err
	}

	return builder.Build()
}
