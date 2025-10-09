package table

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/apache/iceberg-go"
)

const TableRootID = -1

type MoveOp string

const (
	MoveOpFirst  MoveOp = "first"
	MoveOpBefore MoveOp = "before"
	MoveOpAfter  MoveOp = "after"
)

type move struct {
	FieldID    int
	RelativeTo int
	Op         MoveOp
}

type UpdateSchema struct {
	txn          *Transaction
	schema       *iceberg.Schema
	lastColumnID int

	deletes map[int]struct{}
	updates map[int]map[int]iceberg.NestedField
	adds    map[int][]iceberg.NestedField
	moves   map[int][]move

	identifierFieldNames map[string]struct{}
	parentID             map[int]int

	addedNameToID            map[string]int
	allowIncompatibleChanges bool
	caseSensitive            bool
	nameMapping              iceberg.NameMapping
	ops                      []func() error
}

type UpdateSchemaOption func(*UpdateSchema)

func WithNameMapping(nameMapping iceberg.NameMapping) UpdateSchemaOption {
	return func(u *UpdateSchema) {
		u.nameMapping = nameMapping
	}
}

func NewUpdateSchema(txn *Transaction, caseSensitive bool, allowIncompatibleChanges bool, opts ...UpdateSchemaOption) *UpdateSchema {
	u := &UpdateSchema{
		txn:          txn,
		schema:       nil,
		lastColumnID: txn.meta.CurrentSchema().HighestFieldID(),

		deletes: make(map[int]struct{}),
		updates: make(map[int]map[int]iceberg.NestedField),
		adds:    make(map[int][]iceberg.NestedField),
		moves:   make(map[int][]move),

		identifierFieldNames: nil,
		parentID:             make(map[int]int),

		addedNameToID:            make(map[string]int),
		allowIncompatibleChanges: allowIncompatibleChanges,
		caseSensitive:            caseSensitive,
		nameMapping:              nil,
		ops:                      make([]func() error, 0),
	}

	for _, opt := range opts {
		opt(u)
	}

	return u
}

func (u *UpdateSchema) init() error {
	if u.txn == nil {
		return errors.New("transaction is nil")
	}
	if u.txn.meta == nil {
		return errors.New("transaction meta is nil")
	}

	u.schema = u.txn.meta.CurrentSchema()
	if u.schema == nil {
		return errors.New("current schema is nil")
	}

	if err := u.initIdentifierFieldNames(); err != nil {
		return err
	}

	if err := u.initParentID(); err != nil {
		return err
	}

	return nil
}

func (u *UpdateSchema) initIdentifierFieldNames() error {
	identifierFieldNames := make(map[string]struct{})
	for _, id := range u.schema.IdentifierFieldIDs {
		name, ok := u.schema.FindColumnName(id)
		if !ok {
			return fmt.Errorf("identifier field %d not found", id)
		}
		identifierFieldNames[name] = struct{}{}
	}

	u.identifierFieldNames = identifierFieldNames
	return nil
}

func (u *UpdateSchema) initParentID() error {
	parents, err := iceberg.IndexParents(u.schema)
	if err != nil {
		return err
	}

	maps.Copy(u.parentID, parents)
	return nil
}

func (u *UpdateSchema) assignNewColumnID() int {
	u.lastColumnID++
	return u.lastColumnID
}

func (u *UpdateSchema) findField(name string) (iceberg.NestedField, bool) {
	if u.caseSensitive {
		return u.schema.FindFieldByName(name)
	} else {
		return u.schema.FindFieldByNameCaseInsensitive(name)
	}
}

func (u *UpdateSchema) isDeleted(fieldID int) bool {
	_, ok := u.deletes[fieldID]
	return ok
}

func (u *UpdateSchema) findParentID(fieldID int) int {
	parentID, ok := u.parentID[fieldID]
	if !ok {
		return TableRootID
	}
	return parentID
}

func (u *UpdateSchema) AddColumn(path []string, fieldType iceberg.Type, doc string, required bool, defaultValue iceberg.Literal) *UpdateSchema {
	u.ops = append(u.ops, func() error {
		return u.addColumn(path, fieldType, doc, required, defaultValue)
	})
	return u
}

func (u *UpdateSchema) addColumn(path []string, fieldType iceberg.Type, doc string, required bool, defaultValue iceberg.Literal) error {
	if len(path) == 0 {
		return errors.New("path is empty")
	}

	fullName := strings.Join(path, ".")

	switch t := fieldType.(type) {
	case *iceberg.ListType, *iceberg.MapType, *iceberg.StructType:
		if defaultValue != nil {
			return fmt.Errorf("default values are not supported for %s", t.String())
		}
	case iceberg.PrimitiveType:
		if required && defaultValue == nil && !u.allowIncompatibleChanges {
			return fmt.Errorf("required field %s has no default value", fullName)
		}
		if defaultValue != nil && !defaultValue.Type().Equals(t) {
			return fmt.Errorf("default value type mismatch: %s != %s", defaultValue.Type(), t)
		}
	default:
		return fmt.Errorf("invalid field type: %T", t)
	}

	parent := path[:len(path)-1]
	parentID := TableRootID

	if len(parent) > 0 {
		parentFullPath := strings.Join(parent, ".")
		parentField, ok := u.findField(parentFullPath)
		if !ok {
			return fmt.Errorf("parent field not found: %s", parentFullPath)
		}

		switch parentType := parentField.Type.(type) {
		case *iceberg.ListType:
			f := parentType.ElementField()
			parentField = f
		case *iceberg.MapType:
			f := parentType.ValueField()
			parentField = f
		}

		if _, ok := parentField.Type.(*iceberg.StructType); !ok {
			return fmt.Errorf("cannot add field to non-struct type: %s", parentFullPath)
		}

		parentID = parentField.ID
	}

	name := path[len(path)-1]
	for _, add := range u.adds[parentID] {
		if add.Name == name {
			return fmt.Errorf("field already exists in adds: %s", fullName)
		}
	}

	// support add field with the same name as deleted field and renamed field
	if field, ok := u.findField(fullName); ok {
		if !u.isDeleted(field.ID) {
			for _, upd := range u.updates[parentID] {
				if upd.Name == name {
					return fmt.Errorf("field already exists: %s", fullName)
				}
			}
		}
	}

	field := iceberg.NestedField{
		Name:     name,
		Type:     fieldType,
		Required: required,
		Doc:      doc,
	}
	if defaultValue != nil {
		field.InitialDefault = defaultValue.Any()
		field.WriteDefault = defaultValue.Any()
	}

	sch, err := iceberg.AssignFreshSchemaIDs(iceberg.NewSchema(0, field), u.assignNewColumnID)
	if err != nil {
		return fmt.Errorf("failed to assign field id: %w", err)
	}
	u.adds[parentID] = append(u.adds[parentID], sch.Field(0))
	u.addedNameToID[fullName] = sch.Field(0).ID
	return nil
}

func (u *UpdateSchema) DeleteColumn(path []string) *UpdateSchema {
	u.ops = append(u.ops, func() error {
		return u.deleteColumn(path)
	})
	return u
}

func (u *UpdateSchema) deleteColumn(path []string) error {
	fullName := strings.Join(path, ".")
	field, ok := u.findField(fullName)
	if !ok {
		return fmt.Errorf("field not found: %s", fullName)
	}

	if _, ok := u.adds[field.ID]; ok {
		return fmt.Errorf("field that has additions cannot be deleted: %s", fullName)
	}

	if _, ok := u.updates[field.ID]; ok {
		return fmt.Errorf("field that has updates cannot be deleted: %s", fullName)
	}

	delete(u.identifierFieldNames, fullName)

	u.deletes[field.ID] = struct{}{}
	return nil
}

type ColumnUpdate struct {
	Name         iceberg.Optional[string]
	FieldType    iceberg.Optional[iceberg.Type]
	Required     iceberg.Optional[bool]
	WriteDefault iceberg.Optional[iceberg.Literal]
	Doc          iceberg.Optional[string]
}

func (u *UpdateSchema) UpdateColumn(path []string, update ColumnUpdate) *UpdateSchema {
	u.ops = append(u.ops, func() error {
		return u.updateColumn(path, update)
	})
	return u
}

func (u *UpdateSchema) updateColumn(path []string, update ColumnUpdate) error {
	if !update.Name.Valid &&
		!update.FieldType.Valid &&
		!update.Required.Valid &&
		!update.WriteDefault.Valid &&
		!update.Doc.Valid {
		return nil
	}

	fullName := strings.Join(path, ".")

	field, ok := u.findField(fullName)
	if !ok {
		return fmt.Errorf("field not found: %s", fullName)
	}

	if u.isDeleted(field.ID) {
		return fmt.Errorf("field that has been deleted cannot be updated: %s", fullName)
	}

	parentID := u.findParentID(field.ID)

	if update.Name.Valid {
		if update.Name.Val == "" {
			return fmt.Errorf("cannot rename field to empty name: %s", fullName)
		}
		if field.Name == update.Name.Val {
			return fmt.Errorf("cannot rename field to the same name: %s", fullName)
		}

		newFullName := strings.Join(append(path[:len(path)-1], update.Name.Val), ".")
		if existingField, ok := u.findField(newFullName); ok {
			if !u.isDeleted(existingField.ID) {
				return fmt.Errorf("field already exists: %s", newFullName)
			}
		}

		for _, add := range u.adds[parentID] {
			if add.Name == update.Name.Val {
				return fmt.Errorf("cannot rename field to added field: %s", newFullName)
			}
		}

		for _, upd := range u.updates[parentID] {
			if upd.Name == update.Name.Val && upd.ID != field.ID {
				return fmt.Errorf("cannot rename field to renamed field: %s", newFullName)
			}
		}

		if _, ok := u.identifierFieldNames[fullName]; ok {
			delete(u.identifierFieldNames, fullName)
			u.identifierFieldNames[newFullName] = struct{}{}
		}
	}

	if update.FieldType.Valid {
		if _, ok := field.Type.(iceberg.PrimitiveType); !ok {
			return fmt.Errorf("cannot update field type for non-primitive type: %s", fullName)
		}
		if !update.FieldType.Val.Equals(field.Type) && !u.allowIncompatibleChanges {
			fieldType, err := iceberg.PromoteType(field.Type, update.FieldType.Val)
			if err != nil {
				return err
			}
			update.FieldType.Val = fieldType
		}
	}

	if update.Required.Valid {
		if field.Required != update.Required.Val {
			if !u.allowIncompatibleChanges && update.Required.Val {
				return fmt.Errorf("cannot change column nullability from optional to required: %s", fullName)
			}
		}
	}

	if update.WriteDefault.Valid {
		if update.WriteDefault.Val == nil {
			if field.Required && !u.allowIncompatibleChanges {
				return fmt.Errorf("cannot change default value of required column to nil: %s", fullName)
			}
		}
	}

	if _, ok := u.updates[parentID]; !ok {
		u.updates[parentID] = make(map[int]iceberg.NestedField)
	}

	updatedField, ok := u.updates[parentID][field.ID]
	if !ok {
		updatedField = field
	}
	if update.Name.Valid {
		updatedField.Name = update.Name.Val
	}
	if update.FieldType.Valid {
		updatedField.Type = update.FieldType.Val
	}
	if update.Required.Valid {
		updatedField.Required = update.Required.Val
	}
	if update.WriteDefault.Valid {
		updatedField.WriteDefault = update.WriteDefault.Val.Any()
	}
	if update.Doc.Valid {
		updatedField.Doc = update.Doc.Val
	}
	u.updates[parentID][field.ID] = updatedField

	return nil
}

func (u *UpdateSchema) RenameColumn(path []string, newName string) *UpdateSchema {
	u.ops = append(u.ops, func() error {
		return u.updateColumn(path, ColumnUpdate{
			Name: iceberg.Optional[string]{
				Valid: true,
				Val:   newName,
			},
		})
	})
	return u
}

func (u *UpdateSchema) MoveColumn(op MoveOp, path, relativeTo []string) *UpdateSchema {
	u.ops = append(u.ops, func() error {
		return u.moveColumn(op, path, relativeTo)
	})
	return u
}

func (u *UpdateSchema) MoveFirst(path []string) *UpdateSchema {
	u.ops = append(u.ops, func() error {
		return u.moveColumn(MoveOpFirst, path, nil)
	})
	return u
}

func (u *UpdateSchema) MoveBefore(path, relativeTo []string) *UpdateSchema {
	u.ops = append(u.ops, func() error {
		return u.moveColumn(MoveOpBefore, path, relativeTo)
	})
	return u
}

func (u *UpdateSchema) MoveAfter(path, relativeTo []string) *UpdateSchema {
	u.ops = append(u.ops, func() error {
		return u.moveColumn(MoveOpAfter, path, relativeTo)
	})
	return u
}

func (u *UpdateSchema) findFieldForMove(name string) (int, bool) {
	field, ok := u.findField(name)
	if ok {
		return field.ID, true
	}
	id, ok := u.addedNameToID[name]
	return id, ok
}

func (u *UpdateSchema) moveColumn(op MoveOp, path []string, relativeTo []string) error {
	fullName := strings.Join(path, ".")
	fieldID, ok := u.findFieldForMove(fullName)
	if !ok {
		return fmt.Errorf("field not found: %s", fullName)
	}

	if u.isDeleted(fieldID) {
		return fmt.Errorf("field that has been deleted cannot be moved: %s", fullName)
	}

	parentID := u.findParentID(fieldID)

	switch op {
	case MoveOpFirst:
		u.moves[parentID] = append(u.moves[parentID], move{
			FieldID:    fieldID,
			RelativeTo: -1,
			Op:         op,
		})
		return nil
	case MoveOpBefore, MoveOpAfter:
		relativeToFullName := strings.Join(relativeTo, ".")
		relativeToFieldID, ok := u.findFieldForMove(relativeToFullName)
		if !ok {
			return fmt.Errorf("relative to field not found: %s", relativeToFullName)
		}

		if relativeToFieldID == fieldID {
			return fmt.Errorf("cannot move a field to itself: %s", fullName)
		}

		if u.findParentID(relativeToFieldID) != parentID {
			return fmt.Errorf("relative to field is not a child of the parent: %s", relativeToFullName)
		}
		u.moves[parentID] = append(u.moves[parentID], move{
			FieldID:    fieldID,
			RelativeTo: relativeToFieldID,
			Op:         op,
		})
		return nil
	default:
		return fmt.Errorf("invalid move operation: %s", op)
	}
}

func (u *UpdateSchema) SetIdentifierField(paths [][]string) *UpdateSchema {
	identifierFieldNames := make(map[string]struct{})
	for _, path := range paths {
		identifierFieldNames[strings.Join(path, ".")] = struct{}{}
	}
	u.identifierFieldNames = identifierFieldNames
	return u
}

func (u *UpdateSchema) BuildUpdates() ([]Update, []Requirement, error) {
	newSchema, err := u.Apply()
	if err != nil {
		return nil, nil, err
	}

	existingSchemaID := -1
	for _, schema := range u.txn.meta.schemaList {
		if newSchema.Equals(schema) {
			existingSchemaID = schema.ID
			break
		}
	}

	requirements := make([]Requirement, 0)
	updates := make([]Update, 0)

	if existingSchemaID != u.schema.ID {
		requirements = append(requirements, AssertCurrentSchemaID(u.schema.ID))
		if existingSchemaID == -1 {
			updates = append(
				updates,
				NewAddSchemaUpdate(newSchema),
				NewSetCurrentSchemaUpdate(newSchema.ID),
			)
		} else {
			updates = append(updates,
				NewSetCurrentSchemaUpdate(newSchema.ID),
			)
		}

		if u.nameMapping != nil {
			updatesMap := make(map[int]iceberg.NestedField)
			for _, upds := range u.updates {
				maps.Copy(updatesMap, upds)
			}
			updatedNameMapping, err := iceberg.UpdateNameMapping(u.nameMapping, updatesMap, u.adds)
			if err != nil {
				return nil, nil, err
			}
			updates = append(updates, NewSetPropertiesUpdate(iceberg.Properties{
				DefaultNameMappingKey: updatedNameMapping.String(),
			}))
		}
	}

	return updates, requirements, nil
}

func (u *UpdateSchema) Apply() (*iceberg.Schema, error) {
	if err := u.init(); err != nil {
		return nil, err
	}

	for _, op := range u.ops {
		if err := op(); err != nil {
			return nil, err
		}
	}

	updates := make(map[int]iceberg.NestedField)
	for _, upds := range u.updates {
		maps.Copy(updates, upds)
	}
	st, err := iceberg.Visit(u.schema, &applyChanges{
		adds:    u.adds,
		updates: updates,
		deletes: u.deletes,
		moves:   u.moves,
	})
	if err != nil {
		return nil, fmt.Errorf("error applying schema changes: %w", err)
	}

	identifierFieldIDs := make([]int, 0)
	newSchema := iceberg.NewSchema(0, st.(*iceberg.StructType).FieldList...)
	for name := range u.identifierFieldNames {
		var field iceberg.NestedField
		var ok bool
		if u.caseSensitive {
			field, ok = newSchema.FindFieldByName(name)
		} else {
			field, ok = newSchema.FindFieldByNameCaseInsensitive(name)
		}
		if !ok {
			return nil, fmt.Errorf("identifier field not found: %s", name)
		}
		identifierFieldIDs = append(identifierFieldIDs, field.ID)
	}

	nextSchemaID := 1
	if len(u.txn.meta.schemaList) > 0 {
		nextSchemaID = 1 + slices.MaxFunc(u.txn.meta.schemaList, func(a, b *iceberg.Schema) int {
			return a.ID - b.ID
		}).ID
	}

	return iceberg.NewSchemaWithIdentifiers(nextSchemaID, identifierFieldIDs, st.(*iceberg.StructType).FieldList...), nil
}

func (u *UpdateSchema) Commit() error {
	updates, requirements, err := u.BuildUpdates()
	if err != nil {
		return err
	}
	if len(updates) == 0 {
		return nil
	}

	return u.txn.apply(updates, requirements)
}

type applyChanges struct {
	adds    map[int][]iceberg.NestedField
	updates map[int]iceberg.NestedField
	deletes map[int]struct{}
	moves   map[int][]move
}

func (a *applyChanges) Schema(schema *iceberg.Schema, structResult iceberg.Type) iceberg.Type {
	added := a.adds[TableRootID]
	moves := a.moves[TableRootID]

	if len(added) > 0 || len(moves) > 0 {
		if newFields := addAndMoveFields(structResult.(*iceberg.StructType).Fields(), added, moves); newFields != nil {
			return &iceberg.StructType{FieldList: newFields}
		}
	}

	return structResult
}

func (a *applyChanges) Struct(structType iceberg.StructType, fieldResults []iceberg.Type) iceberg.Type {
	hasChanges := false
	newFields := make([]iceberg.NestedField, 0)

	for i, resultType := range fieldResults {
		if resultType == nil {
			hasChanges = true
			continue
		}

		field := structType.Fields()[i]

		name := field.Name
		doc := field.Doc
		required := field.Required
		writeDefault := field.WriteDefault

		if update, ok := a.updates[field.ID]; ok {
			name = update.Name
			doc = update.Doc
			required = update.Required
			writeDefault = update.WriteDefault
		}
		if field.Name == name &&
			field.Type.Equals(resultType) &&
			field.Required == required &&
			field.Doc == doc &&
			field.WriteDefault == writeDefault {
			newFields = append(newFields, field)
		} else {
			hasChanges = true
			newFields = append(newFields, iceberg.NestedField{
				ID:             field.ID,
				Name:           field.Name,
				Type:           resultType,
				Required:       required,
				Doc:            doc,
				InitialDefault: field.InitialDefault,
				WriteDefault:   writeDefault,
			})
		}
	}

	if hasChanges {
		return &iceberg.StructType{FieldList: newFields}
	}

	return &structType
}

func (a *applyChanges) Field(field iceberg.NestedField, fieldResult iceberg.Type) iceberg.Type {
	if _, ok := a.deletes[field.ID]; ok {
		return nil
	}

	if update, ok := a.updates[field.ID]; ok && !field.Type.Equals(update.Type) {
		return update.Type
	}

	st, ok := fieldResult.(*iceberg.StructType)
	if !ok {
		return fieldResult
	}

	added := a.adds[field.ID]
	moves := a.moves[field.ID]
	if len(added) > 0 || len(moves) > 0 {
		newFields := addAndMoveFields(st.FieldList, added, moves)
		if len(newFields) > 0 {
			return &iceberg.StructType{FieldList: newFields}
		}
	}

	return fieldResult
}

func (a *applyChanges) List(listType iceberg.ListType, elementResult iceberg.Type) iceberg.Type {
	elementType := a.Field(listType.ElementField(), elementResult)
	if elementType == nil {
		panic(fmt.Sprintf("cannot delete element type from list: %s", elementResult))
	}

	return &iceberg.ListType{
		ElementID:       listType.ElementID,
		Element:         elementType,
		ElementRequired: listType.ElementRequired,
	}
}

func (a *applyChanges) Map(mapType iceberg.MapType, keyResult, valueResult iceberg.Type) iceberg.Type {
	keyID := mapType.KeyID
	if _, ok := a.deletes[keyID]; ok {
		panic(fmt.Errorf("cannot delete map keys: %s", mapType.String()))
	}

	if _, ok := a.updates[keyID]; ok {
		panic(fmt.Errorf("cannot update map keys: %s", mapType.String()))
	}

	if _, ok := a.adds[keyID]; ok {
		panic(fmt.Errorf("cannot add fields to map keys: %s", mapType.String()))
	}

	if !mapType.KeyType.Equals(keyResult) {
		panic(fmt.Errorf("cannot alter map keys: %s", mapType.String()))
	}

	valueField := mapType.ValueField()
	valueType := a.Field(valueField, valueResult)

	if valueType == nil {
		panic(fmt.Errorf("cannot delete value type from map: %s", mapType.String()))
	}

	return &iceberg.MapType{
		KeyID:         mapType.KeyID,
		KeyType:       mapType.KeyType,
		ValueID:       mapType.ValueID,
		ValueType:     valueType,
		ValueRequired: mapType.ValueRequired,
	}
}

func (a *applyChanges) Primitive(primitive iceberg.PrimitiveType) iceberg.Type {
	return primitive
}

func addFields(fields []iceberg.NestedField, adds []iceberg.NestedField) []iceberg.NestedField {
	return append(fields, adds...)
}

func moveFields(fields []iceberg.NestedField, moves []move) []iceberg.NestedField {
	reordered := slices.Clone(fields)
	for _, move := range moves {
		var fieldToMove iceberg.NestedField
		var fieldIndex int
		found := false
		for i, field := range reordered {
			if field.ID == move.FieldID {
				fieldToMove = field
				fieldIndex = i
				found = true
				break
			}
		}
		if !found {
			continue
		}

		reordered = append(reordered[:fieldIndex], reordered[fieldIndex+1:]...)

		switch move.Op {
		case MoveOpFirst:
			reordered = append([]iceberg.NestedField{fieldToMove}, reordered...)
		case MoveOpBefore, MoveOpAfter:
			var relativeIndex int
			found = false
			for i, field := range reordered {
				if field.ID == move.RelativeTo {
					relativeIndex = i
					found = true
					break
				}
			}
			if !found {
				continue
			}

			if move.Op == MoveOpBefore {
				reordered = append(reordered[:relativeIndex], append([]iceberg.NestedField{fieldToMove}, reordered[relativeIndex:]...)...)
			} else {
				reordered = append(reordered[:relativeIndex+1], append([]iceberg.NestedField{fieldToMove}, reordered[relativeIndex+1:]...)...)
			}
		}
	}
	return reordered
}

func addAndMoveFields(fields []iceberg.NestedField, adds []iceberg.NestedField, moves []move) []iceberg.NestedField {
	if len(adds) > 0 {
		added := addFields(fields, adds)
		if len(moves) > 0 {
			return moveFields(added, moves)
		} else {
			return added
		}
	} else if len(moves) > 0 {
		return moveFields(fields, moves)
	}
	if len(adds) == 0 {
		return nil
	}
	return fields
}
