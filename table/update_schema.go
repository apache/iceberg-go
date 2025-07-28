package table

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
)

// UpdateSchema accumulates operations and validates on Build/Commit
type UpdateSchema struct {
	txn          *Transaction
	schema       *iceberg.Schema
	lastColumnID int

	operations []SchemaOperation
	errors     []error // Collect validation errors

	allowIncompatibleChanges bool
	identifierFields         map[string]struct{}
	caseSensitive            bool
}

// Operation represents a deferred schema operation
type SchemaOperation interface {
	Apply(builder *schemaBuilder) error
	Validate(schema *iceberg.Schema, settings *validationSettings) error
	String() string
}

// Validation settings passed to operations
type validationSettings struct {
	allowIncompatibleChanges bool
	caseSensitive            bool
}

type schemaBuilder struct {
	deletes      map[int]struct{}
	updates      map[int]*iceberg.NestedField
	adds         map[int][]*iceberg.NestedField
	moves        map[int][]moveReq
	lastColumnID int
	baseSchema   *iceberg.Schema
	settings     *validationSettings
}

// Operation Types
// Each schema update operation is represented by a struct that implements the SchemaOperation interface
// and has a Validate and Apply method.
// The Validate method validates the operation and returns an error if the operation is invalid.
// The Apply method applies the operation to the schema builder.
type addColumnOp struct {
	path           []string
	required       bool
	dataType       iceberg.Type
	doc            string
	initialDefault any
}

type updateColumnOp struct {
	path    []string
	updates ColumnUpdate
}

type deleteColumnOp struct {
	path []string
}

type moveColumnOp struct {
	columnToMove    []string
	referenceColumn []string
	op              moveOp
}

type ColumnUpdate struct {
	Type     iceberg.Optional[iceberg.Type] // nil means no change
	Doc      iceberg.Optional[string]       // nil means no change
	Default  any                            // nil means no change
	Required iceberg.Optional[bool]         // nil means no change
}

// Move operation
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
		lastColumnID:             lastColumnID,
		operations:               make([]SchemaOperation, 0),
		errors:                   make([]error, 0),
		allowIncompatibleChanges: false,
		identifierFields:         make(map[string]struct{}),
		caseSensitive:            true,
	}
}

// AllowIncompatibleChanges permits incompatible schema changes
func (us *UpdateSchema) AllowIncompatibleChanges() *UpdateSchema {
	us.allowIncompatibleChanges = true
	return us
}

// SetCaseSensitive controls case sensitivity for field lookups
func (us *UpdateSchema) SetCaseSensitive(caseSensitive bool) *UpdateSchema {
	us.caseSensitive = caseSensitive
	return us
}

// AddColumn queues an add column operation - validation deferred until Build is called
func (us *UpdateSchema) AddColumn(path []string, required bool, dataType iceberg.Type, doc string, initialDefault any) *UpdateSchema {
	op := &addColumnOp{
		path:           path,
		required:       required,
		dataType:       dataType,
		doc:            doc,
		initialDefault: initialDefault,
	}
	us.operations = append(us.operations, op)
	return us
}

// UpdateColumn queues an update column operation - validation deferred until Build is called
func (us *UpdateSchema) UpdateColumn(path []string, updates ColumnUpdate) *UpdateSchema {
	op := &updateColumnOp{
		path:    path,
		updates: updates,
	}
	us.operations = append(us.operations, op)
	return us
}

// DeleteColumn queues a delete column operation - validation deferred until Build is called
func (us *UpdateSchema) DeleteColumn(path []string) *UpdateSchema {
	op := &deleteColumnOp{
		path: path,
	}
	us.operations = append(us.operations, op)
	return us
}

// Move queues a move column operation - validation deferred until Build is called
func (us *UpdateSchema) Move(columnToMove, referenceColumn []string, op moveOp) *UpdateSchema {
	moveOp := &moveColumnOp{
		columnToMove:    columnToMove,
		referenceColumn: referenceColumn,
		op:              op,
	}
	us.operations = append(us.operations, moveOp)
	return us
}

/// Operation Methods

// Reset clears all queued operations and errors - validation deferred until Build is called
func (us *UpdateSchema) Reset() *UpdateSchema {
	us.operations = us.operations[:0]
	us.errors = us.errors[:0]
	return us
}

// RemoveLastOperation removes the most recently added operation
func (us *UpdateSchema) RemoveLastOperation() *UpdateSchema {
	if len(us.operations) > 0 {
		us.operations = us.operations[:len(us.operations)-1]
	}
	return us
}

// GetQueuedOperations returns a copy of the queued operations
func (us *UpdateSchema) GetQueuedOperations() []string {
	result := make([]string, len(us.operations))
	for i, op := range us.operations {
		result[i] = op.String()
	}
	return result
}

// Validate runs all deferred validations for operations without building
// basically just checks if the operations are valid
func (us *UpdateSchema) Validate() error {
	// Clear the previous errors
	// because if validation is called multiple times, the errors will accumulate even after removing operations causing error
	us.errors = us.errors[:0]

	settings := &validationSettings{
		allowIncompatibleChanges: us.allowIncompatibleChanges,
		caseSensitive:            us.caseSensitive,
	}

	for _, op := range us.operations {
		if err := op.Validate(us.schema, settings); err != nil {
			us.errors = append(us.errors, err)
		}
	}

	if len(us.errors) > 0 {
		return fmt.Errorf("validation failed with %d errors: %v", len(us.errors), us.errors)
	}
	return nil
}

// Build validates and constructs the final schema
func (us *UpdateSchema) Build() (*iceberg.Schema, error) {
	// First validate all operations
	if err := us.Validate(); err != nil {
		return nil, err
	}

	// No operations means no changes
	if len(us.operations) == 0 {
		return us.schema, nil
	}

	// Create builder and apply operations
	builder := &schemaBuilder{
		deletes:      make(map[int]struct{}),
		updates:      make(map[int]*iceberg.NestedField),
		adds:         make(map[int][]*iceberg.NestedField),
		moves:        make(map[int][]moveReq),
		lastColumnID: us.lastColumnID,
		baseSchema:   us.schema,
		settings: &validationSettings{
			allowIncompatibleChanges: us.allowIncompatibleChanges,
			caseSensitive:            us.caseSensitive,
		},
	}

	// Apply operations in order
	for _, op := range us.operations {
		if err := op.Apply(builder); err != nil {
			return nil, fmt.Errorf("failed to apply operation %s: %w", op.String(), err)
		}
	}

	// Build final schema
	return us.buildFinalSchema(builder)
}

// Commit validates, builds, and commits the schema changes
func (us *UpdateSchema) Commit() error {
	updates, requirements, err := us.CommitUpdates()
	if err != nil {
		return err
	}

	if len(updates) == 0 {
		return nil
	}

	return us.txn.apply(updates, requirements)
}

// CommitUpdates returns the updates and requirements needed
func (us *UpdateSchema) CommitUpdates() ([]Update, []Requirement, error) {
	// If there are no operations, return nil updates and requirements
	if len(us.operations) == 0 {
		return nil, nil, nil
	}

	newSchema, err := us.Build()
	if err != nil {
		return nil, nil, err
	}

	// Check if equivalent schema exists
	existingSchemaID := us.findExistingSchemaInTransaction(newSchema)

	var updates []Update
	var requirements []Requirement

	// for Commit Contention
	requirements = append(requirements, AssertCurrentSchemaID(us.schema.ID))

	if existingSchemaID == nil {
		// Get the final lastColumnID from the builder
		builder := &schemaBuilder{
			deletes:      make(map[int]struct{}),
			updates:      make(map[int]*iceberg.NestedField),
			adds:         make(map[int][]*iceberg.NestedField),
			moves:        make(map[int][]moveReq),
			lastColumnID: us.lastColumnID,
			baseSchema:   us.schema,
			settings: &validationSettings{
				allowIncompatibleChanges: us.allowIncompatibleChanges,
				caseSensitive:            us.caseSensitive,
			},
		}

		for _, op := range us.operations {
			if err := op.Apply(builder); err != nil {
				return nil, nil, fmt.Errorf("failed to recompute lastColumnID: %w", err)
			}
		}
		updates = append(updates,
			NewAddSchemaUpdate(newSchema, builder.lastColumnID, false),
			NewSetCurrentSchemaUpdate(newSchema.ID),
		)
	} else {
		updates = append(updates, NewSetCurrentSchemaUpdate(*existingSchemaID))
	}

	if nameMapUpdates := us.getNameMappingUpdates(newSchema); len(nameMapUpdates) > 0 {
		updates = append(updates, nameMapUpdates...)
	}

	return updates, requirements, nil
}

/// AddColumn Operation

// Implementation of Operation interface for addColumnOp
func (op *addColumnOp) Validate(schema *iceberg.Schema, settings *validationSettings) error {
	if len(op.path) == 0 {
		return errors.New("AddColumn: path must contain at least the new column name")
	}

	// Check if field already exists
	if existing := findField(schema, op.path, settings.caseSensitive); existing != nil {
		return fmt.Errorf("cannot add column; name already exists: %s", strings.Join(op.path, "."))
	}

	// Validate parent exists and is a nested type
	if len(op.path) > 1 {
		parentPath := op.path[:len(op.path)-1]
		pf := findField(schema, parentPath, settings.caseSensitive)
		if pf == nil {
			return fmt.Errorf("cannot find parent struct: %s", strings.Join(parentPath, "."))
		}

		switch pf.Type.(type) {
		case *iceberg.StructType, *iceberg.MapType, *iceberg.ListType:
			// Valid parent types
		default:
			return fmt.Errorf("parent is not a nested type: %s", strings.Join(parentPath, "."))
		}
	}

	// Validate required column has default or incompatible changes are allowed
	if op.required && op.initialDefault == nil && !settings.allowIncompatibleChanges {
		return fmt.Errorf("cannot add required column without default value: %s", strings.Join(op.path, "."))
	}

	// Validate default value type compatibility
	if op.initialDefault != nil {
		if err := validateDefaultValue(op.dataType, op.initialDefault); err != nil {
			return err
		}
	}

	return nil
}

func (op *addColumnOp) Apply(builder *schemaBuilder) error {
	colName := op.path[len(op.path)-1]
	parentID := -1

	if len(op.path) > 1 {
		parentPath := op.path[:len(op.path)-1]
		pf := findField(builder.baseSchema, parentPath, builder.settings.caseSensitive)
		if pf == nil {
			return fmt.Errorf("cannot find parent struct: %s", strings.Join(parentPath, "."))
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
			return fmt.Errorf("parent is not a nested type: %s", strings.Join(parentPath, "."))
		}
	}

	newID := builder.assignNewColumnID()
	nf := &iceberg.NestedField{
		Name:           colName,
		ID:             newID,
		Required:       op.required,
		Type:           op.dataType,
		Doc:            op.doc,
		InitialDefault: op.initialDefault,
	}

	builder.adds[parentID] = append(builder.adds[parentID], nf)
	return nil
}

func (op *addColumnOp) String() string {
	return fmt.Sprintf("AddColumn(%s, required=%t, type=%s)",
		strings.Join(op.path, "."), op.required, op.dataType.String())
}

/// UpdateColumn Operation

// Implementation for updateColumnOp
func (op *updateColumnOp) Validate(schema *iceberg.Schema, settings *validationSettings) error {
	field := findField(schema, op.path, settings.caseSensitive)
	if field == nil {
		return fmt.Errorf("cannot update missing column: %s", strings.Join(op.path, "."))
	}

	// Type promotion validation
	if op.updates.Type.Valid && !field.Type.Equals(op.updates.Type.Val) {
		_, err := iceberg.PromoteType(field.Type, op.updates.Type.Val)
		if err != nil {
			return fmt.Errorf("cannot update type of column: %s: %s -> %s: %w",
				strings.Join(op.path, "."), field.Type.String(), op.updates.Type.Val.String(), err)
		}
	}

	// Required flag validation
	if op.updates.Required.Valid && field.Required != op.updates.Required.Val {
		isRequired := op.updates.Required.Val
		if isRequired && !settings.allowIncompatibleChanges {
			return fmt.Errorf("cannot change column nullability: %s: optional -> required",
				strings.Join(op.path, "."))
		}
	}

	return nil
}

func (op *updateColumnOp) Apply(builder *schemaBuilder) error {
	field := findField(builder.baseSchema, op.path, builder.settings.caseSensitive)
	if field == nil {
		return fmt.Errorf("cannot update missing column: %s", strings.Join(op.path, "."))
	}

	// Create a copy of the field for modification
	updatedField := *field
	hasChanges := false

	// Update type if provided
	if op.updates.Type.Valid && !field.Type.Equals(op.updates.Type.Val) {
		newType, err := iceberg.PromoteType(field.Type, op.updates.Type.Val)
		if err != nil {
			return fmt.Errorf("cannot update type of column: %s: %s -> %s: %w",
				strings.Join(op.path, "."), field.Type.String(), op.updates.Type.Val.String(), err)
		}
		updatedField.Type = newType
		hasChanges = true
	}

	// Update documentation if provided
	if op.updates.Doc.Valid && field.Doc != op.updates.Doc.Val {
		updatedField.Doc = op.updates.Doc.Val
		hasChanges = true
	}

	// Update default value if provided
	if op.updates.Default != nil && field.InitialDefault != op.updates.Default {
		updatedField.InitialDefault = op.updates.Default
		hasChanges = true
	}

	// Update required flag if provided
	if op.updates.Required.Valid && field.Required != op.updates.Required.Val {
		updatedField.Required = op.updates.Required.Val
		hasChanges = true
	}

	// Only update the map if changes were made
	if hasChanges {
		builder.updates[field.ID] = &updatedField
	}

	return nil
}

func (op *updateColumnOp) String() string {
	var changes []string
	if op.updates.Type.Valid {
		changes = append(changes, fmt.Sprintf("type=%s", op.updates.Type.Val.String()))
	}
	if op.updates.Doc.Valid {
		changes = append(changes, fmt.Sprintf("doc=%s", op.updates.Doc.Val))
	}
	if op.updates.Default != nil {
		changes = append(changes, "default=<value>")
	}
	if op.updates.Required.Valid {
		changes = append(changes, fmt.Sprintf("required=%t", op.updates.Required.Val))
	}
	return fmt.Sprintf("UpdateColumn(%s, %s)",
		strings.Join(op.path, "."), strings.Join(changes, ", "))
}

/// DeleteColumn Operation

// Implementation for deleteColumnOp
func (op *deleteColumnOp) Validate(schema *iceberg.Schema, settings *validationSettings) error {
	field := findField(schema, op.path, settings.caseSensitive)
	if field == nil {
		return fmt.Errorf("cannot delete missing column: %s", strings.Join(op.path, "."))
	}
	return nil
}

func (op *deleteColumnOp) Apply(builder *schemaBuilder) error {
	field := findField(builder.baseSchema, op.path, builder.settings.caseSensitive)
	if field == nil {
		return fmt.Errorf("cannot delete missing column: %s", strings.Join(op.path, "."))
	}

	// Check for conflicts with adds/updates
	if _, ok := builder.adds[field.ID]; ok {
		return fmt.Errorf("cannot delete a column that has additions: %s", strings.Join(op.path, "."))
	}

	if _, ok := builder.updates[field.ID]; ok {
		return fmt.Errorf("cannot delete a column that has updates: %s", strings.Join(op.path, "."))
	}

	builder.deletes[field.ID] = struct{}{}
	return nil
}

func (op *deleteColumnOp) String() string {
	return fmt.Sprintf("DeleteColumn(%s)", strings.Join(op.path, "."))
}

/// MoveColumn Operation

// Implementation for moveColumnOp
func (op *moveColumnOp) Validate(schema *iceberg.Schema, settings *validationSettings) error {
	// Validate column to move exists
	colField := findField(schema, op.columnToMove, settings.caseSensitive)
	if colField == nil {
		return fmt.Errorf("cannot move missing column: %s", strings.Join(op.columnToMove, "."))
	}

	// Validate reference column for before/after operations
	if op.op == OpBefore || op.op == OpAfter {
		other := findField(schema, op.referenceColumn, settings.caseSensitive)
		if other == nil {
			return fmt.Errorf("reference column for move not found: %s", strings.Join(op.referenceColumn, "."))
		}

		// Check same parent
		colParentID := parentIDForPath(schema, op.columnToMove, settings.caseSensitive)
		refParentID := parentIDForPath(schema, op.referenceColumn, settings.caseSensitive)
		if colParentID != refParentID {
			return errors.New("cannot move column across different parent structs")
		}

		if other.ID == colField.ID {
			return errors.New("cannot move column relative to itself")
		}
	}

	return nil
}

func (op *moveColumnOp) Apply(builder *schemaBuilder) error {
	colField := findField(builder.baseSchema, op.columnToMove, builder.settings.caseSensitive)
	if colField == nil {
		return fmt.Errorf("cannot move missing column: %s", strings.Join(op.columnToMove, "."))
	}

	parentID := parentIDForPath(builder.baseSchema, op.columnToMove, builder.settings.caseSensitive)

	var otherID int
	if op.op == OpBefore || op.op == OpAfter {
		other := findField(builder.baseSchema, op.referenceColumn, builder.settings.caseSensitive)
		if other == nil {
			return fmt.Errorf("reference column for move not found: %s", strings.Join(op.referenceColumn, "."))
		}
		otherID = other.ID
	}

	builder.moves[parentID] = append(builder.moves[parentID], moveReq{
		fieldID:      colField.ID,
		otherFieldID: otherID,
		op:           op.op,
	})

	return nil
}

func (op *moveColumnOp) String() string {
	if op.op == OpFirst {
		return fmt.Sprintf("Move(%s, %s)", strings.Join(op.columnToMove, "."), op.op)
	}
	return fmt.Sprintf("Move(%s, %s %s)",
		strings.Join(op.columnToMove, "."), op.op, strings.Join(op.referenceColumn, "."))
}

// Helper methods for schemaBuilder
func (b *schemaBuilder) assignNewColumnID() int {
	next := b.lastColumnID + 1
	b.lastColumnID = next
	return next
}

// Build the final schema using existing logic
func (us *UpdateSchema) buildFinalSchema(builder *schemaBuilder) (*iceberg.Schema, error) {
	newFields, err := rebuild(us.schema.AsStruct().FieldList, -1, builder)
	if err != nil {
		return nil, err
	}

	idList := us.schema.IdentifierFieldIDs
	newID := us.schema.ID + 1

	return iceberg.NewSchemaWithIdentifiers(newID, idList, newFields...), nil
}

// Rebuild schema with builder state
func rebuild(fields []iceberg.NestedField, parentID int, builder *schemaBuilder) ([]iceberg.NestedField, error) {
	var out []iceberg.NestedField

	// iterate over the current fields to apply updates and deletes
	for _, f := range fields {
		if _, gone := builder.deletes[f.ID]; gone {
			continue
		}
		if upd, ok := builder.updates[f.ID]; ok {
			f = *upd
		}

		switch t := f.Type.(type) {
		case *iceberg.StructType:
			fields, err := rebuild(t.Fields(), f.ID, builder)
			if err != nil {
				return nil, fmt.Errorf("error rebuilding struct type: %w", err)
			}
			f.Type = &iceberg.StructType{FieldList: fields}

		case *iceberg.ListType:
			el := t.ElementField()
			fields, err := rebuild([]iceberg.NestedField{el}, el.ID, builder)
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
			fields, err := rebuild([]iceberg.NestedField{val}, val.ID, builder)
			if err != nil {
				return nil, fmt.Errorf("error rebuilding map type: %w", err)
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
	for _, nf := range builder.adds[parentID] {
		out = append(out, *nf)
	}

	// check if there are any moves for this parent id (-1 means root)
	if reqs := builder.moves[parentID]; len(reqs) > 0 {
		var err error
		out, err = reorder(out, reqs)
		if err != nil {
			return nil, fmt.Errorf("error reordering fields: %w", err)
		}
	}

	return out, nil
}

// Reorder fields based on move operations
func reorder(fields []iceberg.NestedField, reqs []moveReq) ([]iceberg.NestedField, error) {
	// find the index of a field by its id
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
				return nil, errors.New("move-before target not found at commit time")
			}
			fields = append(fields[:idx],
				append([]iceberg.NestedField{f}, fields[idx:]...)...)
		case OpAfter:
			idx := indexOf(m.otherFieldID)
			if idx == -1 {
				return nil, errors.New("move-after target not found at commit time")
			}
			fields = append(fields[:idx+1],
				append([]iceberg.NestedField{f}, fields[idx+1:]...)...)
		default:
			return nil, fmt.Errorf("unknown move op: %s", m.op)
		}
	}

	return fields, nil
}

// Helper functions
func findField(schema *iceberg.Schema, path []string, caseSensitive bool) *iceberg.NestedField {
	name := strings.Join(path, ".")

	var field iceberg.NestedField
	var ok bool

	if caseSensitive {
		field, ok = schema.FindFieldByName(name)
	} else {
		field, ok = schema.FindFieldByNameCaseInsensitive(name)
	}

	if !ok {
		return nil
	}

	return &field
}

func parentIDForPath(schema *iceberg.Schema, path []string, caseSensitive bool) int {
	if len(path) == 1 {
		return -1
	}

	if f := findField(schema, path[:len(path)-1], caseSensitive); f != nil {
		return f.ID
	}

	return -1
}

func validateDefaultValue(typ iceberg.Type, val any) error {
	// Defaults are only allowed on primitive columns
	prim, ok := typ.(iceberg.PrimitiveType)
	if !ok {
		return fmt.Errorf("defaults are only allowed on primitive columns, got %s", typ.Type())
	}

	lit, err := iceberg.LiteralFromAny(val)
	if err != nil {
		return err
	}

	litType := lit.Type()

	// Exact match ?
	if litType.Equals(prim) {
		return nil
	}

	return fmt.Errorf("default literal of type %s is not assignable to of type %s", litType.String(), prim.Type())
}

func (us *UpdateSchema) findExistingSchemaInTransaction(newSchema *iceberg.Schema) *int {
	for _, schema := range us.txn.tbl.metadata.Schemas() {
		if newSchema.Equals(schema) {
			return &schema.ID
		}
	}
	return nil
}

func (us *UpdateSchema) getNameMappingUpdates(newSchema *iceberg.Schema) []Update {
	// Only update name mapping if we have adds/updates that might need it
	hasAdds := false
	hasUpdates := false
	for _, op := range us.operations {
		switch op.(type) {
		case *addColumnOp:
			hasAdds = true
		case *updateColumnOp:
			hasUpdates = true
		}
	}

	if !hasAdds && !hasUpdates {
		return nil
	}

	completeMapping := newSchema.NameMapping()

	mappingJson, err := json.Marshal(completeMapping)
	if err != nil {
		return nil
	}

	return []Update{
		NewSetPropertiesUpdate(iceberg.Properties{
			DefaultNameMappingKey: string(mappingJson),
		}),
	}
}
