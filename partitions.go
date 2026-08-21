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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net/url"
	"slices"
	"strings"
)

const (
	PartitionDataIDStart   = 1000
	InitialPartitionSpecID = 0
	unassignedFieldID      = 0
)

// UnpartitionedSpec is the default unpartitioned spec which can
// be used for comparisons or to just provide a convenience for referencing
// the same unpartitioned spec object.
var UnpartitionedSpec = &PartitionSpec{id: 0}

// PartitionField represents how one partition value is derived from the
// source column by transformation.
type PartitionField struct {
	// SourceIDs contains the source column ids from the table's schema.
	// For single-argument transforms this will have exactly one element.
	// For multi-argument transforms this will have multiple elements.
	SourceIDs []int `json:"-"`
	// FieldID is the partition field id across all the table partition specs
	FieldID int `json:"field-id"`
	// Name is the name of the partition field itself
	Name string `json:"name"`
	// Transform is the transform used to produce the partition value
	Transform Transform `json:"transform"`

	// escapedName is a cached URL-escaped version of Name for performance
	// This is populated during initialization and not serialized
	escapedName string
}

// SourceID returns the first source column id. For single-argument transforms
// this is the only source column. For multi-argument transforms this is the
// first source column.
func (p PartitionField) SourceID() int {
	if len(p.SourceIDs) == 0 {
		return 0
	}

	return p.SourceIDs[0]
}

// EscapedName returns the URL-escaped version of the partition field name.
// initialize() pre-populates escapedName for specs built through a constructor.
func (p *PartitionField) EscapedName() string {
	if p.escapedName != "" {
		return p.escapedName
	}

	return url.QueryEscape(p.Name)
}

func (p PartitionField) MarshalJSON() ([]byte, error) {
	if len(p.SourceIDs) == 1 && p.SourceIDs[0] == 0 {
		if _, isVoid := p.Transform.(VoidTransform); isVoid {
			return json.Marshal(struct {
				FieldID   int       `json:"field-id"`
				Name      string    `json:"name"`
				Transform Transform `json:"transform"`
			}{p.FieldID, p.Name, p.Transform})
		}
	}
	if len(p.SourceIDs) > 1 {
		return json.Marshal(struct {
			SourceIDs []int     `json:"source-ids"`
			FieldID   int       `json:"field-id"`
			Name      string    `json:"name"`
			Transform Transform `json:"transform"`
		}{p.SourceIDs, p.FieldID, p.Name, p.Transform})
	}

	return json.Marshal(struct {
		SourceID  int       `json:"source-id"`
		FieldID   int       `json:"field-id"`
		Name      string    `json:"name"`
		Transform Transform `json:"transform"`
	}{p.SourceID(), p.FieldID, p.Name, p.Transform})
}

func (p PartitionField) Equals(other PartitionField) bool {
	return slices.Equal(p.SourceIDs, other.SourceIDs) &&
		p.FieldID == other.FieldID &&
		p.Name == other.Name &&
		p.Transform.Equals(other.Transform)
}

func (p *PartitionField) String() string {
	if len(p.SourceIDs) > 1 {
		return fmt.Sprintf("%d: %s: %s(%v)", p.FieldID, p.Name, p.Transform, p.SourceIDs)
	}

	return fmt.Sprintf("%d: %s: %s(%d)", p.FieldID, p.Name, p.Transform, p.SourceID())
}

func (p *PartitionField) UnmarshalJSON(b []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return fmt.Errorf("%w: failed to unmarshal partition field", err)
	}

	_, hasSourceID := raw["source-id"]
	_, hasSourceIDs := raw["source-ids"]
	if hasSourceID && hasSourceIDs {
		return fmt.Errorf("%w: partition field cannot contain both source-id and source-ids", ErrInvalidPartitionSpec)
	}

	transformJSON, hasTransform := raw["transform"]
	if !hasTransform || string(transformJSON) == "null" {
		return fmt.Errorf("%w: partition field requires a transform", ErrInvalidTransform)
	}

	var sourceID int
	if hasSourceID {
		if err := unmarshalJSONField(raw["source-id"], "source-id", &sourceID); err != nil {
			return err
		}
	}
	var sourceIDs []int
	if hasSourceIDs {
		if err := unmarshalJSONField(raw["source-ids"], "source-ids", &sourceIDs); err != nil {
			return err
		}
	}
	var transformString string
	if err := unmarshalJSONField(transformJSON, "transform", &transformString); err != nil {
		return err
	}

	var fieldID int
	if fieldIDJSON, ok := raw["field-id"]; ok {
		if err := unmarshalJSONField(fieldIDJSON, "field-id", &fieldID); err != nil {
			return err
		}
	}
	var name string
	if nameJSON, ok := raw["name"]; ok {
		if err := unmarshalJSONField(nameJSON, "name", &name); err != nil {
			return err
		}
	}

	next := PartitionField{
		FieldID: fieldID,
		Name:    name,
	}

	var err error
	if next.Transform, err = ParseTransform(transformString); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidPartitionSpec, err)
	}
	if err := validateTransform(next.Transform); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidPartitionSpec, err)
	}

	if hasSourceIDs && len(sourceIDs) == 0 {
		return fmt.Errorf("%w: partition source-ids cannot be empty", ErrInvalidPartitionSpec)
	}
	if !hasSourceID && !hasSourceIDs {
		if _, isVoid := next.Transform.(VoidTransform); !isVoid {
			return fmt.Errorf("%w: partition field requires source-id or source-ids", ErrInvalidPartitionSpec)
		}
		// Preserve compatibility with historical source-less void tombstones.
		next.SourceIDs = []int{0}
	} else if len(sourceIDs) > 0 {
		next.SourceIDs = sourceIDs
	} else {
		next.SourceIDs = []int{sourceID}
	}
	for _, sourceID := range next.SourceIDs {
		_, isVoid := next.Transform.(VoidTransform)
		if sourceID <= 0 && (!isVoid || hasSourceID || hasSourceIDs) {
			return fmt.Errorf("%w: partition source ID must be positive: %d", ErrInvalidPartitionSpec, sourceID)
		}
	}
	if next.Name == "" {
		return fmt.Errorf("%w: partition name cannot be empty", ErrInvalidPartitionSpec)
	}

	*p = next

	return nil
}

func unmarshalJSONField(data json.RawMessage, field string, value any) error {
	if err := json.Unmarshal(data, value); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			typeErr.Struct = ""
			typeErr.Field = field
		}

		return err
	}
	return nil
}

// PartitionSpec captures the transformation from table data to partition values
type PartitionSpec struct {
	// any change to a PartitionSpec will produce a new spec id
	id     int
	fields []PartitionField

	// this is populated by initialize after creation
	sourceIdToFields map[int][]PartitionField
}

type PartitionOption func(*PartitionSpec) error

// BindToSchema creates a new PartitionSpec by copying the fields from the
// existing spec verifying compatibility with the schema.
//
// If newSpecID is not nil, it will be used as the spec id for the new spec.
// Otherwise, the existing spec id will be used.
// If a field in the spec is incompatible with the schema, an error will be
// returned.
func (p *PartitionSpec) BindToSchema(schema *Schema, lastPartitionID *int, newSpecID *int) (PartitionSpec, error) {
	opts := make([]PartitionOption, 0)
	if newSpecID != nil {
		opts = append(opts, WithSpecID(*newSpecID))
	} else {
		opts = append(opts, WithSpecID(p.id))
	}

	for _, field := range p.Fields() {
		if len(field.SourceIDs) == 1 && field.SourceIDs[0] == 0 {
			if _, isVoid := field.Transform.(VoidTransform); isVoid {
				opts = append(opts, func(spec *PartitionSpec) error {
					spec.fields = append(spec.fields, clonePartitionField(field))

					return nil
				})

				continue
			}
		}
		opts = append(opts, AddPartitionFieldBySourceID(field.SourceID(), field.Name, field.Transform, schema, &field.FieldID))
	}

	freshSpec, err := NewPartitionSpecOpts(opts...)
	if err != nil {
		return PartitionSpec{}, err
	}
	if err = freshSpec.assignPartitionFieldIds(lastPartitionID); err != nil {
		return PartitionSpec{}, err
	}

	return freshSpec, err
}

func NewPartitionSpecOpts(opts ...PartitionOption) (PartitionSpec, error) {
	spec := PartitionSpec{
		id: 0,
	}
	for _, opt := range opts {
		if err := opt(&spec); err != nil {
			return PartitionSpec{}, fmt.Errorf("%w: %w", ErrInvalidPartitionSpec, err)
		}
	}
	// Validate the assembled field set rather than each field as it is added:
	// the redundancy check needs every field in place, and routing the builder
	// through the same validator as UnmarshalJSON keeps one definition of a
	// redundant field, so a spec this constructor accepts is one this package's
	// UnmarshalJSON can read back. NewPartitionSpec and NewPartitionSpecID do
	// not validate, so that guarantee covers this constructor only.
	//
	// ErrInvalidPartitionSpec is attached exactly once per error, by whichever
	// layer knows the sentinel: options return bare errors and the loop above
	// wraps them, while validatePartitionFields wraps its own.
	if err := validatePartitionFields(spec.fields); err != nil {
		return PartitionSpec{}, err
	}
	spec.initialize()

	return spec, nil
}

func WithSpecID(id int) PartitionOption {
	return func(p *PartitionSpec) error {
		if id < 0 {
			return fmt.Errorf("spec id must be non-negative: %d", id)
		}
		p.id = id

		return nil
	}
}

func AddPartitionFieldByName(sourceName string, targetName string, transform Transform, schema *Schema, fieldID *int) PartitionOption {
	return func(p *PartitionSpec) error {
		if schema == nil {
			return errors.New("cannot add partition field with nil schema")
		}
		field, ok := schema.FindFieldByName(sourceName)

		if !ok {
			return fmt.Errorf("cannot find source column with name: %s in schema", sourceName)
		}
		err := p.addSpecFieldInternal(targetName, field, transform, fieldID)
		if err != nil {
			return err
		}

		return nil
	}
}

func AddPartitionFieldBySourceID(sourceID int, targetName string, transform Transform, schema *Schema, fieldID *int) PartitionOption {
	return func(p *PartitionSpec) error {
		if schema == nil {
			return errors.New("cannot add partition field with nil schema")
		}
		field, ok := schema.FindFieldByID(sourceID)
		if !ok {
			return fmt.Errorf("cannot find source column with id: %d in schema", sourceID)
		}
		err := p.addSpecFieldInternal(targetName, field, transform, fieldID)
		if err != nil {
			return err
		}

		return nil
	}
}

func (p *PartitionSpec) addSpecFieldInternal(targetName string, field NestedField, transform Transform, fieldID *int) error {
	if targetName == "" {
		return errors.New("cannot use empty partition name")
	}
	if err := validateTransform(transform); err != nil {
		return err
	}
	for _, existingField := range p.fields {
		if existingField.Name == targetName {
			return errors.New("duplicate partition name: " + targetName)
		}
	}
	var fieldIDValue int
	if fieldID == nil {
		fieldIDValue = unassignedFieldID
	} else {
		fieldIDValue = *fieldID
	}
	unboundField := PartitionField{
		SourceIDs: []int{field.ID},
		FieldID:   fieldIDValue,
		Name:      targetName,
		Transform: transform,
	}
	p.fields = append(p.fields, unboundField)

	return nil
}

func validateTransform(transform Transform) error {
	switch t := transform.(type) {
	case BucketTransform:
		return t.validateNumBuckets()
	case *BucketTransform:
		return t.validateNumBuckets()
	case TruncateTransform:
		return t.validateWidth()
	case *TruncateTransform:
		return t.validateWidth()
	case UnknownTransform:
		// The zero value is constructible from outside the package and would
		// serialize as "transform": "".
		if t.String() == "" {
			return fmt.Errorf("%w: unknown transform has no name", ErrInvalidTransform)
		}

		return nil
	default:
		return nil
	}
}

func (p *PartitionSpec) Len() int {
	return len(p.fields)
}

func (ps *PartitionSpec) assignPartitionFieldIds(lastAssignedFieldIDPtr *int) error {
	// This is set_field_ids from iceberg-rust
	// Already assigned partition ids. If we see one of these during iteration,
	// we skip it.
	assignedIds := make(map[int]struct{})
	for _, field := range ps.fields {
		if field.FieldID != unassignedFieldID {
			if _, exists := assignedIds[field.FieldID]; exists {
				return fmt.Errorf("duplicate field ID provided: %d", field.FieldID)
			}
			assignedIds[field.FieldID] = struct{}{}
		}
	}

	lastAssignedFieldID := ps.LastAssignedFieldID()
	if lastAssignedFieldIDPtr != nil {
		lastAssignedFieldID = *lastAssignedFieldIDPtr
	}
	for i := range ps.fields {
		if ps.fields[i].FieldID == unassignedFieldID {
			// Find the next available ID by incrementing from the last known good ID.
			lastAssignedFieldID++
			for {
				if _, exists := assignedIds[lastAssignedFieldID]; !exists {
					break // Found an unused ID.
				}
				lastAssignedFieldID++
			}

			// Assign the new ID and immediately record it as used.
			ps.fields[i].FieldID = lastAssignedFieldID
		} else {
			lastAssignedFieldID = max(lastAssignedFieldID, ps.fields[i].FieldID)
		}
	}

	return nil
}

// NewPartitionSpec creates a new PartitionSpec with the given fields.
//
// The fields are not verified against a schema, use NewPartitionSpecOpts if you have to ensure compatibility.
//
// The fields are not checked for redundancy either, so this accepts a spec that
// UnmarshalJSON would reject, meaning the result may not survive a metadata
// round trip. Use NewPartitionSpecOpts when the spec has to be readable back.
func NewPartitionSpec(fields ...PartitionField) PartitionSpec {
	return NewPartitionSpecID(InitialPartitionSpecID, fields...)
}

// NewPartitionSpecID creates a new PartitionSpec with the given fields and id.
//
// The fields are not verified against a schema, use NewPartitionSpecOpts if you have to ensure compatibility.
//
// The fields are not checked for redundancy either, so this accepts a spec that
// UnmarshalJSON would reject, meaning the result may not survive a metadata
// round trip. Use NewPartitionSpecOpts when the spec has to be readable back.
func NewPartitionSpecID(id int, fields ...PartitionField) PartitionSpec {
	fieldCopies := make([]PartitionField, len(fields))
	for i, field := range fields {
		fieldCopies[i] = clonePartitionField(field)
	}
	ret := PartitionSpec{id: id, fields: fieldCopies}
	ret.initialize()

	return ret
}

// CompatibleWith returns true if this partition spec is considered
// compatible with the passed in partition spec. This means that the two
// specs have equivalent field lists regardless of the spec id.
func (ps *PartitionSpec) CompatibleWith(other *PartitionSpec) bool {
	if ps == other {
		return true
	}

	if len(ps.fields) != len(other.fields) {
		return false
	}

	return slices.EqualFunc(ps.fields, other.fields, func(left, right PartitionField) bool {
		return slices.Equal(left.SourceIDs, right.SourceIDs) && left.Name == right.Name &&
			left.Transform.Equals(right.Transform)
	})
}

// Equals returns true iff the field lists are the same AND the spec id
// is the same between this partition spec and the provided one.
func (ps PartitionSpec) Equals(other PartitionSpec) bool {
	return ps.id == other.id && slices.EqualFunc(ps.fields, other.fields, func(a, b PartitionField) bool {
		return a.Equals(b)
	})
}

// Fields returns an iterator over the partition fields in this spec.
func (ps *PartitionSpec) Fields() iter.Seq2[int, PartitionField] {
	return func(yield func(int, PartitionField) bool) {
		for i, field := range ps.fields {
			if !yield(i, clonePartitionField(field)) {
				return
			}
		}
	}
}

func (ps PartitionSpec) MarshalJSON() ([]byte, error) {
	if ps.fields == nil {
		ps.fields = []PartitionField{}
	}

	return json.Marshal(struct {
		ID     int              `json:"spec-id"`
		Fields []PartitionField `json:"fields"`
	}{ps.id, ps.fields})
}

func (ps *PartitionSpec) UnmarshalJSON(b []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return fmt.Errorf("%w: invalid partition spec JSON: %w", ErrInvalidPartitionSpec, err)
	}

	id := InitialPartitionSpecID
	if rawID, ok := raw["spec-id"]; ok {
		if bytes.Equal(bytes.TrimSpace(rawID), []byte("null")) {
			return fmt.Errorf("%w: partition spec spec-id cannot be null", ErrInvalidPartitionSpec)
		}
		if err := json.Unmarshal(rawID, &id); err != nil {
			return fmt.Errorf("%w: invalid partition spec ID: %w", ErrInvalidPartitionSpec, err)
		}
	}
	if id < 0 {
		return fmt.Errorf("%w: spec ID must be non-negative: %d", ErrInvalidPartitionSpec, id)
	}

	rawFields, ok := raw["fields"]
	if !ok || bytes.Equal(bytes.TrimSpace(rawFields), []byte("null")) {
		return fmt.Errorf("%w: partition spec is missing required fields", ErrInvalidPartitionSpec)
	}
	var rawFieldList []json.RawMessage
	if err := json.Unmarshal(rawFields, &rawFieldList); err != nil {
		return fmt.Errorf("%w: invalid partition spec fields: %w", ErrInvalidPartitionSpec, err)
	}

	fields := make([]PartitionField, len(rawFieldList))
	for i, rawField := range rawFieldList {
		var keys map[string]json.RawMessage
		if err := json.Unmarshal(rawField, &keys); err != nil {
			return fmt.Errorf("%w: invalid partition field JSON: %w", ErrInvalidPartitionSpec, err)
		}
		if rawFieldID, ok := keys["field-id"]; ok {
			var fieldID *int
			if err := json.Unmarshal(rawFieldID, &fieldID); err != nil {
				return fmt.Errorf("%w: invalid partition field ID: %w", ErrInvalidPartitionSpec, err)
			}
			if fieldID == nil {
				return fmt.Errorf("%w: partition field ID cannot be null", ErrInvalidPartitionSpec)
			}
		}
		if err := json.Unmarshal(rawField, &fields[i]); err != nil {
			return fmt.Errorf("%w: invalid partition field: %w", ErrInvalidPartitionSpec, err)
		}
	}
	if err := validatePartitionFields(fields); err != nil {
		return err
	}

	decoded := PartitionSpec{id: id, fields: fields}
	if err := decoded.assignPartitionFieldIds(nil); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidPartitionSpec, err)
	}
	decoded.initialize()
	*ps = decoded

	return nil
}

// validatePartitionFields rejects duplicate partition names and redundant
// fields (the same transform applied to the same source columns). It is the
// single definition of a well-formed field set, shared by NewPartitionSpecOpts
// and UnmarshalJSON so the builder cannot produce a spec the parser rejects.
//
// Repeated void fields are exempt: void is the tombstone written for a dropped
// partition field, so a spec that has dropped several fields legitimately
// carries more than one.
//
// TODO: this does not implement Java's dedupName rule, which collapses year,
// month, day and hour to a single name per source column and so rejects
// year(ts) alongside month(ts). We accept that pairing, which means a spec
// built here can be refused by Java's PartitionSpec.Builder. UpdateSpec.addField
// already carries an added-vs-added guard for time transforms, so the rule
// exists in the codebase but not in this validator.
func validatePartitionFields(fields []PartitionField) error {
	names := make(map[string]struct{}, len(fields))
	// Keyed by source IDs only; the transforms behind each key are compared with
	// Transform.Equals rather than their rendered names, so a transform whose
	// String() is not injective cannot make two distinct transforms collide.
	bySource := make(map[string][]Transform, len(fields))
	for _, field := range fields {
		if _, ok := names[field.Name]; ok {
			return fmt.Errorf("%w: duplicate partition name: %s", ErrInvalidPartitionSpec, field.Name)
		}
		names[field.Name] = struct{}{}
		// Reject a nil transform before the redundancy comparison below calls
		// Equals on it. The option constructors accept any Transform and
		// validateTransform's default branch passes nil through, so this is the
		// first place a nil would be dereferenced.
		if field.Transform == nil {
			return fmt.Errorf("%w: partition field %s has no transform", ErrInvalidPartitionSpec, field.Name)
		}
		if _, isVoid := field.Transform.(VoidTransform); isVoid {
			continue
		}

		key := fmt.Sprint(field.SourceIDs)
		for _, existing := range bySource[key] {
			if existing.Equals(field.Transform) {
				return fmt.Errorf("%w: redundant partition field for source IDs %v and transform %s",
					ErrInvalidPartitionSpec, field.SourceIDs, field.Transform)
			}
		}
		bySource[key] = append(bySource[key], field.Transform)
	}

	return nil
}

func (ps *PartitionSpec) initialize() {
	ps.sourceIdToFields = make(map[int][]PartitionField)

	for i := range ps.fields {
		ps.fields[i].escapedName = url.QueryEscape(ps.fields[i].Name)
		ps.sourceIdToFields[ps.fields[i].SourceID()] = append(ps.sourceIdToFields[ps.fields[i].SourceID()], ps.fields[i])
	}
}

func (ps *PartitionSpec) ID() int        { return ps.id }
func (ps *PartitionSpec) NumFields() int { return len(ps.fields) }
func (ps *PartitionSpec) Field(i int) PartitionField {
	return clonePartitionField(ps.fields[i])
}

func (ps PartitionSpec) IsUnpartitioned() bool {
	if len(ps.fields) == 0 {
		return true
	}

	for _, f := range ps.fields {
		if _, ok := f.Transform.(VoidTransform); !ok {
			return false
		}
	}

	return true
}

func (ps *PartitionSpec) FieldsBySourceID(fieldID int) []PartitionField {
	fields := ps.sourceIdToFields[fieldID]
	if fields == nil {
		return nil
	}

	clones := make([]PartitionField, len(fields))
	for i, field := range fields {
		clones[i] = clonePartitionField(field)
	}

	return clones
}

func clonePartitionField(field PartitionField) PartitionField {
	field.SourceIDs = slices.Clone(field.SourceIDs)
	switch transform := field.Transform.(type) {
	case *BucketTransform:
		if transform != nil {
			cloned := *transform
			field.Transform = &cloned
		}
	case *TruncateTransform:
		if transform != nil {
			cloned := *transform
			field.Transform = &cloned
		}
	}

	return field
}

func (ps PartitionSpec) String() string {
	var b strings.Builder
	b.WriteByte('[')
	for i, f := range ps.fields {
		if i == 0 {
			b.WriteString("\n")
		}
		b.WriteString("\t")
		b.WriteString(f.String())
		b.WriteString("\n")
	}
	b.WriteByte(']')

	return b.String()
}

func (ps *PartitionSpec) LastAssignedFieldID() int {
	if len(ps.fields) == 0 {
		return PartitionDataIDStart - 1
	}

	id := ps.fields[0].FieldID
	for _, f := range ps.fields[1:] {
		if f.FieldID > id {
			id = f.FieldID
		}
	}

	if id == unassignedFieldID {
		// If no fields have been assigned an ID, return the default starting ID.
		return PartitionDataIDStart - 1
	}

	return id
}

type resolvedPartitionField struct {
	field      PartitionField
	resultType Type
}

func (ps *PartitionSpec) resolvedPartitionFields(schema *Schema) []resolvedPartitionField {
	fields := make([]resolvedPartitionField, 0, len(ps.fields))
	for _, field := range ps.fields {
		sourceType := Type(UnknownType{})
		if typ, ok := schema.FindTypeByID(field.SourceID()); ok {
			sourceType = typ
		}

		fields = append(fields, resolvedPartitionField{
			field:      field,
			resultType: field.Transform.ResultType(sourceType),
		})
	}

	return fields
}

// PartitionType produces a struct of the partition spec.
//
// The partition fields should be optional:
//   - All partition transforms are required to produce null if the input value
//     is null. This can happen when the source column is optional.
//   - Partition fields may be added later, in which case not all files would
//     have the result field and it may be null.
//
// There is a case where we can guarantee that a partition field in the first
// and only partition spec that uses a required source column will never be
// null, but it doesn't seem worth tracking this case.
//
// If a source column is missing, UnknownType is passed to the transform. This
// retains the field's position and lets transforms with fixed result types,
// such as bucket, continue to resolve their result type.
func (ps *PartitionSpec) PartitionType(schema *Schema) *StructType {
	resolvedFields := ps.resolvedPartitionFields(schema)
	nestedFields := make([]NestedField, 0, len(resolvedFields))
	for _, field := range resolvedFields {
		nestedFields = append(nestedFields, NestedField{
			ID:       field.field.FieldID,
			Name:     field.field.Name,
			Type:     field.resultType,
			Required: false,
		})
	}

	return &StructType{FieldList: nestedFields}
}

// PartitionToPath produces a proper partition path from the data and schema by
// converting the values to human readable strings and properly escaping.
//
// The path will be in the form of `name1=value1/name2=value2/...`.
//
// This does not apply the transforms to the data, it is assumed the provided data
// has already been transformed appropriately.
func (ps *PartitionSpec) PartitionToPath(data StructLike, sc *Schema) string {
	resolvedFields := ps.resolvedPartitionFields(sc)

	if len(resolvedFields) == 0 {
		return ""
	}

	// Use strings.Builder for efficient string concatenation
	// Estimate capacity: escaped_name + "=" + escaped_value + "/" per field
	var sb strings.Builder
	estimatedSize := 0
	for i := range resolvedFields {
		estimatedSize += len(resolvedFields[i].field.EscapedName()) + 20 // name + "=" + avg value + "/"
	}
	sb.Grow(estimatedSize)

	for i := range resolvedFields {
		if i > 0 {
			sb.WriteByte('/')
		}

		// Use pre-escaped field name (now guaranteed to be initialized)
		sb.WriteString(resolvedFields[i].field.EscapedName())
		sb.WriteByte('=')

		// Only escape the value (which changes per row)
		valueStr := resolvedFields[i].field.Transform.ToHumanStrType(resolvedFields[i].resultType, data.Get(i))
		sb.WriteString(url.QueryEscape(valueStr))
	}

	return sb.String()
}

// GeneratePartitionFieldName returns default partition field name based on field transform type
//
// The default names are aligned with other client implementations
// https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/BaseUpdatePartitionSpec.java#L518-L563
func GeneratePartitionFieldName(schema *Schema, field PartitionField) (string, error) {
	if len(field.Name) > 0 {
		return field.Name, nil
	}

	sourceName, exists := schema.FindColumnName(field.SourceID())
	if !exists {
		return "", fmt.Errorf("could not find field with id %d", field.SourceID())
	}

	transform := field.Transform
	switch t := transform.(type) {
	case UnknownTransform:
		// A generated name would embed the transform's brackets, e.g.
		// "id_custom_transform[42]". Make the caller supply one.
		return "", fmt.Errorf("%w: partition field using unknown transform %s must be given an explicit name",
			ErrInvalidTransform, t)
	case IdentityTransform:
		return sourceName, nil
	case VoidTransform:
		return sourceName + "_null", nil
	case BucketTransform:
		return fmt.Sprintf("%s_bucket_%d", sourceName, t.NumBuckets), nil
	case TruncateTransform:
		return fmt.Sprintf("%s_trunc_%d", sourceName, t.Width), nil
	default:
		return sourceName + "_" + t.String(), nil
	}
}
