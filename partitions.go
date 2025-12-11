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
	// SourceID is the source column id of the table's schema
	SourceID int `json:"source-id"`
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

// EscapedName returns the URL-escaped version of the partition field name.
func (p *PartitionField) EscapedName() string {
	if p.escapedName == "" {
		p.escapedName = url.QueryEscape(p.Name)
	}

	return p.escapedName
}

func (p PartitionField) Equals(other PartitionField) bool {
	return p.SourceID == other.SourceID &&
		p.FieldID == other.FieldID &&
		p.Name == other.Name &&
		p.Transform.Equals(other.Transform)
}

func (p *PartitionField) String() string {
	return fmt.Sprintf("%d: %s: %s(%d)", p.FieldID, p.Name, p.Transform, p.SourceID)
}

func (p *PartitionField) UnmarshalJSON(b []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return fmt.Errorf("%w: failed to unmarshal partition field", err)
	}

	if _, ok := raw["source-id"]; ok {
		if _, ok := raw["source-ids"]; ok {
			return errors.New("partition field cannot contain both source-id and source-ids")
		}
	}

	type Alias PartitionField
	aux := struct {
		TransformString string `json:"transform"`
		SourceIDs       []int  `json:"source-ids,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(p),
	}

	err := json.Unmarshal(b, &aux)
	if err != nil {
		return err
	}

	if len(aux.SourceIDs) > 0 {
		if len(aux.SourceIDs) != 1 {
			return errors.New("partition field source-ids must contain exactly one id")
		}
		p.SourceID = aux.SourceIDs[0]
	}

	if p.Transform, err = ParseTransform(aux.TransformString); err != nil {
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

	for field := range p.Fields() {
		opts = append(opts, AddPartitionFieldBySourceID(field.SourceID, field.Name, field.Transform, schema, &field.FieldID))
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
	spec.initialize()

	return spec, nil
}

func WithSpecID(id int) PartitionOption {
	return func(p *PartitionSpec) error {
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
	if err := p.checkForRedundantPartitions(field.ID, transform); err != nil {
		return err
	}
	unboundField := PartitionField{
		SourceID:  field.ID,
		FieldID:   fieldIDValue,
		Name:      targetName,
		Transform: transform,
	}
	p.fields = append(p.fields, unboundField)

	return nil
}

func (p *PartitionSpec) checkForRedundantPartitions(sourceID int, transform Transform) error {
	if fields, ok := p.sourceIdToFields[sourceID]; ok {
		for _, f := range fields {
			if f.Transform.Equals(transform) {
				return fmt.Errorf("cannot add redundant partition with source id %d and transform %s. A partition with the same source id and transform already exists with name: %s",
					sourceID,
					transform,
					f.Name)
			}
		}
	}

	return nil
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
func NewPartitionSpec(fields ...PartitionField) PartitionSpec {
	return NewPartitionSpecID(InitialPartitionSpecID, fields...)
}

// NewPartitionSpecID creates a new PartitionSpec with the given fields and id.
//
// The fields are not verified against a schema, use NewPartitionSpecOpts if you have to ensure compatibility.
func NewPartitionSpecID(id int, fields ...PartitionField) PartitionSpec {
	ret := PartitionSpec{id: id, fields: fields}
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
		return left.SourceID == right.SourceID && left.Name == right.Name &&
			left.Transform == right.Transform
	})
}

// Equals returns true iff the field lists are the same AND the spec id
// is the same between this partition spec and the provided one.
func (ps PartitionSpec) Equals(other PartitionSpec) bool {
	return ps.id == other.id && slices.Equal(ps.fields, other.fields)
}

// Fields returns a clone of the partition fields in this spec.
func (ps *PartitionSpec) Fields() iter.Seq[PartitionField] {
	if ps.fields == nil {
		return slices.Values([]PartitionField{})
	}

	return slices.Values(ps.fields)
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
	aux := struct {
		ID     int              `json:"spec-id"`
		Fields []PartitionField `json:"fields"`
	}{ID: ps.id, Fields: ps.fields}

	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}

	ps.id, ps.fields = aux.ID, aux.Fields
	ps.initialize()

	return nil
}

func (ps *PartitionSpec) initialize() {
	ps.sourceIdToFields = make(map[int][]PartitionField)

	for i := range ps.fields {
		ps.sourceIdToFields[ps.fields[i].SourceID] = append(ps.sourceIdToFields[ps.fields[i].SourceID], ps.fields[i])
	}
}

func (ps *PartitionSpec) ID() int                    { return ps.id }
func (ps *PartitionSpec) NumFields() int             { return len(ps.fields) }
func (ps *PartitionSpec) Field(i int) PartitionField { return ps.fields[i] }

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
	return slices.Clone(ps.sourceIdToFields[fieldID])
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

// PartitionType produces a struct of the partition spec.
//
// The partition fields should be optional:
//   - All partition transforms are required to produce null if the input value
//     is null. This can happen when the source column is optional.
//   - Partition fields may be added later, in which case not all files would
//     have the result field and it may be null.
//
// There is a case where we can guarantee that a partition field in the first
// and only parittion spec that uses a required source column will never be
// null, but it doesn't seem worth tracking this case.
func (ps *PartitionSpec) PartitionType(schema *Schema) *StructType {
	nestedFields := []NestedField{}
	for _, field := range ps.fields {
		sourceType, ok := schema.FindTypeByID(field.SourceID)
		if !ok {
			continue
		}
		resultType := field.Transform.ResultType(sourceType)
		nestedFields = append(nestedFields, NestedField{
			ID:       field.FieldID,
			Name:     field.Name,
			Type:     resultType,
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
func (ps *PartitionSpec) PartitionToPath(data structLike, sc *Schema) string {
	partType := ps.PartitionType(sc)

	if len(partType.FieldList) == 0 {
		return ""
	}

	// Use strings.Builder for efficient string concatenation
	// Estimate capacity: escaped_name + "=" + escaped_value + "/" per field
	var sb strings.Builder
	estimatedSize := 0
	for i := range partType.Fields() {
		estimatedSize += len(ps.fields[i].EscapedName()) + 20 // name + "=" + avg value + "/"
	}
	sb.Grow(estimatedSize)

	for i := range partType.Fields() {
		if i > 0 {
			sb.WriteByte('/')
		}

		// Use pre-escaped field name (now guaranteed to be initialized)
		sb.WriteString(ps.fields[i].EscapedName())
		sb.WriteByte('=')

		// Only escape the value (which changes per row)
		valueStr := ps.fields[i].Transform.ToHumanStr(data.Get(i))
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

	sourceName, exists := schema.FindColumnName(field.SourceID)
	if !exists {
		return "", fmt.Errorf("could not find field with id %d", field.SourceID)
	}

	transform := field.Transform
	switch t := transform.(type) {
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
