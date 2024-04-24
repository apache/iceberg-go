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
	"fmt"
	"strings"

	"golang.org/x/exp/slices"
)

const (
	partitionDataIDStart   = 1000
	InitialPartitionSpecID = 0
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
	FieldID int `json:"field-id,omitempty"`
	// Name is the name of the partition field itself
	Name string `json:"name"`
	// Transform is the transform used to produce the partition value
	Transform Transform `json:"transform"`
}

func (p *PartitionField) String() string {
	return fmt.Sprintf("%d: %s: %s(%d)", p.FieldID, p.Name, p.Transform, p.SourceID)
}

func (p *PartitionField) UnmarshalJSON(b []byte) error {
	type Alias PartitionField
	aux := struct {
		TransformString string `json:"transform"`
		*Alias
	}{
		Alias: (*Alias)(p),
	}

	err := json.Unmarshal(b, &aux)
	if err != nil {
		return err
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

func NewPartitionSpec(fields ...PartitionField) PartitionSpec {
	return NewPartitionSpecID(InitialPartitionSpecID, fields...)
}

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
func (ps *PartitionSpec) Equals(other PartitionSpec) bool {
	return ps.id == other.id && slices.Equal(ps.fields, other.fields)
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
	for _, f := range ps.fields {
		ps.sourceIdToFields[f.SourceID] =
			append(ps.sourceIdToFields[f.SourceID], f)
	}
}

func (ps *PartitionSpec) ID() int                    { return ps.id }
func (ps *PartitionSpec) NumFields() int             { return len(ps.fields) }
func (ps *PartitionSpec) Field(i int) PartitionField { return ps.fields[i] }

func (ps *PartitionSpec) IsUnpartitioned() bool {
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
		return partitionDataIDStart - 1
	}

	id := ps.fields[0].FieldID
	for _, f := range ps.fields[1:] {
		if f.FieldID > id {
			id = f.FieldID
		}
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
