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
	"encoding"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/exp/slices"
)

const (
	partitionDataIDStart   = 1000
	InitialPartitionSpecID = 0
)

// UnpartitionedPartitionSpec is the default unpartitioned spec which can
// be used for comparisons or to just provide a convenience for referencing
// the same unpartitioned spec object.
var UnpartitionedPartitionSpec = &PartitionSpec{id: 0}

// PartitionField represents how one partition value is derived from the
// source column by transformation.
type PartitionField struct {
	// SourceID is the source column ID of the table's schema
	SourceID int `json:"source-id"`
	// FieldID is the partition field id across all the table partition specs
	FieldID int `json:"field-id"`
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
func (ps *PartitionSpec) IsUnpartitioned() bool      { return len(ps.fields) == 0 }
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
		return partitionDataIDStart
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

// ParseTransform takes the string representation of a transform as
// defined in the iceberg spec, and produces the appropriate Transform
// object or an error if the string is not a valid transform string.
func ParseTransform(s string) (Transform, error) {
	switch {
	case strings.HasPrefix(s, "bucket"):
		matches := regexFromBrackets.FindStringSubmatch(s)
		if len(matches) != 2 {
			break
		}

		n, _ := strconv.Atoi(matches[1])
		return BucketTransform{N: n}, nil
	case strings.HasPrefix(s, "truncate"):
		matches := regexFromBrackets.FindStringSubmatch(s)
		if len(matches) != 2 {
			break
		}

		n, _ := strconv.Atoi(matches[1])
		return TruncateTransform{W: n}, nil
	default:
		switch s {
		case "identity":
			return IdentityTransform{}, nil
		case "void":
			return VoidTransform{}, nil
		case "year":
			return YearTransform{}, nil
		case "month":
			return MonthTransform{}, nil
		case "day":
			return DayTransform{}, nil
		case "hour":
			return HourTransform{}, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrInvalidTransform, s)
}

// Transform is an interface for the various Transformation types
// in partition specs. Currently they do not yet provide actual
// transformation functions or implementation. That will come later as
// data reading gets implemented.
type Transform interface {
	fmt.Stringer
	encoding.TextMarshaler
	ResultType(t Type) Type
}

// IdentityTransform uses the identity function, performing no transformation
// but instead partitioning on the value itself.
type IdentityTransform struct{}

func (t IdentityTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (IdentityTransform) String() string { return "identity" }

func (IdentityTransform) ResultType(t Type) Type { return t }

// VoidTransform is a transformation that always returns nil.
type VoidTransform struct{}

func (t VoidTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (VoidTransform) String() string { return "void" }

func (VoidTransform) ResultType(t Type) Type { return t }

// BucketTransform transforms values into a bucket partition value. It is
// parameterized by a number of buckets. Bucket partition transforms use
// a 32-bit hash of the source value to produce a positive value by mod
// the bucket number.
type BucketTransform struct {
	N int
}

func (t BucketTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t BucketTransform) String() string { return fmt.Sprintf("bucket[%d]", t.N) }

func (BucketTransform) ResultType(Type) Type { return PrimitiveTypes.Int32 }

// TruncateTransform is a transformation for truncating a value to a specified width.
type TruncateTransform struct {
	W int
}

func (t TruncateTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t TruncateTransform) String() string { return fmt.Sprintf("truncate[%d]", t.W) }

func (TruncateTransform) ResultType(t Type) Type { return t }

// YearTransform transforms a datetime value into a year value.
type YearTransform struct{}

func (t YearTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (YearTransform) String() string { return "year" }

func (YearTransform) ResultType(Type) Type { return PrimitiveTypes.Int32 }

// MonthTransform transforms a datetime value into a month value.
type MonthTransform struct{}

func (t MonthTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (MonthTransform) String() string { return "month" }

func (MonthTransform) ResultType(Type) Type { return PrimitiveTypes.Int32 }

// DayTransform transforms a datetime value into a date value.
type DayTransform struct{}

func (t DayTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (DayTransform) String() string { return "day" }

func (DayTransform) ResultType(Type) Type { return PrimitiveTypes.Date }

// HourTransform transforms a datetime value into an hour value.
type HourTransform struct{}

func (t HourTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (HourTransform) String() string { return "hour" }

func (HourTransform) ResultType(Type) Type { return PrimitiveTypes.Int32 }
