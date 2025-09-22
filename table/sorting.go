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
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"

	"github.com/apache/iceberg-go"
)

type SortDirection string

const (
	SortASC  SortDirection = "asc"
	SortDESC SortDirection = "desc"
)

type NullOrder string

const (
	NullsFirst NullOrder = "nulls-first"
	NullsLast  NullOrder = "nulls-last"
)

var (
	ErrInvalidSortOrderID   = errors.New("invalid sort order ID")
	ErrInvalidTransform     = errors.New("invalid transform, must be a valid transform string or a transform object")
	ErrInvalidSortDirection = errors.New("invalid sort direction, must be 'asc' or 'desc'")
	ErrInvalidNullOrder     = errors.New("invalid null order, must be 'nulls-first' or 'nulls-last'")
)

// SortField describes a field used in a sort order definition.
type SortField struct {
	// SourceID is the source column id from the table's schema
	SourceID int `json:"source-id"`
	// Transform is the tranformation used to produce values to be
	// sorted on from the source column.
	Transform iceberg.Transform `json:"transform"`
	// Direction is an enum indicating ascending or descending direction.
	Direction SortDirection `json:"direction"`
	// NullOrder describes the order of null values when sorting
	// should be only either nulls-first or nulls-last enum values.
	NullOrder NullOrder `json:"null-order"`
}

func (s *SortField) String() string {
	if _, ok := s.Transform.(iceberg.IdentityTransform); ok {
		return fmt.Sprintf("%d %s %s", s.SourceID, s.Direction, s.NullOrder)
	}

	return fmt.Sprintf("%s(%d) %s %s", s.Transform, s.SourceID, s.Direction, s.NullOrder)
}

func (s *SortField) MarshalJSON() ([]byte, error) {
	if s.Direction == "" {
		s.Direction = SortASC
	}

	if s.NullOrder == "" {
		if s.Direction == SortASC {
			s.NullOrder = NullsFirst
		} else {
			s.NullOrder = NullsLast
		}
	}

	type Alias SortField

	return json.Marshal((*Alias)(s))
}

func (s *SortField) UnmarshalJSON(b []byte) error {
	type Alias SortField
	aux := struct {
		TransformString string `json:"transform"`
		*Alias
	}{
		Alias: (*Alias)(s),
	}

	err := json.Unmarshal(b, &aux)
	if err != nil {
		return err
	}

	if s.Transform, err = iceberg.ParseTransform(aux.TransformString); err != nil {
		return err
	}

	switch s.Direction {
	case SortASC, SortDESC:
	default:
		return ErrInvalidSortDirection
	}

	switch s.NullOrder {
	case NullsFirst, NullsLast:
	default:
		return ErrInvalidNullOrder
	}

	return nil
}

const (
	InitialSortOrderID  = 1
	UnsortedSortOrderID = 0
)

// A default Sort Order indicating no sort order at all
var UnsortedSortOrder = SortOrder{orderID: UnsortedSortOrderID, fields: []SortField{}}

// SortOrder describes how the data is sorted within the table.
//
// Data can be sorted within partitions by columns to gain performance. The
// order of the sort fields within the list defines the order in which the
// sort is applied to the data.
type SortOrder struct {
	orderID int
	fields  []SortField
}

func (s SortOrder) OrderID() int {
	return s.orderID
}

func (s SortOrder) Fields() iter.Seq[SortField] {
	return slices.Values(s.fields)
}

func (s SortOrder) Len() int {
	return len(s.fields)
}

func (s SortOrder) MarshalJSON() ([]byte, error) {
	type Alias struct {
		OrderID int         `json:"order-id"`
		Fields  []SortField `json:"fields"`
	}

	return json.Marshal(Alias{
		s.orderID,
		s.fields,
	})
}

func (s *SortOrder) UnmarshalJSON(b []byte) error {
	type Alias struct {
		OrderID int         `json:"order-id"`
		Fields  []SortField `json:"fields"`
	}
	aux := Alias{-1, nil}

	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}

	if len(aux.Fields) == 0 && aux.OrderID == -1 {
		aux.Fields = []SortField{}
		aux.OrderID = 0
	}

	if aux.OrderID == -1 {
		aux.OrderID = InitialSortOrderID
	}

	newOrder, err := NewSortOrder(aux.OrderID, aux.Fields)
	if err != nil {
		return err
	}

	*s = newOrder

	return nil
}

// NewSortOrder creates a new SortOrder.
//
// The orderID must be greater than or equal to 0.
// If orderID is 0, no fields can be passed, this is equal to UnsortedSortOrder.
// Fields need to have non-nil Transform, valid Direction and NullOrder values.
func NewSortOrder(orderID int, fields []SortField) (SortOrder, error) {
	if orderID < 0 {
		return SortOrder{}, fmt.Errorf("%w: sort order ID %d must be a non-negative integer",
			ErrInvalidSortOrderID, orderID)
	}

	if orderID == 0 && len(fields) != 0 {
		return SortOrder{}, fmt.Errorf("%w: sort order ID 0 is reserved for unsorted order", ErrInvalidSortOrderID)
	}

	if fields == nil {
		fields = []SortField{}
	}
	for idx, field := range fields {
		if field.Transform == nil {
			return SortOrder{}, fmt.Errorf("%w: sort field at index %d has no transform", ErrInvalidTransform, idx)
		}
		if field.Direction != SortASC && field.Direction != SortDESC {
			return SortOrder{}, fmt.Errorf("%w: sort field at index %d", ErrInvalidSortDirection, idx)
		}
		if field.NullOrder != NullsFirst && field.NullOrder != NullsLast {
			return SortOrder{}, fmt.Errorf("%w: sort field at index %d", ErrInvalidNullOrder, idx)
		}
	}

	return SortOrder{orderID, fields}, nil
}

func (s SortOrder) IsUnsorted() bool {
	return len(s.fields) == 0
}

func (s *SortOrder) CheckCompatibility(schema *iceberg.Schema) error {
	if s == nil {
		return nil
	}

	for _, field := range s.fields {
		f, ok := schema.FindFieldByID(field.SourceID)
		if !ok {
			return fmt.Errorf("sort field with source id %d not found in schema", field.SourceID)
		}

		if _, ok := f.Type.(iceberg.PrimitiveType); !ok {
			return fmt.Errorf("cannot sort by non-primitive source field: %s", f.Type.Type())
		}

		// FIXME: field.Transform should be made required
		if field.Transform != nil && !field.Transform.CanTransform(f.Type) {
			return fmt.Errorf("invalid source type %s for transform %s", f.Type.Type(), field.Transform)
		}
	}

	return nil
}

func (s SortOrder) Equals(rhs SortOrder) bool {
	return s.orderID == rhs.orderID &&
		slices.Equal(s.fields, rhs.fields)
}

func (s SortOrder) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d: ", s.orderID)
	b.WriteByte('[')
	for i, f := range s.fields {
		if i == 0 {
			b.WriteByte('\n')
		}
		b.WriteString(f.String())
		b.WriteByte('\n')
	}
	b.WriteByte(']')

	return b.String()
}

// AssignFreshSortOrderIDs updates and reassigns the field source IDs from the old schema
// to the corresponding fields in the fresh schema, while also giving the Sort Order a fresh
// ID of 0 (the initial Sort Order ID).
func AssignFreshSortOrderIDs(sortOrder SortOrder, old, fresh *iceberg.Schema) (SortOrder, error) {
	return AssignFreshSortOrderIDsWithID(sortOrder, old, fresh, InitialSortOrderID)
}

// AssignFreshSortOrderIDsWithID is like AssignFreshSortOrderIDs but allows specifying the id of the
// returned SortOrder.
func AssignFreshSortOrderIDsWithID(sortOrder SortOrder, old, fresh *iceberg.Schema, sortOrderID int) (SortOrder, error) {
	if sortOrder.Equals(UnsortedSortOrder) {
		return UnsortedSortOrder, nil
	}

	fields := make([]SortField, 0, len(sortOrder.fields))
	for _, field := range sortOrder.fields {
		originalField, ok := old.FindColumnName(field.SourceID)
		if !ok {
			return SortOrder{}, fmt.Errorf("cannot find source column id %s in old schema", field.String())
		}
		freshField, ok := fresh.FindFieldByName(originalField)
		if !ok {
			return SortOrder{}, fmt.Errorf("cannot find field %s in fresh schema", originalField)
		}

		fields = append(fields, SortField{
			SourceID:  freshField.ID,
			Transform: field.Transform,
			Direction: field.Direction,
			NullOrder: field.NullOrder,
		})
	}

	return SortOrder{orderID: sortOrderID, fields: fields}, nil
}
