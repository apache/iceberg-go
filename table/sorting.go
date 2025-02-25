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
var UnsortedSortOrder = SortOrder{OrderID: UnsortedSortOrderID, Fields: []SortField{}}

// SortOrder describes how the data is sorted within the table.
//
// Data can be sorted within partitions by columns to gain performance. The
// order of the sort fields within the list defines the order in which the
// sort is applied to the data.
type SortOrder struct {
	OrderID int         `json:"order-id"`
	Fields  []SortField `json:"fields"`
}

func (s SortOrder) Equals(rhs SortOrder) bool {
	return s.OrderID == rhs.OrderID &&
		slices.Equal(s.Fields, rhs.Fields)
}

func (s SortOrder) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d: ", s.OrderID)
	b.WriteByte('[')
	for i, f := range s.Fields {
		if i == 0 {
			b.WriteByte('\n')
		}
		b.WriteString(f.String())
		b.WriteByte('\n')
	}
	b.WriteByte(']')

	return b.String()
}

func (s *SortOrder) UnmarshalJSON(b []byte) error {
	type Alias SortOrder
	aux := (*Alias)(s)

	if err := json.Unmarshal(b, aux); err != nil {
		return err
	}

	if len(s.Fields) == 0 {
		s.Fields = []SortField{}
		s.OrderID = 0

		return nil
	}

	if s.OrderID == 0 {
		s.OrderID = InitialSortOrderID // initialize default sort order id
	}

	return nil
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

	fields := make([]SortField, 0, len(sortOrder.Fields))
	for _, field := range sortOrder.Fields {
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

	return SortOrder{OrderID: sortOrderID, Fields: fields}, nil
}
