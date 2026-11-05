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
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/iceberg-go"
)

// UpdateSortOrder builds a replacement for the table's default sort order.
// Replacement starts with no fields, rather than copying the existing order.
// Calling Commit stages the change in the transaction; Transaction.Commit
// persists it to the catalog.
type UpdateSortOrder struct {
	txn           *Transaction
	caseSensitive bool
	fields        []sortOrderColumn
}

type sortOrderColumn struct {
	name      string
	transform iceberg.Transform
	direction SortDirection
	nullOrder NullOrder
}

// UpdateSortOrder starts a sort order replacement in this transaction.
// Names are resolved using the transaction's current schema. An empty
// replacement resets the default sort order to unsorted (order ID 0).
func (t *Transaction) UpdateSortOrder(caseSensitive bool) *UpdateSortOrder {
	return NewUpdateSortOrder(t, caseSensitive)
}

// NewUpdateSortOrder creates a builder for replacing the default sort order.
func NewUpdateSortOrder(t *Transaction, caseSensitive bool) *UpdateSortOrder {
	return &UpdateSortOrder{txn: t, caseSensitive: caseSensitive}
}

// Asc adds a column sorted in ascending order with the requested null ordering.
func (u *UpdateSortOrder) Asc(column string, nullOrder NullOrder) *UpdateSortOrder {
	return u.AddField(column, iceberg.IdentityTransform{}, SortASC, nullOrder)
}

// Desc adds a column sorted in descending order with the requested null ordering.
func (u *UpdateSortOrder) Desc(column string, nullOrder NullOrder) *UpdateSortOrder {
	return u.AddField(column, iceberg.IdentityTransform{}, SortDESC, nullOrder)
}

// AddField adds a transformed sort field. A nil or incompatible transform is
// rejected when the replacement is applied or committed.
func (u *UpdateSortOrder) AddField(column string, transform iceberg.Transform, direction SortDirection, nullOrder NullOrder) *UpdateSortOrder {
	// Copy pointer-backed transforms so callers cannot mutate a pending sort order.
	u.fields = append(u.fields, sortOrderColumn{
		name: column, transform: cloneSortField(SortField{Transform: transform}).Transform,
		direction: direction, nullOrder: nullOrder,
	})

	return u
}

// Apply returns the replacement sort order, including its proposed order ID,
// without modifying the transaction. An equivalent historical order reuses
// its existing ID.
func (u *UpdateSortOrder) Apply() (SortOrder, error) {
	meta, err := u.stagedMetadata()
	if err != nil {
		return SortOrder{}, err
	}

	return u.buildOrder(meta)
}

func (u *UpdateSortOrder) stagedMetadata() (Metadata, error) {
	if u == nil {
		return nil, fmt.Errorf("%w: sort order update is nil", ErrInvalidMetadata)
	}

	builder, err := u.txn.txnMeta()
	if err != nil {
		return nil, err
	}

	return builder.Build()
}

func isNilSortTransform(transform iceberg.Transform) bool {
	if transform == nil {
		return true
	}

	value := reflect.ValueOf(transform)

	return value.Kind() == reflect.Pointer && value.IsNil()
}

func (u *UpdateSortOrder) buildOrder(meta Metadata) (SortOrder, error) {
	if len(u.fields) == 0 {
		return UnsortedSortOrder, nil
	}

	schema := meta.CurrentSchema()
	var lowerNameToID map[string]int
	if !u.caseSensitive {
		// Java's case-insensitive schema index rejects names that differ only
		// in case. The generic Go lookup currently overwrites such entries in
		// map iteration order, so do not use it for a metadata-changing API.
		byName, err := iceberg.IndexByName(schema)
		if err != nil {
			return SortOrder{}, err
		}
		lowerNameToID = make(map[string]int, len(byName))
		for name, id := range byName {
			lower := strings.ToLower(name)
			if previous, exists := lowerNameToID[lower]; exists && previous != id {
				return SortOrder{}, fmt.Errorf("%w: ambiguous case-insensitive sort column %q", iceberg.ErrInvalidSchema, lower)
			}
			lowerNameToID[lower] = id
		}
	}
	fields := make([]SortField, 0, len(u.fields))
	for _, col := range u.fields {
		if isNilSortTransform(col.transform) {
			return SortOrder{}, fmt.Errorf("%w: sort field %q has no transform", ErrInvalidTransform, col.name)
		}

		switch col.transform.(type) {
		case iceberg.UnknownTransform, *iceberg.UnknownTransform:
			return SortOrder{}, fmt.Errorf("%w: cannot add sort field %q with unknown transform %s",
				ErrInvalidTransform, col.name, col.transform)
		}

		var field iceberg.NestedField
		var found bool
		if u.caseSensitive {
			field, found = schema.FindFieldByName(col.name)
		} else if id, ok := lowerNameToID[strings.ToLower(col.name)]; ok {
			field, found = schema.FindFieldByID(id)
		}
		if !found {
			return SortOrder{}, fmt.Errorf("cannot bind sort field %q: %w", col.name, iceberg.ErrInvalidSchema)
		}
		// Expression binding panics on complex fields. Reject them before
		// constructing an expression, as Iceberg only sorts primitive values.
		if _, ok := field.Type.(iceberg.PrimitiveType); !ok {
			return SortOrder{}, fmt.Errorf("cannot sort by non-primitive source field: %s", field.Type.Type())
		}

		fields = append(fields, SortField{
			SourceIDs: []int{field.ID},
			Transform: col.transform,
			Direction: col.direction,
			NullOrder: col.nullOrder,
		})
	}

	// Validate the field definitions and transform/type compatibility before
	// deciding whether to reuse an existing sort order.
	proposed, err := NewSortOrder(InitialSortOrderID, fields)
	if err != nil {
		return SortOrder{}, err
	}
	if err := proposed.CheckCompatibility(schema); err != nil {
		return SortOrder{}, err
	}

	nextID := InitialSortOrderID
	for _, previous := range meta.SortOrders() {
		if sameSortFields(previous, proposed) {
			return previous, nil
		}
		nextID = max(nextID, previous.OrderID()+1)
	}

	return NewSortOrder(nextID, fields)
}

// BuildUpdates produces Iceberg metadata updates and optimistic concurrency
// requirements. Reusing an order only changes the default order ID; a new
// order is added before it becomes the default.
func (u *UpdateSortOrder) BuildUpdates() ([]Update, []Requirement, error) {
	meta, err := u.stagedMetadata()
	if err != nil {
		return nil, nil, err
	}

	order, err := u.buildOrder(meta)
	if err != nil {
		return nil, nil, err
	}
	if order.OrderID() == meta.DefaultSortOrder() {
		return nil, nil, nil
	}

	// A transaction may replace the sort order more than once. Pin the
	// original base order, not the changing staged order, so all replacements
	// retain one consistent concurrency requirement at catalog commit time.
	reqs := []Requirement{AssertDefaultSortOrderID(u.txn.tbl.Metadata().DefaultSortOrder())}
	if meta.CurrentSchema().ID == u.txn.tbl.Metadata().CurrentSchema().ID {
		reqs = append(reqs, AssertCurrentSchemaID(meta.CurrentSchema().ID))
	}

	for _, previous := range meta.SortOrders() {
		if previous.OrderID() == order.OrderID() {
			return []Update{NewSetDefaultSortOrderUpdate(order.OrderID())}, reqs, nil
		}
	}

	// -1 refers to the last added sort order, as specified by the REST
	// catalog commit protocol. This also works when adding the unsorted order.
	return []Update{
		NewAddSortOrderUpdate(&order),
		NewSetDefaultSortOrderUpdate(-1),
	}, reqs, nil
}

// Commit stages this replacement in the transaction. It is a no-op if the
// proposed order is already the default.
func (u *UpdateSortOrder) Commit() error {
	updates, reqs, err := u.BuildUpdates()
	if err != nil {
		return err
	}
	if len(updates) == 0 {
		return nil
	}

	return u.txn.apply(updates, reqs)
}
