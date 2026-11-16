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

	"github.com/apache/iceberg-go"
)

// ReplaceSortOrderBuilder builds a replacement for the table's default sort order.
// Replacement starts with no fields, rather than copying the existing order.
// Calling Commit stages the change in the transaction; Transaction.Commit
// persists it to the catalog.
type ReplaceSortOrderBuilder struct {
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

// NewReplaceSortOrder starts a sort order replacement in this transaction.
// Names are resolved using the transaction's current schema. An empty
// replacement resets the default sort order to unsorted (order ID 0).
func (t *Transaction) NewReplaceSortOrder(caseSensitive bool) *ReplaceSortOrderBuilder {
	return NewReplaceSortOrderBuilder(t, caseSensitive)
}

// NewReplaceSortOrderBuilder creates a builder for replacing the default sort order.
func NewReplaceSortOrderBuilder(t *Transaction, caseSensitive bool) *ReplaceSortOrderBuilder {
	return &ReplaceSortOrderBuilder{txn: t, caseSensitive: caseSensitive}
}

// Asc adds a column sorted in ascending order with the requested null ordering.
// Go writers currently support only top-level sort columns; appending data with
// a nested-field sort order fails until the order is replaced.
func (u *ReplaceSortOrderBuilder) Asc(column string, nullOrder NullOrder) *ReplaceSortOrderBuilder {
	return u.AddField(column, iceberg.IdentityTransform{}, SortASC, nullOrder)
}

// Desc adds a column sorted in descending order with the requested null ordering.
// Go writers currently support only top-level sort columns; appending data with
// a nested-field sort order fails until the order is replaced.
func (u *ReplaceSortOrderBuilder) Desc(column string, nullOrder NullOrder) *ReplaceSortOrderBuilder {
	return u.AddField(column, iceberg.IdentityTransform{}, SortDESC, nullOrder)
}

// AddField adds a transformed sort field. A nil or incompatible transform is
// rejected when the replacement is applied or committed. Go writers currently
// support only top-level sort columns; appending data with a nested-field sort
// order fails until the order is replaced.
func (u *ReplaceSortOrderBuilder) AddField(column string, transform iceberg.Transform, direction SortDirection, nullOrder NullOrder) *ReplaceSortOrderBuilder {
	u.fields = append(u.fields, sortOrderColumn{
		name: column, transform: transform,
		direction: direction, nullOrder: nullOrder,
	})

	return u
}

// Apply returns the replacement sort order, including its proposed order ID,
// without modifying the transaction. An equivalent historical order reuses
// its existing ID.
func (u *ReplaceSortOrderBuilder) Apply() (SortOrder, error) {
	if u == nil {
		return SortOrder{}, fmt.Errorf("%w: sort order replacement builder is nil", ErrInvalidMetadata)
	}

	meta, err := u.txn.txnMeta()
	if err != nil {
		return SortOrder{}, err
	}

	return u.buildOrder(meta)
}

func (u *ReplaceSortOrderBuilder) buildOrder(meta *MetadataBuilder) (SortOrder, error) {
	if len(u.fields) == 0 {
		return UnsortedSortOrder, nil
	}

	schema := meta.CurrentSchema()
	fields := make([]SortField, 0, len(u.fields))
	for _, col := range u.fields {
		if col.transform == nil {
			return SortOrder{}, fmt.Errorf("%w: sort field %q has no transform", ErrInvalidTransform, col.name)
		}

		switch col.transform.(type) {
		case iceberg.UnknownTransform:
			return SortOrder{}, fmt.Errorf("%w: cannot add sort field %q with unknown transform %s",
				ErrInvalidTransform, col.name, col.transform)
		}

		var field iceberg.NestedField
		var found bool
		if u.caseSensitive {
			field, found = schema.FindFieldByName(col.name)
		} else {
			field, found = schema.FindFieldByNameCaseInsensitive(col.name)
		}
		if !found {
			if !u.caseSensitive {
				if _, err := iceberg.Reference(col.name).Bind(schema, false); err != nil {
					return SortOrder{}, fmt.Errorf("cannot bind sort field %q: %w", col.name, err)
				}
			}

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

	proposed.orderID = meta.reuseOrCreateNewSortOrderID(&proposed)

	return proposed, nil
}

// Commit stages this replacement in the transaction. It is a no-op if the
// proposed order is already the default.
func (u *ReplaceSortOrderBuilder) Commit() error {
	order, err := u.Apply()
	if err != nil {
		return err
	}

	return u.txn.ReplaceSortOrder(order)
}
