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

package table_test

import (
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestAssignFreshSortOrderIDsPreservesMultipleSources(t *testing.T) {
	old := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 11, Name: "left", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 22, Name: "right", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 33, Name: "priority", Type: iceberg.PrimitiveTypes.Int64},
	)
	fresh := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 3, Name: "left", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 1, Name: "right", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 2, Name: "priority", Type: iceberg.PrimitiveTypes.Int64},
	)
	multiArg, err := iceberg.ParseTransform("custom_two_arg")
	require.NoError(t, err)
	order, err := table.NewSortOrder(4, []table.SortField{
		{
			SourceIDs: []int{11, 22},
			Transform: multiArg,
			Direction: table.SortASC,
			NullOrder: table.NullsFirst,
		},
		{
			SourceIDs: []int{33},
			Transform: iceberg.IdentityTransform{},
			Direction: table.SortDESC,
			NullOrder: table.NullsLast,
		},
	})
	require.NoError(t, err)

	updated, err := table.AssignFreshSortOrderIDsWithID(order, old, fresh, 7)
	require.NoError(t, err)
	require.Equal(t, 7, updated.OrderID())
	require.Equal(t, []int{3, 1}, updated.Field(0).SourceIDs)
	require.Equal(t, []int{2}, updated.Field(1).SourceIDs)
	require.Equal(t, []int{11, 22}, order.Field(0).SourceIDs)
	require.NoError(t, updated.CheckCompatibility(fresh))

	encoded, err := json.Marshal(updated)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"order-id": 7,
		"fields": [
			{"source-ids": [3, 1], "transform": "custom_two_arg", "direction": "asc", "null-order": "nulls-first"},
			{"source-id": 2, "transform": "identity", "direction": "desc", "null-order": "nulls-last"}
		]
	}`, string(encoded))
}

func TestAssignFreshSortOrderIDsRejectsMissingSecondarySource(t *testing.T) {
	multiArg, err := iceberg.ParseTransform("custom_two_arg")
	require.NoError(t, err)
	order, err := table.NewSortOrder(4, []table.SortField{{
		SourceIDs: []int{11, 22},
		Transform: multiArg,
		Direction: table.SortASC,
		NullOrder: table.NullsFirst,
	}})
	require.NoError(t, err)

	old := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 11, Name: "left", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 22, Name: "right", Type: iceberg.PrimitiveTypes.Int32},
	)
	fresh := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "left", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "right", Type: iceberg.PrimitiveTypes.Int32},
	)

	t.Run("old schema", func(t *testing.T) {
		missingOld := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 11, Name: "left", Type: iceberg.PrimitiveTypes.String},
		)
		_, err := table.AssignFreshSortOrderIDs(order, missingOld, fresh)
		require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
		require.ErrorContains(t, err, "source column id 22")
	})

	t.Run("fresh schema", func(t *testing.T) {
		missingFresh := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "left", Type: iceberg.PrimitiveTypes.String},
		)
		_, err := table.AssignFreshSortOrderIDs(order, old, missingFresh)
		require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
		require.ErrorContains(t, err, "cannot find field right")
	})
}

func TestNewMetadataPreservesMultiSourceSortField(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 11, Name: "left", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 21, Name: "right", Type: iceberg.PrimitiveTypes.Int64},
	)
	transform, err := iceberg.ParseTransform("custom_two_arg")
	require.NoError(t, err)
	order, err := table.NewSortOrder(4, []table.SortField{{
		SourceIDs: []int{21, 11},
		Transform: transform,
		Direction: table.SortASC,
		NullOrder: table.NullsFirst,
	}})
	require.NoError(t, err)

	metadata, err := table.NewMetadata(schema, nil, order, "s3://bucket/table", iceberg.Properties{"format-version": "3"})
	require.NoError(t, err)
	left, ok := metadata.CurrentSchema().FindFieldByName("left")
	require.True(t, ok)
	right, ok := metadata.CurrentSchema().FindFieldByName("right")
	require.True(t, ok)
	require.Equal(t, []int{right.ID, left.ID}, metadata.SortOrder().Field(0).SourceIDs)
}
