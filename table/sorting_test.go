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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerializeUnsortedSortOrder(t *testing.T) {
	data, err := json.Marshal(table.UnsortedSortOrder)
	require.NoError(t, err)
	assert.JSONEq(t, `{"order-id": 0, "fields": []}`, string(data))
}

func TestSerializeSortOrder(t *testing.T) {
	sortOrder, err := table.NewSortOrder(
		22,
		[]table.SortField{
			{SourceID: 19, Transform: iceberg.IdentityTransform{}, NullOrder: table.NullsFirst, Direction: table.SortASC},
			{SourceID: 25, Transform: iceberg.BucketTransform{NumBuckets: 4}, NullOrder: table.NullsLast, Direction: table.SortDESC},
			{SourceID: 22, Transform: iceberg.VoidTransform{}, NullOrder: table.NullsFirst, Direction: table.SortASC},
		},
	)
	require.NoError(t, err)
	data, err := json.Marshal(sortOrder)
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"order-id": 22,
		"fields": [
			{"source-id": 19, "transform": "identity", "direction": "asc", "null-order": "nulls-first"},
			{"source-id": 25, "transform": "bucket[4]", "direction": "desc", "null-order": "nulls-last"},
			{"source-id": 22, "transform": "void", "direction": "asc", "null-order": "nulls-first"}
		]
	}`, string(data))
}

func TestUnmarshalSortOrderDefaults(t *testing.T) {
	var order table.SortOrder
	require.NoError(t, json.Unmarshal([]byte(`{"fields": []}`), &order))
	assert.Equal(t, table.UnsortedSortOrder, order)

	require.NoError(t, json.Unmarshal([]byte(`{"fields": [{"source-id": 19, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}]}`), &order))
	assert.Equal(t, table.InitialSortOrderID, order.OrderID())
}

func TestUnmarshalInvalidSortOrderID(t *testing.T) {
	var order table.SortOrder
	require.ErrorContains(t, json.Unmarshal([]byte(`{"order-id": 0, "fields": [{"source-id": 19, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}]}`), &order), "invalid sort order ID: sort order ID 0 is reserved for unsorted order")
}

func TestUnmarshalInvalidSortDirection(t *testing.T) {
	badJson := `{
		"order-id": 22,
		"fields": [
			{"source-id": 19, "transform": "identity", "direction": "foobar", "null-order": "nulls-first"},
			{"source-id": 25, "transform": "bucket[4]", "direction": "desc", "null-order": "nulls-last"},
			{"source-id": 22, "transform": "void", "direction": "asc", "null-order": "nulls-first"}
		]
	}`

	var order table.SortOrder
	err := json.Unmarshal([]byte(badJson), &order)
	assert.ErrorIs(t, err, table.ErrInvalidSortDirection)
}

func TestUnmarshalInvalidSortNullOrder(t *testing.T) {
	badJson := `{
		"order-id": 22,
		"fields": [
			{"source-id": 19, "transform": "identity", "direction": "asc", "null-order": "foobar"},
			{"source-id": 25, "transform": "bucket[4]", "direction": "desc", "null-order": "nulls-last"},
			{"source-id": 22, "transform": "void", "direction": "asc", "null-order": "nulls-first"}
		]
	}`

	var order table.SortOrder
	err := json.Unmarshal([]byte(badJson), &order)
	assert.ErrorIs(t, err, table.ErrInvalidNullOrder)
}

func TestUnmarshalInvalidSortTransform(t *testing.T) {
	badJson := `{
		"order-id": 22,
		"fields": [
			{"source-id": 19, "transform": "foobar", "direction": "asc", "null-order": "nulls-first"},
			{"source-id": 25, "transform": "bucket[4]", "direction": "desc", "null-order": "nulls-last"},
			{"source-id": 22, "transform": "void", "direction": "asc", "null-order": "nulls-first"}
		]
	}`

	var order table.SortOrder
	err := json.Unmarshal([]byte(badJson), &order)
	assert.ErrorIs(t, err, iceberg.ErrInvalidTransform)
}
