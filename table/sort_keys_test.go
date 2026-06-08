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
	"testing"

	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveSortKeys_Unsorted(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	keys, err := resolveSortKeys(UnsortedSortOrder, schema)
	require.NoError(t, err)
	assert.Nil(t, keys, "unsorted order must produce no sort keys")
}

func TestResolveSortKeys_DirectionAndNullOrder(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "a", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 2, Name: "b", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	order, err := NewSortOrder(1, []SortField{
		{SourceIDs: []int{2}, Transform: iceberg.IdentityTransform{}, Direction: SortDESC, NullOrder: NullsFirst},
		{SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{}, Direction: SortASC, NullOrder: NullsLast},
	})
	require.NoError(t, err)

	keys, err := resolveSortKeys(order, schema)
	require.NoError(t, err)
	require.Len(t, keys, 2)

	assert.Equal(t, 1, keys[0].ColumnIndex, "first key should target column index 1 (source id 2)")
	assert.Equal(t, compute.SortOrderDescending, keys[0].Order)
	assert.Equal(t, compute.SortNullsAtStart, keys[0].NullPlacement)

	assert.Equal(t, 0, keys[1].ColumnIndex, "second key should target column index 0 (source id 1)")
	assert.Equal(t, compute.SortOrderAscending, keys[1].Order)
	assert.Equal(t, compute.SortNullsAtEnd, keys[1].NullPlacement)
}

func TestResolveSortKeys_MissingSourceID(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "a", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	order, err := NewSortOrder(1, []SortField{
		{SourceIDs: []int{99}, Transform: iceberg.IdentityTransform{}, Direction: SortASC, NullOrder: NullsFirst},
	})
	require.NoError(t, err)

	_, err = resolveSortKeys(order, schema)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "source id 99")
}
