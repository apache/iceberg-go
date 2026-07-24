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
			{SourceIDs: []int{19}, Transform: iceberg.IdentityTransform{}, NullOrder: table.NullsFirst, Direction: table.SortASC},
			{SourceIDs: []int{25}, Transform: iceberg.BucketTransform{NumBuckets: 4}, NullOrder: table.NullsLast, Direction: table.SortDESC},
			{SourceIDs: []int{22}, Transform: iceberg.VoidTransform{}, NullOrder: table.NullsFirst, Direction: table.SortASC},
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

func TestNewSortOrderRejectsNilTransform(t *testing.T) {
	_, err := table.NewSortOrder(1, []table.SortField{{
		SourceIDs: []int{19},
		NullOrder: table.NullsFirst,
		Direction: table.SortASC,
	}})
	require.Error(t, err)
	assert.ErrorIs(t, err, table.ErrInvalidTransform)
	assert.Contains(t, err.Error(), "has no transform")

	_, err = table.NewSortOrder(1, []table.SortField{{
		NullOrder: table.NullsFirst,
		Direction: table.SortASC,
	}})
	require.Error(t, err)
	assert.ErrorIs(t, err, table.ErrInvalidTransform)
	assert.Contains(t, err.Error(), "has no transform")
}

func TestNewSortOrderRejectsInvalidSourceIDs(t *testing.T) {
	for _, tt := range []struct {
		name      string
		sourceIDs []int
	}{
		{
			name:      "missing",
			sourceIDs: nil,
		},
		{
			name:      "empty",
			sourceIDs: []int{},
		},
		{
			name:      "negative",
			sourceIDs: []int{-1},
		},
		{
			name:      "multi arg with negative",
			sourceIDs: []int{1, -1},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := table.NewSortOrder(1, []table.SortField{{
				SourceIDs: tt.sourceIDs,
				Transform: iceberg.IdentityTransform{},
				NullOrder: table.NullsFirst,
				Direction: table.SortASC,
			}})
			require.Error(t, err)
			assert.ErrorIs(t, err, table.ErrInvalidSortSourceID)
		})
	}
}

func TestNewSortOrderRejectsZeroSourceID(t *testing.T) {
	_, err := table.NewSortOrder(1, []table.SortField{{
		SourceIDs: []int{0},
		Transform: iceberg.IdentityTransform{},
		NullOrder: table.NullsFirst,
		Direction: table.SortASC,
	}})
	require.Error(t, err)
	assert.ErrorIs(t, err, table.ErrInvalidSortSourceID)
}

func TestNewSortOrderAcceptsValidTransform(t *testing.T) {
	sortOrder, err := table.NewSortOrder(1, []table.SortField{{
		SourceIDs: []int{19},
		Transform: iceberg.IdentityTransform{},
		NullOrder: table.NullsFirst,
		Direction: table.SortASC,
	}})
	require.NoError(t, err)
	assert.Equal(t, 1, sortOrder.OrderID())
	assert.Equal(t, 1, sortOrder.Len())
}

// Unknown sort transforms are tolerated on write, matching Java (canTransform
// stays true and nothing rejects them).
func TestNewSortOrderAcceptsUnknownTransform(t *testing.T) {
	unknown, err := iceberg.ParseTransform("custom_transform[42]")
	require.NoError(t, err)

	order, err := table.NewSortOrder(1, []table.SortField{{
		SourceIDs: []int{19},
		Transform: unknown,
		NullOrder: table.NullsFirst,
		Direction: table.SortASC,
	}})
	require.NoError(t, err)
	assert.Equal(t, unknown, order.Field(0).Transform)
}

func TestSortOrderCheckCompatibilityWithValidTransform(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 19, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	sortOrder, err := table.NewSortOrder(1, []table.SortField{{
		SourceIDs: []int{19},
		Transform: iceberg.IdentityTransform{},
		NullOrder: table.NullsFirst,
		Direction: table.SortASC,
	}})
	require.NoError(t, err)
	require.NoError(t, sortOrder.CheckCompatibility(schema))
}

func TestSortOrderUnmarshalRejectsZeroSourceID(t *testing.T) {
	var sortOrder table.SortOrder
	err := json.Unmarshal([]byte(`{"order-id": 1, "fields": [{"source-id": 0, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}]}`), &sortOrder)
	require.Error(t, err)
	assert.ErrorIs(t, err, table.ErrInvalidSortSourceID)
	assert.ErrorContains(t, err, "source ID must be positive: 0")
}

func TestSortOrderCheckCompatibilityRejectsMissingSourceIDInSchema(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 19, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	var sortOrder table.SortOrder
	require.NoError(t, json.Unmarshal([]byte(`{"order-id": 1, "fields": [{"source-ids": [19, 999], "transform": "identity", "direction": "asc", "null-order": "nulls-first"}]}`), &sortOrder))

	err := sortOrder.CheckCompatibility(schema)
	require.Error(t, err)
	assert.ErrorContains(t, err, "sort field with source id 999 not found in schema")
}

func TestUnmarshalSortOrderDefaults(t *testing.T) {
	var order table.SortOrder
	require.NoError(t, json.Unmarshal([]byte(`{"fields": []}`), &order))
	assert.Equal(t, table.UnsortedSortOrder, order)

	require.NoError(t, json.Unmarshal([]byte(`{"fields": [{"source-id": 19, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}]}`), &order))
	assert.Equal(t, table.InitialSortOrderID, order.OrderID())
}

func TestUnmarshalSortOrderRejectsInvalidSourceIDs(t *testing.T) {
	for _, tt := range []struct {
		name     string
		jsonData string
		wantErr  string
	}{
		{
			name:     "missing",
			jsonData: `{"order-id": 1, "fields": [{"transform": "identity", "direction": "asc", "null-order": "nulls-first"}]}`,
			wantErr:  "exactly one of source-id or source-ids is required",
		},
		{
			name:     "zero source-id",
			jsonData: `{"order-id": 1, "fields": [{"source-id": 0, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}]}`,
			wantErr:  "source ID must be positive: 0",
		},
		{
			name:     "negative",
			jsonData: `{"order-id": 1, "fields": [{"source-id": -1, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}]}`,
			wantErr:  "source ID must be positive: -1",
		},
		{
			name:     "empty source-ids",
			jsonData: `{"order-id": 1, "fields": [{"source-ids": [], "transform": "identity", "direction": "asc", "null-order": "nulls-first"}]}`,
			wantErr:  "source-ids must not be empty",
		},
		{
			name:     "source-ids with zero",
			jsonData: `{"order-id": 1, "fields": [{"source-ids": [1, 0], "transform": "identity", "direction": "asc", "null-order": "nulls-first"}]}`,
			wantErr:  "source ID must be positive: 0",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var order table.SortOrder
			err := json.Unmarshal([]byte(tt.jsonData), &order)
			require.Error(t, err)
			assert.ErrorIs(t, err, table.ErrInvalidSortSourceID)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
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

// v3 readers must load sort orders that use unknown transforms, preserving
// them for round-trip rather than failing.
func TestUnmarshalUnknownSortTransform(t *testing.T) {
	j := `{
		"order-id": 22,
		"fields": [
			{"source-id": 19, "transform": "foobar", "direction": "asc", "null-order": "nulls-first"},
			{"source-id": 25, "transform": "bucket[4]", "direction": "desc", "null-order": "nulls-last"},
			{"source-id": 22, "transform": "void", "direction": "asc", "null-order": "nulls-first"}
		]
	}`

	var order table.SortOrder
	err := json.Unmarshal([]byte(j), &order)
	require.NoError(t, err)
	require.Equal(t, 3, order.Len())

	first := order.Field(0)
	_, ok := first.Transform.(iceberg.UnknownTransform)
	assert.True(t, ok, "unknown transform should parse to UnknownTransform")
	assert.Equal(t, "foobar", first.Transform.String())
}

// A malformed known sort transform still errors.
func TestUnmarshalInvalidSortTransform(t *testing.T) {
	badJson := `{
		"order-id": 22,
		"fields": [
			{"source-id": 25, "transform": "bucket[0]", "direction": "desc", "null-order": "nulls-last"}
		]
	}`

	var order table.SortOrder
	err := json.Unmarshal([]byte(badJson), &order)
	assert.ErrorIs(t, err, iceberg.ErrInvalidTransform)
}

func TestSortFieldMultiArgSourceIDs(t *testing.T) {
	t.Run("unmarshal with source-ids", func(t *testing.T) {
		jsonData := `{"source-ids": [2, 3], "transform": "identity", "direction": "asc", "null-order": "nulls-first"}`
		var field table.SortField
		err := json.Unmarshal([]byte(jsonData), &field)
		require.NoError(t, err)
		assert.Equal(t, 2, field.SourceID())
		assert.Equal(t, []int{2, 3}, field.SourceIDs)
	})

	t.Run("unmarshal with both source-id and source-ids errors", func(t *testing.T) {
		jsonData := `{"source-id": 1, "source-ids": [2], "transform": "identity", "direction": "asc", "null-order": "nulls-first"}`
		var field table.SortField
		err := json.Unmarshal([]byte(jsonData), &field)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot contain both source-id and source-ids")
	})

	t.Run("unmarshal rejects missing/invalid source ids", func(t *testing.T) {
		for _, tt := range []struct {
			name     string
			jsonData string
			wantErr  string
		}{
			{
				name:     "missing",
				jsonData: `{"transform": "identity", "direction": "asc", "null-order": "nulls-first"}`,
				wantErr:  "exactly one of source-id or source-ids is required",
			},
			{
				name:     "zero source-id",
				jsonData: `{"source-id": 0, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}`,
				wantErr:  "source ID must be positive: 0",
			},
			{
				name:     "negative source-id",
				jsonData: `{"source-id": -1, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}`,
				wantErr:  "source ID must be positive: -1",
			},
			{
				name:     "empty source-ids",
				jsonData: `{"source-ids": [], "transform": "identity", "direction": "asc", "null-order": "nulls-first"}`,
				wantErr:  "source-ids must not be empty",
			},
			{
				name:     "source-ids with zero",
				jsonData: `{"source-ids": [1, 0], "transform": "identity", "direction": "asc", "null-order": "nulls-first"}`,
				wantErr:  "source ID must be positive: 0",
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				var field table.SortField
				err := json.Unmarshal([]byte(tt.jsonData), &field)
				require.Error(t, err)
				assert.ErrorIs(t, err, table.ErrInvalidSortSourceID)
				assert.ErrorContains(t, err, tt.wantErr)
			})
		}
	})

	t.Run("marshal multi-arg round-trip", func(t *testing.T) {
		field := table.SortField{
			SourceIDs: []int{2, 3},
			Transform: iceberg.IdentityTransform{},
			Direction: table.SortASC,
			NullOrder: table.NullsFirst,
		}
		data, err := json.Marshal(&field)
		require.NoError(t, err)
		assert.Contains(t, string(data), `"source-ids"`)
		assert.NotContains(t, string(data), `"source-id"`)

		var decoded table.SortField
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)
		assert.Equal(t, 2, decoded.SourceID())
		assert.Equal(t, []int{2, 3}, decoded.SourceIDs)
	})

	t.Run("marshal single-arg uses source-id", func(t *testing.T) {
		field := table.SortField{
			SourceIDs: []int{1},
			Transform: iceberg.IdentityTransform{},
			Direction: table.SortASC,
			NullOrder: table.NullsFirst,
		}
		data, err := json.Marshal(&field)
		require.NoError(t, err)
		assert.Contains(t, string(data), `"source-id"`)
		assert.NotContains(t, string(data), `"source-ids"`)
	})
}

func TestSortFieldMarshalAppliesDefaultsWithoutMutatingReceiver(t *testing.T) {
	field := table.SortField{
		SourceIDs: []int{1},
		Transform: iceberg.IdentityTransform{},
	}
	expected := `{
		"source-id": 1,
		"transform": "identity",
		"direction": "asc",
		"null-order": "nulls-first"
	}`

	for _, tt := range []struct {
		name  string
		input any
	}{
		{"value", field},
		{"pointer", &field},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.input)
			require.NoError(t, err)
			assert.JSONEq(t, expected, string(data))
		})
	}

	assert.Empty(t, field.Direction)
	assert.Empty(t, field.NullOrder)
}
