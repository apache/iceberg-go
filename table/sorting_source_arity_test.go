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
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestSingleArgumentSortTransformsRejectMultipleSources(t *testing.T) {
	transforms := []iceberg.Transform{
		iceberg.IdentityTransform{},
		iceberg.VoidTransform{},
		iceberg.BucketTransform{NumBuckets: 8},
		iceberg.TruncateTransform{Width: 3},
		iceberg.YearTransform{},
		iceberg.MonthTransform{},
		iceberg.DayTransform{},
		iceberg.HourTransform{},
		&iceberg.DayTransform{},
	}

	for _, transform := range transforms {
		t.Run(fmt.Sprintf("%T", transform), func(t *testing.T) {
			field := table.SortField{
				SourceIDs: []int{1, 2},
				Transform: transform,
				Direction: table.SortASC,
				NullOrder: table.NullsFirst,
			}

			_, err := table.NewSortOrder(1, []table.SortField{field})
			require.ErrorIs(t, err, table.ErrInvalidSortSourceID)
			require.ErrorContains(t, err, "requires a single source ID")

			// Historical sort fields may have been written with extra source IDs.
			// Reading and re-serializing them must not make table metadata unusable.
			encodedField, err := json.Marshal(field)
			require.NoError(t, err)
			require.Contains(t, string(encodedField), `"source-ids":[1,2]`)

			fieldJSON := fmt.Sprintf(`{"source-ids":[1,2],"transform":%q,"direction":"asc","null-order":"nulls-first"}`, transform.String())
			var decodedField table.SortField
			require.NoError(t, json.Unmarshal([]byte(fieldJSON), &decodedField))
			require.Equal(t, []int{1, 2}, decodedField.SourceIDs)

			orderJSON := []byte(`{"order-id":1,"fields":[` + fieldJSON + `]}`)
			var decodedOrder table.SortOrder
			require.NoError(t, json.Unmarshal(orderJSON, &decodedOrder))
			require.Equal(t, []int{1, 2}, decodedOrder.Field(0).SourceIDs)
			_, err = json.Marshal(decodedOrder)
			require.NoError(t, err)

			// Incoming unbound create-table and add-sort-order payloads are strict.
			var unbound table.UnboundSortOrder
			err = json.Unmarshal(orderJSON, &unbound)
			require.ErrorIs(t, err, table.ErrInvalidSortSourceID)

			var updates table.Updates
			err = json.Unmarshal([]byte(`[{"action":"add-sort-order","sort-order":`+string(orderJSON)+`}]`), &updates)
			require.ErrorIs(t, err, table.ErrInvalidSortSourceID)
		})
	}
}

func TestUnknownMultiSourceSortTransformRoundTrip(t *testing.T) {
	unknown, err := iceberg.ParseTransform("future_multi")
	require.NoError(t, err)

	order, err := table.NewSortOrder(1, []table.SortField{{
		SourceIDs: []int{1, 2},
		Transform: unknown,
		Direction: table.SortASC,
		NullOrder: table.NullsFirst,
	}})
	require.NoError(t, err)

	encoded, err := json.Marshal(order)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"source-ids":[1,2]`)

	var decoded table.SortOrder
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Equal(t, []int{1, 2}, decoded.Field(0).SourceIDs)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "first", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "second", Type: iceberg.PrimitiveTypes.String},
	)
	require.NoError(t, decoded.CheckCompatibility(schema))
}
