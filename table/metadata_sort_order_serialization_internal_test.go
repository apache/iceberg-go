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
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestSetDefaultSortOrderIDReplayAfterReuse(t *testing.T) {
	base, err := ParseMetadataString(ExampleTableMetadataV2)
	require.NoError(t, err)

	for _, tc := range []struct {
		name       string
		reuseFirst bool
	}{
		{name: "older staged order", reuseFirst: true},
		{name: "last emitted order", reuseFirst: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder, err := MetadataBuilderFromBase(base, "")
			require.NoError(t, err)

			first, err := NewSortOrder(InitialSortOrderID, []SortField{{
				SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{},
				Direction: SortASC, NullOrder: NullsFirst,
			}})
			require.NoError(t, err)
			second, err := NewSortOrder(InitialSortOrderID, []SortField{{
				SourceIDs: []int{2}, Transform: iceberg.IdentityTransform{},
				Direction: SortDESC, NullOrder: NullsLast,
			}})
			require.NoError(t, err)

			require.NoError(t, builder.AddSortOrder(&first))
			require.NoError(t, builder.AddSortOrder(&second))
			require.NotEqual(t, first.OrderID(), second.OrderID())

			reused := &second
			if tc.reuseFirst {
				reused = &first
			}
			require.NoError(t, builder.AddSortOrder(reused))
			require.NoError(t, builder.SetDefaultSortOrderID(-1))

			built, err := builder.Build()
			require.NoError(t, err)
			require.Equal(t, reused.OrderID(), built.DefaultSortOrder())
			require.Len(t, built.SortOrders(), len(base.SortOrders())+2)

			// -1 on the wire refers to the last add-sort-order update, not
			// the latest AddSortOrder call. An older reused order needs its ID.
			wantWireID := -1
			if tc.reuseFirst {
				wantWireID = first.OrderID()
			}
			lastUpdate, err := json.Marshal(builder.updates[len(builder.updates)-1])
			require.NoError(t, err)
			require.JSONEq(t,
				fmt.Sprintf(`{"action":"set-default-sort-order","sort-order-id":%d}`, wantWireID),
				string(lastUpdate))

			// The commit's serialized updates must produce the staged default
			// when applied to another metadata builder, as a catalog does.
			replayed, err := MetadataBuilderFromBase(base, "")
			require.NoError(t, err)
			for _, update := range builder.updates {
				require.NoError(t, update.Apply(replayed))
			}
			replayedMeta, err := replayed.Build()
			require.NoError(t, err)
			require.Equal(t, built.DefaultSortOrder(), replayedMeta.DefaultSortOrder())
		})
	}
}

func TestLegacyBuiltInSortOrderWithExtraSourcesSurvivesMetadataCommit(t *testing.T) {
	for _, tc := range []struct {
		name         string
		defaultOrder bool
	}{
		{name: "historical non-default order"},
		{name: "default order", defaultOrder: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(ExampleTableMetadataV2), &raw))

			var orders []json.RawMessage
			require.NoError(t, json.Unmarshal(raw["sort-orders"], &orders))
			legacyOrder := json.RawMessage(`{"order-id":9,"fields":[{"source-ids":[2,3],"transform":"identity","direction":"asc","null-order":"nulls-first"}]}`)
			orders = append(orders, legacyOrder)
			var err error
			raw["sort-orders"], err = json.Marshal(orders)
			require.NoError(t, err)
			if tc.defaultOrder {
				raw["default-sort-order-id"] = json.RawMessage(`9`)
			}

			encoded, err := json.Marshal(raw)
			require.NoError(t, err)
			metadata, err := ParseMetadataBytes(encoded)
			require.NoError(t, err)
			require.Len(t, metadata.SortOrders(), 2)

			builder, err := MetadataBuilderFromBase(metadata, "")
			require.NoError(t, err)
			require.NoError(t, builder.SetProperties(iceberg.Properties{"test.sort-order-compatibility": "true"}))
			updated, err := builder.Build()
			require.NoError(t, err)
			serialized, err := json.Marshal(updated)
			require.NoError(t, err)
			require.Contains(t, string(serialized), `"source-ids":[2,3]`)

			reloaded, err := ParseMetadataBytes(serialized)
			require.NoError(t, err)
			var found bool
			for _, order := range reloaded.SortOrders() {
				if order.OrderID() == 9 {
					require.Equal(t, []int{2, 3}, order.Field(0).SourceIDs)
					found = true
				}
			}
			require.True(t, found, "legacy sort order should survive the metadata update")
		})
	}
}
