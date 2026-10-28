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
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSortedCompactionRecords_NestedAndNulls sorts rows spread over several
// batches, with nulls in the sort key and a struct column (which Arrow's
// chunked Take cannot gather without concatenating) carried along.
func TestSortedCompactionRecords_NestedAndNulls(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "s", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 3, Name: "x", Type: iceberg.PrimitiveTypes.String},
		}}},
	)
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "s", Type: arrow.StructOf(arrow.Field{Name: "x", Type: arrow.BinaryTypes.String, Nullable: true}), Nullable: true},
	}, nil)

	inputs := []string{
		`[{"id": 4, "s": {"x": "d"}}, {"id": null, "s": {"x": "n"}}, {"id": 1, "s": {"x": "a"}}]`,
		`[{"id": 3, "s": {"x": "c"}}]`,
		`[{"id": 5, "s": null}, {"id": 2, "s": {"x": "b"}}]`,
	}
	records := func(yield func(arrow.RecordBatch, error) bool) {
		for _, in := range inputs {
			rec, _, err := array.RecordFromJSON(mem, sc, strings.NewReader(in))
			if err != nil {
				yield(nil, err)

				return
			}
			if !yield(rec, nil) {
				return
			}
		}
	}

	order, err := NewSortOrder(1, []SortField{{
		SourceIDs: []int{1}, Transform: iceberg.IdentityTransform{},
		Direction: SortASC, NullOrder: NullsFirst,
	}})
	require.NoError(t, err)

	// mem tracks only the input batches: arrow-go's sort_indices kernel does
	// not release its scratch buffers, so compute allocations stay untracked.
	ctx := t.Context()
	var got []map[string]any
	var sizes []int64
	for rec, err := range sortedCompactionRecords(ctx, *iceberg.UnpartitionedSpec, schema, order, 4, records) {
		require.NoError(t, err)
		sizes = append(sizes, rec.NumRows())
		b, err := rec.MarshalJSON()
		rec.Release()
		require.NoError(t, err)
		var rows []map[string]any
		require.NoError(t, json.Unmarshal(b, &rows))
		got = append(got, rows...)
	}

	assert.Equal(t, []int64{4, 2}, sizes)
	want := []map[string]any{
		{"id": nil, "s": map[string]any{"x": "n"}},
		{"id": float64(1), "s": map[string]any{"x": "a"}},
		{"id": float64(2), "s": map[string]any{"x": "b"}},
		{"id": float64(3), "s": map[string]any{"x": "c"}},
		{"id": float64(4), "s": map[string]any{"x": "d"}},
		{"id": float64(5), "s": nil},
	}
	assert.Equal(t, want, got)
}
