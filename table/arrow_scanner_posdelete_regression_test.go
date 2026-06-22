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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProcessPositionalDeletesAcrossBatches is the regression net for the
// positional-delete index bug: processPositionalDeletes applies deletes one Arrow
// batch at a time, but the surviving-row indices must index into the *current
// batch*, not into the whole file. combinePositionalDeletes therefore has to
// rebase the global file positions [start, end) to batch-local coordinates.
//
// Before the rebase, the second and later batches of a single data file passed
// indices >= the batch length into compute.Take and the scan failed with
// "index error: N out of bounds" (N being a multiple of the parquet batch size,
// e.g. 131072). This test feeds two consecutive batches with a delete located in
// the *second* batch — exactly the case the old code got wrong.
func TestProcessPositionalDeletesAcrossBatches(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "val", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	// Two batches: global positions 0,1,2 then 3,4. Delete global position 3
	// (the first row of the SECOND batch). The survivors are 0,1,2 and 4.
	batches := []arrow.RecordBatch{
		mustLoadRecordBatchFromJSON(schema, `[{"val": 10}, {"val": 11}, {"val": 12}]`),
		mustLoadRecordBatchFromJSON(schema, `[{"val": 13}, {"val": 14}]`),
	}
	expected := []string{
		`[{"val":10},{"val":11},{"val":12}]`,
		`[{"val":14}]`,
	}

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	defer mem.AssertSize(t, 0)

	// processPositionalDeletes owns (releases) each input batch it is handed.
	deletes := set[int64]{3: {}}
	processFn := processPositionalDeletes(ctx, deletes, (&rowPositionSource{}).cursor())

	for i, b := range batches {
		out, err := processFn(b)
		require.NoErrorf(t, err, "batch %d must not return an out-of-bounds error", i)

		gotJSON, err := out.MarshalJSON()
		require.NoError(t, err)
		assert.JSONEq(t, expected[i], string(gotJSON))

		out.Release()
	}
}
