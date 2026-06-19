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
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestScanRowLineagePreservedAcrossPrunedRowGroups: a filter that prunes a
// leading row group must not renumber survivors' _row_id (= first_row_id +
// ORIGINAL position). Two row groups (ids 1..5, 6..10); id > 5 prunes the first
// via stats, and the unfiltered baseline spans both, exercising multiple batches.
func TestScanRowLineagePreservedAcrossPrunedRowGroups(t *testing.T) {
	ctx := context.Background()
	tbl := newV3RowLineageTestTable(t)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetProperties(iceberg.Properties{table.ParquetRowGroupLimitKey: "5"}))
	tbl, err := tx.Commit(ctx)
	require.NoError(t, err)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},{"id":3,"data":"c"},{"id":4,"data":"d"},` +
			`{"id":5,"data":"e"},{"id":6,"data":"f"},{"id":7,"data":"g"},{"id":8,"data":"h"},` +
			`{"id":9,"data":"i"},{"id":10,"data":"j"}]`,
	})
	require.NoError(t, err)
	defer data.Release()

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	full := rowIDByID(t, tbl)
	require.Equal(t, map[int64]int64{1: 0, 2: 1, 3: 2, 4: 3, 5: 4, 6: 5, 7: 6, 8: 7, 9: 8, 10: 9}, full)

	got := rowIDByID(t, tbl,
		table.WithRowFilter(iceberg.GreaterThan(iceberg.Reference("id"), int64(5))))

	assert.Equal(t, map[int64]int64{6: 5, 7: 6, 8: 7, 9: 8, 10: 9}, got,
		"a pruned leading row group must not renumber surviving rows' _row_id")
}
