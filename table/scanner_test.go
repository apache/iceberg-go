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

//go:build integration

package table_test

import (
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanner(t *testing.T) {
	cat, err := catalog.NewRestCatalog("rest", "http://localhost:8181")
	require.NoError(t, err)

	props := iceberg.Properties{
		io.S3Region:      "us-east-1",
		io.S3AccessKeyID: "admin", io.S3SecretAccessKey: "password"}

	tests := []struct {
		table            string
		expr             iceberg.BooleanExpression
		expectedNumTasks int
	}{
		{"test_all_types", iceberg.AlwaysTrue{}, 5},
		{"test_all_types", iceberg.LessThan(iceberg.Reference("intCol"), int32(3)), 3},
		{"test_all_types", iceberg.GreaterThanEqual(iceberg.Reference("intCol"), int32(3)), 2},
		{"test_partitioned_by_identity",
			iceberg.GreaterThanEqual(iceberg.Reference("ts"), "2023-03-05T00:00:00+00:00"), 8},
		{"test_partitioned_by_identity",
			iceberg.LessThan(iceberg.Reference("ts"), "2023-03-05T00:00:00+00:00"), 4},
		{"test_partitioned_by_years", iceberg.AlwaysTrue{}, 2},
		{"test_partitioned_by_years", iceberg.LessThan(iceberg.Reference("dt"), "2023-03-05"), 1},
		{"test_partitioned_by_years", iceberg.GreaterThanEqual(iceberg.Reference("dt"), "2023-03-05"), 1},
		{"test_partitioned_by_months", iceberg.GreaterThanEqual(iceberg.Reference("dt"), "2023-03-05"), 1},
		{"test_partitioned_by_days", iceberg.GreaterThanEqual(iceberg.Reference("ts"), "2023-03-05T00:00:00+00:00"), 4},
		{"test_partitioned_by_hours", iceberg.GreaterThanEqual(iceberg.Reference("ts"), "2023-03-05T00:00:00+00:00"), 8},
		{"test_partitioned_by_truncate", iceberg.GreaterThanEqual(iceberg.Reference("letter"), "e"), 8},
		{"test_partitioned_by_bucket", iceberg.GreaterThanEqual(iceberg.Reference("number"), int32(5)), 6},
		{"test_uuid_and_fixed_unpartitioned", iceberg.AlwaysTrue{}, 5},
		{"test_uuid_and_fixed_unpartitioned", iceberg.EqualTo(iceberg.Reference("uuid_col"), "102cb62f-e6f8-4eb0-9973-d9b012ff0967"), 1},
	}

	for _, tt := range tests {
		t.Run(tt.table+" "+tt.expr.String(), func(t *testing.T) {
			ident := catalog.ToRestIdentifier("default", tt.table)

			tbl, err := cat.LoadTable(context.Background(), ident, props)
			require.NoError(t, err)

			scan := tbl.Scan(tt.expr, 0, true, "*")
			tasks, err := scan.PlanFiles(context.Background())
			require.NoError(t, err)

			assert.Len(t, tasks, tt.expectedNumTasks)
		})
	}
}

func TestScannerWithDeletes(t *testing.T) {
	cat, err := catalog.NewRestCatalog("rest", "http://localhost:8181")
	require.NoError(t, err)

	props := iceberg.Properties{
		io.S3Region:      "us-east-1",
		io.S3AccessKeyID: "admin", io.S3SecretAccessKey: "password"}

	ident := catalog.ToRestIdentifier("default", "test_positional_mor_deletes")

	tbl, err := cat.LoadTable(context.Background(), ident, props)
	require.NoError(t, err)

	scan := tbl.Scan(iceberg.AlwaysTrue{}, 0, true, "*")
	tasks, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)

	assert.Len(t, tasks, 1)
	assert.Len(t, tasks[0].DeleteFiles, 1)

	tagScan, err := scan.UseRef("tag_12")
	require.NoError(t, err)

	tasks, err = tagScan.PlanFiles(context.Background())
	require.NoError(t, err)

	assert.Len(t, tasks, 1)
	assert.Len(t, tasks[0].DeleteFiles, 0)

	_, err = tagScan.UseRef("without_5")
	assert.ErrorIs(t, err, iceberg.ErrInvalidArgument)

	tagScan, err = scan.UseRef("without_5")
	require.NoError(t, err)

	tasks, err = tagScan.PlanFiles(context.Background())
	require.NoError(t, err)

	assert.Len(t, tasks, 1)
	assert.Len(t, tasks[0].DeleteFiles, 1)
}
