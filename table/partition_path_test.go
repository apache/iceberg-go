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
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionPathPlanMatchesPartitionToPath(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "created_ts_tz", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
		iceberg.NestedField{ID: 2, Name: "str", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	unknown, err := iceberg.ParseTransform("custom_transform[42]")
	require.NoError(t, err)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000,
			Transform: iceberg.IdentityTransform{}, Name: "created ts/tz",
		},
		iceberg.PartitionField{
			SourceIDs: []int{2}, FieldID: 1001,
			Transform: iceberg.TruncateTransform{Width: 8}, Name: "str#part",
		},
		iceberg.PartitionField{
			SourceIDs: []int{3}, FieldID: 1002,
			Transform: iceberg.BucketTransform{NumBuckets: 16}, Name: "id_bucket",
		},
		iceberg.PartitionField{
			SourceIDs: []int{4}, FieldID: 1003,
			Transform: iceberg.IdentityTransform{}, Name: "missing",
		},
		iceberg.PartitionField{
			SourceIDs: []int{3}, FieldID: 1004,
			Transform: unknown, Name: "unknown",
		},
	)

	recordSchema := arrow.NewSchema([]arrow.Field{
		{Name: "created_ts_tz", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
		{Name: "str", Type: arrow.BinaryTypes.String},
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)
	plan, err := newPartitionExtractionPlan(spec, schema, recordSchema)
	require.NoError(t, err)

	record := partitionRecord{
		iceberg.Timestamp(1705314600000000),
		"partition/value",
		int32(7),
		nil,
		int32(42),
	}

	assert.Equal(t, spec.PartitionToPath(record, schema), plan.pathPlan.format(record))
	assert.Equal(t,
		"created+ts%2Ftz=2024-01-15T10%3A30%3A00%2B00%3A00/str%23part=partition%2Fvalue/id_bucket=7/missing=null/unknown=42",
		plan.pathPlan.format(record))
}
