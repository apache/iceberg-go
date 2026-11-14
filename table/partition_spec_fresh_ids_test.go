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
	"github.com/stretchr/testify/require"
)

func TestNewMetadataPreservesMultiSourcePartitionField(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 11, Name: "left", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 22, Name: "right", Type: iceberg.PrimitiveTypes.Int64},
	)
	transform, err := iceberg.ParseTransform("custom_two_arg")
	require.NoError(t, err)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{22, 11}, FieldID: 1000, Name: "combined", Transform: transform,
	})

	metadata, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder,
		"s3://bucket/table", iceberg.Properties{"format-version": "3"})
	require.NoError(t, err)
	left, ok := metadata.CurrentSchema().FindFieldByName("left")
	require.True(t, ok)
	right, ok := metadata.CurrentSchema().FindFieldByName("right")
	require.True(t, ok)

	freshSpec := metadata.PartitionSpec()
	field := freshSpec.Field(0)
	require.Equal(t, []int{right.ID, left.ID}, field.SourceIDs)

	encoded, err := json.Marshal(freshSpec)
	require.NoError(t, err)
	var serialized struct {
		Fields []iceberg.PartitionField `json:"fields"`
	}
	require.NoError(t, json.Unmarshal(encoded, &serialized))
	require.Len(t, serialized.Fields, 1)
	require.Equal(t, field.SourceIDs, serialized.Fields[0].SourceIDs)
}

func TestNewMetadataRejectsMissingSecondaryPartitionSource(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 11, Name: "left", Type: iceberg.PrimitiveTypes.Int32},
	)
	transform, err := iceberg.ParseTransform("custom_two_arg")
	require.NoError(t, err)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{11, 22}, FieldID: 1000, Name: "combined", Transform: transform,
	})

	_, err = table.NewMetadata(schema, &spec, table.UnsortedSortOrder,
		"s3://bucket/table", iceberg.Properties{"format-version": "3"})
	require.ErrorIs(t, err, table.ErrInvalidMetadata)
	require.ErrorContains(t, err, "partition source field 22 not found in schema")
}
