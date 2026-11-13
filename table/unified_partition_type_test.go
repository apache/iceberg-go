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

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestUnifiedPartitionTypeUsesAllActiveSpecs(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	oldSpec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "region", Transform: iceberg.IdentityTransform{},
	})
	newSpec := iceberg.NewPartitionSpecID(1, iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1001, Name: "category", Transform: iceberg.IdentityTransform{},
	})
	lastPartitionID := 1001
	meta := &metadataV2{commonMetadata: commonMetadata{
		FormatVersion:   2,
		UUID:            uuid.New(),
		LastColumnId:    2,
		SchemaList:      []*iceberg.Schema{schema},
		CurrentSchemaID: 0,
		Specs:           []iceberg.PartitionSpec{oldSpec, newSpec},
		DefaultSpecID:   1,
		LastPartitionID: &lastPartitionID,
		SnapshotRefs:    map[string]SnapshotRef{},
	}}

	partitionType, err := unifiedPartitionType(meta)
	require.NoError(t, err)
	require.Equal(t, []int{1000, 1001}, []int{
		partitionType.Field(0).ID,
		partitionType.Field(1).ID,
	})
	require.Equal(t, []string{"region", "category"}, []string{
		partitionType.Field(0).Name,
		partitionType.Field(1).Name,
	})
}

func TestUnifiedPartitionTypeRejectsIncompatibleFieldReuse(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "first", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "second", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	metadataFor := func(specs []iceberg.PartitionSpec) Metadata {
		lastPartitionID := 1000

		return &metadataV2{commonMetadata: commonMetadata{
			FormatVersion:   2,
			UUID:            uuid.New(),
			LastColumnId:    2,
			SchemaList:      []*iceberg.Schema{schema},
			CurrentSchemaID: 0,
			Specs:           specs,
			DefaultSpecID:   specs[len(specs)-1].ID(),
			LastPartitionID: &lastPartitionID,
		}}
	}
	field := func(specID, sourceID int, transform iceberg.Transform) iceberg.PartitionSpec {
		return iceberg.NewPartitionSpecID(specID, iceberg.PartitionField{
			SourceIDs: []int{sourceID}, FieldID: 1000, Name: "part", Transform: transform,
		})
	}

	tests := []struct {
		name  string
		specs []iceberg.PartitionSpec
		valid bool
	}{
		{
			name:  "void transition is compatible",
			specs: []iceberg.PartitionSpec{field(0, 1, iceberg.IdentityTransform{}), field(1, 1, iceberg.VoidTransform{})},
			valid: true,
		},
		{
			name:  "pointer void transition is compatible",
			specs: []iceberg.PartitionSpec{field(0, 1, iceberg.IdentityTransform{}), field(1, 1, &iceberg.VoidTransform{})},
			valid: true,
		},
		{
			name:  "different source ids",
			specs: []iceberg.PartitionSpec{field(0, 1, iceberg.IdentityTransform{}), field(1, 2, iceberg.IdentityTransform{})},
		},
		{
			name:  "different transforms",
			specs: []iceberg.PartitionSpec{field(0, 1, iceberg.IdentityTransform{}), field(1, 1, iceberg.BucketTransform{NumBuckets: 16})},
		},
		{
			name: "void cannot hide incompatible history",
			specs: []iceberg.PartitionSpec{
				field(0, 1, iceberg.BucketTransform{NumBuckets: 16}),
				field(1, 1, iceberg.IdentityTransform{}),
				field(2, 1, iceberg.VoidTransform{}),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := unifiedPartitionType(metadataFor(tt.specs))
			if tt.valid {
				require.NoError(t, err)

				return
			}
			require.ErrorIs(t, err, iceberg.ErrInvalidPartitionSpec)
		})
	}

	unknown, err := iceberg.ParseTransform("future_transform")
	require.NoError(t, err)
	_, err = unifiedPartitionType(metadataFor([]iceberg.PartitionSpec{field(0, 1, unknown)}))
	require.ErrorIs(t, err, iceberg.ErrInvalidPartitionSpec)
}
