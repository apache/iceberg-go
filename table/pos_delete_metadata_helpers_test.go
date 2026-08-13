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
	"github.com/stretchr/testify/require"
)

// addUnsortedSortOrder gives a metadata builder the unsorted default order
// so v2+ "default sort order id must be set" validation passes. Position-
// delete writes in these tests do not depend on sort order.
func addUnsortedSortOrder(t *testing.T, mb *MetadataBuilder) {
	t.Helper()
	so := UnsortedSortOrder
	require.NoError(t, mb.AddSortOrder(&so))
	require.NoError(t, mb.SetDefaultSortOrderID(0))
}

func clonePositionalDeleteSchema() *iceberg.Schema {
	return iceberg.NewSchema(iceberg.PositionalDeleteSchema.ID, iceberg.PositionalDeleteSchema.Fields()...)
}

func newPositionDeleteUnpartitionedMetadata(t *testing.T, formatVersion int) *MetadataBuilder {
	t.Helper()
	mb, err := NewMetadataBuilder(formatVersion)
	require.NoError(t, err)
	require.NoError(t, mb.AddSchema(clonePositionalDeleteSchema()))
	require.NoError(t, mb.SetCurrentSchemaID(0))
	require.NoError(t, mb.AddPartitionSpec(iceberg.UnpartitionedSpec, true))
	require.NoError(t, mb.SetDefaultSpecID(0))
	require.NoError(t, mb.SetLoc("file:///pos-delete-test"))
	addUnsortedSortOrder(t, mb)

	return mb
}

func newPositionDeletePartitionedMetadata(t *testing.T, formatVersion int) *MetadataBuilder {
	t.Helper()
	mb, err := NewMetadataBuilder(formatVersion)
	require.NoError(t, err)
	require.NoError(t, mb.AddSchema(iceberg.NewSchema(0,
		append(iceberg.PositionalDeleteSchema.Fields(),
			iceberg.NestedField{Name: "age", ID: 2, Type: iceberg.Int64Type{}})...)))
	require.NoError(t, mb.SetCurrentSchemaID(0))
	partitionSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2},
		Name:      "age_bucket",
		Transform: iceberg.BucketTransform{NumBuckets: 2},
	})
	require.NoError(t, mb.AddPartitionSpec(&partitionSpec, true))
	require.NoError(t, mb.SetDefaultSpecID(0))
	require.NoError(t, mb.SetLoc("file:///pos-delete-test-partitioned"))
	addUnsortedSortOrder(t, mb)

	return mb
}

// unpartitionedContexts builds the partitionContextByFilePath map that
// positionDeleteRecordsToDataFiles requires on the v3+ DV path. Production
// callers populate the map from each data file's own (SpecID, Partition);
// tests on unpartitioned tables construct only the data file paths, so this
// helper saves the boilerplate of writing {specID: 0, partitionData: nil}
// per path in every test.
func unpartitionedContexts(paths ...string) map[string]partitionContext {
	m := make(map[string]partitionContext, len(paths))
	for _, p := range paths {
		m[p] = partitionContext{specID: 0, partitionData: nil}
	}

	return m
}
