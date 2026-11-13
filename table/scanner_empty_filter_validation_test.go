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
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanFilesValidatesRowFilterWithoutSnapshot(t *testing.T) {
	tbl := newRowFilterValidationTable(t, newRowFilterValidationMetadata(t))
	filter := iceberg.EqualTo(iceberg.Reference("missing"), int64(1))

	tasks, err := tbl.Scan(WithRowFilter(filter)).PlanFiles(t.Context())

	require.Error(t, err)
	assert.ErrorContains(t, err, "missing")
	assert.Empty(t, tasks)
}

func TestPlanFilesValidatesRowFilterWithEmptyManifestList(t *testing.T) {
	metadata := newRowFilterValidationMetadata(t)
	emptySnapshotMetadata, snapshotID, _ := addEmptyManifestListSnapshot(t, metadata, nil)
	tbl := newRowFilterValidationTable(t, emptySnapshotMetadata)
	filter := iceberg.EqualTo(iceberg.Reference("missing"), int64(1))

	tasks, err := tbl.Scan(WithRowFilter(filter)).PlanFiles(t.Context())

	require.Error(t, err)
	assert.ErrorContains(t, err, "missing")
	assert.Empty(t, tasks)
	assert.Equal(t, snapshotID, tbl.CurrentSnapshot().SnapshotID)
}

func TestPlanFilesValidatesRowFilterWithSnapshotAsOf(t *testing.T) {
	metadata := newRowFilterValidationMetadata(t)
	emptySnapshotMetadata, _, timestampMs := addEmptyManifestListSnapshot(t, metadata, nil)
	tbl := newRowFilterValidationTable(t, emptySnapshotMetadata)
	filter := iceberg.EqualTo(iceberg.Reference("missing"), int64(1))

	tasks, err := tbl.Scan(WithSnapshotAsOf(timestampMs), WithRowFilter(filter)).PlanFiles(t.Context())

	require.Error(t, err)
	assert.ErrorContains(t, err, "missing")
	assert.Empty(t, tasks)
}

func TestPlanFilesValidatesRowFilterAgainstSnapshotSchema(t *testing.T) {
	oldSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	metadata, err := NewMetadata(oldSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder,
		"s3://test-bucket/row-filter-validation", nil)
	require.NoError(t, err)

	builder, err := MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)
	currentSchema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "added", Type: iceberg.PrimitiveTypes.Int64},
	)
	require.NoError(t, builder.AddSchema(currentSchema))
	require.NoError(t, builder.SetCurrentSchemaID(currentSchema.ID))
	metadata, err = builder.Build()
	require.NoError(t, err)

	oldSchemaID := oldSchema.ID
	metadata, snapshotID, _ := addEmptyManifestListSnapshot(t, metadata, &oldSchemaID)
	tbl := newRowFilterValidationTable(t, metadata)
	filter := iceberg.EqualTo(iceberg.Reference("added"), int64(1))

	tasks, err := tbl.Scan(WithSnapshotID(snapshotID), WithRowFilter(filter)).PlanFiles(t.Context())

	require.Error(t, err)
	assert.ErrorContains(t, err, "added")
	assert.Empty(t, tasks)
}

func TestPlanFilesAcceptsValidRowFilterOnEmptyScans(t *testing.T) {
	metadata := newRowFilterValidationMetadata(t)
	emptySnapshotMetadata, _, _ := addEmptyManifestListSnapshot(t, metadata, nil)
	validFilter := iceberg.EqualTo(iceberg.Reference("id"), int64(1))

	for _, tc := range []struct {
		name     string
		metadata Metadata
	}{
		{name: "no snapshot", metadata: metadata},
		{name: "empty manifest list", metadata: emptySnapshotMetadata},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl := newRowFilterValidationTable(t, tc.metadata)

			tasks, err := tbl.Scan(WithRowFilter(validFilter)).PlanFiles(t.Context())

			require.NoError(t, err)
			assert.Empty(t, tasks)
		})
	}
}

func TestPlanFilesValidatesRowFilterBeforeRemotePlanning(t *testing.T) {
	metadata := newRowFilterValidationMetadata(t)
	planner := &fakeScanPlanner{supports: true}
	scan := (&Table{metadata: metadata}).Scan(
		WithScanPlanningMode(ScanPlanningRemote),
		WithRowFilter(iceberg.EqualTo(iceberg.Reference("missing"), int64(1))),
	)
	scan.planner = planner

	tasks, err := scan.PlanFiles(t.Context())

	require.Error(t, err)
	assert.ErrorContains(t, err, "missing")
	assert.Empty(t, tasks)
	assert.False(t, planner.called, "remote planning should not receive an invalid row filter")
}

func newRowFilterValidationMetadata(t *testing.T) Metadata {
	t.Helper()

	metadata, err := NewMetadata(
		iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		),
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"s3://test-bucket/row-filter-validation",
		nil,
	)
	require.NoError(t, err)

	return metadata
}

func addEmptyManifestListSnapshot(
	t *testing.T,
	metadata Metadata,
	schemaID *int,
) (Metadata, int64, int64) {
	t.Helper()

	const snapshotID = int64(1)
	timestampMs := metadata.LastUpdatedMillis() + 1
	builder, err := MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID:     snapshotID,
		SequenceNumber: 1,
		TimestampMs:    timestampMs,
		ManifestList:   "",
		Summary:        &Summary{Operation: OpAppend},
		SchemaID:       schemaID,
	}))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, snapshotID, BranchRef))

	metadata, err = builder.Build()
	require.NoError(t, err)

	return metadata, snapshotID, timestampMs
}

func newRowFilterValidationTable(t *testing.T, metadata Metadata) *Table {
	t.Helper()

	return New(Identifier{"db", "row-filter-validation"}, metadata,
		"s3://test-bucket/row-filter-validation/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, nil)
}
