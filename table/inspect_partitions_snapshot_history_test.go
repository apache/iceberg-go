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
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestInspectPartitionsUsesHistoricalSnapshotTimestamp(t *testing.T) {
	t.Parallel()

	const millisToMicros = int64(1000)
	const (
		historicalSnapshotID = int64(1)
		currentSnapshotID    = int64(2)
	)

	tbl, historicalTimestamp := inspectPartitionHistoricalSnapshotTable(
		t, historicalSnapshotID, currentSnapshotID,
	)
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })

	rr, err := tbl.Inspect(WithInspectAllocator(mem)).Partitions(t.Context())
	require.NoError(t, err)
	defer rr.Release()

	record := collectRecord(t, rr)
	defer record.Release()
	require.EqualValues(t, 1, record.NumRows())

	updatedAtIndex := record.Schema().FieldIndices("last_updated_at")
	require.Len(t, updatedAtIndex, 1)
	updatedSnapshotIndex := record.Schema().FieldIndices("last_updated_snapshot_id")
	require.Len(t, updatedSnapshotIndex, 1)

	updatedAt := record.Column(updatedAtIndex[0]).(*array.Timestamp)
	updatedSnapshot := record.Column(updatedSnapshotIndex[0]).(*array.Int64)
	require.EqualValues(t, historicalTimestamp*millisToMicros, updatedAt.Value(0))
	require.Equal(t, historicalSnapshotID, updatedSnapshot.Value(0))
}

func inspectPartitionHistoricalSnapshotTable(
	t testing.TB,
	historicalSnapshotID, currentSnapshotID int64,
) (*Table, int64) {
	t.Helper()

	spec := partitionedSpec()
	schema := simpleSchema()
	meta, err := NewMetadata(
		schema, &spec, UnsortedSortOrder, "mem://snapshot-history/table", nil,
	)
	require.NoError(t, err)
	historicalTimestamp := meta.LastUpdatedMillis() + 1
	currentTimestamp := historicalTimestamp + 1
	builder, err := MetadataBuilderFromBase(meta, "metadata.json")
	require.NoError(t, err)
	fs := iceio.NewMemFS()

	fileBuilder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		"mem://snapshot-history/table/data/file.parquet",
		iceberg.ParquetFile,
		map[int]any{1000: int32(7)},
		nil,
		nil,
		1,
		1,
	)
	require.NoError(t, err)

	historicalSequence := int64(1)
	currentSequence := int64(2)
	// The current snapshot's manifest keeps a file last added by the historical
	// snapshot, so last_updated_* must resolve through snapshot history.
	manifestPath := "mem://snapshot-history/table/metadata/manifest.avro"
	var manifestBuffer bytes.Buffer
	manifest, err := iceberg.WriteManifest(
		manifestPath,
		&manifestBuffer,
		2,
		spec,
		schema,
		currentSnapshotID,
		[]iceberg.ManifestEntry{iceberg.NewManifestEntry(
			iceberg.EntryStatusEXISTING,
			&historicalSnapshotID,
			&historicalSequence,
			&historicalSequence,
			fileBuilder.Build(),
		)},
	)
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(manifestPath, manifestBuffer.Bytes()))

	manifestListPath := "mem://snapshot-history/table/metadata/snap-2.avro"
	var listBuffer bytes.Buffer
	require.NoError(t, iceberg.WriteManifestList(
		2,
		&listBuffer,
		currentSnapshotID,
		&historicalSnapshotID,
		&currentSequence,
		0,
		[]iceberg.ManifestFile{manifest},
	))
	require.NoError(t, fs.WriteFile(manifestListPath, listBuffer.Bytes()))

	schemaID := schema.ID
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID:     historicalSnapshotID,
		SequenceNumber: historicalSequence,
		TimestampMs:    historicalTimestamp,
		ManifestList:   "mem://snapshot-history/table/metadata/snap-1.avro",
		SchemaID:       &schemaID,
		Summary:        &Summary{Operation: OpAppend},
	}))
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID:       currentSnapshotID,
		ParentSnapshotID: &historicalSnapshotID,
		SequenceNumber:   currentSequence,
		TimestampMs:      currentTimestamp,
		ManifestList:     manifestListPath,
		SchemaID:         &schemaID,
		Summary:          &Summary{Operation: OpAppend},
	}))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, currentSnapshotID, BranchRef))

	built, err := builder.Build()
	require.NoError(t, err)

	return New(
		Identifier{"db", "tbl"},
		built,
		"metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil },
		nil,
	), historicalTimestamp
}
