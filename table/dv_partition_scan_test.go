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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestLocalScanValidatesDeletionVectorPartitionsAcrossSchemaPromotion(t *testing.T) {
	for _, tt := range []struct {
		name          string
		dvPartition   int64
		wantPartition int64
	}{
		{name: "matching promoted value", dvPartition: 7, wantPartition: 7},
		{name: "mismatched promoted value", dvPartition: 8, wantPartition: 7},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			fs := iceio.NewMemFS()
			const location = "mem://dv-partition-check/table"
			const dataPath = location + "/data/file.parquet"
			const dvPath = location + "/data/delete.puffin"
			const dataManifestPath = location + "/metadata/data-v1.avro"
			const deleteManifestPath = location + "/metadata/deletes-v2.avro"
			const firstManifestListPath = location + "/metadata/snap-1.avro"
			const currentManifestListPath = location + "/metadata/snap-2.avro"

			oldSchema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true})
			promotedSchema := iceberg.NewSchema(1,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true})
			spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
			})

			meta, err := NewMetadata(oldSchema, &spec, UnsortedSortOrder, location,
				iceberg.Properties{PropertyFormatVersion: "3"})
			require.NoError(t, err)

			dataFile, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, dataPath,
				iceberg.ParquetFile, map[int]any{1000: int32(tt.wantPartition)}, nil, nil, 1, 100)
			require.NoError(t, err)
			const firstSnapshotID, firstSequenceNumber = int64(1), int64(1)
			dataEntry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED,
				new(firstSnapshotID), new(firstSequenceNumber), nil, dataFile.Build())
			var dataManifestBytes bytes.Buffer
			dataManifest, err := iceberg.WriteManifest(dataManifestPath, &dataManifestBytes, 3,
				spec, oldSchema, firstSnapshotID, []iceberg.ManifestEntry{dataEntry})
			require.NoError(t, err)
			require.NoError(t, fs.WriteFile(ctx, dataManifestPath, dataManifestBytes.Bytes()))

			var firstManifestListBytes bytes.Buffer
			require.NoError(t, iceberg.WriteManifestList(3, &firstManifestListBytes,
				firstSnapshotID, nil, new(firstSequenceNumber), 0, []iceberg.ManifestFile{dataManifest}))
			require.NoError(t, fs.WriteFile(ctx, firstManifestListPath, firstManifestListBytes.Bytes()))

			builder, err := MetadataBuilderFromBase(meta, "")
			require.NoError(t, err)
			require.NoError(t, builder.AddSnapshot(&Snapshot{
				SnapshotID: firstSnapshotID, SequenceNumber: firstSequenceNumber,
				TimestampMs: meta.LastUpdatedMillis() + 1, ManifestList: firstManifestListPath,
				SchemaID: new(oldSchema.ID), Summary: &Summary{Operation: OpAppend},
				FirstRowID: new(int64(0)), AddedRows: new(int64(1)),
			}))
			require.NoError(t, builder.SetSnapshotRef(MainBranch, firstSnapshotID, BranchRef))
			meta, err = builder.Build()
			require.NoError(t, err)

			dvFile, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes, dvPath,
				iceberg.PuffinFile, map[int]any{1000: tt.dvPartition}, nil, nil, 1, 100)
			require.NoError(t, err)
			dvFile.ContentOffset(0).ContentSizeInBytes(10).ReferencedDataFile(dataPath)
			const currentSnapshotID, currentSequenceNumber = int64(2), int64(2)
			dvEntry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED,
				new(currentSnapshotID), new(currentSequenceNumber), nil, dvFile.Build())
			var deleteManifestBytes bytes.Buffer
			deleteWriter, err := iceberg.NewManifestWriter(3, &deleteManifestBytes, spec, promotedSchema,
				currentSnapshotID, iceberg.WithManifestWriterContent(iceberg.ManifestContentDeletes))
			require.NoError(t, err)
			require.NoError(t, deleteWriter.Add(dvEntry))
			require.NoError(t, deleteWriter.Close())
			deleteManifest, err := deleteWriter.ToManifestFile(deleteManifestPath, int64(deleteManifestBytes.Len()))
			require.NoError(t, err)
			require.NoError(t, fs.WriteFile(ctx, deleteManifestPath, deleteManifestBytes.Bytes()))

			// Reuse the first snapshot's manifest-list entry so its assigned row ID
			// stays attached to the historical data manifest.
			firstList, err := iceberg.ReadManifestList(bytes.NewReader(firstManifestListBytes.Bytes()))
			require.NoError(t, err)
			require.Len(t, firstList, 1)
			var currentManifestListBytes bytes.Buffer
			require.NoError(t, iceberg.WriteManifestList(3, &currentManifestListBytes,
				currentSnapshotID, new(firstSnapshotID), new(currentSequenceNumber), 1,
				[]iceberg.ManifestFile{firstList[0], deleteManifest}))
			require.NoError(t, fs.WriteFile(ctx, currentManifestListPath, currentManifestListBytes.Bytes()))

			builder, err = MetadataBuilderFromBase(meta, "")
			require.NoError(t, err)
			require.NoError(t, builder.AddSchema(promotedSchema))
			require.NoError(t, builder.SetCurrentSchemaID(promotedSchema.ID))
			require.NoError(t, builder.AddSnapshot(&Snapshot{
				SnapshotID: currentSnapshotID, ParentSnapshotID: new(firstSnapshotID),
				SequenceNumber: currentSequenceNumber, TimestampMs: meta.LastUpdatedMillis() + 1,
				ManifestList: currentManifestListPath, SchemaID: new(promotedSchema.ID),
				Summary:    &Summary{Operation: OpDelete},
				FirstRowID: new(int64(1)), AddedRows: new(int64(0)),
			}))
			require.NoError(t, builder.SetSnapshotRef(MainBranch, currentSnapshotID, BranchRef))
			meta, err = builder.Build()
			require.NoError(t, err)

			tbl := New(Identifier{"db", "dv_partition"}, meta, "metadata.json",
				func(context.Context) (iceio.IO, error) { return fs, nil }, nil)
			tasks, err := tbl.Scan().planFiles(ctx, false)
			if tt.dvPartition == tt.wantPartition {
				require.NoError(t, err)
				require.Len(t, tasks, 1)
				require.Len(t, tasks[0].DeletionVectorFiles, 1)
				require.Equal(t, dvPath, tasks[0].DeletionVectorFiles[0].FilePath())
				require.IsType(t, int32(0), dataFilePartition(tasks[0].File)[1000])
				require.IsType(t, int64(0), dataFilePartition(tasks[0].DeletionVectorFiles[0])[1000])
			} else {
				require.ErrorIs(t, err, ErrInvalidMetadata)
				require.Nil(t, tasks)
				require.ErrorContains(t, err, dvPath)
				require.ErrorContains(t, err, dataPath)
				require.ErrorContains(t, err, "map[1000:8]")
				require.ErrorContains(t, err, "map[1000:7]")
			}
		})
	}
}
