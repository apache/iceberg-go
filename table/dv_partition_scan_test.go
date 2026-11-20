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
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestLocalScanValidatesDeletionVectorPartitionsAcrossSchemaPromotion(t *testing.T) {
	for _, tt := range []struct {
		name         string
		oldType      iceberg.Type
		promotedType iceberg.Type
		dataValue    any
		dvValue      any
		mismatch     bool
	}{
		{
			name: "matching integer promotion", oldType: iceberg.PrimitiveTypes.Int32,
			promotedType: iceberg.PrimitiveTypes.Int64, dataValue: int32(7), dvValue: int64(7),
		},
		{
			name: "mismatched integer promotion", oldType: iceberg.PrimitiveTypes.Int32,
			promotedType: iceberg.PrimitiveTypes.Int64, dataValue: int32(7), dvValue: int64(8), mismatch: true,
		},
		{
			name: "matching float promotion", oldType: iceberg.PrimitiveTypes.Float32,
			promotedType: iceberg.PrimitiveTypes.Float64, dataValue: float32(4.5), dvValue: float64(4.5),
		},
		{
			name: "mismatched float promotion", oldType: iceberg.PrimitiveTypes.Float32,
			promotedType: iceberg.PrimitiveTypes.Float64, dataValue: float32(4.5), dvValue: float64(4.6), mismatch: true,
		},
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
				iceberg.NestedField{ID: 1, Name: "id", Type: tt.oldType, Required: true},
				iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String})
			promotedSchema := iceberg.NewSchema(1,
				iceberg.NestedField{ID: 1, Name: "id", Type: tt.promotedType, Required: true},
				iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String})
			spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
			})

			meta, err := NewMetadata(oldSchema, &spec, UnsortedSortOrder, location,
				iceberg.Properties{PropertyFormatVersion: "3", WriteDeleteModeKey: WriteModeCopyOnWrite})
			require.NoError(t, err)

			dataFile, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, dataPath,
				iceberg.ParquetFile, map[int]any{1000: tt.dataValue}, nil, nil, 1, 100)
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
				iceberg.PuffinFile, map[int]any{1000: tt.dvValue}, nil, nil, 1, 100)
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
			for _, projected := range []bool{false, true} {
				mode := "full"
				if projected {
					mode = "projected"
				}
				t.Run(mode, func(t *testing.T) {
					tasks, err := tbl.Scan().planFiles(ctx, projected)
					if !tt.mismatch {
						require.NoError(t, err)
						require.Len(t, tasks, 1)
						require.Len(t, tasks[0].DeletionVectorFiles, 1)
						require.Equal(t, dvPath, tasks[0].DeletionVectorFiles[0].FilePath())
						require.IsType(t, tt.dataValue, dataFilePartition(tasks[0].File)[1000])
						require.IsType(t, tt.dvValue, dataFilePartition(tasks[0].DeletionVectorFiles[0])[1000])
					} else {
						require.ErrorIs(t, err, ErrInvalidMetadata)
						require.Nil(t, tasks)
						require.ErrorContains(t, err, dvPath)
						require.ErrorContains(t, err, dataPath)
						require.ErrorContains(t, err, fmt.Sprintf("map[1000:%v]", tt.dvValue))
						require.ErrorContains(t, err, fmt.Sprintf("map[1000:%v]", tt.dataValue))
					}
				})
			}
			t.Run("should validate a promoted partition if the changelog adds a DV", func(t *testing.T) {
				tasks, err := tbl.NewIncrementalChangelogScan().PlanFiles(t.Context())
				if tt.mismatch {
					require.ErrorIs(t, err, ErrInvalidMetadata)
					require.ErrorContains(t, err, dvPath)
					require.ErrorContains(t, err, dataPath)

					return
				}

				require.NoError(t, err)
				require.Len(t, tasks, 2)
				deleted, ok := tasks[1].(DeletedRowsScanTask)
				require.True(t, ok)
				require.Equal(t, dataPath, deleted.ScanTask().File.FilePath())
				require.Equal(t, []string{dvPath}, changelogFilePaths(deleted.AddedDeletes()))
			})
			if tt.mismatch {
				t.Run("copy-on-write delete", func(t *testing.T) {
					// A non-partition predicate without file bounds requires a
					// row rewrite. The invalid DV must fail before file reads.
					filter := iceberg.EqualTo(iceberg.Reference("payload"), "remove")
					err := tbl.NewTransaction().Delete(t.Context(), filter, nil)
					require.ErrorIs(t, err, ErrInvalidMetadata)
					require.ErrorContains(t, err, dvPath)
					require.ErrorContains(t, err, dataPath)
				})
			}
		})
	}
}

func TestLocalScanMatchesOldFloatDVAfterRewriteManifests(t *testing.T) {
	ctx := context.Background()
	fs := iceio.NewMemFS()
	const location = "mem://dv-partition-rewrite/table"
	const dataPath = location + "/data/file.parquet"
	const secondDataPath = location + "/data/second.parquet"
	const dvPath = location + "/data/delete.puffin"
	const firstManifestPath = location + "/metadata/data-v1.avro"
	const secondManifestPath = location + "/metadata/data-v2.avro"
	const deleteManifestPath = location + "/metadata/deletes-v2.avro"
	const firstManifestListPath = location + "/metadata/snap-1.avro"
	const currentManifestListPath = location + "/metadata/snap-2.avro"

	floatSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "score", Type: iceberg.PrimitiveTypes.Float32, Required: true})
	doubleSchema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "score", Type: iceberg.PrimitiveTypes.Float64, Required: true})
	spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "score", Transform: iceberg.IdentityTransform{},
	})
	meta, err := NewMetadata(floatSchema, &spec, UnsortedSortOrder, location,
		iceberg.Properties{PropertyFormatVersion: "3", ManifestMergeEnabledKey: "false"})
	require.NoError(t, err)

	const firstSnapshotID, firstSequenceNumber = int64(1), int64(1)
	var dataManifests []iceberg.ManifestFile
	for _, item := range []struct {
		path         string
		manifestPath string
	}{{dataPath, firstManifestPath}, {secondDataPath, secondManifestPath}} {
		dataFile, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, item.path,
			iceberg.ParquetFile, map[int]any{1000: float32(4.5)}, nil, nil, 1, 100)
		require.NoError(t, err)
		entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED,
			new(firstSnapshotID), new(firstSequenceNumber), nil, dataFile.Build())
		var manifestBytes bytes.Buffer
		manifest, err := iceberg.WriteManifest(item.manifestPath, &manifestBytes, 3,
			spec, floatSchema, firstSnapshotID, []iceberg.ManifestEntry{entry})
		require.NoError(t, err)
		require.NoError(t, fs.WriteFile(ctx, item.manifestPath, manifestBytes.Bytes()))
		dataManifests = append(dataManifests, manifest)
	}

	var firstManifestListBytes bytes.Buffer
	require.NoError(t, iceberg.WriteManifestList(3, &firstManifestListBytes,
		firstSnapshotID, nil, new(firstSequenceNumber), 0, dataManifests))
	require.NoError(t, fs.WriteFile(ctx, firstManifestListPath, firstManifestListBytes.Bytes()))
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID: firstSnapshotID, SequenceNumber: firstSequenceNumber,
		TimestampMs: meta.LastUpdatedMillis() + 1, ManifestList: firstManifestListPath,
		SchemaID: new(floatSchema.ID), Summary: &Summary{Operation: OpAppend},
		FirstRowID: new(int64(0)), AddedRows: new(int64(2)),
	}))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, firstSnapshotID, BranchRef))
	meta, err = builder.Build()
	require.NoError(t, err)

	dvFile, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes, dvPath,
		iceberg.PuffinFile, map[int]any{1000: float32(4.5)}, nil, nil, 1, 100)
	require.NoError(t, err)
	dvFile.ContentOffset(0).ContentSizeInBytes(10).ReferencedDataFile(dataPath)
	const currentSnapshotID, currentSequenceNumber = int64(2), int64(2)
	dvEntry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED,
		new(currentSnapshotID), new(currentSequenceNumber), nil, dvFile.Build())
	var deleteManifestBytes bytes.Buffer
	deleteWriter, err := iceberg.NewManifestWriter(3, &deleteManifestBytes, spec, floatSchema,
		currentSnapshotID, iceberg.WithManifestWriterContent(iceberg.ManifestContentDeletes))
	require.NoError(t, err)
	require.NoError(t, deleteWriter.Add(dvEntry))
	require.NoError(t, deleteWriter.Close())
	deleteManifest, err := deleteWriter.ToManifestFile(deleteManifestPath, int64(deleteManifestBytes.Len()))
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(ctx, deleteManifestPath, deleteManifestBytes.Bytes()))

	firstList, err := iceberg.ReadManifestList(bytes.NewReader(firstManifestListBytes.Bytes()))
	require.NoError(t, err)
	require.Len(t, firstList, 2)
	var currentManifestListBytes bytes.Buffer
	require.NoError(t, iceberg.WriteManifestList(3, &currentManifestListBytes,
		currentSnapshotID, new(firstSnapshotID), new(currentSequenceNumber), 2,
		[]iceberg.ManifestFile{firstList[0], firstList[1], deleteManifest}))
	require.NoError(t, fs.WriteFile(ctx, currentManifestListPath, currentManifestListBytes.Bytes()))

	builder, err = MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	require.NoError(t, builder.AddSchema(doubleSchema))
	require.NoError(t, builder.SetCurrentSchemaID(doubleSchema.ID))
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID: currentSnapshotID, ParentSnapshotID: new(firstSnapshotID),
		SequenceNumber: currentSequenceNumber, TimestampMs: meta.LastUpdatedMillis() + 1,
		ManifestList: currentManifestListPath, SchemaID: new(floatSchema.ID),
		Summary: &Summary{Operation: OpDelete}, FirstRowID: new(int64(2)), AddedRows: new(int64(0)),
	}))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, currentSnapshotID, BranchRef))
	meta, err = builder.Build()
	require.NoError(t, err)

	cat := &reqValidatingCatalog{metadata: meta}
	tbl := New(Identifier{"db", "dv_rewrite"}, meta, "metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil }, cat)
	txn := tbl.NewTransaction()
	result, err := txn.RewriteManifests(ctx)
	require.NoError(t, err)
	require.Len(t, result.RewrittenManifests, 2)
	require.Len(t, result.AddedManifests, 1)
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	manifests, err := tbl.CurrentSnapshot().Manifests(ctx, fs)
	require.NoError(t, err)
	var rewrittenDataManifests, retainedDeleteManifests int
	for _, manifest := range manifests {
		switch manifest.ManifestContent() {
		case iceberg.ManifestContentData:
			rewrittenDataManifests++
		case iceberg.ManifestContentDeletes:
			retainedDeleteManifests++
			require.Equal(t, deleteManifestPath, manifest.FilePath())
		}
	}
	require.Equal(t, 1, rewrittenDataManifests)
	require.Equal(t, 1, retainedDeleteManifests)

	// Remove the partition field before dropping its source column. The
	// historical spec remains necessary to interpret the rewritten manifests.
	specTxn := tbl.NewTransaction()
	require.NoError(t, specTxn.UpdateSpec(true).RemoveField("score").Commit())
	tbl, err = specTxn.Commit(ctx)
	require.NoError(t, err)

	schemaTxn := tbl.NewTransaction()
	require.NoError(t, schemaTxn.UpdateSchema(true, false).DeleteColumn([]string{"score"}).Commit())
	tbl, err = schemaTxn.Commit(ctx)
	require.NoError(t, err)
	_, hasCurrentSource := tbl.Schema().FindFieldByID(1)
	require.False(t, hasCurrentSource)
	currentSpec := tbl.Spec()
	require.Zero(t, currentSpec.NumFields())
	historicalSpec := tbl.Metadata().PartitionSpecByID(0)
	require.NotNil(t, historicalSpec)
	require.Equal(t, 1, historicalSpec.NumFields())

	for _, projected := range []bool{false, true} {
		mode := "full"
		if projected {
			mode = "projected"
		}
		t.Run(mode, func(t *testing.T) {
			tasks, err := tbl.Scan().planFiles(ctx, projected)
			require.NoError(t, err)
			require.Len(t, tasks, 2)
			for _, task := range tasks {
				if task.File.FilePath() != dataPath {
					continue
				}
				require.Len(t, task.DeletionVectorFiles, 1)
				require.Equal(t, dvPath, task.DeletionVectorFiles[0].FilePath())
				require.IsType(t, float64(0), dataFilePartition(task.File)[1000])
				require.IsType(t, float32(0), dataFilePartition(task.DeletionVectorFiles[0])[1000])

				return
			}
			t.Fatalf("scan did not plan target data file %s", dataPath)
		})
	}
}
