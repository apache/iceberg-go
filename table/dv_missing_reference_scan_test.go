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

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalScanRejectsDeletionVectorWithoutReferencedDataFile(t *testing.T) {
	ctx := context.Background()
	const dataPath = "mem://dv-missing-ref/table/data/file.parquet"
	const dvPath = "mem://dv-missing-ref/table/data/delete.puffin"

	for _, tt := range []struct {
		name    string
		ref     *string
		wantErr bool
	}{
		{name: "missing", wantErr: true},
		{name: "empty", ref: new(""), wantErr: true},
		{name: "valid", ref: new(dataPath)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fs := iceio.NewMemFS()
			schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64})
			spec := iceberg.NewPartitionSpec()
			meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "mem://dv-missing-ref/table",
				iceberg.Properties{PropertyFormatVersion: "3"})
			require.NoError(t, err)
			builder, err := MetadataBuilderFromBase(meta, "")
			require.NoError(t, err)

			const snapshotID, sequenceNumber = int64(1), int64(1)
			data, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, dataPath,
				iceberg.ParquetFile, nil, nil, nil, 1, 100)
			require.NoError(t, err)
			dataEntry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, new(snapshotID), new(sequenceNumber), nil, data.Build())
			const dataManifestPath = "mem://dv-missing-ref/table/metadata/data.avro"
			var dataBytes bytes.Buffer
			dataManifest, err := iceberg.WriteManifest(dataManifestPath, &dataBytes, 3, spec, schema, snapshotID, []iceberg.ManifestEntry{dataEntry})
			require.NoError(t, err)
			require.NoError(t, fs.WriteFile(ctx, dataManifestPath, dataBytes.Bytes()))

			dv, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes, dvPath,
				iceberg.PuffinFile, nil, nil, nil, 1, 100)
			require.NoError(t, err)
			dv.ContentOffset(0).ContentSizeInBytes(10)
			if tt.ref != nil {
				dv.ReferencedDataFile(*tt.ref)
			}
			dvEntry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, new(snapshotID), new(sequenceNumber), nil, dv.Build())
			const deleteManifestPath = "mem://dv-missing-ref/table/metadata/deletes.avro"
			var deleteBytes bytes.Buffer
			writer, err := iceberg.NewManifestWriter(3, &deleteBytes, spec, schema, snapshotID,
				iceberg.WithManifestWriterContent(iceberg.ManifestContentDeletes))
			require.NoError(t, err)
			require.NoError(t, writer.Add(dvEntry))
			require.NoError(t, writer.Close())
			deleteManifest, err := writer.ToManifestFile(deleteManifestPath, int64(deleteBytes.Len()))
			require.NoError(t, err)
			require.NoError(t, fs.WriteFile(ctx, deleteManifestPath, deleteBytes.Bytes()))

			const manifestListPath = "mem://dv-missing-ref/table/metadata/snap.avro"
			var listBytes bytes.Buffer
			require.NoError(t, iceberg.WriteManifestList(3, &listBytes, snapshotID, nil, new(sequenceNumber), 0,
				[]iceberg.ManifestFile{dataManifest, deleteManifest}))
			require.NoError(t, fs.WriteFile(ctx, manifestListPath, listBytes.Bytes()))
			require.NoError(t, builder.AddSnapshot(&Snapshot{
				SnapshotID: snapshotID, SequenceNumber: sequenceNumber, TimestampMs: meta.LastUpdatedMillis() + 1,
				ManifestList: manifestListPath, Summary: &Summary{Operation: OpAppend}, FirstRowID: new(int64(0)), AddedRows: new(int64(1)),
			}))
			require.NoError(t, builder.SetSnapshotRef(MainBranch, snapshotID, BranchRef))
			metadata, err := builder.Build()
			require.NoError(t, err)
			tbl := New(Identifier{"db", "tbl"}, metadata, "metadata.json",
				func(context.Context) (iceio.IO, error) { return fs, nil }, nil)

			for _, mode := range []struct {
				name      string
				projected bool
			}{
				{name: "full"},
				{name: "projected", projected: true},
			} {
				t.Run(mode.name, func(t *testing.T) {
					scan := tbl.Scan()
					tasks, err := scan.planFiles(ctx, mode.projected)
					if tt.wantErr {
						require.ErrorIs(t, err, ErrInvalidMetadata)
						require.ErrorContains(t, err, "referenced_data_file")
						require.Nil(t, tasks)
					} else {
						require.NoError(t, err)
						require.Len(t, tasks, 1)
						require.Len(t, tasks[0].DeletionVectorFiles, 1)
						assert.Equal(t, dvPath, tasks[0].DeletionVectorFiles[0].FilePath())
					}
				})
			}
		})
	}
}
