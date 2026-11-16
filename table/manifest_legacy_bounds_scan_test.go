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
	"bytes"
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestScanLegacyEmptyStringPartitionDeleteBounds(t *testing.T) {
	for _, deleteType := range []string{"position", "equality"} {
		t.Run(deleteType, func(t *testing.T) {
			ctx := t.Context()
			cat, err := hadoop.NewCatalog(ctx, "local", t.TempDir(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, cat.Close()) })
			require.NoError(t, cat.CreateNamespace(ctx, catalog.ToIdentifier("db"), nil))
			schema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
				iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String},
			)
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{2}, FieldID: 1000, Name: "region", Transform: iceberg.IdentityTransform{},
			})
			tbl, err := cat.CreateTable(ctx, catalog.ToIdentifier("db", "events"), schema,
				catalog.WithPartitionSpec(&spec), catalog.WithProperties(iceberg.Properties{
					table.PropertyFormatVersion: "2",
					table.WriteDeleteModeKey:    table.WriteModeMergeOnRead,
				}))
			require.NoError(t, err)
			tbl = appendRows(t, tbl, `[{"id":1,"region":""},{"id":2,"region":""},{"id":3,"region":"us"}]`)

			if deleteType == "position" {
				tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(1)), nil)
				require.NoError(t, err)
			} else {
				arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
				require.NoError(t, err)
				records, release := makeEqDeleteRecords(t, arrowSchema, `[{"id":1,"region":""}]`)
				defer release()
				tx := tbl.NewTransaction()
				require.NoError(t, tx.CommitEqualityDeletes(ctx, []int{1}, records, nil))
				tbl, err = tx.Commit(ctx)
				require.NoError(t, err)
			}

			fs, err := tbl.FS(ctx)
			require.NoError(t, err)
			snapshot := tbl.CurrentSnapshot()
			manifests, err := snapshot.Manifests(ctx, fs)
			require.NoError(t, err)
			require.Len(t, manifests, 2)
			for i, manifest := range manifests {
				// Reproduce #2407 explicitly so this remains a legacy-reader
				// regression after the empty-string writer is fixed by #2510.
				summary := iceberg.FieldSummary{}
				if manifest.ManifestContent() == iceberg.ManifestContentData {
					upper := []byte("us")
					summary.UpperBound = &upper
				}
				manifests[i] = iceberg.NewManifestBuilder(2, manifest.FilePath(), manifest.Length(),
					manifest.PartitionSpecID(), manifest.SnapshotID()).
					Content(manifest.ManifestContent()).
					SequenceNumber(manifest.SequenceNumber(), manifest.MinSequenceNumber()).
					AddedFiles(manifest.AddedDataFiles()).ExistingFiles(manifest.ExistingDataFiles()).
					DeletedFiles(manifest.DeletedDataFiles()).
					AddedRows(manifest.AddedRows()).ExistingRows(manifest.ExistingRows()).
					DeletedRows(manifest.DeletedRows()).
					Partitions([]iceberg.FieldSummary{summary}).Build()
			}
			var list bytes.Buffer
			require.NoError(t, iceberg.WriteManifestList(2, &list, snapshot.SnapshotID,
				snapshot.ParentSnapshotID, &snapshot.SequenceNumber, 0, manifests))
			wfs, ok := fs.(iceio.WriteFileIO)
			require.True(t, ok)
			require.NoError(t, wfs.WriteFile(ctx, snapshot.ManifestList, list.Bytes()))

			// Use a fresh table so cached manifests cannot hide the legacy summaries.
			legacy := table.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(),
				func(context.Context) (iceio.IO, error) { return fs, nil }, nil)
			emptyPartition := iceberg.EqualTo(iceberg.Reference("region"), "")
			tasks, err := legacy.Scan(table.WithRowFilter(emptyPartition)).PlanFiles(ctx)
			require.NoError(t, err)
			require.Len(t, tasks, 1)
			if deleteType == "position" {
				require.Len(t, tasks[0].DeleteFiles, 1)
			} else {
				require.Len(t, tasks[0].EqualityDeleteFiles, 1)
			}
			require.ElementsMatch(t, []int64{2, 3}, scanIDs(t, legacy))
			require.Equal(t, []int64{2}, scanIDsMatching(t, legacy, emptyPartition),
				"filtered scans must apply the legacy delete manifest")
			require.Equal(t, []int64{3}, scanIDsMatching(t, legacy,
				iceberg.EqualTo(iceberg.Reference("region"), "us")))
		})
	}
}
