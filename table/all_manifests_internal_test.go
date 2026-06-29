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
	"fmt"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestAllManifestsCompletesAfterErrorChannelCloses(t *testing.T) {
	const snapshotCount = 8

	tbl, expectedPaths := tableWithManifestLists(t, snapshotCount)

	done := make(chan []string, 1)
	errs := make(chan error, 1)

	go func() {
		paths := make([]string, 0, snapshotCount)
		for mf, err := range tbl.AllManifests(context.Background()) {
			if err != nil {
				errs <- err

				return
			}

			paths = append(paths, mf.FilePath())
			if len(paths) == 1 {
				time.Sleep(10 * time.Millisecond)
			}
		}
		done <- paths
	}()

	select {
	case err := <-errs:
		require.NoError(t, err)
	case paths := <-done:
		require.Equal(t, expectedPaths, paths)
	case <-time.After(time.Second):
		t.Fatal("AllManifests did not complete")
	}
}

func tableWithManifestLists(t *testing.T, snapshotCount int) (*Table, []string) {
	t.Helper()

	memFS := iceio.NewMemFS()
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})

	const tableLocation = "mem://default/all-manifests"
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, tableLocation,
		iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)

	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	schemaID := meta.CurrentSchema().ID
	expectedPaths := make([]string, 0, snapshotCount)
	var parentID *int64
	for i := range snapshotCount {
		snapshotID := int64(i + 1)
		seqNum := snapshotID
		manifestPath := fmt.Sprintf("%s/metadata/manifest-%02d.avro", tableLocation, snapshotID)
		manifest := iceberg.NewManifestFile(2, manifestPath, 1, 0, snapshotID).
			SequenceNum(seqNum, seqNum).
			AddedFiles(1).
			AddedRows(1).
			Build()
		expectedPaths = append(expectedPaths, manifestPath)

		manifestListPath := fmt.Sprintf("%s/metadata/snap-%02d.avro", tableLocation, snapshotID)
		var listBuf bytes.Buffer
		require.NoError(t, iceberg.WriteManifestList(2, &listBuf, snapshotID, parentID, &seqNum, 0,
			[]iceberg.ManifestFile{manifest}))
		require.NoError(t, memFS.WriteFile(manifestListPath, listBuf.Bytes()))

		snapshot := Snapshot{
			SnapshotID:       snapshotID,
			ParentSnapshotID: parentID,
			SequenceNumber:   seqNum,
			TimestampMs:      meta.LastUpdatedMillis() + snapshotID,
			ManifestList:     manifestListPath,
			Summary:          &Summary{Operation: OpAppend},
			SchemaID:         &schemaID,
		}
		require.NoError(t, builder.AddSnapshot(&snapshot))

		nextParentID := snapshotID
		parentID = &nextParentID
	}
	require.NoError(t, builder.SetSnapshotRef(MainBranch, int64(snapshotCount), BranchRef))

	built, err := builder.Build()
	require.NoError(t, err)

	tbl := New(Identifier{"db", "all_manifests"}, built, tableLocation+"/metadata/metadata.json",
		func(context.Context) (iceio.IO, error) {
			return memFS, nil
		}, nil)

	return tbl, expectedPaths
}
