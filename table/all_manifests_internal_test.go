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
	"sync"
	"testing"
	"time"

	"github.com/DataDog/iceberg-go"
	iceio "github.com/DataDog/iceberg-go/io"
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

func TestAllManifestsLimitsConcurrentReads(t *testing.T) {
	const snapshotCount = 64

	var trackingFS *manifestTrackingIO
	tbl, _ := tableWithManifestListsUsingIO(t, snapshotCount, func(memFS *iceio.MemFS) iceio.IO {
		trackingFS = &manifestTrackingIO{IO: memFS, delay: 5 * time.Millisecond}

		return trackingFS
	})

	for mf, err := range tbl.AllManifests(context.Background()) {
		require.NoError(t, err)
		require.NotNil(t, mf)
	}

	trackingFS.mu.Lock()
	maxOpen := trackingFS.maxOpen
	trackingFS.mu.Unlock()
	require.LessOrEqual(t, maxOpen, allManifestsMaxWorkers)
}

func TestAllManifestsWorkerCount(t *testing.T) {
	tests := []struct {
		name          string
		snapshotCount int
		want          int
	}{
		{name: "empty", snapshotCount: 0, want: 1},
		{name: "single snapshot", snapshotCount: 1, want: 1},
		{name: "at limit", snapshotCount: allManifestsMaxWorkers, want: allManifestsMaxWorkers},
		{name: "above limit", snapshotCount: allManifestsMaxWorkers + 1, want: allManifestsMaxWorkers},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, allManifestsWorkerCount(tt.snapshotCount))
		})
	}
}

type manifestTrackingIO struct {
	iceio.IO
	mu      sync.Mutex
	open    int
	maxOpen int
	delay   time.Duration
}

func (fs *manifestTrackingIO) Open(name string) (iceio.File, error) {
	f, err := fs.IO.Open(name)
	if err != nil {
		return nil, err
	}

	fs.mu.Lock()
	fs.open++
	if fs.open > fs.maxOpen {
		fs.maxOpen = fs.open
	}
	fs.mu.Unlock()
	time.Sleep(fs.delay)

	return &manifestTrackingFile{File: f, onClose: func() {
		fs.mu.Lock()
		fs.open--
		fs.mu.Unlock()
	}}, nil
}

type manifestTrackingFile struct {
	iceio.File
	onClose func()
	once    sync.Once
}

func (f *manifestTrackingFile) Close() error {
	f.once.Do(f.onClose)

	return f.File.Close()
}

func tableWithManifestLists(t *testing.T, snapshotCount int) (*Table, []string) {
	return tableWithManifestListsUsingIO(t, snapshotCount, nil)
}

func tableWithManifestListsUsingIO(t *testing.T, snapshotCount int, readIOFn func(*iceio.MemFS) iceio.IO) (*Table, []string) {
	t.Helper()

	memFS := iceio.NewMemFS()
	var readIO iceio.IO = memFS
	if readIOFn != nil {
		readIO = readIOFn(memFS)
	}
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
			return readIO, nil
		}, nil)

	return tbl, expectedPaths
}
