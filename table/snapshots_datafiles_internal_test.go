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
	"os"
	"path/filepath"
	"testing"

	"github.com/DataDog/iceberg-go"
	iceio "github.com/DataDog/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildSnapshotTestDataFile(t *testing.T, path string, content iceberg.ManifestEntryContent) iceberg.DataFile {
	t.Helper()

	b, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
		content, path, iceberg.ParquetFile, nil, nil, nil, 10, 1024)
	require.NoError(t, err)

	return b.Build()
}

// writeSnapshotTestManifest writes a manifest containing the given files
// with status ADDED, existingFiles with status EXISTING (live files
// carried over from a prior snapshot), and deletedFiles with status
// DELETED (tombstones for files removed by the snapshot).
func writeSnapshotTestManifest(t *testing.T, fs iceio.WriteFileIO, path string, content iceberg.ManifestContent, files, existingFiles, deletedFiles []iceberg.DataFile) iceberg.ManifestFile {
	t.Helper()

	var buf bytes.Buffer
	wr, err := iceberg.NewManifestWriter(2, &buf, *iceberg.UnpartitionedSpec,
		tableSchemaSimple, 1, iceberg.WithManifestWriterContent(content))
	require.NoError(t, err)

	snapshotID := int64(1)
	for _, df := range files {
		require.NoError(t, wr.Add(iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, &snapshotID, nil, nil, df)))
	}
	// Non-ADDED entries must carry explicit sequence numbers.
	seq := int64(1)
	for _, df := range existingFiles {
		require.NoError(t, wr.Existing(iceberg.NewManifestEntry(
			iceberg.EntryStatusEXISTING, &snapshotID, &seq, &seq, df)))
	}
	for _, df := range deletedFiles {
		require.NoError(t, wr.Delete(iceberg.NewManifestEntry(
			iceberg.EntryStatusDELETED, &snapshotID, &seq, &seq, df)))
	}
	require.NoError(t, wr.Close())
	require.NoError(t, fs.WriteFile(path, buf.Bytes()))

	mf, err := wr.ToManifestFile(path, int64(buf.Len()), iceberg.WithManifestFileContent(content))
	require.NoError(t, err)

	return mf
}

// writeSnapshotTestManifestList writes a manifest list referencing the
// given manifests in order and returns its path.
func writeSnapshotTestManifestList(t *testing.T, fs iceio.WriteFileIO, dir string, manifests []iceberg.ManifestFile) string {
	t.Helper()

	listPath := dir + "/metadata/manifest-list.avro"
	out, err := fs.Create(listPath)
	require.NoError(t, err)
	seq := int64(1)
	require.NoError(t, iceberg.WriteManifestList(2, out, 1, nil, &seq, 0, manifests))
	require.NoError(t, out.Close())

	return listPath
}

// Why: a manifest entry with status DELETED is a tombstone recording a
// removal, not a file reachable from the snapshot. Yielding it would make
// existence and duplicate checks (ReplaceFiles' belong-to-table check,
// AddFiles' duplicate walk, classifyFilesForDeletions) treat a file
// deleted by the snapshot as still live. ADDED and EXISTING entries are
// both live and must keep flowing through.
// Condition: a snapshot whose data and delete manifests each carry one
// ADDED, one EXISTING, and one DELETED entry.
// Assertion: the ADDED and EXISTING entries are yielded; the DELETED
// ones are not.
func TestSnapshotDataFilesSkipsDeletedEntries(t *testing.T) {
	fs := iceio.LocalFS{}
	dir := filepath.ToSlash(t.TempDir())
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "metadata"), 0o755))

	addedData := dir + "/data/added.parquet"
	existingData := dir + "/data/existing.parquet"
	goneData := dir + "/data/gone.parquet"
	addedDel := dir + "/data/added-del.parquet"
	existingDel := dir + "/data/existing-del.parquet"
	goneDel := dir + "/data/gone-del.parquet"

	manifests := []iceberg.ManifestFile{
		writeSnapshotTestManifest(t, fs, dir+"/metadata/manifest-0.avro",
			iceberg.ManifestContentData,
			[]iceberg.DataFile{buildSnapshotTestDataFile(t, addedData, iceberg.EntryContentData)},
			[]iceberg.DataFile{buildSnapshotTestDataFile(t, existingData, iceberg.EntryContentData)},
			[]iceberg.DataFile{buildSnapshotTestDataFile(t, goneData, iceberg.EntryContentData)}),
		writeSnapshotTestManifest(t, fs, dir+"/metadata/manifest-1.avro",
			iceberg.ManifestContentDeletes,
			[]iceberg.DataFile{buildSnapshotTestDataFile(t, addedDel, iceberg.EntryContentPosDeletes)},
			[]iceberg.DataFile{buildSnapshotTestDataFile(t, existingDel, iceberg.EntryContentPosDeletes)},
			[]iceberg.DataFile{buildSnapshotTestDataFile(t, goneDel, iceberg.EntryContentPosDeletes)}),
	}
	snap := Snapshot{
		SnapshotID:     1,
		SequenceNumber: 1,
		ManifestList:   writeSnapshotTestManifestList(t, fs, dir, manifests),
	}

	var got []string
	for df, err := range snap.dataFiles(fs, nil) {
		require.NoError(t, err)
		got = append(got, df.FilePath())
	}

	assert.Equal(t, []string{addedData, existingData, addedDel, existingDel}, got,
		"ADDED and EXISTING entries must be yielded; DELETED tombstones must not")
}
