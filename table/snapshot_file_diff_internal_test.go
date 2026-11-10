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
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func diffTestFile(t *testing.T, path string, content iceberg.ManifestEntryContent) iceberg.DataFile {
	t.Helper()
	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, content, path, iceberg.ParquetFile,
		nil, nil, nil, 10, 1024)
	require.NoError(t, err)

	return builder.Build()
}

func writeDiffTestManifest(
	t *testing.T, fs iceio.WriteFileIO, version int, path string,
	content iceberg.ManifestContent, snapshotID int64,
	added, existing, deleted []iceberg.DataFile,
) iceberg.ManifestFile {
	t.Helper()
	var buf bytes.Buffer
	wr, err := iceberg.NewManifestWriter(
		version, &buf, *iceberg.UnpartitionedSpec, simpleSchema(), snapshotID,
		iceberg.WithManifestWriterContent(content))
	require.NoError(t, err)

	seq := int64(1)
	for _, file := range added {
		require.NoError(t, wr.Add(iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, &snapshotID, nil, nil, file)))
	}
	for _, file := range existing {
		var dataSeq, fileSeq *int64
		if version > 1 {
			dataSeq, fileSeq = &seq, &seq
		}
		require.NoError(t, wr.Existing(iceberg.NewManifestEntry(
			iceberg.EntryStatusEXISTING, &snapshotID, dataSeq, fileSeq, file)))
	}
	for _, file := range deleted {
		var dataSeq, fileSeq *int64
		if version > 1 {
			dataSeq, fileSeq = &seq, &seq
		}
		require.NoError(t, wr.Delete(iceberg.NewManifestEntry(
			iceberg.EntryStatusDELETED, &snapshotID, dataSeq, fileSeq, file)))
	}
	require.NoError(t, wr.Close())
	require.NoError(t, fs.WriteFile(context.Background(), path, buf.Bytes()))
	manifest, err := wr.ToManifestFile(
		path, int64(buf.Len()), iceberg.WithManifestFileContent(content))
	require.NoError(t, err)

	return manifest
}

func writeDiffTestManifestList(
	t *testing.T, fs iceio.WriteFileIO, version int, path string,
	snapshotID, sequenceNumber int64, manifests []iceberg.ManifestFile,
) string {
	t.Helper()
	out, err := fs.Create(context.Background(), path)
	require.NoError(t, err)
	var sequence *int64
	if version > 1 {
		sequence = &sequenceNumber
	}
	require.NoError(t, iceberg.WriteManifestList(version, out, snapshotID, nil,
		sequence, 0, manifests))
	require.NoError(t, out.Close())

	return path
}

func diffTestTable(
	t *testing.T, version int, fs iceio.IO, snapshots ...Snapshot,
) *Table {
	t.Helper()
	spec := *iceberg.UnpartitionedSpec
	meta, err := NewMetadata(simpleSchema(), &spec, UnsortedSortOrder,
		"mem://diff/table", iceberg.Properties{PropertyFormatVersion: strconv.Itoa(version)})
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)

	for i := range snapshots {
		require.NoError(t, builder.AddSnapshot(&snapshots[i]))
	}
	require.NotEmpty(t, snapshots)
	require.NoError(t, builder.SetSnapshotRef(
		MainBranch, snapshots[len(snapshots)-1].SnapshotID, BranchRef))
	meta, err = builder.Build()
	require.NoError(t, err)

	return New(Identifier{"db", "diff"}, meta, "mem://diff/table/metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil }, nil)
}

func diffFilePaths(files []iceberg.DataFile) []string {
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.FilePath())
	}

	return paths
}

func TestDiffSnapshotFilesV1EmbeddedManifests(t *testing.T) {
	fs := iceio.NewMemFS()
	prefix := "mem://diff/v1/"
	a := diffTestFile(t, prefix+"a.parquet", iceberg.EntryContentData)
	b := diffTestFile(t, prefix+"b.parquet", iceberg.EntryContentData)
	c := diffTestFile(t, prefix+"c.parquet", iceberg.EntryContentData)
	ghost := diffTestFile(t, prefix+"ghost.parquet", iceberg.EntryContentData)

	shared := writeDiffTestManifest(t, fs, 1, prefix+"shared.avro",
		iceberg.ManifestContentData, 1, []iceberg.DataFile{a}, nil, nil)
	before := writeDiffTestManifest(t, fs, 1, prefix+"before.avro",
		iceberg.ManifestContentData, 1, []iceberg.DataFile{b}, nil, nil)
	after := writeDiffTestManifest(t, fs, 1, prefix+"after.avro",
		iceberg.ManifestContentData, 2, []iceberg.DataFile{c}, nil, []iceberg.DataFile{ghost})

	now := time.Now().UnixMilli()
	first := Snapshot{
		SnapshotID: 1, TimestampMs: now,
		ManifestLocations: []string{shared.FilePath(), before.FilePath()},
	}
	firstID := first.SnapshotID
	second := Snapshot{
		SnapshotID: 2, TimestampMs: now + 1, ParentSnapshotID: &firstID,
		ManifestLocations: []string{shared.FilePath(), after.FilePath()},
	}
	tbl := diffTestTable(t, 1, fs, first, second)

	diff, err := tbl.Inspect().DiffSnapshotFiles(context.Background(), 1, 2)
	require.NoError(t, err)
	require.True(t, diff.HasChanges())
	require.Equal(t, int64(1), diff.FromSnapshotID)
	require.Equal(t, int64(2), diff.ToSnapshotID)
	require.Equal(t, []string{c.FilePath()}, diffFilePaths(diff.AddedDataFiles))
	require.Equal(t, []string{b.FilePath()}, diffFilePaths(diff.RemovedDataFiles))
	require.Empty(t, diff.AddedDeleteFiles)
	require.Empty(t, diff.RemovedDeleteFiles)

	reverse, err := tbl.Inspect().DiffSnapshotFiles(context.Background(), 2, 1)
	require.NoError(t, err)
	require.Equal(t, []string{b.FilePath()}, diffFilePaths(reverse.AddedDataFiles))
	require.Equal(t, []string{c.FilePath()}, diffFilePaths(reverse.RemovedDataFiles))
}

func TestDiffSnapshotFilesV1ManifestLists(t *testing.T) {
	fs := iceio.NewMemFS()
	prefix := "mem://diff/v1-lists/"
	reused := diffTestFile(t, prefix+"reused.parquet", iceberg.EntryContentData)
	before := diffTestFile(t, prefix+"before.parquet", iceberg.EntryContentData)
	after := diffTestFile(t, prefix+"after.parquet", iceberg.EntryContentData)

	shared := writeDiffTestManifest(t, fs, 1, prefix+"shared.avro",
		iceberg.ManifestContentData, 1, []iceberg.DataFile{reused}, nil, nil)
	oldManifest := writeDiffTestManifest(t, fs, 1, prefix+"old.avro",
		iceberg.ManifestContentData, 1, []iceberg.DataFile{before}, nil, nil)
	newManifest := writeDiffTestManifest(t, fs, 1, prefix+"new.avro",
		iceberg.ManifestContentData, 2, nil, []iceberg.DataFile{after}, nil)
	fromList := writeDiffTestManifestList(t, fs, 1, prefix+"before-list.avro", 1, 0,
		[]iceberg.ManifestFile{shared, oldManifest})
	toList := writeDiffTestManifestList(t, fs, 1, prefix+"after-list.avro", 2, 0,
		[]iceberg.ManifestFile{shared, newManifest})
	now := time.Now().UnixMilli()
	parent := int64(1)
	tbl := diffTestTable(t, 1, fs,
		Snapshot{SnapshotID: 1, TimestampMs: now, ManifestList: fromList},
		Snapshot{SnapshotID: 2, TimestampMs: now + 1, ParentSnapshotID: &parent, ManifestList: toList})

	diff, err := tbl.Inspect().DiffSnapshotFiles(context.Background(), 1, 2)
	require.NoError(t, err)
	require.Equal(t, []string{after.FilePath()}, diffFilePaths(diff.AddedDataFiles))
	require.Equal(t, []string{before.FilePath()}, diffFilePaths(diff.RemovedDataFiles))
	require.Empty(t, diff.AddedDeleteFiles)
	require.Empty(t, diff.RemovedDeleteFiles)
}

func TestDiffSnapshotFilesV2ManifestRewriteAndDeleteFiles(t *testing.T) {
	fs := iceio.NewMemFS()
	prefix := "mem://diff/v2/"
	moved := diffTestFile(t, prefix+"moved.parquet", iceberg.EntryContentData)
	added := diffTestFile(t, prefix+"added.parquet", iceberg.EntryContentData)
	removedDelete := diffTestFile(t, prefix+"removed-delete.parquet", iceberg.EntryContentPosDeletes)
	addedDelete := diffTestFile(t, prefix+"added-delete.parquet", iceberg.EntryContentEqDeletes)

	// Deliberately leave the shared manifest's Avro file absent. Its descriptor
	// exists in both lists, so reading it would be an unnecessary I/O error.
	shared := iceberg.NewManifestBuilder(
		2, prefix+"unchanged-manifest.avro", 100, 0, 1).SequenceNumber(1, 1).AddedFiles(1).Build()
	oldData := writeDiffTestManifest(t, fs, 2, prefix+"old-data.avro",
		iceberg.ManifestContentData, 1, []iceberg.DataFile{moved}, nil, nil)
	newData := writeDiffTestManifest(t, fs, 2, prefix+"new-data.avro",
		iceberg.ManifestContentData, 2, []iceberg.DataFile{added},
		[]iceberg.DataFile{moved}, nil)
	oldDeletes := writeDiffTestManifest(t, fs, 2, prefix+"old-deletes.avro",
		iceberg.ManifestContentDeletes, 1,
		[]iceberg.DataFile{removedDelete}, nil, nil)
	newDeletes := writeDiffTestManifest(t, fs, 2, prefix+"new-deletes.avro",
		iceberg.ManifestContentDeletes, 2,
		[]iceberg.DataFile{addedDelete}, nil, nil)

	// A shared manifest that cannot be opened must not be read during diff.
	fromList := writeDiffTestManifestList(t, fs, 2, prefix+"list1.avro", 1, 1,
		[]iceberg.ManifestFile{shared, oldData, oldDeletes})
	toList := writeDiffTestManifestList(t, fs, 2, prefix+"list2.avro", 2, 2,
		[]iceberg.ManifestFile{newDeletes, shared, newData})
	now := time.Now().UnixMilli()
	fromID := int64(1)
	tbl := diffTestTable(t, 2, fs,
		Snapshot{SnapshotID: 1, SequenceNumber: 1, TimestampMs: now, ManifestList: fromList},
		Snapshot{
			SnapshotID: 2, SequenceNumber: 2, TimestampMs: now + 1,
			ParentSnapshotID: &fromID, ManifestList: toList,
		})

	diff, err := tbl.Inspect().DiffSnapshotFiles(context.Background(), 1, 2)
	require.NoError(t, err)
	require.Equal(t, []string{added.FilePath()}, diffFilePaths(diff.AddedDataFiles))
	require.Empty(t, diff.RemovedDataFiles, "manifest-only rewrite does not remove a data file")
	require.Equal(t, []string{addedDelete.FilePath()}, diffFilePaths(diff.AddedDeleteFiles))
	require.Equal(t, []string{removedDelete.FilePath()}, diffFilePaths(diff.RemovedDeleteFiles))
}

func diffTestDeletionVector(
	t *testing.T, puffinPath, dataPath string, offset int64,
) iceberg.DataFile {
	t.Helper()
	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		puffinPath, iceberg.PuffinFile, nil, nil, nil, 10, 1024)
	require.NoError(t, err)

	return builder.
		ReferencedDataFile(dataPath).
		ContentOffset(offset).
		ContentSizeInBytes(32).
		Build()
}

func TestDiffSnapshotFilesV3DeletionVectorsSharePuffinFile(t *testing.T) {
	fs := iceio.NewMemFS()
	prefix := "mem://diff/v3/"
	puffin := prefix + "deletes.puffin"
	oldA := diffTestDeletionVector(t, puffin, prefix+"a.parquet", 100)
	newA := diffTestDeletionVector(t, puffin, prefix+"a.parquet", 200)
	sameB := diffTestDeletionVector(t, puffin, prefix+"b.parquet", 300)

	// Two DVs can live in one Puffin file. A replacement must not be hidden
	// by path-only deduplication, nor should the retained DV be reported.
	oldManifest := writeDiffTestManifest(t, fs, 3, prefix+"old.avro",
		iceberg.ManifestContentDeletes, 1, []iceberg.DataFile{oldA, sameB}, nil, nil)
	newC := diffTestDeletionVector(t, puffin, prefix+"c.parquet", 150)
	newD := diffTestDeletionVector(t, puffin, prefix+"d.parquet", 350)
	newManifest := writeDiffTestManifest(t, fs, 3, prefix+"new.avro",
		iceberg.ManifestContentDeletes, 2, []iceberg.DataFile{newD, sameB, newA, newC}, nil, nil)

	fromList := writeDiffTestManifestList(t, fs, 3, prefix+"from-list.avro", 1, 1,
		[]iceberg.ManifestFile{oldManifest})
	toList := writeDiffTestManifestList(t, fs, 3, prefix+"to-list.avro", 2, 2,
		[]iceberg.ManifestFile{newManifest})
	now := time.Now().UnixMilli()
	parent, zero := int64(1), int64(0)
	tbl := diffTestTable(t, 3, fs,
		Snapshot{
			SnapshotID: 1, SequenceNumber: 1, TimestampMs: now, ManifestList: fromList,
			FirstRowID: &zero, AddedRows: &zero,
		},
		Snapshot{
			SnapshotID: 2, SequenceNumber: 2, TimestampMs: now + 1,
			ParentSnapshotID: &parent, ManifestList: toList, FirstRowID: &zero, AddedRows: &zero,
		})

	diff, err := tbl.Inspect().DiffSnapshotFiles(context.Background(), 1, 2)
	require.NoError(t, err)
	require.True(t, diff.HasChanges())
	require.Empty(t, diff.AddedDataFiles)
	require.Empty(t, diff.RemovedDataFiles)
	require.Len(t, diff.AddedDeleteFiles, 3)
	require.Len(t, diff.RemovedDeleteFiles, 1)
	var offsets []int64
	for _, file := range diff.AddedDeleteFiles {
		require.Equal(t, puffin, file.FilePath())
		offsets = append(offsets, *file.ContentOffset())
	}
	require.Equal(t, []int64{150, 200, 350}, offsets)
	require.Equal(t, puffin, diff.RemovedDeleteFiles[0].FilePath())
	require.Equal(t, int64(100), *diff.RemovedDeleteFiles[0].ContentOffset())
}

func TestDiffSnapshotFilesUnrelatedSnapshotsAndSorting(t *testing.T) {
	fs := iceio.NewMemFS()
	prefix := "mem://diff/branches/"
	dataZ := diffTestFile(t, prefix+"z.parquet", iceberg.EntryContentData)
	dataA := diffTestFile(t, prefix+"a.parquet", iceberg.EntryContentData)
	dataB := diffTestFile(t, prefix+"b.parquet", iceberg.EntryContentData)

	left := writeDiffTestManifest(t, fs, 1, prefix+"left.avro",
		iceberg.ManifestContentData, 10, []iceberg.DataFile{dataZ, dataA}, nil, nil)
	right := writeDiffTestManifest(t, fs, 1, prefix+"right.avro",
		iceberg.ManifestContentData, 20, []iceberg.DataFile{dataB}, nil, nil)
	now := time.Now().UnixMilli()
	tbl := diffTestTable(t, 1, fs,
		Snapshot{
			SnapshotID: 10, TimestampMs: now,
			ManifestLocations: []string{left.FilePath()},
		},
		Snapshot{
			SnapshotID: 20, TimestampMs: now + 1,
			ManifestLocations: []string{right.FilePath()},
		})

	diff, err := tbl.Inspect().DiffSnapshotFiles(context.Background(), 10, 20)
	require.NoError(t, err)
	require.Equal(t, []string{dataB.FilePath()}, diffFilePaths(diff.AddedDataFiles))
	require.Equal(t, []string{dataA.FilePath(), dataZ.FilePath()}, diffFilePaths(diff.RemovedDataFiles))
}

func TestDiffSnapshotFilesErrorsAndNoop(t *testing.T) {
	fs := iceio.NewMemFS()
	now := time.Now().UnixMilli()
	tbl := diffTestTable(t, 1, fs,
		Snapshot{SnapshotID: 1, TimestampMs: now, ManifestLocations: []string{}})

	_, err := tbl.Inspect().DiffSnapshotFiles(context.Background(), 99, 1)
	require.ErrorIs(t, err, ErrSnapshotNotFound)
	_, err = tbl.Inspect().DiffSnapshotFiles(context.Background(), 1, 99)
	require.ErrorIs(t, err, ErrSnapshotNotFound)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = tbl.Inspect().DiffSnapshotFiles(canceled, 1, 1)
	require.ErrorIs(t, err, context.Canceled)

	// Identical snapshot IDs are a no-op and must not need storage access.
	noIO := *tbl
	noIO.fsF = nil
	diff, err := noIO.Inspect().DiffSnapshotFiles(context.Background(), 1, 1)
	require.NoError(t, err)
	require.False(t, diff.HasChanges())
	require.Empty(t, diff.AddedDataFiles)

	// A missing referenced manifest is an error, not an incomplete diff.
	missing := diffTestTable(t, 1, fs,
		Snapshot{
			SnapshotID: 1, TimestampMs: now,
			ManifestLocations: []string{"mem://diff/missing.avro"},
		},
		Snapshot{
			SnapshotID: 2, TimestampMs: now + 1,
			ManifestLocations: []string{},
		})
	_, err = missing.Inspect().DiffSnapshotFiles(context.Background(), 1, 2)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrSnapshotNotFound))
	require.Contains(t, fmt.Sprint(err), "missing.avro")
}

func TestSnapshotDiffRejectsDuplicateLiveFiles(t *testing.T) {
	fs := iceio.NewMemFS()
	const prefix = "mem://diff/duplicate/"
	file := diffTestFile(t, prefix+"same.parquet", iceberg.EntryContentData)
	first := writeDiffTestManifest(t, fs, 1, prefix+"first.avro",
		iceberg.ManifestContentData, 1, []iceberg.DataFile{file}, nil, nil)
	second := writeDiffTestManifest(t, fs, 1, prefix+"second.avro",
		iceberg.ManifestContentData, 1, []iceberg.DataFile{file}, nil, nil)

	_, err := snapshotDiffLiveFiles(t.Context(), fs, []iceberg.ManifestFile{first, second})
	require.ErrorIs(t, err, ErrInvalidMetadata)
	require.ErrorContains(t, err, "duplicate live content file")
}

func TestSnapshotDiffRejectsPathSharedByDataAndDeleteFile(t *testing.T) {
	fs := iceio.NewMemFS()
	const prefix = "mem://diff/shared-path/"
	path := prefix + "shared.parquet"
	data := diffTestFile(t, path, iceberg.EntryContentData)
	deleteFile := diffTestFile(t, path, iceberg.EntryContentPosDeletes)

	dataManifest := writeDiffTestManifest(t, fs, 2, prefix+"data.avro",
		iceberg.ManifestContentData, 1, []iceberg.DataFile{data}, nil, nil)
	deleteManifest := writeDiffTestManifest(t, fs, 2, prefix+"delete.avro",
		iceberg.ManifestContentDeletes, 1, []iceberg.DataFile{deleteFile}, nil, nil)

	// A non-vector content file is identified by its path, even if a
	// malformed manifest claims a different content type for that path.
	_, err := snapshotDiffLiveFiles(t.Context(), fs, []iceberg.ManifestFile{dataManifest, deleteManifest})
	require.ErrorIs(t, err, ErrInvalidMetadata)
	require.ErrorContains(t, err, "duplicate live content file")
}

func TestSnapshotDiffRejectsChangedContentTypeAtSamePath(t *testing.T) {
	fs := iceio.NewMemFS()
	const prefix = "mem://diff/changed-content/"
	path := prefix + "shared.parquet"
	data := diffTestFile(t, path, iceberg.EntryContentData)
	deleteFile := diffTestFile(t, path, iceberg.EntryContentPosDeletes)
	old := writeDiffTestManifest(t, fs, 2, prefix+"old.avro",
		iceberg.ManifestContentData, 1, []iceberg.DataFile{data}, nil, nil)
	newManifest := writeDiffTestManifest(t, fs, 2, prefix+"new.avro",
		iceberg.ManifestContentDeletes, 2, []iceberg.DataFile{deleteFile}, nil, nil)
	fromList := writeDiffTestManifestList(t, fs, 2, prefix+"from.avro", 1, 1, []iceberg.ManifestFile{old})
	toList := writeDiffTestManifestList(t, fs, 2, prefix+"to.avro", 2, 2, []iceberg.ManifestFile{newManifest})
	now := time.Now().UnixMilli()
	tbl := diffTestTable(t, 2, fs,
		Snapshot{SnapshotID: 1, SequenceNumber: 1, TimestampMs: now, ManifestList: fromList},
		Snapshot{SnapshotID: 2, SequenceNumber: 2, TimestampMs: now + 1, ManifestList: toList})

	_, err := tbl.Inspect().DiffSnapshotFiles(t.Context(), 1, 2)
	require.ErrorIs(t, err, ErrInvalidMetadata)
	require.ErrorContains(t, err, "changed type")
}

func TestSnapshotDiffSkipsDeletedOnlyManifests(t *testing.T) {
	fs := iceio.NewMemFS()
	// The manifest does not exist on disk. Both live counts are known to
	// be zero, so opening it would cause an unnecessary I/O error.
	manifest := iceberg.NewManifestBuilder(2, "mem://diff/deleted-only.avro", 100, 0, 1).
		SequenceNumber(1, 1).AddedFiles(0).ExistingFiles(0).DeletedFiles(1).Build()
	files, err := snapshotDiffLiveFiles(t.Context(), fs, []iceberg.ManifestFile{manifest})
	require.NoError(t, err)
	require.Empty(t, files)
}

func TestSnapshotDiffRejectsDeletionVectorWithoutReferencedDataFile(t *testing.T) {
	for _, tc := range []struct {
		name              string
		setEmptyReference bool
	}{
		{name: "missing"},
		{name: "empty", setEmptyReference: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := iceio.NewMemFS()
			builder, err := iceberg.NewDataFileBuilder(
				*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
				"mem://diff/missing-ref.puffin", iceberg.PuffinFile, nil, nil, nil, 10, 1024)
			require.NoError(t, err)
			builder.ContentOffset(0).ContentSizeInBytes(32)
			if tc.setEmptyReference {
				builder.ReferencedDataFile("")
			}
			manifest := writeDiffTestManifest(t, fs, 3, "mem://diff/missing-ref.avro",
				iceberg.ManifestContentDeletes, 1, []iceberg.DataFile{builder.Build()}, nil, nil)

			_, err = snapshotDiffLiveFiles(t.Context(), fs, []iceberg.ManifestFile{manifest})
			require.ErrorIs(t, err, ErrInvalidMetadata)
			require.ErrorContains(t, err, "referenced_data_file")
		})
	}
}

func TestSnapshotDiffRejectsInvalidDeletionVectorRanges(t *testing.T) {
	for _, tc := range []struct {
		name   string
		offset *int64
		length *int64
	}{
		{name: "missing offset", length: new(int64(32))},
		{name: "missing length", offset: new(int64(0))},
		{name: "negative offset", offset: new(int64(-1)), length: new(int64(32))},
		{name: "zero length", offset: new(int64(0)), length: new(int64(0))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := iceio.NewMemFS()
			builder, err := iceberg.NewDataFileBuilder(
				*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
				"mem://diff/invalid.puffin", iceberg.PuffinFile, nil, nil, nil, 10, 1024)
			require.NoError(t, err)
			builder.ReferencedDataFile("mem://diff/data.parquet")
			if tc.offset != nil {
				builder.ContentOffset(*tc.offset)
			}
			if tc.length != nil {
				builder.ContentSizeInBytes(*tc.length)
			}
			manifest := writeDiffTestManifest(t, fs, 3, "mem://diff/invalid.avro",
				iceberg.ManifestContentDeletes, 1, []iceberg.DataFile{builder.Build()}, nil, nil)
			_, err = snapshotDiffLiveFiles(t.Context(), fs, []iceberg.ManifestFile{manifest})
			require.ErrorIs(t, err, ErrInvalidMetadata)
			require.ErrorContains(t, err, "invalid offset or length")
		})
	}
}

func TestSnapshotDiffLiveFilesCanceledBeforeReading(t *testing.T) {
	fs := iceio.NewMemFS()
	manifest := iceberg.NewManifestBuilder(2, "mem://diff/unreadable.avro", 10, 0, 1).
		SequenceNumber(1, 1).AddedFiles(1).Build()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := snapshotDiffLiveFiles(ctx, fs, []iceberg.ManifestFile{manifest})
	require.ErrorIs(t, err, context.Canceled)
}

func TestSnapshotDiffReadsV1ManifestsWithUnknownLiveCounts(t *testing.T) {
	fs := iceio.NewMemFS()
	const prefix = "mem://diff/v1-unknown-counts/"
	file := diffTestFile(t, prefix+"data.parquet", iceberg.EntryContentData)
	written := writeDiffTestManifest(t, fs, 1, prefix+"manifest.avro",
		iceberg.ManifestContentData, 1, []iceberg.DataFile{file}, nil, nil)

	for _, tt := range []struct {
		name            string
		added, existing int32
	}{
		{name: "both unknown", added: -1, existing: -1},
		{name: "added unknown", added: -1, existing: 0},
		{name: "existing unknown", added: 0, existing: -1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			manifest := iceberg.NewManifestBuilder(1, written.FilePath(), written.Length(),
				written.PartitionSpecID(), written.SnapshotID()).
				AddedFiles(tt.added).ExistingFiles(tt.existing).Build()

			files, err := snapshotDiffLiveFiles(t.Context(), fs, []iceberg.ManifestFile{manifest})
			require.NoError(t, err)
			require.Len(t, files, 1)
			for _, got := range files {
				require.Equal(t, file.FilePath(), got.FilePath())
			}
		})
	}
}

func TestSnapshotDiffLiveFilesOmitsColumnStats(t *testing.T) {
	fs := iceio.NewMemFS()
	const prefix = "mem://diff/projected-stats/"
	builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
		iceberg.EntryContentData, prefix+"data.parquet", iceberg.ParquetFile,
		nil, nil, nil, 10, 1024)
	require.NoError(t, err)
	original := builder.
		ColumnSizes(map[int]int64{1: 80}).
		ValueCounts(map[int]int64{1: 10}).
		NullValueCounts(map[int]int64{1: 0}).
		NaNValueCounts(map[int]int64{1: 0}).
		LowerBoundValues(map[int][]byte{1: {0x01}}).
		UpperBoundValues(map[int][]byte{1: {0x0a}}).
		SplitOffsets([]int64{4, 64}).Build()
	require.NotEmpty(t, original.ColumnSizes())
	manifest := writeDiffTestManifest(t, fs, 2, prefix+"manifest.avro",
		iceberg.ManifestContentData, 1, []iceberg.DataFile{original}, nil, nil)

	files, err := snapshotDiffLiveFiles(t.Context(), fs, []iceberg.ManifestFile{manifest})
	require.NoError(t, err)
	require.Len(t, files, 1)
	for _, got := range files {
		require.Equal(t, original.FilePath(), got.FilePath())
		require.Equal(t, original.Count(), got.Count())
		require.Equal(t, original.FileSizeBytes(), got.FileSizeBytes())
		require.Equal(t, []int64{4, 64}, got.SplitOffsets())
		require.Empty(t, got.ColumnSizes())
		require.Empty(t, got.ValueCounts())
		require.Empty(t, got.NullValueCounts())
		require.Empty(t, got.NaNValueCounts())
		require.Empty(t, got.LowerBoundValues())
		require.Empty(t, got.UpperBoundValues())
	}
}

func TestSnapshotDiffDuplicateErrorsFollowManifestOrder(t *testing.T) {
	fs := iceio.NewMemFS()
	const prefix = "mem://diff/parallel-duplicates/"
	first := diffTestFile(t, prefix+"duplicate-a.parquet", iceberg.EntryContentData)
	second := diffTestFile(t, prefix+"duplicate-b.parquet", iceberg.EntryContentData)
	manifests := make([]iceberg.ManifestFile, 0, 10)
	for index := range 10 {
		var file iceberg.DataFile
		switch index {
		case 0, 4:
			file = first
		case 1, 9:
			file = second
		default:
			file = diffTestFile(t, fmt.Sprintf("%sunique-%02d.parquet", prefix, index),
				iceberg.EntryContentData)
		}
		manifests = append(manifests, writeDiffTestManifest(t, fs, 2,
			fmt.Sprintf("%smanifest-%02d.avro", prefix, index),
			iceberg.ManifestContentData, 1, []iceberg.DataFile{file}, nil, nil))
	}

	// Ten manifests exceed the eight-reader limit. Regardless of completion
	// order, the first duplicate in manifest order must be reported.
	for range 10 {
		_, err := snapshotDiffLiveFiles(t.Context(), fs, manifests)
		require.ErrorIs(t, err, ErrInvalidMetadata)
		require.ErrorContains(t, err, "duplicate-a.parquet")
		require.ErrorContains(t, err, "manifest-04.avro")
	}
}

func TestDiffSnapshotFilesV3DeletionVectorLengthChange(t *testing.T) {
	fs := iceio.NewMemFS()
	const prefix = "mem://diff/v3-length/"
	puffin, dataPath := prefix+"deletes.puffin", prefix+"data.parquet"
	before := diffTestDeletionVector(t, puffin, dataPath, 100)
	builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
		iceberg.EntryContentPosDeletes, puffin, iceberg.PuffinFile,
		nil, nil, nil, 10, 1024)
	require.NoError(t, err)
	after := builder.ReferencedDataFile(dataPath).ContentOffset(100).ContentSizeInBytes(48).Build()

	oldManifest := writeDiffTestManifest(t, fs, 3, prefix+"old.avro",
		iceberg.ManifestContentDeletes, 1, []iceberg.DataFile{before}, nil, nil)
	newManifest := writeDiffTestManifest(t, fs, 3, prefix+"new.avro",
		iceberg.ManifestContentDeletes, 2, []iceberg.DataFile{after}, nil, nil)
	fromList := writeDiffTestManifestList(t, fs, 3, prefix+"from-list.avro", 1, 1,
		[]iceberg.ManifestFile{oldManifest})
	toList := writeDiffTestManifestList(t, fs, 3, prefix+"to-list.avro", 2, 2,
		[]iceberg.ManifestFile{newManifest})
	now := time.Now().UnixMilli()
	parent, zero := int64(1), int64(0)
	tbl := diffTestTable(t, 3, fs,
		Snapshot{SnapshotID: 1, SequenceNumber: 1, TimestampMs: now,
			ManifestList: fromList, FirstRowID: &zero, AddedRows: &zero},
		Snapshot{SnapshotID: 2, SequenceNumber: 2, TimestampMs: now + 1,
			ParentSnapshotID: &parent, ManifestList: toList, FirstRowID: &zero, AddedRows: &zero})

	diff, err := tbl.Inspect().DiffSnapshotFiles(t.Context(), 1, 2)
	require.NoError(t, err)
	require.True(t, diff.HasChanges())
	require.Empty(t, diff.AddedDataFiles)
	require.Empty(t, diff.RemovedDataFiles)
	require.Len(t, diff.AddedDeleteFiles, 1)
	require.Len(t, diff.RemovedDeleteFiles, 1)
	require.Equal(t, puffin, diff.AddedDeleteFiles[0].FilePath())
	require.Equal(t, puffin, diff.RemovedDeleteFiles[0].FilePath())
	require.Equal(t, int64(100), *diff.AddedDeleteFiles[0].ContentOffset())
	require.Equal(t, int64(100), *diff.RemovedDeleteFiles[0].ContentOffset())
	require.Equal(t, int64(48), *diff.AddedDeleteFiles[0].ContentSizeInBytes())
	require.Equal(t, int64(32), *diff.RemovedDeleteFiles[0].ContentSizeInBytes())
}
