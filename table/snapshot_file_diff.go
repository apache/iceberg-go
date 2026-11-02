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
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// SnapshotFileDiff describes the physical file changes between two snapshots.
// Data and delete files are reported separately; position-delete files and
// deletion vectors are both included in the delete-file slices.
//
// A rewrite that replaces data files may change these slices even when the
// logical table rows are unchanged. This is a file-level diff, not a row diff.
type SnapshotFileDiff struct {
	FromSnapshotID int64
	ToSnapshotID   int64

	AddedDataFiles     []iceberg.DataFile
	RemovedDataFiles   []iceberg.DataFile
	AddedDeleteFiles   []iceberg.DataFile
	RemovedDeleteFiles []iceberg.DataFile
}

// HasChanges reports whether either snapshot references physical files the
// other does not.
func (d SnapshotFileDiff) HasChanges() bool {
	return len(d.AddedDataFiles)+len(d.RemovedDataFiles)+
		len(d.AddedDeleteFiles)+len(d.RemovedDeleteFiles) != 0
}

// DiffSnapshotFiles compares the live files referenced by any two snapshots
// still present in the table's metadata. The snapshots need not be ancestors;
// for example, they may belong to different branches. The order of the
// arguments determines the direction of additions and removals.
//
// Iceberg manifests are immutable. Shared manifests therefore contain
// identical live entries and can be skipped without opening their Avro files.
// For the remaining manifests, ADDED and EXISTING entries are live; DELETED
// entries are tombstones and must not be included. Data-file paths identify
// immutable content, so a file rewritten into a different manifest does not
// count as added or removed. In v3, deletion vectors use the Puffin
// location, blob offset, and blob length as their identity.
//
// This works for v1 snapshots that embed manifest paths, as well as v1-v3
// snapshots with manifest lists. It reads only metadata and never reads the
// Parquet, Avro, or ORC data files.
func (t Table) DiffSnapshotFiles(ctx context.Context, fromSnapshotID, toSnapshotID int64) (SnapshotFileDiff, error) {
	diff := SnapshotFileDiff{
		FromSnapshotID: fromSnapshotID,
		ToSnapshotID:   toSnapshotID,
	}
	if err := ctx.Err(); err != nil {
		return diff, err
	}
	if t.metadata == nil {
		return diff, fmt.Errorf("%w: table metadata is not available", ErrInvalidMetadata)
	}

	from := t.metadata.SnapshotByID(fromSnapshotID)
	if from == nil {
		return diff, fmt.Errorf("%w: from snapshot %d", ErrSnapshotNotFound, fromSnapshotID)
	}
	to := t.metadata.SnapshotByID(toSnapshotID)
	if to == nil {
		return diff, fmt.Errorf("%w: to snapshot %d", ErrSnapshotNotFound, toSnapshotID)
	}
	if fromSnapshotID == toSnapshotID {
		return diff, nil
	}
	if t.fsF == nil {
		return diff, fmt.Errorf("%w: table file IO is not configured", ErrInvalidOperation)
	}

	fs, err := t.fsF(ctx)
	if err != nil {
		return diff, fmt.Errorf("resolve table file IO: %w", err)
	}
	if fs == nil {
		return diff, fmt.Errorf("%w: table file IO is nil", ErrInvalidOperation)
	}
	// Share the resolved FileIO between the two manifest-list loads and the
	// changed manifest entries. Keep the normal bounded manifest-list cache.
	fsF := func(context.Context) (iceio.IO, error) { return fs, nil }

	fromSet, err := t.manifestSetWithFSF(ctx, *from, fsF)
	if err != nil {
		return diff, fmt.Errorf("read from snapshot %d manifests: %w", fromSnapshotID, err)
	}
	toSet, err := t.manifestSetWithFSF(ctx, *to, fsF)
	if err != nil {
		return diff, fmt.Errorf("read to snapshot %d manifests: %w", toSnapshotID, err)
	}

	fromOnly, toOnly := uniqueSnapshotManifests(fromSet.borrowAllManifests(), toSet.borrowAllManifests())
	if len(fromOnly) == 0 && len(toOnly) == 0 {
		return diff, nil
	}

	fromFiles, err := snapshotDiffLiveFiles(ctx, fs, fromOnly)
	if err != nil {
		return diff, fmt.Errorf("read from snapshot %d entries: %w", fromSnapshotID, err)
	}
	toFiles, err := snapshotDiffLiveFiles(ctx, fs, toOnly)
	if err != nil {
		return diff, fmt.Errorf("read to snapshot %d entries: %w", toSnapshotID, err)
	}

	// A manifest rewrite can move the same data file into a different
	// manifest without modifying table contents. Cancel those paths out.
	for key := range fromFiles {
		if _, ok := toFiles[key]; ok {
			delete(fromFiles, key)
			delete(toFiles, key)
		}
	}

	for _, file := range fromFiles {
		if file.ContentType() == iceberg.EntryContentData {
			diff.RemovedDataFiles = append(diff.RemovedDataFiles, file)
		} else {
			diff.RemovedDeleteFiles = append(diff.RemovedDeleteFiles, file)
		}
	}
	for _, file := range toFiles {
		if file.ContentType() == iceberg.EntryContentData {
			diff.AddedDataFiles = append(diff.AddedDataFiles, file)
		} else {
			diff.AddedDeleteFiles = append(diff.AddedDeleteFiles, file)
		}
	}

	// Maps have unstable iteration order. Stable paths make the API useful
	// for reproducible audit logs and change-detection checkpoints.
	sortSnapshotDiffFiles(diff.AddedDataFiles)
	sortSnapshotDiffFiles(diff.RemovedDataFiles)
	sortSnapshotDiffFiles(diff.AddedDeleteFiles)
	sortSnapshotDiffFiles(diff.RemovedDeleteFiles)

	return diff, nil
}

// uniqueSnapshotManifests skips immutable manifests that are present in both
// snapshots. No ancestry walk is needed, and different branches work.
func uniqueSnapshotManifests(from, to []iceberg.ManifestFile) (fromOnly, toOnly []iceberg.ManifestFile) {
	fromPaths := make(map[string]struct{}, len(from))
	toPaths := make(map[string]struct{}, len(to))
	for _, manifest := range from {
		fromPaths[manifest.FilePath()] = struct{}{}
	}
	for _, manifest := range to {
		toPaths[manifest.FilePath()] = struct{}{}
		if _, shared := fromPaths[manifest.FilePath()]; !shared {
			toOnly = append(toOnly, manifest)
		}
	}
	for _, manifest := range from {
		if _, shared := toPaths[manifest.FilePath()]; !shared {
			fromOnly = append(fromOnly, manifest)
		}
	}

	return fromOnly, toOnly
}

// snapshotDiffFileKey uses the spec's identity for content files. A v3 Puffin
// file may hold several different deletion vector blobs, so its path alone
// is not enough to identify the vector.
type snapshotDiffFileKey struct {
	path    string
	content iceberg.ManifestEntryContent
	offset  int64
	length  int64
}

func snapshotFileDiffKey(file iceberg.DataFile) (snapshotDiffFileKey, error) {
	key := snapshotDiffFileKey{path: file.FilePath(), content: file.ContentType()}
	if IsDeletionVector(file) {
		offset := file.ContentOffset()
		length := file.ContentSizeInBytes()
		if offset == nil || length == nil {
			return key, fmt.Errorf("%w: deletion vector %q has no content offset or length",
				ErrInvalidMetadata, file.FilePath())
		}
		key.offset = *offset
		key.length = *length
	}

	return key, nil
}

func snapshotDiffLiveFiles(ctx context.Context, fs iceio.IO, manifests []iceberg.ManifestFile) (map[snapshotDiffFileKey]iceberg.DataFile, error) {
	files := make(map[snapshotDiffFileKey]iceberg.DataFile)
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for entry, err := range manifest.Entries(fs, true) {
			if err != nil {
				return nil, fmt.Errorf("manifest %q: %w", manifest.FilePath(), err)
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if entry == nil || entry.DataFile() == nil {
				return nil, fmt.Errorf("%w: manifest %q contains a nil data file", ErrInvalidMetadata, manifest.FilePath())
			}
			file := entry.DataFile()
			switch file.ContentType() {
			case iceberg.EntryContentData, iceberg.EntryContentPosDeletes, iceberg.EntryContentEqDeletes:
				key, err := snapshotFileDiffKey(file)
				if err != nil {
					return nil, fmt.Errorf("manifest %q: %w", manifest.FilePath(), err)
				}
				files[key] = file
			default:

				return nil, fmt.Errorf("%w: unknown content type %d in manifest %q", ErrInvalidMetadata,
					file.ContentType(), manifest.FilePath())
			}
		}
	}

	return files, nil
}

func sortSnapshotDiffFiles(files []iceberg.DataFile) {
	slices.SortFunc(files, func(a, b iceberg.DataFile) int {
		// Keys are validated while reading the manifest entries.
		ak, _ := snapshotFileDiffKey(a)
		bk, _ := snapshotFileDiffKey(b)

		return cmp.Or(
			cmp.Compare(ak.path, bk.path),
			cmp.Compare(ak.content, bk.content),
			cmp.Compare(ak.offset, bk.offset),
			cmp.Compare(ak.length, bk.length),
		)
	})
}
