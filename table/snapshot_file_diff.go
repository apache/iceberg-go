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
	"iter"
	"slices"

	"golang.org/x/sync/errgroup"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// SnapshotFileDiff describes the physical file changes between two snapshots.
// Data and delete files are reported separately; position-delete files and
// equality deletes, and deletion vectors are included in the delete-file slices.
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
// snapshots with manifest lists. It reads only manifest metadata, omitting
// column statistics to bound memory use. Returned files must not be passed
// to a ManifestWriter because their statistics are incomplete. It never reads
// Parquet, Avro, or ORC data files.
func (i InspectTable) DiffSnapshotFiles(ctx context.Context, fromSnapshotID, toSnapshotID int64) (SnapshotFileDiff, error) {
	t := i.tbl
	diff := SnapshotFileDiff{
		FromSnapshotID: fromSnapshotID,
		ToSnapshotID:   toSnapshotID,
	}
	if err := ctx.Err(); err != nil {
		return diff, err
	}
	metadata := t.Metadata()
	from := metadata.SnapshotByID(fromSnapshotID)
	if from == nil {
		return diff, fmt.Errorf("%w: from snapshot %d", ErrSnapshotNotFound, fromSnapshotID)
	}
	to := metadata.SnapshotByID(toSnapshotID)
	if to == nil {
		return diff, fmt.Errorf("%w: to snapshot %d", ErrSnapshotNotFound, toSnapshotID)
	}
	if fromSnapshotID == toSnapshotID {
		return diff, nil
	}
	provider := t.ManifestProvider()
	fromManifests, err := provider.Manifests(ctx, *from)
	if err != nil {
		return diff, fmt.Errorf("read from snapshot %d manifests: %w", fromSnapshotID, err)
	}
	toManifests, err := provider.Manifests(ctx, *to)
	if err != nil {
		return diff, fmt.Errorf("read to snapshot %d manifests: %w", toSnapshotID, err)
	}

	fromOnly, toOnly := uniqueSnapshotManifests(fromManifests, toManifests)
	if len(fromOnly) == 0 && len(toOnly) == 0 {
		return diff, nil
	}

	fs, err := provider.FS(ctx)
	if err != nil {
		return diff, fmt.Errorf("resolve table file IO: %w", err)
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
	for key, fromFile := range fromFiles {
		if toFile, ok := toFiles[key]; ok {
			if fromFile.ContentType() != toFile.ContentType() {
				return diff, fmt.Errorf("%w: content file %q changed type between snapshots",
					ErrInvalidMetadata, key.path)
			}
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
func uniqueSnapshotManifests(from, to iter.Seq[iceberg.ManifestFile]) (fromOnly, toOnly []iceberg.ManifestFile) {
	fromPaths := make(map[string]struct{})
	fromManifests := make([]iceberg.ManifestFile, 0)
	for manifest := range from {
		fromPaths[manifest.FilePath()] = struct{}{}
		fromManifests = append(fromManifests, manifest)
	}

	toPaths := make(map[string]struct{})
	for manifest := range to {
		toPaths[manifest.FilePath()] = struct{}{}
		if _, shared := fromPaths[manifest.FilePath()]; !shared {
			toOnly = append(toOnly, manifest)
		}
	}
	// A manifest iterator may be single-use. Retain the descriptors in the
	// first pass so removals do not depend on replaying the input sequence.
	for _, manifest := range fromManifests {
		if _, shared := toPaths[manifest.FilePath()]; !shared {
			fromOnly = append(fromOnly, manifest)
		}
	}

	return fromOnly, toOnly
}

// snapshotFileDiffKey identifies regular content files by path and v3
// deletion vectors by Puffin path, blob offset, and length. Reuse the same
// key as transaction validation so both APIs agree about DV identity.
func snapshotFileDiffKey(file iceberg.DataFile) (deletionVectorBlobKey, error) {
	key := deletionVectorBlobKey{path: file.FilePath()}
	if key.path == "" {
		return key, fmt.Errorf("%w: content file path is empty", ErrInvalidMetadata)
	}
	if IsDeletionVector(file) {
		if ref := file.ReferencedDataFile(); ref == nil || *ref == "" {
			return key, fmt.Errorf("%w: deletion vector %q is missing referenced_data_file",
				ErrInvalidMetadata, file.FilePath())
		}
		offset, length := file.ContentOffset(), file.ContentSizeInBytes()
		if offset == nil || length == nil || *offset < 0 || *length <= 0 {
			return key, fmt.Errorf("%w: deletion vector %q has an invalid offset or length",
				ErrInvalidMetadata, file.FilePath())
		}
		key.offset, key.length = *offset, *length
	}

	return key, nil
}

// Read changed manifests in parallel, then merge in manifest order so
// duplicate-file diagnostics do not depend on goroutine scheduling.
func snapshotDiffLiveFiles(ctx context.Context, fs iceio.IO, manifests []iceberg.ManifestFile) (map[deletionVectorBlobKey]iceberg.DataFile, error) {
	const maxConcurrentManifests = 8

	manifestFiles := make([][]iceberg.DataFile, len(manifests))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(min(maxConcurrentManifests, len(manifests)))

	for i, manifest := range manifests {
		// Always call group.Wait even if an earlier worker failed or the
		// context was canceled. Otherwise readers could outlive this call.
		if groupCtx.Err() != nil {
			break
		}
		// Unknown v1 counts are treated as nonzero by these helpers.
		if !manifest.HasAddedFiles() && !manifest.HasExistingFiles() {
			continue
		}

		group.Go(func() error {
			for entry, err := range iceberg.EntriesWithProjection(groupCtx, fs, manifest, true, iceberg.ManifestEntryProjection{}) {
				if err != nil {
					return fmt.Errorf("manifest %q: %w", manifest.FilePath(), err)
				}
				if err := groupCtx.Err(); err != nil {
					return err
				}
				// ReadEntry already validates content types and returns a
				// non-nil entry with a non-nil data file on success.
				manifestFiles[i] = append(manifestFiles[i], entry.DataFile())
			}

			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	files := make(map[deletionVectorBlobKey]iceberg.DataFile)
	for i, manifest := range manifests {
		for _, file := range manifestFiles[i] {
			key, err := snapshotFileDiffKey(file)
			if err != nil {
				return nil, fmt.Errorf("manifest %q: %w", manifest.FilePath(), err)
			}
			if _, duplicate := files[key]; duplicate {
				return nil, fmt.Errorf("%w: duplicate live content file %q in manifest %q",
					ErrInvalidMetadata, file.FilePath(), manifest.FilePath())
			}
			files[key] = file
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
			cmp.Compare(ak.offset, bk.offset),
			cmp.Compare(ak.length, bk.length),
		)
	})
}
