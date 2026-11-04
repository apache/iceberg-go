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
	"context"

	"github.com/apache/iceberg-go"
)

// danglingDeleteKey follows the scan-planning identity of a delete file.
// Regular position/equality deletes are identified by path. Multiple deletion
// vectors can share a Puffin file, so each vector needs its own blob identity.
type danglingDeleteKey struct {
	path       string
	referenced string
	offset     int64
	length     int64
	vector     bool
}

func keyForDanglingDelete(df iceberg.DataFile) danglingDeleteKey {
	key := danglingDeleteKey{path: df.FilePath()}
	if !IsDeletionVector(df) {
		return key
	}

	key.vector = true
	key.referenced = explicitReferencedDataFile(df)
	key.offset, key.length = -1, -1
	if offset := df.ContentOffset(); offset != nil {
		key.offset = *offset
	}
	if length := df.ContentSizeInBytes(); length != nil {
		key.length = *length
	}

	return key
}

// decideDanglingDeleteFiles mirrors Java's RemoveDanglingDeleteFilesAction:
// delete files that are referenced by at least one scan task are still live.
// This includes metrics pruning and DV supersession, not only partition and
// sequence checks. Do not inspect physical delete-file contents.
func decideDanglingDeleteFiles(candidates []iceberg.DataFile, tasks []FileScanTask) []iceberg.DataFile {
	referenced := make(map[danglingDeleteKey]struct{})
	for _, task := range tasks {
		for _, df := range task.DeleteFiles {
			referenced[keyForDanglingDelete(df)] = struct{}{}
		}
		for _, df := range task.EqualityDeleteFiles {
			referenced[keyForDanglingDelete(df)] = struct{}{}
		}
		for _, df := range task.DeletionVectorFiles {
			referenced[keyForDanglingDelete(df)] = struct{}{}
		}
	}

	var dangling []iceberg.DataFile
	for _, df := range candidates {
		if _, ok := referenced[keyForDanglingDelete(df)]; !ok {
			dangling = append(dangling, df)
		}
	}

	return dangling
}

// PlanDanglingDeleteFiles identifies delete files that are not attached to any
// data file by an unfiltered scan of the current snapshot. It reads metadata
// only; no physical data, delete files, or Puffin blobs are opened.
//
// Position and equality deletes use the scan planner's partition, sequence,
// and metrics checks. DVs are identified by their blob, not just Puffin path.
// Unknown delete sequence numbers and incomplete DV metadata are retained.
func (t Table) PlanDanglingDeleteFiles(ctx context.Context) ([]iceberg.DataFile, error) {
	snap := t.CurrentSnapshot()
	if snap == nil || t.metadata.FormatVersion() < 2 {
		return nil, nil
	}

	fs, err := t.FS(ctx)
	if err != nil {
		return nil, err
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, err
	}

	seen := make(map[danglingDeleteKey]struct{})
	var candidates []iceberg.DataFile
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if manifest.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		for entry, err := range manifest.Entries(fs, true) {
			if err != nil {
				return nil, err
			}
			df := entry.DataFile()
			if df.ContentType() != iceberg.EntryContentPosDeletes &&
				df.ContentType() != iceberg.EntryContentEqDeletes {
				continue
			}
			if entry.DataSequenceNumber() < 0 {
				continue
			}
			if IsDeletionVector(df) &&
				(df.ReferencedDataFile() == nil || df.ContentOffset() == nil || df.ContentSizeInBytes() == nil) {
				continue
			}

			key := keyForDanglingDelete(df)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			candidates = append(candidates, df)
		}
	}

	// No delete files means no scan planning, which matters for append-only tables.
	if len(candidates) == 0 {
		return nil, nil
	}

	scan := t.Scan()
	defer scan.Close()
	tasks, err := scan.PlanFiles(ctx)
	if err != nil {
		return nil, err
	}

	return decideDanglingDeleteFiles(candidates, tasks), nil
}

// RemoveDanglingDeleteFiles removes metadata references to whole dangling
// delete files in one replace snapshot. It never deletes the physical files,
// which may still be reachable through older snapshots. Returns the new table
// and number of removed delete-file references; no-op returns the input table.
//
// As in Java's removeDanglingDeleteFiles action, unpartitioned tables are
// eligible for cleanup. Position-delete files containing both live and dangling
// records are retained and can be cleaned by a separate position-delete rewrite.
func (t Table) RemoveDanglingDeleteFiles(ctx context.Context) (*Table, int, error) {
	candidates, err := t.PlanDanglingDeleteFiles(ctx)
	if err != nil {
		return nil, 0, err
	}
	if len(candidates) == 0 {
		return &t, 0, nil
	}

	txn := t.NewTransaction()
	if err := txn.AssertRefSnapshotID(""); err != nil {
		return nil, 0, err
	}
	rewrite := txn.NewRewrite(nil)
	for _, df := range candidates {
		rewrite.DeleteFile(df)
	}
	if err := rewrite.Commit(ctx); err != nil {
		return nil, 0, err
	}
	updated, err := txn.Commit(ctx)
	if err != nil {
		return nil, 0, err
	}

	return updated, len(candidates), nil
}
