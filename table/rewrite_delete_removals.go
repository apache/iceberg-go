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
	"context"
	"fmt"

	"github.com/apache/iceberg-go"
	iceberginternal "github.com/apache/iceberg-go/internal"
)

// removeAutomaticDeleteFile queues a delete file that became removable as a
// consequence of rewriting data files. These files are removed in the same
// snapshot, but they are not considered source files for an explicit delete
// file rewrite.
func (r *RewriteFiles) removeAutomaticDeleteFile(df iceberg.DataFile, deletionVector bool) {
	if r.err != nil {
		return
	}
	if df == nil {
		r.err = fmt.Errorf("%w: automatic delete removal got nil data file", ErrInvalidOperation)

		return
	}

	if df.ContentType() != iceberg.EntryContentPosDeletes || IsDeletionVector(df) != deletionVector {
		want := "position delete file"
		if deletionVector {
			want = "deletion vector"
		}
		r.err = fmt.Errorf("%w: automatic delete removal expected a %s for %q",
			ErrInvalidOperation, want, df.FilePath())

		return
	}
	r.autoDeleteFilesToRemove = append(r.autoDeleteFilesToRemove, df)
}

// automaticDeleteRemovals checks the worker candidates against the final rewrite set.
// When it selects a position-delete removal, it returns the snapshot used
// for that decision so staging can reject a change to the parent.
func (r *RewriteFiles) automaticDeleteRemovals(ctx context.Context) ([]iceberg.DataFile, *Snapshot, error) {
	if len(r.autoDeleteFilesToRemove) == 0 {
		return nil, nil, nil
	}

	rewrittenPaths := make(map[string]struct{}, len(r.dataFilesToDelete))
	for _, df := range r.dataFilesToDelete {
		rewrittenPaths[df.FilePath()] = struct{}{}
	}

	positionPaths := make(map[string]struct{})
	for _, df := range r.autoDeleteFilesToRemove {
		if !IsDeletionVector(df) {
			positionPaths[df.FilePath()] = struct{}{}
		}
	}
	var deadPositionPaths map[string]struct{}
	var checkedParent *Snapshot
	if len(rewrittenPaths) > 0 && len(positionPaths) > 0 {
		var err error
		deadPositionPaths, checkedParent, err = r.deadPositionDeletePaths(ctx, rewrittenPaths, positionPaths)
		if err != nil {
			return nil, nil, err
		}
		if len(deadPositionPaths) == 0 {
			checkedParent = nil
		}
	}

	seenDVs := make(map[string]deletionVectorBlobKey)
	retained := r.autoDeleteFilesToRemove[:0]
	for _, df := range r.autoDeleteFilesToRemove {
		switch {
		case IsDeletionVector(df):
			ref := iceberginternal.BorrowedDataFileReferencedDataFile(df)
			if ref == nil || *ref == "" {
				return nil, nil, fmt.Errorf("%w: automatic deletion vector %q has no referenced data file", ErrInvalidOperation, df.FilePath())
			}
			if _, rewritten := rewrittenPaths[*ref]; !rewritten {
				continue
			}
			key, err := snapshotFileDiffKey(df)
			if err != nil {
				return nil, nil, fmt.Errorf("%w: %w", ErrInvalidOperation, err)
			}
			if prior, seen := seenDVs[*ref]; seen {
				if prior != key {
					return nil, nil, fmt.Errorf("%w: conflicting automatic deletion vectors for data file %q", ErrInvalidOperation, *ref)
				}

				continue
			}
			seenDVs[*ref] = key
		default:
			path := df.FilePath()
			if _, dead := deadPositionPaths[path]; !dead {
				continue
			}
			delete(deadPositionPaths, path)
		}
		retained = append(retained, df)
	}

	return retained, checkedParent, nil
}

func (r *RewriteFiles) deadPositionDeletePaths(
	ctx context.Context, rewrittenPaths, expectedDeletePaths map[string]struct{},
) (map[string]struct{}, *Snapshot, error) {
	meta, err := r.txn.txnMeta()
	if err != nil {
		return nil, nil, err
	}
	snapshot := r.txn.planningSnapshot(meta)
	if snapshot == nil {
		return nil, nil, nil
	}
	fs, err := r.txn.tbl.fsF(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("open table IO for position-delete cleanup: %w", err)
	}
	dead, err := collectDeadPositionDeletes(ctx, fs, snapshot, rewrittenPaths, expectedDeletePaths)
	if err != nil {
		return nil, nil, fmt.Errorf("validate automatic position-delete removals: %w", err)
	}

	paths := make(map[string]struct{}, len(dead))
	for _, df := range dead {
		paths[df.FilePath()] = struct{}{}
	}

	return paths, snapshot, nil
}
