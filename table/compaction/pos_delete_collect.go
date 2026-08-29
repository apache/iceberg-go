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

package compaction

import (
	"context"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// CollectDeadPositionDeletes walks the given snapshot's manifests and returns
// the position-delete files made dead by a rewrite that removes rewrittenPaths.
// A dead position delete references only data files the rewrite is removing, so
// after the rewrite it applies to nothing and is safe to expunge in the same
// commit.
//
// Scope resolution mirrors Java ContentFileUtil.referencedDataFile and
// DeleteFileIndex:
//
//   - File-scoped — explicit referenced_data_file, or equal file_path
//     lower/upper bounds — is dead iff its single target is in rewrittenPaths.
//   - Partition-scoped — no single target — is dead iff no surviving
//     (non-rewritten) data file in the same (specID, partition) has a sequence
//     number <= the delete's. Such a survivor is one the delete still applies
//     to, so the delete must be retained for a later dangling-delete pass.
//
// Deletion vectors (Puffin position deletes) are intentionally excluded: they
// are 1:1 with their data file and expunged per-group via
// [table.CompactionGroupResult.SafeDeletionVectors].
//
// rewrittenPaths is the union of every old data file path being replaced across
// all rewrite groups. This is the whole-rewrite re-check that
// [table.CollectSafePositionDeletes]'s caller contract requires whenever
// multiple groups land in one rewrite snapshot; the result feeds
// [table.RewriteDataFilesOptions].ExtraDeleteFilesToRemove. Like
// [CollectDeadEqualityDeletesWithSpecs], the returned files are safe to remove in the
// same commit that stages the rewrite: a concurrent
// commit cannot resurrect them, because any concurrent delete touching a
// rewritten file is rejected by the rewrite validator and any concurrent data
// file gets a sequence number strictly greater than every preexisting delete.
//
// Two-pass design: the first pass walks delete manifests to resolve candidates,
// deciding file-scoped ones immediately; the more expensive data-manifest walk
// runs only when a partition-scoped candidate needs a survivor check.
func CollectDeadPositionDeletes(
	ctx context.Context,
	fs iceio.IO,
	snap *table.Snapshot,
	rewrittenPaths map[string]struct{},
) ([]iceberg.DataFile, error) {
	return table.CollectDeadPositionDeletes(ctx, fs, snap, rewrittenPaths)
}
