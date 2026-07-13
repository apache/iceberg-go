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
	"slices"

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
// [CollectDeadEqualityDeletes], the returned files are safe to remove in the
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
	if snap == nil || len(rewrittenPaths) == 0 {
		return nil, nil
	}

	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, err
	}

	// First pass: delete manifests, resolving each candidate's expunge scope.
	candidates, err := collectPositionDeleteCandidates(ctx, fs, manifests)
	if err != nil {
		return nil, err
	}

	// Second pass runs only when a partition-scoped candidate needs a survivor
	// check; file-scoped ones are decided from rewrittenPaths alone.
	var minSurvivorSeq map[string]int64
	if slices.ContainsFunc(candidates, func(c positionDeleteCandidate) bool { return c.fileScopedTarget == "" }) {
		minSurvivorSeq, err = minSurvivorSeqByPartition(ctx, fs, manifests, rewrittenPaths)
		if err != nil {
			return nil, err
		}
	}

	return decideDeadPositionDeletes(candidates, rewrittenPaths, minSurvivorSeq), nil
}

// positionDeleteCandidate is a classic position-delete file resolved to its
// expunge scope: fileScopedTarget is the single referenced data file for a
// file-scoped delete, or "" for a partition-scoped one, which is then keyed by
// partitionKey and gated by the delete's sequence number seq.
type positionDeleteCandidate struct {
	df               iceberg.DataFile
	fileScopedTarget string
	partitionKey     string
	seq              int64
}

// collectPositionDeleteCandidates walks the delete manifests and returns the
// classic position-delete files (deletion vectors excluded), deduplicated by
// path and resolved to their expunge scope.
func collectPositionDeleteCandidates(ctx context.Context, fs iceio.IO, manifests []iceberg.ManifestFile) ([]positionDeleteCandidate, error) {
	var candidates []positionDeleteCandidate
	seen := make(map[string]struct{})
	for _, m := range manifests {
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		if m.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		for e, err := range m.Entries(fs, true) {
			if err != nil {
				return nil, err
			}
			df := e.DataFile()
			if df.ContentType() != iceberg.EntryContentPosDeletes || isDeletionVector(df) {
				continue
			}
			path := df.FilePath()
			if _, ok := seen[path]; ok {
				continue
			}
			seen[path] = struct{}{}

			if target := referencedDataFilePath(df); target != "" {
				candidates = append(candidates, positionDeleteCandidate{df: df, fileScopedTarget: target})

				continue
			}

			// Partition-scoped. A negative sequence number is the "unset"
			// sentinel; retain rather than risk expunging a delete whose
			// applicability cannot be established.
			if seq := e.SequenceNum(); seq >= 0 {
				candidates = append(candidates, positionDeleteCandidate{
					df:           df,
					partitionKey: partitionBucketKey(df.SpecID(), df.Partition()),
					seq:          seq,
				})
			}
		}
	}

	return candidates, nil
}

// minSurvivorSeqByPartition walks the data manifests and returns, per (specID,
// partition), the smallest sequence number among surviving (non-rewritten) data
// files. A missing key means the partition has no survivor.
func minSurvivorSeqByPartition(ctx context.Context, fs iceio.IO, manifests []iceberg.ManifestFile, rewrittenPaths map[string]struct{}) (map[string]int64, error) {
	minSeq := make(map[string]int64)
	for _, m := range manifests {
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		if m.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		for e, err := range m.Entries(fs, true) {
			if err != nil {
				return nil, err
			}
			df := e.DataFile()
			if df.ContentType() != iceberg.EntryContentData {
				continue
			}
			if _, rewritten := rewrittenPaths[df.FilePath()]; rewritten {
				continue
			}
			seq := max(e.SequenceNum(), 0)
			key := partitionBucketKey(df.SpecID(), df.Partition())
			if cur, ok := minSeq[key]; ok {
				seq = min(seq, cur)
			}
			minSeq[key] = seq
		}
	}

	return minSeq, nil
}

// decideDeadPositionDeletes returns the candidates that are dead after the
// rewrite. A file-scoped delete is dead when its single target is rewritten. A
// partition-scoped delete is dead when no surviving data file in its partition
// predates it — position deletes apply when data.seq <= delete.seq, so a
// survivor with seq <= the delete's seq keeps it alive.
func decideDeadPositionDeletes(candidates []positionDeleteCandidate, rewrittenPaths map[string]struct{}, minSurvivorSeq map[string]int64) []iceberg.DataFile {
	dead := make([]iceberg.DataFile, 0, len(candidates))
	for _, c := range candidates {
		if c.fileScopedTarget != "" {
			if _, rewritten := rewrittenPaths[c.fileScopedTarget]; rewritten {
				dead = append(dead, c.df)
			}

			continue
		}
		if cur, ok := minSurvivorSeq[c.partitionKey]; !ok || cur > c.seq {
			dead = append(dead, c.df)
		}
	}

	return dead
}

// isDeletionVector reports whether df is a deletion vector: a Puffin file with
// position-delete content. Mirrors table.isDeletionVector, reimplemented here
// because that predicate is unexported.
func isDeletionVector(df iceberg.DataFile) bool {
	return df.FileFormat() == iceberg.PuffinFile &&
		df.ContentType() == iceberg.EntryContentPosDeletes
}
