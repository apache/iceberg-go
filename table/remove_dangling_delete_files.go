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
	"fmt"
	"math"

	"github.com/apache/iceberg-go"
)

type danglingDeleteCandidate struct {
	file     iceberg.DataFile
	sequence int64
}

type danglingDataFile struct {
	partition string
	sequence  int64
}

// PlanDanglingDeleteFiles identifies whole delete files that cannot affect
// any data file in the current snapshot. No physical files are read or removed.
//
// Position deletes apply at an equal or newer sequence; equality deletes
// require a strictly newer sequence. Unpartitioned equality deletes are global.
// Deletion vectors are omitted: v3 data-file replacement removes them.
func (t Table) PlanDanglingDeleteFiles(ctx context.Context) ([]iceberg.DataFile, error) {
	snap := t.CurrentSnapshot()
	if snap == nil || t.metadata.FormatVersion() < 2 || t.Spec().IsUnpartitioned() {
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

	minByPartition := make(map[string]int64)
	minAll := int64(math.MaxInt64)
	var candidates []danglingDeleteCandidate
	seenDeletes := make(map[string]struct{})
	// Only position deletes with an explicit reference need a path lookup.
	// Tracking those paths avoids retaining every live data-file path in memory.
	referencedPaths := make(map[string]struct{})

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
			if IsDeletionVector(df) || entry.DataSequenceNumber() < 0 {
				continue
			}
			if _, ok := seenDeletes[df.FilePath()]; ok {
				continue
			}
			seenDeletes[df.FilePath()] = struct{}{}
			candidates = append(candidates, danglingDeleteCandidate{
				file: df, sequence: entry.DataSequenceNumber(),
			})
			if df.ContentType() == iceberg.EntryContentPosDeletes {
				if target := explicitReferencedDataFile(df); target != "" {
					referencedPaths[target] = struct{}{}
				}
			}
		}
	}

	// A table with no eligible delete files needs no live-data index.
	// Read delete manifests first so metadata-only no-ops stay cheap.
	if len(candidates) == 0 {
		return nil, nil
	}

	// Collect the minimum data sequence by partition, but only retain file
	// details for paths that are explicitly referenced by position deletes.
	dataByPath := make(map[string]danglingDataFile, len(referencedPaths))
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if manifest.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		for entry, err := range manifest.Entries(fs, true) {
			if err != nil {
				return nil, err
			}
			df := entry.DataFile()
			if df.ContentType() != iceberg.EntryContentData {
				continue
			}
			key, err := canonicalPartitionKey(df.SpecID(), dataFilePartition(df))
			if err != nil {
				return nil, fmt.Errorf("data file %s: %w", df.FilePath(), err)
			}
			seq := max(int64(0), entry.DataSequenceNumber())
			if current, ok := minByPartition[key]; !ok || seq < current {
				minByPartition[key] = seq
			}
			minAll = min(minAll, seq)
			if _, referenced := referencedPaths[df.FilePath()]; referenced {
				dataByPath[df.FilePath()] = danglingDataFile{partition: key, sequence: seq}
			}
		}
	}

	return decideDanglingDeleteFiles(t.metadata, candidates, dataByPath, minByPartition, minAll)
}

func decideDanglingDeleteFiles(
	meta Metadata,
	candidates []danglingDeleteCandidate,
	dataByPath map[string]danglingDataFile,
	minByPartition map[string]int64,
	minAll int64,
) ([]iceberg.DataFile, error) {
	var dangling []iceberg.DataFile
	for _, candidate := range candidates {
		df := candidate.file
		spec := meta.PartitionSpecByID(int(df.SpecID()))
		if spec == nil {
			return nil, fmt.Errorf("delete file %s references unknown partition spec %d", df.FilePath(), df.SpecID())
		}
		key, err := canonicalPartitionKey(df.SpecID(), dataFilePartition(df))
		if err != nil {
			return nil, fmt.Errorf("delete file %s: %w", df.FilePath(), err)
		}
		minSeq, exists := minByPartition[key]

		switch df.ContentType() {
		case iceberg.EntryContentPosDeletes:
			// Only an explicit referenced_data_file narrows applicability to one path.
			// Equal file_path bounds are statistics, not a deletion scope.
			if target := explicitReferencedDataFile(df); target != "" {
				data, ok := dataByPath[target]
				if ok && data.partition == key && data.sequence <= candidate.sequence {
					continue
				}
			} else if exists && minSeq <= candidate.sequence {
				continue
			}
		case iceberg.EntryContentEqDeletes:
			if spec.IsUnpartitioned() {
				if minAll < candidate.sequence {
					continue
				}
			} else if exists && minSeq < candidate.sequence {
				continue
			}
		default:
			continue
		}
		dangling = append(dangling, df)
	}

	return dangling, nil
}

// RemoveDanglingDeleteFiles removes metadata references to whole dangling
// delete files in one replace snapshot. It never deletes the physical files,
// which may still be reachable through older snapshots. Returns the new table
// and number of removed delete-file references; no-op returns the input table.
//
// As in Java's removeDanglingDeleteFiles action, unpartitioned tables are
// skipped. Position-delete files containing both live and dangling records
// are retained and can be cleaned by a separate position-delete rewrite.
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
