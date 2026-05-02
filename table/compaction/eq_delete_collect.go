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

// CollectDeadEqualityDeletes walks the given snapshot's manifests and
// returns the equality delete files that are provably dead after a
// rewrite removes the data files in rewrittenPaths.
//
// "Dead" means: no surviving data file in the snapshot (excluding
// rewrittenPaths) could ever apply to the eq-delete per the v2 reader
// predicate (see [DecideDeadEqualityDeletes] for the exact rule).
//
// rewrittenPaths is the union of every old data file path being
// replaced by a planned rewrite (across all groups). The caller
// typically computes this from a compaction plan.
//
// The returned files are safe to remove during the same commit that
// stages the rewrite — they cannot be resurrected by any concurrent
// commit because (1) any concurrent eq-delete is rejected by the
// rewrite-validator, and (2) any concurrent data file gets a fresh
// sequence number strictly greater than every preexisting eq-delete,
// so it cannot satisfy E.seq > D.seq for any preexisting E.
//
// Two-pass design: the first pass walks delete manifests only to
// discover candidate eq-deletes; if there are none, the more
// expensive data-manifest walk is skipped.
func CollectDeadEqualityDeletes(
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

	// First pass: delete manifests only, collecting eq-delete candidates.
	var candidates []iceberg.ManifestEntry
	for _, m := range manifests {
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		if m.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		entries, err := m.FetchEntries(fs, true)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if e.DataFile().ContentType() != iceberg.EntryContentEqDeletes {
				continue
			}
			candidates = append(candidates, e)
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	// Second pass: data manifests, building the survivor survey.
	survey := NewSurvivorSurvey()
	for _, m := range manifests {
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		if m.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		entries, err := m.FetchEntries(fs, true)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			df := e.DataFile()
			if df.ContentType() != iceberg.EntryContentData {
				continue
			}
			if _, beingRewritten := rewrittenPaths[df.FilePath()]; beingRewritten {
				continue
			}
			survey.AddSurvivor(df.Partition(), e.SequenceNum())
		}
	}

	return DecideDeadEqualityDeletes(survey, candidates), nil
}
