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
	"fmt"
	"maps"
	"math"
	"slices"

	"github.com/apache/iceberg-go"
)

// SurvivorSurvey describes the surviving data files in a snapshot
// AFTER a planned rewrite has logically removed its rewrite set. It
// is the input to [DecideDeadEqualityDeletes].
//
// EmptyPartMinSeq is the smallest sequence number among unpartitioned
// surviving data files. Per the Iceberg v2 reader predicate (see
// table/scanner.go matchEqualityDeletesToData), an unpartitioned data
// file applies to every equality delete — so it is always part of an
// eq-delete's "applicable survivors" minimum.
//
// PartMinSeq maps the partition tuple (encoded via partitionMatchKey)
// to the smallest sequence number among surviving partitioned data
// files in that tuple. The key intentionally does NOT include
// SpecID — the reader's predicate ignores SpecID, so the writer-side
// cleanup must too.
//
// Sentinel "no survivor in this bucket" is math.MaxInt64.
type SurvivorSurvey struct {
	EmptyPartMinSeq int64
	PartMinSeq      map[string]int64
}

// NewSurvivorSurvey returns a survey initialized with the no-survivor
// sentinel for the empty-partition bucket and an empty per-partition
// map. Callers populate via AddSurvivor.
func NewSurvivorSurvey() *SurvivorSurvey {
	return &SurvivorSurvey{
		EmptyPartMinSeq: math.MaxInt64,
		PartMinSeq:      make(map[string]int64),
	}
}

// AddSurvivor records a surviving data file's (partition, seq) into
// the survey. partition is the data file's partition tuple from
// iceberg.DataFile.Partition() (nil/empty for unpartitioned tables);
// seq is the data file's sequence number from the manifest entry.
//
// Defensive: if seq < 0 (sentinel for "unset"), the file is recorded
// with seq=0 (smallest real value), which keeps it permanently alive
// against every eq-delete. Better to preserve uncertain state than to
// drop deletes that may still apply.
//
// Pointer receiver: EmptyPartMinSeq is int64 (value type) and we need
// updates to persist. Callers must hold the survey by value or pointer
// consistently — typical pattern is `survey := NewSurvivorSurvey()`
// followed by `survey.AddSurvivor(...)` which Go auto-addresses.
func (s *SurvivorSurvey) AddSurvivor(partition map[int]any, seq int64) {
	if seq < 0 {
		seq = 0
	}
	if len(partition) == 0 {
		s.EmptyPartMinSeq = min(seq, s.EmptyPartMinSeq)

		return
	}
	key := partitionMatchKey(partition)
	if cur, ok := s.PartMinSeq[key]; ok {
		seq = min(seq, cur)
	}
	s.PartMinSeq[key] = seq
}

// applicableMinSeq returns the smallest surviving-D seq number that
// the eq-delete with the given partition could apply to. Mirrors the
// scanner's predicate: an unpartitioned surviving D applies to every
// E (so EmptyPartMinSeq is always in the min); an unpartitioned E
// applies to every surviving D (so we min across every PartMinSeq).
func (s *SurvivorSurvey) applicableMinSeq(eqPartition map[int]any) int64 {
	if len(eqPartition) == 0 {
		if len(s.PartMinSeq) == 0 {
			return s.EmptyPartMinSeq
		}

		return min(s.EmptyPartMinSeq, slices.Min(slices.Collect(maps.Values(s.PartMinSeq))))
	}
	if v, ok := s.PartMinSeq[partitionMatchKey(eqPartition)]; ok {
		return min(s.EmptyPartMinSeq, v)
	}

	return s.EmptyPartMinSeq
}

// DecideDeadEqualityDeletes is the pure spec-correctness predicate for
// equality-delete cleanup during compaction. Given a survey of
// surviving data file seqs (already excluding the rewrite set) and a
// list of equality-delete manifest entries, it returns the eq-delete
// files that no surviving data file could ever apply to.
//
// The rule is identical to scanner.matchEqualityDeletesToData (the
// reader-side filter):
//
//	E applies to D iff E.seq > D.seq AND (
//	    len(E.partition) == 0 ||
//	    len(D.partition) == 0 ||
//	    partitionsMatch(E.partition, D.partition)
//	)
//
// E is dead iff no applicable surviving D has D.seq < E.seq —
// equivalently, the applicable min-seq is >= E.seq.
//
// SpecID is intentionally NOT part of the predicate. The Iceberg-go
// reader does not consult it; if the executor used a stricter
// predicate it could drop eq-deletes the reader still applies, causing
// silent data loss under partition-spec evolution.
//
// Defensive: candidates with sequence number < 0 (sentinel for unset)
// are skipped — preserved rather than risk dropping an unidentifiable
// file.
//
// Dedup by file path: the same eq-delete file may appear in multiple
// manifest entries after manifest merging.
func DecideDeadEqualityDeletes(survey *SurvivorSurvey, candidates []iceberg.ManifestEntry) []iceberg.DataFile {
	if survey == nil || len(candidates) == 0 {
		return nil
	}

	dead := make([]iceberg.DataFile, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, e := range candidates {
		df := e.DataFile()
		path := df.FilePath()
		if _, ok := seen[path]; ok {
			continue
		}
		if e.SequenceNum() < 0 {
			continue
		}
		if survey.applicableMinSeq(df.Partition()) >= e.SequenceNum() {
			seen[path] = struct{}{}
			dead = append(dead, df)
		}
	}

	return dead
}

// partitionBucketKey returns a deterministic string key for a
// (specID, partition) tuple. Used by the planner to group tasks for
// bin-packing — different specs must NOT mix because a compacted
// output file inherits a single spec.
func partitionBucketKey(specID int32, part map[int]any) string {
	if len(part) == 0 {
		return fmt.Sprintf("%d:_", specID)
	}

	return string(appendPartitionTuple(fmt.Appendf(nil, "%d:", specID), part))
}

// partitionMatchKey returns a deterministic string key for a
// partition tuple alone (no spec id). Used by the eq-delete cleanup
// to bucket survivors and candidates by the same key the reader uses
// for applicability matching.
//
// Empty / nil partition → empty string sentinel. Callers must
// special-case empty partitions per the global-applicability rule;
// this helper does NOT collapse empty partitions into a fake
// per-spec bucket because that would break the reader-equivalence.
func partitionMatchKey(part map[int]any) string {
	if len(part) == 0 {
		return ""
	}

	return string(appendPartitionTuple(nil, part))
}

// appendPartitionTuple emits the sorted "id=value;" tuple into dst.
// Caller is responsible for any leading prefix (e.g. specID).
func appendPartitionTuple(dst []byte, part map[int]any) []byte {
	ids := make([]int, 0, len(part))
	for id := range part {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		dst = fmt.Appendf(dst, "%d=%v;", id, part[id])
	}

	return dst
}
