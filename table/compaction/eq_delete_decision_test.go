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

package compaction_test

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDecideDeadEqualityDeletes_PredicateMatchesScanner exercises the
// pure decision logic against the scenarios that earlier code reviews
// flagged as silent-data-loss risks. The scanner predicate
// (table/scanner.go matchEqualityDeletesToData) is:
//
//	E applies to D iff E.seq > D.seq AND (
//	    len(E.partition) == 0 ||
//	    len(D.partition) == 0 ||
//	    partitionsMatch(E.partition, D.partition)
//	)
//
// Critically: SpecID is NOT part of the predicate, and either-side
// empty-partition makes it apply globally. The cleanup must agree.
func TestDecideDeadEqualityDeletes_PredicateMatchesScanner(t *testing.T) {
	type tc struct {
		name        string
		survivors   []survivor
		eqPart      map[int]any
		eqSeq       int64
		expectsDead bool
	}

	cases := []tc{
		{
			name:        "fully-rewritten-bucket: empty survey ⇒ dead",
			survivors:   nil,
			eqPart:      map[int]any{1000: "us"},
			eqSeq:       5,
			expectsDead: true,
		},
		{
			name: "partition match, surviving D has lower seq ⇒ alive",
			survivors: []survivor{
				{partition: map[int]any{1000: "us"}, seq: 3},
			},
			eqPart:      map[int]any{1000: "us"},
			eqSeq:       5,
			expectsDead: false,
		},
		{
			name: "partition match, surviving D has higher seq ⇒ dead",
			survivors: []survivor{
				{partition: map[int]any{1000: "us"}, seq: 9},
			},
			eqPart:      map[int]any{1000: "us"},
			eqSeq:       5,
			expectsDead: true,
		},
		{
			name: "untouched partition does NOT protect us-eq-delete",
			survivors: []survivor{
				{partition: map[int]any{1000: "eu"}, seq: 1},
			},
			eqPart:      map[int]any{1000: "us"},
			eqSeq:       5,
			expectsDead: true,
		},
		{
			name: "unpartitioned survivor protects partitioned eq-delete (cross-spec / global rule)",
			survivors: []survivor{
				{partition: nil, seq: 1},
				{partition: map[int]any{1000: "us"}, seq: 999},
			},
			eqPart:      map[int]any{1000: "us"},
			eqSeq:       5,
			expectsDead: false, // empty-part D with seq=1 < 5 keeps E alive
		},
		{
			name: "unpartitioned eq-delete: any partitioned survivor with low seq keeps it alive",
			survivors: []survivor{
				{partition: map[int]any{1000: "eu"}, seq: 2},
			},
			eqPart:      nil, // unpartitioned eq-delete
			eqSeq:       5,
			expectsDead: false,
		},
		{
			name: "unpartitioned eq-delete: every survivor has higher seq ⇒ dead",
			survivors: []survivor{
				{partition: map[int]any{1000: "us"}, seq: 7},
				{partition: map[int]any{1000: "eu"}, seq: 8},
			},
			eqPart:      nil,
			eqSeq:       5,
			expectsDead: true,
		},
		{
			name: "boundary: D.seq == E.seq ⇒ dead (strict-greater rule from scanner)",
			survivors: []survivor{
				{partition: map[int]any{1000: "us"}, seq: 5},
			},
			eqPart:      map[int]any{1000: "us"},
			eqSeq:       5,
			expectsDead: true,
		},
		{
			name: "defensive: candidate with seq < 0 is preserved (sentinel for unset)",
			survivors: []survivor{
				{partition: map[int]any{1000: "us"}, seq: 100},
			},
			eqPart:      map[int]any{1000: "us"},
			eqSeq:       -1,
			expectsDead: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			survey := compaction.NewSurvivorSurvey()
			for _, s := range c.survivors {
				survey.AddSurvivor(s.partition, s.seq)
			}
			candidate := makeEqDeleteEntry(t, c.eqPart, c.eqSeq, "/path/eq.parquet")

			got := compaction.DecideDeadEqualityDeletes(survey, []iceberg.ManifestEntry{candidate})

			if c.expectsDead {
				assert.Len(t, got, 1, "expected eq-delete to be classified dead")
			} else {
				assert.Empty(t, got, "expected eq-delete to be preserved (alive)")
			}
		})
	}
}

// TestDecideDeadEqualityDeletes_DedupesByPath verifies the executor
// only emits each dead eq-delete file once even if the same path
// appears in multiple manifest entries (post manifest-merging this
// can happen).
func TestDecideDeadEqualityDeletes_DedupesByPath(t *testing.T) {
	survey := compaction.NewSurvivorSurvey()

	a1 := makeEqDeleteEntry(t, nil, 5, "/eq-a.parquet")
	a2 := makeEqDeleteEntry(t, nil, 5, "/eq-a.parquet")
	b := makeEqDeleteEntry(t, nil, 5, "/eq-b.parquet")

	got := compaction.DecideDeadEqualityDeletes(survey, []iceberg.ManifestEntry{a1, a2, b})
	require.Len(t, got, 2)

	paths := []string{got[0].FilePath(), got[1].FilePath()}
	assert.Contains(t, paths, "/eq-a.parquet")
	assert.Contains(t, paths, "/eq-b.parquet")
}

// TestSurvivorSurvey_AddSurvivor_DefensiveSeq asserts that a survivor
// with sequence number < 0 is recorded as if seq=0 — guaranteeing it
// stays "alive" against every eq-delete.
func TestSurvivorSurvey_AddSurvivor_DefensiveSeq(t *testing.T) {
	survey := compaction.NewSurvivorSurvey()
	survey.AddSurvivor(nil, -1) // unset seq sentinel

	// An eq-delete with seq=1 should still be considered alive against
	// the seq=0-effective survivor.
	candidate := makeEqDeleteEntry(t, nil, 1, "/eq.parquet")
	got := compaction.DecideDeadEqualityDeletes(survey, []iceberg.ManifestEntry{candidate})
	assert.Empty(t, got, "negative-seq survivor must keep eq-delete alive")
}

type survivor struct {
	partition map[int]any
	seq       int64
}

// makeEqDeleteEntry constructs a real iceberg.ManifestEntry containing
// a real DataFile (built via NewDataFileBuilder). The pure predicate
// reads only path, partition, content type, and seq, so a minimal
// builder configuration is enough.
func makeEqDeleteEntry(t *testing.T, part map[int]any, seq int64, path string) iceberg.ManifestEntry {
	t.Helper()

	// Build a partition spec that matches the partition map's keys so
	// NewDataFileBuilder accepts the partition data. Partition values
	// are passed through to DataFile.Partition() unchanged.
	fields := make([]iceberg.PartitionField, 0, len(part))
	for id := range part {
		fields = append(fields, iceberg.PartitionField{
			SourceIDs: []int{id},
			FieldID:   id,
			Name:      "p" + intToStr(id),
			Transform: iceberg.IdentityTransform{},
		})
	}
	var spec iceberg.PartitionSpec
	if len(fields) == 0 {
		spec = *iceberg.UnpartitionedSpec
	} else {
		spec = iceberg.NewPartitionSpec(fields...)
	}

	builder, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentEqDeletes, path, iceberg.ParquetFile,
		part, nil, nil /* records */, 1 /* fileSize */, 128,
	)
	require.NoError(t, err)
	df := builder.Build()

	entryBuilder := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, nil, df)
	entryBuilder.SequenceNum(seq)

	return entryBuilder.Build()
}

func intToStr(n int) string {
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	if neg {
		digits = append([]byte{'-'}, digits...)
	}

	return string(digits)
}
