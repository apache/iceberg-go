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

	"github.com/DataDog/iceberg-go"
	"github.com/DataDog/iceberg-go/table/compaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDecideDeadEqualityDeletesWithSpecs_Predicate exercises the pure decision logic.
// The cleanup predicate is:
//
//	E applies to D iff E.seq > D.seq AND (
//	    E.spec.isUnpartitioned() ||
//	    (E.specID == D.specID && E.partition == D.partition)
//	)
func TestDecideDeadEqualityDeletesWithSpecs_Predicate(t *testing.T) {
	type tc struct {
		name        string
		survivors   []survivor
		eqSpecID    int32
		eqPart      map[int]any
		eqSeq       int64
		expectsDead bool
	}

	cases := []tc{
		{
			name:        "fully-rewritten-bucket: empty survey ⇒ dead",
			survivors:   nil,
			eqSpecID:    1,
			eqPart:      map[int]any{1000: "us"},
			eqSeq:       5,
			expectsDead: true,
		},
		{
			name: "partition match, surviving D has lower seq ⇒ alive",
			survivors: []survivor{
				{specID: 1, partition: map[int]any{1000: "us"}, seq: 3},
			},
			eqSpecID:    1,
			eqPart:      map[int]any{1000: "us"},
			eqSeq:       5,
			expectsDead: false,
		},
		{
			name: "partition match, surviving D has higher seq ⇒ dead",
			survivors: []survivor{
				{specID: 1, partition: map[int]any{1000: "us"}, seq: 9},
			},
			eqSpecID:    1,
			eqPart:      map[int]any{1000: "us"},
			eqSeq:       5,
			expectsDead: true,
		},
		{
			name: "untouched partition does NOT protect us-eq-delete",
			survivors: []survivor{
				{specID: 1, partition: map[int]any{1000: "eu"}, seq: 1},
			},
			eqSpecID:    1,
			eqPart:      map[int]any{1000: "us"},
			eqSeq:       5,
			expectsDead: true,
		},
		{
			name: "unpartitioned survivor does not protect partitioned eq-delete",
			survivors: []survivor{
				{specID: 0, partition: nil, seq: 1},
				{specID: 1, partition: map[int]any{1000: "us"}, seq: 999},
			},
			eqSpecID:    1,
			eqPart:      map[int]any{1000: "us"},
			eqSeq:       5,
			expectsDead: true,
		},
		{
			name: "same partition in a different spec does not protect partitioned eq-delete",
			survivors: []survivor{
				{specID: 3, partition: map[int]any{1000: "us"}, seq: 1},
			},
			eqSpecID:    1,
			eqPart:      map[int]any{1000: "us"},
			eqSeq:       5,
			expectsDead: true,
		},
		{
			name: "unpartitioned eq-delete: any partitioned survivor with low seq keeps it alive",
			survivors: []survivor{
				{specID: 1, partition: map[int]any{1000: "eu"}, seq: 2},
			},
			eqSpecID:    0,
			eqPart:      nil, // unpartitioned eq-delete
			eqSeq:       5,
			expectsDead: false,
		},
		{
			name: "unpartitioned eq-delete: every survivor has higher seq ⇒ dead",
			survivors: []survivor{
				{specID: 1, partition: map[int]any{1000: "us"}, seq: 7},
				{specID: 1, partition: map[int]any{1000: "eu"}, seq: 8},
			},
			eqSpecID:    0,
			eqPart:      nil,
			eqSeq:       5,
			expectsDead: true,
		},
		{
			name: "void-only eq-delete spec: differently partitioned survivor keeps it alive",
			survivors: []survivor{
				{specID: 1, partition: map[int]any{1000: "us"}, seq: 1},
			},
			eqSpecID:    2,
			eqPart:      map[int]any{1000: nil},
			eqSeq:       5,
			expectsDead: false,
		},
		{
			name: "boundary: D.seq == E.seq ⇒ dead (strict-greater rule from scanner)",
			survivors: []survivor{
				{specID: 1, partition: map[int]any{1000: "us"}, seq: 5},
			},
			eqSpecID:    1,
			eqPart:      map[int]any{1000: "us"},
			eqSeq:       5,
			expectsDead: true,
		},
		{
			name: "defensive: candidate with seq < 0 is preserved (sentinel for unset)",
			survivors: []survivor{
				{specID: 1, partition: map[int]any{1000: "us"}, seq: 100},
			},
			eqSpecID:    1,
			eqPart:      map[int]any{1000: "us"},
			eqSeq:       -1,
			expectsDead: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			specs := compactionTestSpecs()
			survey := compaction.NewSurvivorSurvey()
			for _, s := range c.survivors {
				survey.AddSurvivorWithSpec(s.specID, s.partition, s.seq)
			}
			candidate := makeEqDeleteEntry(
				t, specs[c.eqSpecID], c.eqPart, c.eqSeq, "/path/eq.parquet")

			got, err := compaction.DecideDeadEqualityDeletesWithSpecs(
				survey, []iceberg.ManifestEntry{candidate}, specs)
			require.NoError(t, err)

			if c.expectsDead {
				assert.Len(t, got, 1, "expected eq-delete to be classified dead")
			} else {
				assert.Empty(t, got, "expected eq-delete to be preserved (alive)")
			}
		})
	}
}

// TestDecideDeadEqualityDeletesWithSpecs_DedupesByPath verifies the executor
// only emits each dead eq-delete file once even if the same path
// appears in multiple manifest entries (post manifest-merging this
// can happen).
func TestDecideDeadEqualityDeletesWithSpecs_DedupesByPath(t *testing.T) {
	specs := compactionTestSpecs()
	survey := compaction.NewSurvivorSurvey()

	a1 := makeEqDeleteEntry(t, specs[0], nil, 5, "/eq-a.parquet")
	a2 := makeEqDeleteEntry(t, specs[0], nil, 5, "/eq-a.parquet")
	b := makeEqDeleteEntry(t, specs[0], nil, 5, "/eq-b.parquet")

	got, err := compaction.DecideDeadEqualityDeletesWithSpecs(
		survey, []iceberg.ManifestEntry{a1, a2, b}, specs)
	require.NoError(t, err)
	require.Len(t, got, 2)

	paths := []string{got[0].FilePath(), got[1].FilePath()}
	assert.Contains(t, paths, "/eq-a.parquet")
	assert.Contains(t, paths, "/eq-b.parquet")
}

func TestDecideDeadEqualityDeletesWithSpecs_RejectsUnknownPartitionSpec(t *testing.T) {
	specs := compactionTestSpecs()
	candidate := makeEqDeleteEntry(
		t, specs[1], map[int]any{1000: "us"}, 5, "/eq.parquet")

	_, err := compaction.DecideDeadEqualityDeletesWithSpecs(
		compaction.NewSurvivorSurvey(),
		[]iceberg.ManifestEntry{candidate},
		compactionTestSpecLookup{},
	)

	assert.ErrorContains(t, err, "partition spec ID 1 not found")
}

func TestDecideDeadEqualityDeletesConservativelyMatchesAcrossSpecs(t *testing.T) {
	specs := compactionTestSpecs()
	partition := map[int]any{1000: "us"}
	survey := compaction.NewSurvivorSurvey()
	survey.AddSurvivor(partition, 1)

	// The compatibility API has no survivor spec ID, so it must preserve a
	// delete whose partition tuple may match even when its spec differs.
	candidate := makeEqDeleteEntry(t, specs[3], partition, 5, "/eq.parquet")

	assert.Empty(t, compaction.DecideDeadEqualityDeletes(
		survey, []iceberg.ManifestEntry{candidate}))

	// Without a lookup, the compatibility API must also allow for a void-only
	// candidate spec, whose non-empty partition tuple still applies globally.
	voidCandidate := makeEqDeleteEntry(t, specs[2], map[int]any{1000: nil}, 5, "/void-eq.parquet")
	assert.Empty(t, compaction.DecideDeadEqualityDeletes(
		survey, []iceberg.ManifestEntry{voidCandidate}))
}

// TestSurvivorSurvey_AddSurvivor_DefensiveSeq asserts that a survivor
// with sequence number < 0 is recorded as if seq=0 — guaranteeing it
// stays "alive" against every eq-delete.
func TestSurvivorSurvey_AddSurvivor_DefensiveSeq(t *testing.T) {
	specs := compactionTestSpecs()
	survey := compaction.NewSurvivorSurvey()
	survey.AddSurvivorWithSpec(0, nil, -1) // unset seq sentinel

	// An eq-delete with seq=1 should still be considered alive against
	// the seq=0-effective survivor.
	candidate := makeEqDeleteEntry(t, specs[0], nil, 1, "/eq.parquet")
	got, err := compaction.DecideDeadEqualityDeletesWithSpecs(
		survey, []iceberg.ManifestEntry{candidate}, specs)
	require.NoError(t, err)
	assert.Empty(t, got, "negative-seq survivor must keep eq-delete alive")
}

type survivor struct {
	specID    int32
	partition map[int]any
	seq       int64
}

type compactionTestSpecLookup map[int32]iceberg.PartitionSpec

func (s compactionTestSpecLookup) PartitionSpecByID(id int) *iceberg.PartitionSpec {
	spec, ok := s[int32(id)]
	if !ok {
		return nil
	}

	return &spec
}

func compactionTestSpecs() compactionTestSpecLookup {
	partitionedField := func() iceberg.PartitionField {
		return iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000, Name: "partition",
			Transform: iceberg.IdentityTransform{},
		}
	}

	return compactionTestSpecLookup{
		0: iceberg.NewPartitionSpecID(0),
		1: iceberg.NewPartitionSpecID(1, partitionedField()),
		2: iceberg.NewPartitionSpecID(2, iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000, Name: "void",
			Transform: iceberg.VoidTransform{},
		}),
		3: iceberg.NewPartitionSpecID(3, partitionedField()),
	}
}

// makeEqDeleteEntry constructs a real iceberg.ManifestEntry containing
// a real DataFile (built via NewDataFileBuilder). The pure predicate
// reads only path, partition, content type, and seq, so a minimal
// builder configuration is enough.
func makeEqDeleteEntry(
	t *testing.T,
	spec iceberg.PartitionSpec,
	part map[int]any,
	seq int64,
	path string,
) iceberg.ManifestEntry {
	t.Helper()

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
