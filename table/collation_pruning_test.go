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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/collation"
	"github.com/stretchr/testify/require"
)

// stringBound returns the binary encoding of a string min/max bound, as stored
// in a data file's lower/upper bound maps.
func stringBound(t *testing.T, s string) []byte {
	t.Helper()
	b, err := iceberg.StringLiteral(s).MarshalBinary()
	require.NoError(t, err)

	return b
}

// schemaWithName builds a single-string-column schema, optionally collated.
func schemaWithName(spec *collation.Spec) *iceberg.Schema {
	typ := iceberg.Type(iceberg.StringType{})
	if spec != nil {
		typ = iceberg.StringTypeWithCollation(spec)
	}

	return iceberg.NewSchema(
		0,
		iceberg.NestedField{ID: 1, Name: "name", Type: typ, Required: true},
	)
}

// TestCollationPruningSafety shows that data-file pruning must not use the
// stored UTF-8 byte-order bounds for a collated column, because collation order
// disagrees with byte order. For each case, a plain (binary) string column
// prunes the file — which would silently drop matching rows if the column were
// actually collated — while the collated column correctly keeps the file.
func TestCollationPruningSafety(t *testing.T) {
	ci := collation.MustParse("en_US-ci")

	cases := []struct {
		name string
		// file holds a single value (lower == upper) encoded as UTF-8 bounds.
		fileValue string
		expr      iceberg.BooleanExpression
	}{
		{
			// File contains only "apple". Under en_US-ci, name = 'APPLE' matches
			// it, but in byte order 'APPLE' < 'apple' so a binary evaluator drops
			// the file.
			name:      "equality APPLE vs apple",
			fileValue: "apple",
			expr:      iceberg.EqualTo(iceberg.Reference("name"), "APPLE"),
		},
		{
			// File contains only "a". Under en_US-ci, 'a' < 'B' so name < 'B'
			// matches, but in byte order 'a' (0x61) > 'B' (0x42) so a binary
			// evaluator drops the file.
			name:      "ordering a < B",
			fileValue: "a",
			expr:      iceberg.LessThan(iceberg.Reference("name"), "B"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bound := stringBound(t, tc.fileValue)
			file := &mockDataFile{
				path:        "file.parquet",
				format:      iceberg.ParquetFile,
				count:       1,
				valueCounts: map[int]int64{1: 1},
				nullCounts:  map[int]int64{1: 0},
				lowerBounds: map[int][]byte{1: bound},
				upperBounds: map[int][]byte{1: bound},
			}

			// Binary column: byte-order bounds are authoritative and the file is
			// pruned. (Correct for a genuinely binary column; catastrophic if the
			// column were collated and an engine ignored that.)
			binEval, err := newInclusiveMetricsEvaluator(schemaWithName(nil), tc.expr, true, true)
			require.NoError(t, err)
			binMatch, err := binEval(file)
			require.NoError(t, err)
			require.False(t, binMatch, "binary column should prune the file via byte-order bounds")

			// Collated column: byte-order bounds are not safe, so the file must be
			// kept (it actually contains a matching value under the collation).
			ciEval, err := newInclusiveMetricsEvaluator(schemaWithName(ci), tc.expr, true, true)
			require.NoError(t, err)
			ciMatch, err := ciEval(file)
			require.NoError(t, err)
			require.True(t, ciMatch, "collated column must keep the file; UTF-8 bounds cannot prune it")
		})
	}
}

// TestExplicitUtf8CollationStillPrunes confirms the guard is precise: an
// explicit utf8 collation is byte-order-equivalent, so pruning still applies.
func TestExplicitUtf8CollationStillPrunes(t *testing.T) {
	bound := stringBound(t, "apple")
	file := &mockDataFile{
		path:        "file.parquet",
		format:      iceberg.ParquetFile,
		count:       1,
		valueCounts: map[int]int64{1: 1},
		nullCounts:  map[int]int64{1: 0},
		lowerBounds: map[int][]byte{1: bound},
		upperBounds: map[int][]byte{1: bound},
	}

	// "zzz" > "apple" in byte order, so an equality predicate prunes.
	expr := iceberg.EqualTo(iceberg.Reference("name"), "zzz")
	eval, err := newInclusiveMetricsEvaluator(schemaWithName(collation.MustParse("utf8")), expr, true, true)
	require.NoError(t, err)
	match, err := eval(file)
	require.NoError(t, err)
	require.False(t, match, "utf8 collation is byte-order-safe and should still prune")
}

// collatedMockDataFile augments mockDataFile with Delta-style collation bounds.
type collatedMockDataFile struct {
	*mockDataFile
	collBounds map[int]iceberg.CollationBoundEntry
}

func (c *collatedMockDataFile) CollationBounds() map[int]iceberg.CollationBoundEntry {
	return c.collBounds
}

// fileWithCollatedValues builds a data file whose single column holds the given
// values, with collation-order min/max bounds tagged with the spec's version
// (the Delta write path) plus the real byte-order min/max bounds a normal writer
// would emit. The two disagree for collated data — which is the whole point —
// and the byte-order bounds are deliberately left intact to prove the evaluator
// ignores them for collated columns.
func fileWithCollatedValues(t *testing.T, spec *collation.Spec, values ...string) *collatedMockDataFile {
	t.Helper()
	entry, ok := iceberg.ComputeCollatedBounds(spec, values)
	require.True(t, ok)

	byteMin, byteMax := values[0], values[0]
	for _, v := range values[1:] {
		if v < byteMin {
			byteMin = v
		}
		if v > byteMax {
			byteMax = v
		}
	}

	return &collatedMockDataFile{
		mockDataFile: &mockDataFile{
			path:        "file.parquet",
			format:      iceberg.ParquetFile,
			count:       int64(len(values)),
			valueCounts: map[int]int64{1: int64(len(values))},
			nullCounts:  map[int]int64{1: 0},
			lowerBounds: map[int][]byte{1: stringBound(t, byteMin)},
			upperBounds: map[int][]byte{1: stringBound(t, byteMax)},
		},
		collBounds: map[int]iceberg.CollationBoundEntry{1: entry},
	}
}

// TestDeltaAlignedCollationPruning shows collation-aware pruning using original-
// value bounds selected under the collation order and gated by a matching
// collation version. For {"Banana","apple"} under en_US-ci the collation-order
// min/max are "apple"/"Banana" (not the byte-order "Banana"/"apple").
func TestDeltaAlignedCollationPruning(t *testing.T) {
	spec := collation.MustParse("en_US-ci").WithVersion("v1")
	sc := schemaWithName(spec)

	cases := []struct {
		name       string
		expr       iceberg.BooleanExpression
		mightMatch bool
	}{
		{"eq above range pruned", iceberg.EqualTo(iceberg.Reference("name"), "CHERRY"), false},
		{"eq in range kept (ci match)", iceberg.EqualTo(iceberg.Reference("name"), "BANANA"), true},
		{"eq min kept", iceberg.EqualTo(iceberg.Reference("name"), "apple"), true},
		{"less than min pruned", iceberg.LessThan(iceberg.Reference("name"), "A"), false},
		{"greater than max pruned", iceberg.GreaterThan(iceberg.Reference("name"), "cherry"), false},
		{"in with one match kept", iceberg.IsIn(iceberg.Reference("name"), "CHERRY", "banana"), true},
		{"in with no match pruned", iceberg.IsIn(iceberg.Reference("name"), "cherry", "date"), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			file := fileWithCollatedValues(t, spec, "Banana", "apple")
			eval, err := newInclusiveMetricsEvaluator(sc, tc.expr, true, true)
			require.NoError(t, err)
			match, err := eval(file)
			require.NoError(t, err)
			require.Equal(t, tc.mightMatch, match)
		})
	}
}

// TestCollationVersionMismatchKeepsFile shows the version gate: bounds computed
// under one collation version must not be trusted by a reader pinned to a
// different version, since ordering (and thus the selected min/max) may differ.
func TestCollationVersionMismatchKeepsFile(t *testing.T) {
	writeSpec := collation.MustParse("en_US-ci").WithVersion("v1")
	file := fileWithCollatedValues(t, writeSpec, "Banana", "apple")

	// Reader pins v2; bounds are tagged v1, so they are ignored and the file is
	// conservatively kept even though "CHERRY" would otherwise be pruned.
	readSpec := collation.MustParse("en_US-ci").WithVersion("v2")
	expr := iceberg.EqualTo(iceberg.Reference("name"), "CHERRY")
	eval, err := newInclusiveMetricsEvaluator(schemaWithName(readSpec), expr, true, true)
	require.NoError(t, err)
	match, err := eval(file)
	require.NoError(t, err)
	require.True(t, match, "version-mismatched collation bounds must not prune")
}

// TestUnversionedCollationKeepsFile shows that without a pinned version, bounds
// cannot be trusted (no stability guarantee), so the file is kept.
func TestUnversionedCollationKeepsFile(t *testing.T) {
	spec := collation.MustParse("en_US-ci") // no version
	file := fileWithCollatedValues(t, spec, "Banana", "apple")
	expr := iceberg.EqualTo(iceberg.Reference("name"), "CHERRY")
	eval, err := newInclusiveMetricsEvaluator(schemaWithName(spec), expr, true, true)
	require.NoError(t, err)
	match, err := eval(file)
	require.NoError(t, err)
	require.True(t, match, "unversioned collation bounds must not prune")
}

// TestVersionedBoundsUnversionedReaderKeepsFile covers the other direction of
// the gate: the file carries v1 bounds but the reader's schema pins no version,
// so the bounds are not trusted and the file is kept.
func TestVersionedBoundsUnversionedReaderKeepsFile(t *testing.T) {
	file := fileWithCollatedValues(t, collation.MustParse("en_US-ci").WithVersion("v1"), "Banana", "apple")
	readSpec := collation.MustParse("en_US-ci") // unversioned reader
	expr := iceberg.EqualTo(iceberg.Reference("name"), "CHERRY")
	eval, err := newInclusiveMetricsEvaluator(schemaWithName(readSpec), expr, true, true)
	require.NoError(t, err)
	match, err := eval(file)
	require.NoError(t, err)
	require.True(t, match, "versioned bounds must not prune for an unversioned reader")
}

// TestStrictEvaluatorConservativeForCollatedColumn shows the strict evaluator
// (used for delete/scan planning, where it must prove ALL rows match) never
// claims a match for an ordering predicate on a collated column, since byte-order
// bounds can't prove collation-order containment. It returns rowsMightNotMatch
// even where byte-order bounds would (wrongly) prove all rows match.
func TestStrictEvaluatorConservativeForCollatedColumn(t *testing.T) {
	spec := collation.MustParse("en_US-ci").WithVersion("v1")
	file := fileWithCollatedValues(t, spec, "apple", "apple")
	// Every value is "apple"; byte-order bounds would prove name <= "apple".
	expr := iceberg.LessThanEqual(iceberg.Reference("name"), "apple")
	eval, err := newStrictMetricsEvaluator(schemaWithName(spec), expr, true, true)
	require.NoError(t, err)
	allMatch, err := eval(file)
	require.NoError(t, err)
	require.False(t, allMatch, "strict eval must not claim all-match for a collated column")
}
