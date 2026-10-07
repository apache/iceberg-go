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

package iceberg

import "github.com/apache/iceberg-go/collation"

// CollationBoundEntry is one collation-aware lower/upper bound for one column in
// one data file, computed under one collation version. A column may carry
// several entries, one per collation version (see CollationColumnStats).
//
//   - Lower and Upper hold the ORIGINAL string values (their normal binary
//     encoding), not ICU sort keys. Sort keys are not stable across ICU/CLDR
//     versions, so storing them would couple every reader to one exact
//     implementation; original values stay readable and let each engine compare
//     with its own collator.
//   - Version identifies the collation implementation version under which the
//     min/max were selected. Collation identifies the collation itself; it is
//     empty when the on-disk layout does not persist it per entry, in which case
//     the field's declared collation applies.
//
// Unlike the original UTF-8 lower/upper bounds (the byte-order min/max), these
// are the min/max under the column's COLLATION order, which can select entirely
// different values (e.g. for {"Banana","apple"} the byte-min is "Banana" but the
// case-insensitive min is "apple").
type CollationBoundEntry struct {
	Collation string `avro:"collation"`
	Version   string `avro:"version"`
	Lower     []byte `avro:"lower_bound"`
	Upper     []byte `avro:"upper_bound"`
}

// ValidFor reports whether this bound may be used for collation-aware pruning
// under spec. It requires a non-empty version that matches exactly, and a
// matching collation when the entry records one: an unversioned or
// version-mismatched bound cannot be trusted to be the true min/max under the
// reader's collation. This is the mechanism that keeps pruning correct across
// ICU/CLDR version changes.
func (e CollationBoundEntry) ValidFor(spec *collation.Spec) bool {
	if e.Version == "" || spec.Version() != e.Version {
		return false
	}

	return e.Collation == "" || e.Collation == spec.String()
}

// CollationColumnStats holds the collation-aware statistics of one collated
// column in one data file. It is the format-neutral, in-memory model: how it is
// physically stored in a manifest is decided by the active collation bounds
// layout (see collationBoundsLayout), not by this type.
type CollationColumnStats struct {
	// MinCodePoint and MaxCodePoint are the smallest and largest Unicode code
	// points present in the column's values in the file, or nil if unknown.
	// Layouts that do not persist them drop them.
	MinCodePoint *int32
	MaxCodePoint *int32
	// Bounds holds one entry per collation version present in the file.
	Bounds []CollationBoundEntry
}

// BoundFor returns the first bound usable for pruning under spec.
func (s CollationColumnStats) BoundFor(spec *collation.Spec) (CollationBoundEntry, bool) {
	for _, e := range s.Bounds {
		if e.ValidFor(spec) {
			return e, true
		}
	}

	return CollationBoundEntry{}, false
}

// CollationBoundsProvider is an optional interface a DataFile may implement to
// expose collation-aware statistics keyed by field ID. The metrics evaluator
// consults it for collated columns; data files that don't implement it (or carry
// no collation stats) are conservatively kept.
type CollationBoundsProvider interface {
	CollationBounds() map[int]CollationColumnStats
}

// CollationVersionRef identifies one (column, collation, version) combination
// that a table carries collation bounds for.
type CollationVersionRef struct {
	FieldID   int
	Collation string
	Version   string
}

// collationBoundsLayout is the seam that decides how collation-aware statistics
// are physically stored in a manifest entry. The Iceberg spec has not yet settled
// the on-disk layout (inline content_stats fields vs derived COLLATE() fields
// reusing lower_bounds/upper_bounds), so everything layout-specific lives behind
// this interface, in collation_layout.go plus its Avro schema fragment in
// internal/collation_layout_schema.go. Swapping those two files swaps the layout;
// the rest of the code works on CollationColumnStats.
type collationBoundsLayout interface {
	// name is a short identifier of the layout.
	name() string
	// encode stores stats on d's Avro-mapped fields.
	encode(d *dataFile, stats map[int]CollationColumnStats)
	// decode reconstructs stats from d's Avro-mapped fields. It runs after the
	// generic column stats maps (lower/upper bounds etc.) are populated.
	decode(d *dataFile) map[int]CollationColumnStats
	// tableMetadata returns any per-table metadata the layout requires (for
	// example derived-field definitions in the table schema), serialized as JSON,
	// or nil if the layout needs none. It is reported separately because it is
	// paid once per table, not once per data file.
	tableMetadata(refs []CollationVersionRef) []byte
}

// CollationLayoutName returns the name of the active collation bounds layout.
func CollationLayoutName() string { return collationLayout.name() }

// CollationLayoutTableMetadata returns the per-table metadata the active layout
// needs for refs, serialized as JSON, or nil if it needs none.
func CollationLayoutTableMetadata(refs []CollationVersionRef) []byte {
	return collationLayout.tableMetadata(refs)
}

// ComputeCodePointRange returns the smallest and largest Unicode code points
// present in values. It returns ok=false for empty input or input with no runes.
func ComputeCodePointRange(values []string) (lo, hi int32, ok bool) {
	for _, v := range values {
		for _, r := range v {
			if !ok {
				lo, hi, ok = r, r, true

				continue
			}
			lo, hi = min(lo, r), max(hi, r)
		}
	}

	return lo, hi, ok
}

// ComputeCollatedBounds selects the collation-order minimum and maximum of
// values and returns them as an original-value bound entry tagged with spec's
// collation and version. It returns ok=false for a binary spec or empty input.
// This is the write-side counterpart consumed by collation-aware pruning.
func ComputeCollatedBounds(spec *collation.Spec, values []string) (CollationBoundEntry, bool) {
	if spec.IsBinary() || len(values) == 0 {
		return CollationBoundEntry{}, false
	}

	cmp := spec.Comparator()
	lo, hi := values[0], values[0]
	for _, v := range values[1:] {
		if cmp(v, lo) < 0 {
			lo = v
		}
		if cmp(v, hi) > 0 {
			hi = v
		}
	}

	encLo, err := StringLiteral(lo).MarshalBinary()
	if err != nil {
		return CollationBoundEntry{}, false
	}
	encHi, err := StringLiteral(hi).MarshalBinary()
	if err != nil {
		return CollationBoundEntry{}, false
	}

	// StringLiteral.MarshalBinary returns bytes that alias the input string's
	// backing memory; copy so the stored bounds can't be corrupted by later
	// reuse/mutation of the source buffer.
	return CollationBoundEntry{
		Collation: spec.String(),
		Version:   spec.Version(),
		Lower:     append([]byte(nil), encLo...),
		Upper:     append([]byte(nil), encHi...),
	}, true
}
