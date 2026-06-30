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

// CollationBoundEntry is the collation-aware lower/upper bound for one column in
// one data file, following the Delta approach to collation statistics. It is
// both the on-disk Avro record (the data_file.collation_bounds map value) and
// the in-memory value consumed by the metrics evaluator.
//
//   - Lower and Upper hold the ORIGINAL string values (their normal binary
//     encoding), not ICU sort keys. Sort keys are not stable across ICU/CLDR
//     versions, so storing them would couple every reader to one exact
//     implementation; original values stay readable and let each engine compare
//     with its own collator.
//   - Collation and Version identify the collation and the implementation version
//     under which the min/max were selected. A reader must only trust the bound
//     when both match its own collation (see ValidFor), because a different
//     version may order values differently and thus pick a different min/max.
//
// Unlike the original UTF-8 lower/upper bounds (the byte-order min/max), these
// are the min/max under the column's COLLATION order, which can select entirely
// different values (e.g. for {"Banana","apple"} the byte-min is "Banana" but the
// case-insensitive min is "apple").
//
// Prototype note: the spec should generalize the data_file.collation_bounds map
// value to a LIST of these entries (Delta's validForCollations), letting one file
// carry bounds for several collation versions at once for smooth engine upgrades.
// This prototype stores a single entry per column.
type CollationBoundEntry struct {
	Collation string `avro:"collation"`
	Version   string `avro:"version"`
	Lower     []byte `avro:"lower_bound"`
	Upper     []byte `avro:"upper_bound"`
}

// ValidFor reports whether this bound may be used for collation-aware pruning
// under spec. It requires the collation identity and a non-empty version to
// match exactly: an unversioned or version-mismatched bound cannot be trusted to
// be the true min/max under the reader's collation. This is the mechanism that
// keeps pruning correct across ICU/CLDR version changes.
func (e CollationBoundEntry) ValidFor(spec *collation.Spec) bool {
	return e.Version != "" && spec.Version() == e.Version && e.Collation == spec.String()
}

// CollationBoundsProvider is an optional interface a DataFile may implement to
// expose collation-aware bounds (the Delta statsWithCollation analogue) keyed by
// field ID. The metrics evaluator consults it for collated columns; data files
// that don't implement it (or carry no collation bounds) are conservatively kept.
type CollationBoundsProvider interface {
	CollationBounds() map[int]CollationBoundEntry
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
