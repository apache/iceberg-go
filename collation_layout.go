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

// This file holds the ACTIVE collation bounds layout. Together with
// internal/collation_layout_schema.go it is the only place where the on-disk
// shape of collation-aware bounds is defined; swapping both files swaps the
// layout without touching the rest of the code.
//
// PROVISIONAL LAYOUT (pending the Iceberg format decision): a standalone,
// multi-version data_file.collation_stats map
//
//	map<field id, record{bounds: array<{collation, version, lower_bound, upper_bound}>}>
//
// It keeps the prototype functional while the spec chooses between inline
// content_stats fields and derived COLLATE() fields; it is not a proposal.
// Code-point ranges are not persisted by this layout.

var collationLayout collationBoundsLayout = standaloneMapLayout{}

// collationStatsValue is the Avro value of the data_file.collation_stats map.
type collationStatsValue struct {
	Bounds []CollationBoundEntry `avro:"bounds"`
}

type standaloneMapLayout struct{}

func (standaloneMapLayout) name() string { return "standalone-map (provisional)" }

func (standaloneMapLayout) encode(d *dataFile, stats map[int]CollationColumnStats) {
	if len(stats) == 0 {
		return
	}

	values := make(map[int]collationStatsValue, len(stats))
	for id, s := range stats {
		values[id] = collationStatsValue{Bounds: s.Bounds}
	}
	d.CollationStatsData = mapToAvroColMap(values)
}

func (standaloneMapLayout) decode(d *dataFile) map[int]CollationColumnStats {
	if d.CollationStatsData == nil {
		return nil
	}

	out := make(map[int]CollationColumnStats, len(*d.CollationStatsData))
	for _, kv := range *d.CollationStatsData {
		out[kv.Key] = CollationColumnStats{Bounds: kv.Value.Bounds}
	}

	return out
}

func (standaloneMapLayout) tableMetadata([]CollationVersionRef) []byte { return nil }
