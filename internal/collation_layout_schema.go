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

package internal

import "github.com/twmb/avro"

// This file holds the Avro schema fragment for the ACTIVE collation bounds
// layout. It is swapped together with collation_layout.go in the iceberg
// package: the two files are the only place where the on-disk shape of
// collation-aware bounds is defined.
//
// PROVISIONAL LAYOUT (pending the Iceberg format decision): a standalone,
// multi-version data_file.collation_stats map
//
//	map<field id, record{bounds: array<{collation, version, lower_bound, upper_bound}>}>
//
// This keeps the prototype functional while the spec settles between the inline
// content_stats layout and the derived-field layout; it is NOT a proposal.
//
// PROTOTYPE field IDs: 9000+ is a deliberately high, experimental range so the
// prototype does not collide with spec-reserved data_file field IDs (146 and up
// are reserved for the V4 content_stats struct, and the spec reserves field IDs
// globally across versions).
func collationStatsFields() []avro.SchemaField {
	entry := avro.SchemaNode{
		Type: "record",
		Name: "collation_bound_entry",
		Fields: []avro.SchemaField{
			fieldNode("collation", StringNode, 9005),
			fieldNode("version", StringNode, 9006),
			fieldNode("lower_bound", BytesNode, 9007),
			fieldNode("upper_bound", BytesNode, 9008),
		},
	}
	value := avro.SchemaNode{
		Type: "record",
		Name: "collation_column_stats",
		Fields: []avro.SchemaField{
			fieldNode("bounds",
				avro.SchemaNode{Type: "array", Items: &entry, Props: map[string]any{"element-id": 9004}},
				9003),
		},
	}

	return []avro.SchemaField{
		fieldNode("collation_stats",
			NullableNode(newMapNode("k9001_v9002", IntNode, value, 9001, 9002)),
			9000, withDefault(nil),
			withDoc("map of column id to multi-version collation-aware bounds (provisional layout)")),
	}
}
