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

// Package codec encodes and decodes iceberg-go values for cross-process
// transport. The bytes it produces are the same Avro bytes a manifest
// carries for the corresponding value, so callers transport iceberg
// internals without inventing a parallel wire schema.
//
// EncodeDataFile / DecodeDataFile move a single [iceberg.DataFile]
// using the manifest-entry encoding for a given partition spec, table
// schema, and format version.
//
// EncodeFileScanTask / DecodeFileScanTask layer on top: each embedded
// DataFile is encoded with [EncodeDataFile], then wrapped alongside the
// scan range and v3 row lineage in a small Avro envelope.
//
// The receiver supplies (spec, schema, version) out of band. Both sides
// in a distributed-processing design already hold table metadata, and
// the per-(partition-type, version) avro schema is cached.
package codec

import (
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal/datafileavro"
)

// EncodeDataFile encodes a single DataFile for cross-process transport
// using the manifest-entry Avro encoding for the given partition spec,
// table schema and format version (1, 2, or 3). The wire format is the
// same one a manifest carries for this data file. The receiver MUST
// call [DecodeDataFile] with the matching (spec, schema, version)
// triple.
//
// df must implement [iceberg.AvroEntryMarshaler]. The iceberg
// package's built-in DataFile implementation satisfies it; external
// implementations of [iceberg.DataFile] can opt in by implementing
// the marshaler interface themselves.
//
// EncodeDataFile is non-mutating and safe to call concurrently with
// any other reader or encoder of the same DataFile, provided the
// underlying implementation honors that contract.
//
// v1 note: v1 manifest entries carry a non-nullable snapshot_id which
// is written as 0 by the iceberg implementation. v1 bytes are not
// usable as a standalone manifest entry — they only round-trip via
// [DecodeDataFile].
//
// distinct_counts (field 111) is deprecated in the spec for all
// versions. Already-set values round-trip on v1 and v2 as a
// read-compatibility artifact; v3 omits the field entirely
// (apache/iceberg#12182). New DataFiles should not set distinct
// counts.
func EncodeDataFile(df iceberg.DataFile, spec iceberg.PartitionSpec, schema *iceberg.Schema, version int) ([]byte, error) {
	m, ok := df.(iceberg.AvroEntryMarshaler)
	if !ok {
		return nil, fmt.Errorf("codec: EncodeDataFile requires a DataFile implementing iceberg.AvroEntryMarshaler, got %T", df)
	}

	return m.MarshalAvroEntry(spec, schema, version)
}

// DecodeDataFile decodes bytes produced by [EncodeDataFile] back into a
// DataFile. The (spec, schema, version) triple must match the encoder;
// passing a different spec or version yields a decode error or silently
// mis-typed partition values.
//
// The returned DataFile carries the partition spec id and the field-id
// lookup tables, so Partition() and the stats accessors return id-keyed
// maps as if the file had been read from a manifest.
func DecodeDataFile(data []byte, spec iceberg.PartitionSpec, schema *iceberg.Schema, version int) (iceberg.DataFile, error) {
	res, err := datafileavro.Unmarshal(data, spec, schema, version)
	if err != nil {
		return nil, err
	}

	return res.(iceberg.DataFile), nil
}
