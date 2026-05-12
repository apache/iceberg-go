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
// the per-(specID, version) avro schema is cached.
package codec

import (
	"fmt"

	"github.com/apache/iceberg-go"
)

// EncodeDataFile encodes a single DataFile for cross-process transport
// using the manifest-entry Avro encoding for the given partition spec,
// table schema and format version (1, 2, or 3). The wire format is the
// same one a manifest carries for this data file. The receiver MUST
// call [DecodeDataFile] with the matching (spec, schema, version)
// triple.
//
// df must be a DataFile produced by the iceberg-go package (for
// example via [iceberg.NewDataFileBuilder] or a manifest read).
// Foreign implementations are rejected.
//
// EncodeDataFile is non-mutating and safe to call concurrently with
// any other reader or encoder of the same DataFile.
//
// distinct_counts round-trips on v1 and v2. The v3 manifest-entry
// schema omits the field (deprecated in the v3 spec, see
// apache/iceberg#12182), so it does not survive encode→decode on v3 —
// callers on v3 that need distinct counts must transport them
// separately.
func EncodeDataFile(df iceberg.DataFile, spec iceberg.PartitionSpec, schema *iceberg.Schema, version int) ([]byte, error) {
	m, ok := df.(avroEntryMarshaler)
	if !ok {
		return nil, fmt.Errorf("codec: EncodeDataFile requires the iceberg package's DataFile implementation, got %T", df)
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
	return iceberg.UnmarshalAvroDataFileEntry(data, spec, schema, version)
}

// avroEntryMarshaler is the iceberg-package side of the DataFile codec.
// The iceberg package's DataFile implementation satisfies it; foreign
// implementations do not, which is how [EncodeDataFile] rejects them.
type avroEntryMarshaler interface {
	MarshalAvroEntry(spec iceberg.PartitionSpec, schema *iceberg.Schema, version int) ([]byte, error)
}
