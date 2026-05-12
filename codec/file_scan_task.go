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

package codec

import (
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/twmb/avro"
)

// EncodeFileScanTask encodes a FileScanTask for cross-process transport.
// Each carried DataFile is encoded with [EncodeDataFile] and wrapped in
// a small record that also carries the scan range and v3 row lineage.
// The (spec, schema, version) triple must match what [DecodeFileScanTask]
// is given on the receiver.
func EncodeFileScanTask(task table.FileScanTask, spec iceberg.PartitionSpec, schema *iceberg.Schema, version int) ([]byte, error) {
	fileBytes, err := EncodeDataFile(task.File, spec, schema, version)
	if err != nil {
		return nil, fmt.Errorf("file: %w", err)
	}
	del, err := encodeDataFileSlice(task.DeleteFiles, spec, schema, version)
	if err != nil {
		return nil, fmt.Errorf("delete files: %w", err)
	}
	eq, err := encodeDataFileSlice(task.EqualityDeleteFiles, spec, schema, version)
	if err != nil {
		return nil, fmt.Errorf("equality delete files: %w", err)
	}
	dv, err := encodeDataFileSlice(task.DeletionVectorFiles, spec, schema, version)
	if err != nil {
		return nil, fmt.Errorf("deletion vector files: %w", err)
	}
	envelope := fileScanTaskEnvelope{
		File:                fileBytes,
		DeleteFiles:         del,
		EqualityDeleteFiles: eq,
		DeletionVectorFiles: dv,
		Start:               task.Start,
		Length:              task.Length,
		FirstRowID:          task.FirstRowID,
		DataSequenceNumber:  task.DataSequenceNumber,
	}

	return fileScanTaskSchema.Encode(&envelope)
}

// DecodeFileScanTask reverses [EncodeFileScanTask]. The triple
// (spec, schema, version) must match the encoder.
func DecodeFileScanTask(data []byte, spec iceberg.PartitionSpec, schema *iceberg.Schema, version int) (table.FileScanTask, error) {
	var envelope fileScanTaskEnvelope
	if _, err := fileScanTaskSchema.Decode(data, &envelope); err != nil {
		return table.FileScanTask{}, fmt.Errorf("decode: %w", err)
	}
	file, err := DecodeDataFile(envelope.File, spec, schema, version)
	if err != nil {
		return table.FileScanTask{}, fmt.Errorf("file: %w", err)
	}
	del, err := decodeDataFileSlice(envelope.DeleteFiles, spec, schema, version)
	if err != nil {
		return table.FileScanTask{}, fmt.Errorf("delete files: %w", err)
	}
	eq, err := decodeDataFileSlice(envelope.EqualityDeleteFiles, spec, schema, version)
	if err != nil {
		return table.FileScanTask{}, fmt.Errorf("equality delete files: %w", err)
	}
	dv, err := decodeDataFileSlice(envelope.DeletionVectorFiles, spec, schema, version)
	if err != nil {
		return table.FileScanTask{}, fmt.Errorf("deletion vector files: %w", err)
	}

	return table.FileScanTask{
		File:                file,
		DeleteFiles:         del,
		EqualityDeleteFiles: eq,
		DeletionVectorFiles: dv,
		Start:               envelope.Start,
		Length:              envelope.Length,
		FirstRowID:          envelope.FirstRowID,
		DataSequenceNumber:  envelope.DataSequenceNumber,
	}, nil
}

// fileScanTaskShape is a compile-time drift guard for FileScanTask.
// Go only permits struct conversion between two types that have
// identical underlying field sequences (names, types, and order; tags
// are ignored), so the var _ below fails to build the moment
// table.FileScanTask gains, loses, renames, retypes, or reorders a
// field. That forces a deliberate decision about whether the change
// must be carried by [EncodeFileScanTask] / [DecodeFileScanTask]; when
// extending, update fileScanTaskEnvelope, the schema JSON below, the
// encode/decode bodies, and this shape together.
type fileScanTaskShape struct {
	File                iceberg.DataFile
	DeleteFiles         []iceberg.DataFile
	EqualityDeleteFiles []iceberg.DataFile
	DeletionVectorFiles []iceberg.DataFile
	Start, Length       int64
	FirstRowID          *int64
	DataSequenceNumber  *int64
}

var _ = table.FileScanTask(fileScanTaskShape{})

// fileScanTaskEnvelope is the avro on-wire shape. The DataFile payloads
// (File and the three delete-file lists) are themselves [EncodeDataFile]
// bytes; this struct only frames them along with the scan range and v3
// lineage.
type fileScanTaskEnvelope struct {
	File                []byte   `avro:"file"`
	DeleteFiles         [][]byte `avro:"delete_files"`
	EqualityDeleteFiles [][]byte `avro:"equality_delete_files"`
	DeletionVectorFiles [][]byte `avro:"deletion_vector_files"`
	Start               int64    `avro:"start"`
	Length              int64    `avro:"length"`
	FirstRowID          *int64   `avro:"first_row_id"`
	DataSequenceNumber  *int64   `avro:"data_sequence_number"`
}

const fileScanTaskSchemaJSON = `{
  "type": "record",
  "name": "file_scan_task",
  "fields": [
    {"name": "file", "type": "bytes"},
    {"name": "delete_files", "type": {"type": "array", "items": "bytes"}},
    {"name": "equality_delete_files", "type": {"type": "array", "items": "bytes"}},
    {"name": "deletion_vector_files", "type": {"type": "array", "items": "bytes"}},
    {"name": "start", "type": "long"},
    {"name": "length", "type": "long"},
    {"name": "first_row_id", "type": ["null", "long"]},
    {"name": "data_sequence_number", "type": ["null", "long"]}
  ]
}`

var fileScanTaskSchema *avro.Schema

func init() {
	s, err := avro.Parse(fileScanTaskSchemaJSON)
	if err != nil {
		panic("codec: fileScanTaskSchema invalid: " + err.Error())
	}
	fileScanTaskSchema = s
}

func encodeDataFileSlice(files []iceberg.DataFile, spec iceberg.PartitionSpec, schema *iceberg.Schema, version int) ([][]byte, error) {
	if len(files) == 0 {
		return nil, nil
	}
	out := make([][]byte, 0, len(files))
	for i, f := range files {
		b, err := EncodeDataFile(f, spec, schema, version)
		if err != nil {
			return nil, fmt.Errorf("entry %d: %w", i, err)
		}
		out = append(out, b)
	}

	return out, nil
}

func decodeDataFileSlice(blobs [][]byte, spec iceberg.PartitionSpec, schema *iceberg.Schema, version int) ([]iceberg.DataFile, error) {
	if len(blobs) == 0 {
		return nil, nil
	}
	out := make([]iceberg.DataFile, 0, len(blobs))
	for i, b := range blobs {
		df, err := DecodeDataFile(b, spec, schema, version)
		if err != nil {
			return nil, fmt.Errorf("entry %d: %w", i, err)
		}
		out = append(out, df)
	}

	return out, nil
}
