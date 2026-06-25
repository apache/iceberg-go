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

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/avro"
	"github.com/twmb/avro/atype"
	"github.com/twmb/avro/ocf"
)

// dayPartitionFieldID is the partition field id used by the day-transform tests.
const dayPartitionFieldID = 1000

// writeDayPartitionManifest encodes a single-entry v2 manifest whose only
// partition field ("ts_day", produced by a day transform) is serialized using
// partFieldNode. By varying partFieldNode between a plain Avro int and an Avro
// int with the "date" logical type, the same day value can be written in either
// of the two encodings that Iceberg engines produce in the wild. The manifest's
// partition spec is written to the Avro metadata exactly as a real writer would,
// so the reader can recognize the day transform.
func writeDayPartitionManifest(t *testing.T, partFieldNode avro.SchemaNode, dayValue int32) ([]byte, ManifestFile) {
	t.Helper()

	field := PartitionField{
		FieldID:   dayPartitionFieldID,
		SourceIDs: []int{1},
		Name:      "ts_day",
		Transform: DayTransform{},
	}
	spec := NewPartitionSpecID(0, field)

	builder, err := NewDataFileBuilder(
		spec,
		EntryContentData,
		"s3://bucket/namespace/table/data/00000-0-day.parquet",
		ParquetFile,
		map[int]any{dayPartitionFieldID: dayValue},
		map[int]string{},
		map[int]int{},
		100,
		1024,
	)
	require.NoError(t, err)

	snapshotID := int64(42)
	seqNum := int64(1)
	entry := NewManifestEntry(EntryStatusADDED, &snapshotID, &seqNum, &seqNum, builder.Build())

	// Build the partition record schema (r102) by hand so the test controls the
	// on-disk Avro encoding of the partition field, independent of how the
	// writer would model a day transform's result type.
	partNode := avro.SchemaNode{
		Type: atype.Record,
		Name: "r102",
		Fields: []avro.SchemaField{
			{Name: "ts_day", Type: partFieldNode, Props: internal.WithFieldID(dayPartitionFieldID)},
		},
	}
	partSchema, err := partNode.Schema()
	require.NoError(t, err)

	entrySchema, err := internal.NewManifestEntrySchema(partSchema, 2)
	require.NoError(t, err)

	// Real manifests carry the partition spec in their metadata; the reader uses
	// it to normalize day-transform values regardless of their Avro encoding.
	specFieldsJSON, err := json.Marshal([]PartitionField{field})
	require.NoError(t, err)

	var buf bytes.Buffer
	wr, err := ocf.NewWriter(&buf, entrySchema,
		// WithSchema writes the explicit JSON schema, preserving the field-id
		// props the manifest reader relies on to map partition values.
		ocf.WithSchema(entrySchema.String()),
		ocf.WithMetadata(map[string][]byte{
			"format-version":    []byte("2"),
			"content":           []byte("data"),
			"partition-spec":    specFieldsJSON,
			"partition-spec-id": []byte("0"),
		}))
	require.NoError(t, err)
	require.NoError(t, wr.Encode(entry))
	require.NoError(t, wr.Close())

	file := &manifestFile{
		Path:    "s3://bucket/namespace/table/metadata/00000-day.avro",
		SpecID:  0,
		Content: ManifestContentData,
	}
	file.setVersion(2)

	return buf.Bytes(), file
}

// TestDayPartitionReadsAvroIntAndDate verifies that a day-transform partition
// value is exposed consistently as iceberg.Date regardless of whether it was
// written as a plain Avro int or as an Avro int carrying the "date" logical
// type. The Iceberg spec gives the day transform result type "date" and
// requires readers to also accept the plain-int encoding as days since
// 1970-01-01 (transform table and note [1] in apache/iceberg#16446), so
// iceberg-go must not leak that Avro encoding difference to users: both must
// surface the same iceberg.Date for a day(...) field. See apache/iceberg-go#1200.
func TestDayPartitionReadsAvroIntAndDate(t *testing.T) {
	const dayValue = int32(19000) // 2022-01-08

	cases := []struct {
		name     string
		partNode avro.SchemaNode
	}{
		{
			name:     "avro int (no logical type)",
			partNode: internal.NullableNode(internal.IntNode),
		},
		{
			name:     "avro date (date logical type)",
			partNode: internal.NullableNode(internal.DateNode),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, file := writeDayPartitionManifest(t, tc.partNode, dayValue)

			entries, err := ReadManifest(file, bytes.NewReader(data), false)
			require.NoError(t, err)
			require.Len(t, entries, 1)

			partition := entries[0].DataFile().Partition()
			got, ok := partition[dayPartitionFieldID]
			require.True(t, ok, "partition value for field %d must be present", dayPartitionFieldID)

			// Both Avro encodings must be normalized to the same iceberg.Date,
			// matching the day(...) transform's logical day value.
			assert.IsType(t, Date(0), got)
			assert.EqualValues(t, dayValue, got)
		})
	}
}
