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
// of the two encodings that Iceberg engines produce in the wild.
func writeDayPartitionManifest(t *testing.T, partFieldNode avro.SchemaNode, dayValue int32) ([]byte, ManifestFile) {
	t.Helper()

	spec := NewPartitionSpecID(0, PartitionField{
		FieldID:   dayPartitionFieldID,
		SourceIDs: []int{1},
		Name:      "ts_day",
		Transform: DayTransform{},
	})

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

	var buf bytes.Buffer
	wr, err := ocf.NewWriter(&buf, entrySchema,
		// WithSchema writes the explicit JSON schema, preserving the field-id
		// props the manifest reader relies on to map partition values.
		ocf.WithSchema(entrySchema.String()),
		ocf.WithMetadata(map[string][]byte{
			"format-version": []byte("2"),
			"content":        []byte("data"),
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

// TestDayPartitionReadsAvroIntAndDate verifies that iceberg-go can read a
// day-transform partition value regardless of whether it was written as a plain
// Avro int or as an Avro int carrying the "date" logical type. Other Iceberg
// implementations (Java, PyIceberg, iceberg-rust) and engines (Trino, Spark)
// accept both encodings, and so must iceberg-go; a manifest normalized to the
// date encoding by one writer must remain readable here. See apache/iceberg#16414.
//
// The two encodings share the same physical layout (a zig-zag int), but the
// Avro decoder hands back a different Go type for each: a plain int decodes to
// int32, while a date-logical int decodes to time.Time, which the manifest
// reader converts to a Date. Both cases must surface the same logical day value.
func TestDayPartitionReadsAvroIntAndDate(t *testing.T) {
	const dayValue = int32(19000) // 2022-01-08

	cases := []struct {
		name     string
		partNode avro.SchemaNode
		want     any
	}{
		{
			name:     "avro int (no logical type)",
			partNode: internal.NullableNode(internal.IntNode),
			want:     dayValue,
		},
		{
			name:     "avro date (date logical type)",
			partNode: internal.NullableNode(internal.DateNode),
			want:     Date(dayValue),
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

			// The decoded Go type differs per encoding, but both represent the
			// same logical day; assert the concrete type and that it maps back
			// to the original day count.
			assert.IsType(t, tc.want, got)
			assert.EqualValues(t, dayValue, got)
		})
	}
}
