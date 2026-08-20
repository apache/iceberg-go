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

// timePartitionFieldID is the partition field id used by the time/timestamp
// logical-type tests.
const timePartitionFieldID = 1001

// writeTimePartitionManifest encodes a single-entry v2 manifest whose only
// partition field carries the given Avro schema node and integer value. The
// node's logical type is written verbatim to the on-disk schema, letting the
// test reproduce manifests from foreign writers that declare a time/timestamp
// logical type on an underlying Avro type the spec does not permit for it.
func writeTimePartitionManifest(t *testing.T, partFieldNode avro.SchemaNode, value any) ([]byte, ManifestFile) {
	t.Helper()

	field := PartitionField{
		FieldID:   timePartitionFieldID,
		SourceIDs: []int{1},
		Name:      "ts",
		Transform: IdentityTransform{},
	}
	spec := NewPartitionSpecID(0, field)

	builder, err := NewDataFileBuilder(
		spec,
		EntryContentData,
		"s3://bucket/namespace/table/data/00000-0-time.parquet",
		ParquetFile,
		map[int]any{timePartitionFieldID: value},
		map[int]string{},
		map[int]int{},
		100,
		1024,
	)
	require.NoError(t, err)

	snapshotID := int64(42)
	seqNum := int64(1)
	entry := NewManifestEntry(EntryStatusADDED, &snapshotID, &seqNum, &seqNum, builder.Build())

	partNode := avro.SchemaNode{
		Type: atype.Record,
		Name: "r102",
		Fields: []avro.SchemaField{
			{Name: "ts", Type: partFieldNode, Props: internal.WithFieldID(timePartitionFieldID)},
		},
	}
	partSchema, err := partNode.Schema()
	require.NoError(t, err)

	entrySchema, err := internal.NewManifestEntrySchema(partSchema, 2)
	require.NoError(t, err)

	specFieldsJSON, err := json.Marshal([]PartitionField{field})
	require.NoError(t, err)

	var buf bytes.Buffer
	wr, err := ocf.NewWriter(&buf, entrySchema,
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
		Path:    "s3://bucket/namespace/table/metadata/00000-time.avro",
		SpecID:  0,
		Content: ManifestContentData,
	}
	file.setVersion(2)

	return buf.Bytes(), file
}

// TestTimeLogicalPartitionScaling verifies that time-millis and
// timestamp-millis partition values decode to microseconds (iceberg.Time and
// iceberg.Timestamp both count microseconds), not milliseconds. See
// apache/iceberg-go#1847.
func TestTimeLogicalPartitionScaling(t *testing.T) {
	cases := []struct {
		name     string
		partNode avro.SchemaNode
		value    any
		want     any
	}{
		{
			name:     "time-micros",
			partNode: internal.NullableNode(internal.TimeNode),
			value:    int64(3_600_000_000), // 01:00:00 in microseconds
			want:     Time(3_600_000_000),
		},
		{
			name:     "time-millis",
			partNode: internal.NullableNode(avro.SchemaNode{Type: atype.Int, LogicalType: atype.TimeMillis}),
			value:    int32(3_600_000), // 01:00:00 in milliseconds
			want:     Time(3_600_000_000),
		},
		{
			name:     "timestamp-micros",
			partNode: internal.NullableNode(internal.TimestampNode),
			value:    int64(1_700_000_000_000_000),
			want:     Timestamp(1_700_000_000_000_000),
		},
		{
			name:     "timestamp-millis",
			partNode: internal.NullableNode(avro.SchemaNode{Type: atype.Long, LogicalType: atype.TimestampMillis, Props: map[string]any{"adjust-to-utc": false}}),
			value:    int64(1_700_000_000_000), // milliseconds
			want:     Timestamp(1_700_000_000_000_000),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, file := writeTimePartitionManifest(t, tc.partNode, tc.value)

			entries, err := ReadManifest(file, bytes.NewReader(data), false)
			require.NoError(t, err)
			require.Len(t, entries, 1)

			partition := entries[0].DataFile().Partition()
			got, ok := partition[timePartitionFieldID]
			require.True(t, ok, "partition value for field %d must be present", timePartitionFieldID)
			assert.IsType(t, tc.want, got)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestTimestampLogicalOnMismatchedTypeDoesNotPanic reproduces a manifest from a
// foreign writer that declares timestamp-millis on an Avro int, which the spec
// does not permit. twmb/avro drops the invalid logical type when decoding and
// yields the raw int32, yet Schema.Root still reports the logical type. Reading
// such a manifest must not panic (previously an unchecked v.(int64) assertion
// crashed here). See apache/iceberg-go#1847.
func TestTimestampLogicalOnMismatchedTypeDoesNotPanic(t *testing.T) {
	partNode := internal.NullableNode(avro.SchemaNode{Type: atype.Int, LogicalType: atype.TimestampMillis})

	data, file := writeTimePartitionManifest(t, partNode, int32(123456))

	entries, err := ReadManifest(file, bytes.NewReader(data), false)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	require.NotPanics(t, func() {
		partition := entries[0].DataFile().Partition()
		got, ok := partition[timePartitionFieldID]
		require.True(t, ok)
		// The raw millisecond value is scaled to microseconds.
		assert.Equal(t, Timestamp(int64(123456)*1000), got)
	})
}
