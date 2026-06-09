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
	"time"

	"github.com/stretchr/testify/require"
)

// A days() partition value can reach the encoder as a Go time.Time when a
// partition map is built directly rather than through Partition()'s typed
// decode. Both the manifest-entry encoder (codec) and the manifest writer must
// convert it to the int days the avro field expects rather than rejecting
// time.Time. Regression test for distributed compaction failing with "staging
// rewrite: avro: field data_file.partition.ts_day: cannot use time.Time with
// Avro type int" on bucket()/days()-partitioned tables.
func TestPartitionEncodeFromTimeTime(t *testing.T) {
	schema := NewSchema(0,
		NestedField{ID: 1, Name: "id", Type: PrimitiveTypes.Int64, Required: true},
		NestedField{ID: 2, Name: "ts", Type: PrimitiveTypes.Timestamp, Required: true},
	)
	spec := NewPartitionSpec(
		PartitionField{SourceIDs: []int{2}, FieldID: 1001, Transform: DayTransform{}, Name: "ts_day"},
	)
	day := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	const epochDays = int32(20455) // 2026-01-02

	b, err := NewDataFileBuilder(spec, EntryContentData,
		"s3://bucket/data/ts_day=2026-01-02/f.parquet", ParquetFile,
		map[int]any{1001: day}, nil, nil, 10, 100)
	require.NoError(t, err)
	df := b.Build()

	t.Run("codec", func(t *testing.T) {
		blob, err := df.(*dataFile).MarshalAvroEntry(spec, schema, 2)
		require.NoError(t, err, "MarshalAvroEntry must accept a date partition held as time.Time")
		decoded, err := unmarshalAvroDataFileEntry(blob, spec, schema, 2)
		require.NoError(t, err)
		require.EqualValues(t, epochDays, decoded.Partition()[1001])
	})

	t.Run("manifest_writer", func(t *testing.T) {
		var buf bytes.Buffer
		w, err := NewManifestWriter(2, &buf, spec, schema, 12345)
		require.NoError(t, err)
		snapshotID := int64(12345)
		seqNum := int64(1)
		require.NoError(t, w.Add(NewManifestEntry(EntryStatusADDED, &snapshotID, &seqNum, &seqNum, df)),
			"manifest writer must accept a date partition held as time.Time")
		require.NoError(t, w.Close())
	})
}
