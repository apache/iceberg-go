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

package table

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/iceberg-go"
)

func TestSnapshotAsOf(t *testing.T) {
	baseTime := time.Now()

	// Create test snapshots with different timestamps
	snapshots := []Snapshot{
		{
			SnapshotID:     1000,
			TimestampMs:    baseTime.Add(1 * time.Hour).UnixMilli(), // 3 hours from now
			SequenceNumber: 1,
			ManifestList:   "s3://bucket/table/snap1.avro",
			Summary:        &Summary{Operation: OpAppend},
		},
		{
			SnapshotID:       2000,
			ParentSnapshotID: &[]int64{1000}[0],
			TimestampMs:      baseTime.Add(2 * time.Hour).UnixMilli(), // 2 hours from now
			SequenceNumber:   2,
			ManifestList:     "s3://bucket/table/snap2.avro",
			Summary:          &Summary{Operation: OpAppend},
		},
		{
			SnapshotID:       3000,
			ParentSnapshotID: &[]int64{2000}[0],
			TimestampMs:      baseTime.Add(3 * time.Hour).UnixMilli(), // 1 hour from now
			SequenceNumber:   3,
			ManifestList:     "s3://bucket/table/snap3.avro",
			Summary:          &Summary{Operation: OpDelete},
		},
	}

	snapshotLog := []SnapshotLogEntry{
		{SnapshotID: 1000, TimestampMs: baseTime.Add(1 * time.Hour).UnixMilli()},
		{SnapshotID: 2000, TimestampMs: baseTime.Add(2 * time.Hour).UnixMilli()},
		{SnapshotID: 3000, TimestampMs: baseTime.Add(3 * time.Hour).UnixMilli()},
	}

	// Create table with metadata from snapshots and log
	meta, err := createTestMetadata(snapshots, snapshotLog)
	require.NoError(t, err)

	table := Table{
		identifier: []string{"db", "table"},
		metadata:   meta,
	}

	t.Run("SnapshotAsOf finds exact timestamp match (inclusive)", func(t *testing.T) {
		timestamp := baseTime.Add(2 * time.Hour).UnixMilli()
		snapshot := table.SnapshotAsOf(timestamp, true)
		require.NotNil(t, snapshot)
		assert.Equal(t, int64(2000), snapshot.SnapshotID)
		assert.Equal(t, timestamp, snapshot.TimestampMs)
	})

	t.Run("SnapshotAsOf finds exact timestamp match (exclusive)", func(t *testing.T) {
		timestamp := baseTime.Add(2 * time.Hour).UnixMilli()
		snapshot := table.SnapshotAsOf(timestamp, false)
		require.NotNil(t, snapshot)
		assert.Equal(t, int64(1000), snapshot.SnapshotID) // Should get previous snapshot
	})

	t.Run("SnapshotAsOf finds snapshot before timestamp", func(t *testing.T) {
		// Query 90 minutes ago (between snapshots 2 and 3)
		timestamp := baseTime.Add(150 * time.Minute).UnixMilli()
		snapshot := table.SnapshotAsOf(timestamp, true)
		require.NotNil(t, snapshot)
		assert.Equal(t, int64(2000), snapshot.SnapshotID) // Should get snapshot 2
	})

	t.Run("SnapshotAsOf finds most recent snapshot for future timestamp", func(t *testing.T) {
		// Query future timestamp
		timestamp := baseTime.Add(4 * time.Hour).UnixMilli()
		snapshot := table.SnapshotAsOf(timestamp, true)
		require.NotNil(t, snapshot)
		assert.Equal(t, int64(3000), snapshot.SnapshotID) // Should get most recent
	})

	t.Run("SnapshotAsOf returns nil for timestamp before first snapshot", func(t *testing.T) {
		// Query before first snapshot
		timestamp := baseTime.Add(-1 * time.Hour).UnixMilli()
		snapshot := table.SnapshotAsOf(timestamp, true)
		assert.Nil(t, snapshot)
	})

	t.Run("SnapshotAsOf returns nil for timestamp equal to first snapshot (exclusive)", func(t *testing.T) {
		timestamp := baseTime.UnixMilli()
		snapshot := table.SnapshotAsOf(timestamp, false)
		assert.Nil(t, snapshot)
	})

	t.Run("SnapshotAsOf with inclusive=true (default behavior)", func(t *testing.T) {
		timestamp := baseTime.Add(2 * time.Hour).UnixMilli()
		snapshot := table.SnapshotAsOf(timestamp, true)
		require.NotNil(t, snapshot)
		assert.Equal(t, int64(2000), snapshot.SnapshotID)
	})
}

func TestTable_WithSnapshotAsOf(t *testing.T) {
	baseTime := time.Now()

	snapshots := []Snapshot{
		{
			SnapshotID:     1000,
			TimestampMs:    baseTime.Add(1 * time.Hour).UnixMilli(),
			SequenceNumber: 1,
			ManifestList:   "s3://bucket/table/snap1.avro",
			Summary:        &Summary{Operation: OpAppend},
		},
		{
			SnapshotID:     2000,
			TimestampMs:    baseTime.Add(2 * time.Hour).UnixMilli(),
			SequenceNumber: 2,
			ManifestList:   "s3://bucket/table/snap2.avro",
			Summary:        &Summary{Operation: OpAppend},
		},
	}

	snapshotLog := []SnapshotLogEntry{
		{SnapshotID: 1000, TimestampMs: baseTime.Add(1 * time.Hour).UnixMilli()},
		{SnapshotID: 2000, TimestampMs: baseTime.Add(2 * time.Hour).UnixMilli()},
	}

	meta, err := createTestMetadata(snapshots, snapshotLog)
	require.NoError(t, err)

	table := Table{
		identifier: []string{"db", "table"},
		metadata:   meta,
	}

	t.Run("WithSnapshotAsOf creates scan with correct snapshot ID", func(t *testing.T) {
		timestamp := baseTime.Add(90 * time.Minute).UnixMilli() // Between snapshots
		scan := table.Scan(WithSnapshotAsOf(timestamp))
		require.NotNil(t, scan)

		// Verify the scan has the correct timestamp
		assert.Equal(t, &timestamp, scan.asOfTimestamp)
	})

	t.Run("WithSnapshotAsOf with additional options", func(t *testing.T) {
		timestamp := baseTime.Add(30 * time.Minute).UnixMilli()
		scan := table.Scan(
			WithSnapshotAsOf(timestamp),
			WithSelectedFields("col1", "col2"),
			WithLimit(100),
		)
		require.NotNil(t, scan)

		// Verify timestamp and other options are set
		assert.Equal(t, &timestamp, scan.asOfTimestamp)
		assert.Equal(t, []string{"col1", "col2"}, scan.selectedFields)
		assert.Equal(t, int64(100), scan.limit)
	})

	t.Run("WithSnapshotAsOf returns error for timestamp with no snapshot during execution", func(t *testing.T) {
		timestamp := baseTime.UnixMilli() // Before first snapshot
		scan := table.Scan(WithSnapshotAsOf(timestamp))
		require.NotNil(t, scan)

		// Error should occur during scan planning, not creation
		_, err := scan.PlanFiles(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no snapshot found for timestamp")
	})

	t.Run("WithSnapshotID clears conflicting WithSnapshotAsOf option", func(t *testing.T) {
		timestamp := baseTime.Add(30 * time.Minute).UnixMilli()
		// WithSnapshotID should clear any previous asOfTimestamp
		scan := table.Scan(WithSnapshotAsOf(timestamp), WithSnapshotID(9999))
		require.NotNil(t, scan)

		// Should use snapshot ID, not timestamp
		assert.Equal(t, &[]int64{9999}[0], scan.snapshotID)
		assert.Nil(t, scan.asOfTimestamp)
	})
}

func TestSnapshotAsOfEdgeCases(t *testing.T) {
	t.Run("Empty snapshot log", func(t *testing.T) {
		meta, err := createTestMetadata(nil, nil)
		require.NoError(t, err)

		table := Table{
			identifier: []string{"db", "table"},
			metadata:   meta,
		}

		snapshot := table.SnapshotAsOf(time.Now().UnixMilli(), true)
		assert.Nil(t, snapshot)

		scan := table.Scan(WithSnapshotAsOf(time.Now().UnixMilli()))
		require.NotNil(t, scan)

		// Error should occur during scan planning
		_, planErr := scan.PlanFiles(context.Background())
		require.Error(t, planErr)
	})

	t.Run("Single snapshot", func(t *testing.T) {
		now := time.Now()
		snapshots := []Snapshot{
			{
				SnapshotID:     1000,
				TimestampMs:    now.UnixMilli(),
				SequenceNumber: 1,
				ManifestList:   "s3://bucket/table/snap1.avro",
			},
		}

		snapshotLog := []SnapshotLogEntry{
			{SnapshotID: 1000, TimestampMs: now.UnixMilli()},
		}

		meta, err := createTestMetadata(snapshots, snapshotLog)
		require.NoError(t, err)

		table := Table{
			identifier: []string{"db", "table"},
			metadata:   meta,
		}

		// Before snapshot
		snapshot := table.SnapshotAsOf(now.Add(-1*time.Hour).UnixMilli(), true)
		assert.Nil(t, snapshot)

		// At snapshot timestamp
		snapshot = table.SnapshotAsOf(now.UnixMilli(), true)
		require.NotNil(t, snapshot)
		assert.Equal(t, int64(1000), snapshot.SnapshotID)

		// After snapshot
		snapshot = table.SnapshotAsOf(now.Add(1*time.Hour).UnixMilli(), true)
		require.NotNil(t, snapshot)
		assert.Equal(t, int64(1000), snapshot.SnapshotID)
	})
}

// createTestMetadata creates metadata with custom snapshots and logs for testing
func createTestMetadata(snapshots []Snapshot, snapshotLog []SnapshotLogEntry) (Metadata, error) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	// Create basic metadata
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder,
		"s3://bucket/table", iceberg.Properties{})
	if err != nil {
		return nil, err
	}

	// If we have custom snapshots or logs, we need to modify the metadata
	if len(snapshots) > 0 || len(snapshotLog) > 0 {
		builder, err := MetadataBuilderFromBase(meta, "")
		if err != nil {
			return nil, err
		}

		// Add snapshots if provided
		for _, snapshot := range snapshots {
			if err = builder.AddSnapshot(&snapshot); err != nil {
				return nil, err
			}
		}

		// Manually set snapshot log entries by directly modifying the builder
		if len(snapshotLog) > 0 {
			builder.snapshotLog = append(builder.snapshotLog, snapshotLog...)
		}

		return builder.Build()
	}

	return meta, nil
}
