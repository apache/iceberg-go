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

	"github.com/DataDog/iceberg-go"
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

func TestSnapshotAsOfWithOutOfOrderSnapshotLog(t *testing.T) {
	baseTime := time.Now().Add(5 * time.Second).UnixMilli()
	snapshots := []Snapshot{
		{SnapshotID: 1000, TimestampMs: baseTime + 2000, SequenceNumber: 1},
		{SnapshotID: 2000, TimestampMs: baseTime + 1000, SequenceNumber: 2},
		{SnapshotID: 3000, TimestampMs: baseTime + 3000, SequenceNumber: 3},
	}
	snapshotLog := []SnapshotLogEntry{
		{SnapshotID: 1000, TimestampMs: baseTime + 2000},
		{SnapshotID: 2000, TimestampMs: baseTime + 1000},
		{SnapshotID: 3000, TimestampMs: baseTime + 3000},
	}

	meta, err := createTestMetadata(snapshots, snapshotLog)
	require.NoError(t, err)

	tbl := Table{
		identifier: []string{"db", "table"},
		metadata:   meta,
	}

	queryTime := baseTime + 2500
	snapshot := tbl.SnapshotAsOf(queryTime, true)
	require.NotNil(t, snapshot)
	assert.Equal(t, int64(1000), snapshot.SnapshotID)

	scan := tbl.Scan(WithSnapshotAsOf(queryTime))
	snapshot, err = scan.ResolveSnapshot()
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	assert.Equal(t, int64(1000), snapshot.SnapshotID)

	snapshot = tbl.SnapshotAsOf(baseTime+2000, false)
	require.NotNil(t, snapshot)
	assert.Equal(t, int64(2000), snapshot.SnapshotID)
}

func TestSnapshotAsOfWithEqualTimestampsUsesFirstLogEntry(t *testing.T) {
	baseTime := time.Now().Add(5 * time.Second).UnixMilli()
	snapshots := []Snapshot{
		{SnapshotID: 1000, TimestampMs: baseTime + 1000, SequenceNumber: 1},
		{SnapshotID: 2000, TimestampMs: baseTime + 1000, SequenceNumber: 2},
	}
	snapshotLog := []SnapshotLogEntry{
		{SnapshotID: 1000, TimestampMs: baseTime + 1000},
		{SnapshotID: 2000, TimestampMs: baseTime + 1000},
	}

	meta, err := createTestMetadata(snapshots, snapshotLog)
	require.NoError(t, err)

	tbl := Table{metadata: meta}
	snapshot := tbl.SnapshotAsOf(baseTime+1000, true)
	require.NotNil(t, snapshot)
	assert.Equal(t, int64(1000), snapshot.SnapshotID)

	scan := tbl.Scan(WithSnapshotAsOf(baseTime + 1000))
	snapshot, err = scan.ResolveSnapshot()
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	assert.Equal(t, int64(1000), snapshot.SnapshotID)
}

func TestResolveSnapshotRejectsUnknownSnapshotLogEntry(t *testing.T) {
	baseTime := time.Now().Add(5 * time.Second).UnixMilli()
	snapshots := []Snapshot{
		{SnapshotID: 1000, TimestampMs: baseTime + 1000, SequenceNumber: 1},
	}
	snapshotLog := []SnapshotLogEntry{
		{SnapshotID: 1000, TimestampMs: baseTime + 1000},
		{SnapshotID: 2000, TimestampMs: baseTime + 2000},
	}

	meta, err := createTestMetadata(snapshots, snapshotLog)
	require.NoError(t, err)

	scan := (Table{metadata: meta}).Scan(WithSnapshotAsOf(baseTime + 2500))
	_, err = scan.ResolveSnapshot()
	require.ErrorIs(t, err, ErrInvalidMetadata)
}

func TestScanUseRefKeepsSnapshotSelectorsExclusive(t *testing.T) {
	txn := newTransactionWithSnapshotRefs(t)
	require.NoError(t, txn.meta.SetSnapshotRef("release", 20, TagRef))

	meta, err := txn.meta.Build()
	require.NoError(t, err)
	tbl := Table{metadata: meta}

	asOfTimestamp := time.Now().UnixMilli()
	asOfScan := tbl.Scan(WithSnapshotAsOf(asOfTimestamp))
	_, err = asOfScan.UseRef("feature")
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "as-of timestamp")
	require.Nil(t, asOfScan.snapshotID)
	require.Equal(t, &asOfTimestamp, asOfScan.asOfTimestamp)

	mainScan, err := asOfScan.UseRef(MainBranch)
	require.NoError(t, err)
	require.NotSame(t, asOfScan, mainScan)
	require.Nil(t, mainScan.snapshotID)
	require.Equal(t, &asOfTimestamp, mainScan.asOfTimestamp)

	snapshotID := int64(10)
	snapshotScan := tbl.Scan(WithSnapshotID(snapshotID))
	_, err = snapshotScan.UseRef("feature")
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "snapshot id")
	require.Equal(t, &snapshotID, snapshotScan.snapshotID)
	require.Nil(t, snapshotScan.asOfTimestamp)

	mainScan, err = snapshotScan.UseRef(MainBranch)
	require.NoError(t, err)
	require.Equal(t, &snapshotID, mainScan.snapshotID)
	require.Nil(t, mainScan.asOfTimestamp)

	conflictingScan := tbl.Scan(WithSnapshotAsOf(asOfTimestamp), WithSnapshotID(snapshotID))
	_, err = conflictingScan.UseRef("feature")
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	mainScan, err = conflictingScan.UseRef(MainBranch)
	require.NoError(t, err, "UseRef(main) should remain a no-op clone for a conflicting scan")
	require.NotSame(t, conflictingScan, mainScan)
	require.ErrorIs(t, mainScan.selectorErr, iceberg.ErrInvalidArgument)

	liveScan := tbl.Scan()
	mainScan, err = liveScan.UseRef(MainBranch)
	require.NoError(t, err)
	require.Nil(t, mainScan.snapshotID)
	require.Nil(t, mainScan.asOfTimestamp)

	branchScan, err := liveScan.UseRef("feature")
	require.NoError(t, err)
	require.Equal(t, int64(20), *branchScan.snapshotID)
	_, err = branchScan.UseRef("release")
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "snapshot id")

	tagScan, err := liveScan.UseRef("release")
	require.NoError(t, err)
	require.Equal(t, int64(20), *tagScan.snapshotID)

	_, err = liveScan.UseRef("unknown")
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "unknown ref=unknown")
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

		assert.Nil(t, scan.Snapshot())
		_, resolveErr := scan.ResolveSnapshot()
		require.Error(t, resolveErr)
		assert.Contains(t, resolveErr.Error(), "no snapshot found for timestamp")

		_, projectionErr := scan.Projection()
		require.Error(t, projectionErr)
		assert.Contains(t, projectionErr.Error(), "no snapshot found for timestamp")

		// Error should occur during scan planning, not creation
		_, err := scan.PlanFiles(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no snapshot found for timestamp")

		_, _, recordsErr := scan.ToArrowRecords(context.Background())
		require.Error(t, recordsErr)
		assert.Contains(t, recordsErr.Error(), "no snapshot found for timestamp")
	})

	t.Run("WithSnapshotID records conflicting WithSnapshotAsOf option", func(t *testing.T) {
		timestamp := baseTime.Add(30 * time.Minute).UnixMilli()
		scan := table.Scan(WithSnapshotAsOf(timestamp), WithSnapshotID(9999))
		require.NotNil(t, scan)

		assert.ErrorIs(t, scan.selectorErr, iceberg.ErrInvalidArgument)
		assert.Nil(t, scan.snapshotID)
		assert.Equal(t, &timestamp, scan.asOfTimestamp)
		_, err := scan.ResolveSnapshot()
		assert.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	})

	t.Run("WithSnapshotAsOf records conflicting WithSnapshotID option", func(t *testing.T) {
		snapshotID := int64(9999)
		timestamp := baseTime.Add(30 * time.Minute).UnixMilli()
		scan := table.Scan(WithSnapshotID(snapshotID), WithSnapshotAsOf(timestamp))
		require.NotNil(t, scan)

		assert.ErrorIs(t, scan.selectorErr, iceberg.ErrInvalidArgument)
		assert.Equal(t, &snapshotID, scan.snapshotID)
		assert.Nil(t, scan.asOfTimestamp)
		_, err := scan.Projection()
		assert.ErrorIs(t, err, iceberg.ErrInvalidArgument)
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
