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
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

type inspectPartitionManifestTrackingIO struct {
	iceio.IO
	active    atomic.Int32
	maxActive atomic.Int32
	failPath  string
}

func (f *inspectPartitionManifestTrackingIO) Open(ctx context.Context, path string) (iceio.File, error) {
	if strings.Contains(path, "/metadata/inspect-") && strings.HasSuffix(path, ".avro") {
		if f.failPath != "" && strings.Contains(path, f.failPath) {
			return nil, fmt.Errorf("injected manifest read error for %s", path)
		}

		active := f.active.Add(1)
		for {
			maxActive := f.maxActive.Load()
			if active <= maxActive || f.maxActive.CompareAndSwap(maxActive, active) {
				break
			}
		}
		// Deliberately delay only manifest opens to expose concurrent I/O
		// without depending on timing assertions.
		time.Sleep(10 * time.Millisecond)
		f.active.Add(-1)
	}

	return f.IO.Open(ctx, path)
}

func inspectParallelPartitionManifestTable(t *testing.T, count int) *Table {
	t.Helper()

	spec := partitionedSpec()
	const snapshotID = int64(1)
	sequenceNumber := int64(1)
	manifests := make([]inspectManifestSpec, 0, count)
	for index := range count {
		files := []struct {
			partition int32
			count     int64
		}{
			{partition: 7, count: 3},
			{partition: 8, count: 5},
		}
		entries := make([]iceberg.ManifestEntry, 0, len(files))
		for _, f := range files {
			path := fmt.Sprintf("mem://default/table-location/data/parallel-%d-%d.parquet", index, f.partition)
			dataFile := newTestDataFileWithCount(t, spec, path, map[int]any{1000: f.partition}, f.count)
			entries = append(entries, iceberg.NewManifestEntry(
				iceberg.EntryStatusADDED, new(snapshotID),
				&sequenceNumber, &sequenceNumber, dataFile,
			))
		}
		manifests = append(manifests, inspectManifestSpec{
			content: iceberg.ManifestContentData,
			entries: entries,
		})
	}

	return inspectFilesTableWithManifests(t, spec, manifests...)
}

func inspectWithTrackingIO(t *testing.T, tbl *Table) (*Table, *inspectPartitionManifestTrackingIO) {
	t.Helper()

	fs, err := tbl.FS(t.Context())
	require.NoError(t, err)

	tracked := &inspectPartitionManifestTrackingIO{IO: fs}
	wrapped := New(Identifier{"db", "parallel-inspect"}, tbl.Metadata(), tbl.MetadataLocation(),
		func(context.Context) (iceio.IO, error) { return tracked, nil }, nil)

	return wrapped, tracked
}

func TestInspectPartitionsParallelManifestAggregation(t *testing.T) {
	for _, count := range []int{1, 8, 17} {
		t.Run(fmt.Sprintf("manifests=%d", count), func(t *testing.T) {
			tbl, tracked := inspectWithTrackingIO(t, inspectParallelPartitionManifestTable(t, count))

			reader, err := tbl.Inspect().Partitions(t.Context())
			require.NoError(t, err)
			defer reader.Release()

			record := collectRecord(t, reader)
			defer record.Release()
			require.EqualValues(t, 2, record.NumRows())

			partitions := record.Column(0).(*array.Struct).Field(0).(*array.Int32)
			recordCounts := record.Column(2).(*array.Int64)
			fileCounts := record.Column(3).(*array.Int32)
			lastSnapshot := record.Column(10).(*array.Int64)
			for index, partition := range []int32{7, 8} {
				require.Equal(t, partition, partitions.Value(index))
				wantRows := int64(count) * 3
				if partition == 8 {
					wantRows = int64(count) * 5
				}
				require.Equal(t, wantRows, recordCounts.Value(index))
				require.EqualValues(t, count, fileCounts.Value(index))
				require.EqualValues(t, 1, lastSnapshot.Value(index))
			}
			require.LessOrEqual(t, tracked.maxActive.Load(), int32(inspectPartitionManifestConcurrency))
			if count > 1 {
				require.GreaterOrEqual(t, tracked.maxActive.Load(), int32(2))
			}
		})
	}
}

func TestInspectPartitionsParallelManifestReadFailure(t *testing.T) {
	tbl, tracked := inspectWithTrackingIO(t, inspectParallelPartitionManifestTable(t, 8))
	tracked.failPath = "inspect-3.avro"

	reader, err := tbl.Inspect().Partitions(t.Context())
	require.ErrorContains(t, err, "injected manifest read error")
	require.Nil(t, reader)
	require.EqualValues(t, 0, tracked.active.Load())
}

func TestInspectPartitionsParallelCancelledContext(t *testing.T) {
	tbl, tracked := inspectWithTrackingIO(t, inspectParallelPartitionManifestTable(t, 8))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	reader, err := tbl.Inspect().Partitions(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, reader)
	require.Zero(t, tracked.maxActive.Load())
}

func TestMergeInspectPartitionAggregateTimestampTies(t *testing.T) {
	const timestamp = int64(123000)
	firstSnapshot := int64(10)
	secondSnapshot := int64(20)
	dst := &inspectPartitionAggregate{
		dataRecordCount: 3, dataFileCount: 1, specID: 1,
		lastUpdatedAt: new(timestamp), lastUpdatedSnapshot: &firstSnapshot,
	}
	sameTime := &inspectPartitionAggregate{
		dataRecordCount: 5, dataFileCount: 2, specID: 2,
		lastUpdatedAt: new(timestamp), lastUpdatedSnapshot: &secondSnapshot,
	}
	mergeInspectPartitionAggregate(dst, sameTime)

	// The earlier manifest wins a timestamp tie, but counts still add up.
	require.EqualValues(t, 8, dst.dataRecordCount)
	require.EqualValues(t, 3, dst.dataFileCount)
	require.EqualValues(t, 1, dst.specID)
	require.Equal(t, firstSnapshot, *dst.lastUpdatedSnapshot)

	newerTime := timestamp + 1
	newer := &inspectPartitionAggregate{
		specID: 3, lastUpdatedAt: &newerTime, lastUpdatedSnapshot: &secondSnapshot,
	}
	mergeInspectPartitionAggregate(dst, newer)
	require.EqualValues(t, 3, dst.specID)
	require.Equal(t, newerTime, *dst.lastUpdatedAt)
	require.Equal(t, secondSnapshot, *dst.lastUpdatedSnapshot)
}
