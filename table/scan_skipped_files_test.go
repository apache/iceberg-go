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
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/metrics"
	"github.com/stretchr/testify/require"
)

// skippedFilesTestTable builds a single append snapshot over a table
// partitioned by identity(category):
//
//   - data manifest A: categories 0, 1 and 2, four files each, with id bounds
//     [0,9], [10,19], [20,29] and [30,39], plus one DELETED entry in category 0;
//   - data manifest B: two files in category 5, so a category filter that
//     excludes 5 prunes the whole manifest;
//   - a delete manifest with one position delete file per category 0, 1 and 2.
//
// A filter of category == 1 AND id >= 25 then rejects 8 live entries in A by
// partition, 2 more by column bounds, and keeps 2.
func skippedFilesTestTable(t *testing.T, rep metrics.Reporter) *Table {
	t.Helper()
	ctx := context.Background()

	const (
		location   = "mem://default/skipped-files"
		snapshotID = int64(1)
	)
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Name: "category", Transform: iceberg.IdentityTransform{},
	})

	fsys, err := iceio.LoadFS(ctx, nil, location)
	require.NoError(t, err)
	fs := fsys.(iceio.WriteFileIO)

	bound := func(v int32) []byte {
		b, err := iceberg.Int32Literal(v).MarshalBinary()
		require.NoError(t, err)

		return b
	}
	dataFile := func(category, lower int32) iceberg.DataFile {
		builder, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData,
			fmt.Sprintf("%s/data/category=%d/id-%d.parquet", location, category, lower),
			iceberg.ParquetFile, map[int]any{1000: category}, nil, nil, 10, 100)
		require.NoError(t, err)

		return builder.
			LowerBoundValues(map[int][]byte{1: bound(lower)}).
			UpperBoundValues(map[int][]byte{1: bound(lower + 9)}).
			Build()
	}
	added := func(df iceberg.DataFile) iceberg.ManifestEntry {
		return iceberg.NewManifestEntry(iceberg.EntryStatusADDED, new(snapshotID), nil, nil, df)
	}
	writeManifest := func(path string, content iceberg.ManifestContent, entries []iceberg.ManifestEntry) iceberg.ManifestFile {
		var buf bytes.Buffer
		writer, err := iceberg.NewManifestWriter(2, &buf, spec, schema, snapshotID,
			iceberg.WithManifestWriterContent(content))
		require.NoError(t, err)
		for _, entry := range entries {
			if entry.Status() == iceberg.EntryStatusDELETED {
				require.NoError(t, writer.Delete(entry))
			} else {
				require.NoError(t, writer.Add(entry))
			}
		}
		require.NoError(t, writer.Close())
		manifest, err := writer.ToManifestFile(path, int64(buf.Len()),
			iceberg.WithManifestFileContent(content))
		require.NoError(t, err)
		require.NoError(t, fs.WriteFile(ctx, path, buf.Bytes()))

		return manifest
	}

	var entriesA []iceberg.ManifestEntry
	for category := range int32(3) {
		for lower := int32(0); lower < 40; lower += 10 {
			entriesA = append(entriesA, added(dataFile(category, lower)))
		}
	}
	// Rejected by the partition filter, but deleted entries are not live, so
	// they must not count as skipped.
	entriesA = append(entriesA, iceberg.NewManifestEntry(
		iceberg.EntryStatusDELETED, new(snapshotID), new(int64(1)), new(int64(1)), dataFile(0, 100)))
	manifestA := writeManifest(location+"/metadata/data-a.avro", iceberg.ManifestContentData, entriesA)
	manifestB := writeManifest(location+"/metadata/data-b.avro", iceberg.ManifestContentData,
		[]iceberg.ManifestEntry{added(dataFile(5, 0)), added(dataFile(5, 10))})

	var deleteEntries []iceberg.ManifestEntry
	for category := range int32(3) {
		builder, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes,
			fmt.Sprintf("%s/data/category=%d/pos-delete.parquet", location, category),
			iceberg.ParquetFile, map[int]any{1000: category}, nil, nil, 1, 10)
		require.NoError(t, err)
		deleteEntries = append(deleteEntries, added(builder.Build()))
	}
	deleteManifest := writeManifest(location+"/metadata/deletes.avro", iceberg.ManifestContentDeletes, deleteEntries)

	listPath := location + "/metadata/snap-1.avro"
	var listBuf bytes.Buffer
	seqNum := int64(1)
	require.NoError(t, iceberg.WriteManifestList(2, &listBuf, snapshotID, nil, &seqNum, 0,
		[]iceberg.ManifestFile{manifestA, manifestB, deleteManifest}))
	require.NoError(t, fs.WriteFile(ctx, listPath, listBuf.Bytes()))

	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, location, nil)
	require.NoError(t, err)
	txn := New(Identifier{"db", "tbl"}, meta, "metadata.json", nil, nil).NewTransaction()
	txn.meta.snapshotList = []Snapshot{{
		SnapshotID:     snapshotID,
		TimestampMs:    1000,
		ManifestList:   listPath,
		SequenceNumber: seqNum,
		SchemaID:       &schema.ID,
		Summary:        &Summary{Operation: OpAppend},
	}}
	txn.meta.snapshotLog = []SnapshotLogEntry{{SnapshotID: snapshotID, TimestampMs: 1000}}
	txn.meta.currentSnapshotID = new(snapshotID)
	built, err := txn.meta.Build()
	require.NoError(t, err)

	return New(Identifier{"db", "tbl"}, built, "metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil }, nil,
		WithMetricsReporter(rep))
}

var skippedFilesTestFilter = iceberg.NewAnd(
	iceberg.EqualTo(iceberg.Reference("category"), int32(1)),
	iceberg.GreaterThanEqual(iceberg.Reference("id"), int32(25)),
)

func singleScanReport(t *testing.T, rep *metrics.InMemoryReporter) metrics.ScanReport {
	t.Helper()
	reports := rep.Reports()
	require.Len(t, reports, 1)
	report, ok := reports[0].(metrics.ScanReport)
	require.True(t, ok)

	return report
}

func TestPlanFilesReportsSkippedDataAndDeleteFiles(t *testing.T) {
	rep := &metrics.InMemoryReporter{}
	tbl := skippedFilesTestTable(t, rep)

	tasks, err := tbl.Scan(WithRowFilter(skippedFilesTestFilter)).PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 2)

	m := singleScanReport(t, rep).Metrics
	// Manifest B is pruned as a whole, so its two files count only toward
	// skipped-data-manifests.
	require.Equal(t, int64(1), m.ScannedDataManifests.Value)
	require.Equal(t, int64(1), m.SkippedDataManifests.Value)
	// 8 live entries rejected by partition plus 2 by id bounds. The DELETED
	// entry is not counted.
	require.NotNil(t, m.SkippedDataFiles)
	require.Equal(t, metrics.UnitCount, m.SkippedDataFiles.Unit)
	require.Equal(t, int64(10), m.SkippedDataFiles.Value)
	require.Equal(t, int64(2), m.ResultDataFiles.Value)
	// The position deletes in categories 0 and 2 are rejected by partition.
	require.NotNil(t, m.SkippedDeleteFiles)
	require.Equal(t, int64(2), m.SkippedDeleteFiles.Value)
}

func TestPlanFilesReportsZeroSkippedFilesWithoutFilter(t *testing.T) {
	rep := &metrics.InMemoryReporter{}
	tbl := skippedFilesTestTable(t, rep)

	tasks, err := tbl.Scan().PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 14)

	// Java always reports every counter, so zero is emitted rather than omitted.
	m := singleScanReport(t, rep).Metrics
	require.NotNil(t, m.SkippedDataFiles)
	require.Equal(t, int64(0), m.SkippedDataFiles.Value)
	require.NotNil(t, m.SkippedDeleteFiles)
	require.Equal(t, int64(0), m.SkippedDeleteFiles.Value)
}

func TestIncrementalAppendScanReportsSkippedDataFiles(t *testing.T) {
	rep := &metrics.InMemoryReporter{}
	tbl := skippedFilesTestTable(t, rep)

	tasks, err := tbl.NewIncrementalAppendScan(WithRowFilter(skippedFilesTestFilter)).
		ToSnapshot(1).PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 2)

	m := singleScanReport(t, rep).Metrics
	require.Equal(t, int64(10), m.SkippedDataFiles.Value)
	require.Equal(t, int64(0), m.SkippedDeleteFiles.Value)
}

func TestIncrementalChangelogScanReportsSkippedDataFiles(t *testing.T) {
	rep := &metrics.InMemoryReporter{}
	tbl := incrementalChangelogTestTable(t)

	// Manifest 2 holds the DELETED entry for partition 1 and the ADDED entry for
	// partition 2, so it survives manifest pruning. A changelog scan reads
	// deleted entries as changes, so the partition evaluator rejecting that
	// entry counts as a skipped file. The other manifests hold only other
	// partitions and are pruned whole.
	tasks, err := tbl.NewIncrementalChangelogScan(
		WithRowFilter(iceberg.EqualTo(iceberg.Reference("id"), int32(2))),
		WithReporter(rep),
	).PlanFiles(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	m := singleScanReport(t, rep).Metrics
	require.Equal(t, int64(1), m.ScannedDataManifests.Value)
	require.Equal(t, int64(1), m.SkippedDataFiles.Value)
	require.Equal(t, int64(0), m.SkippedDeleteFiles.Value)
}
