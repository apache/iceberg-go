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
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func BenchmarkInspectPartitionsSnapshotHistory(b *testing.B) {
	for _, count := range []int{0, 1, 100, 1000} {
		b.Run(fmt.Sprintf("snapshots=%d", count), func(b *testing.B) {
			tbl := inspectPartitionSnapshotHistoryTable(b, count)
			inspector := tbl.Inspect()
			ctx := context.Background()
			if snapshot := tbl.metadata.CurrentSnapshot(); snapshot != nil {
				_, err := tbl.manifestSet(ctx, *snapshot)
				require.NoError(b, err)
			}
			b.ReportAllocs()
			for b.Loop() {
				rr, err := inspector.Partitions(ctx)
				if err != nil {
					b.Fatal(err)
				}
				var rows int64
				for rr.Next() {
					rows += rr.RecordBatch().NumRows()
				}
				err = rr.Err()
				rr.Release()
				if err != nil {
					b.Fatal(err)
				}
				wantRows := int64(min(count, 1))
				if rows != wantRows {
					b.Fatalf("got %d rows, want %d", rows, wantRows)
				}
			}
		})
	}
}

func inspectPartitionSnapshotHistoryTable(t testing.TB, count int) *Table {
	t.Helper()
	spec := partitionedSpec()
	schema := simpleSchema()
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "mem://snapshot-history/table", nil)
	require.NoError(t, err)
	builder, err := MetadataBuilderFromBase(meta, "metadata.json")
	require.NoError(t, err)
	fs := iceio.NewMemFS()
	builder.snapshotList = make([]Snapshot, count)
	for index := range count {
		id := int64(index + 1)
		builder.snapshotList[index] = Snapshot{
			SnapshotID: id, SequenceNumber: id, TimestampMs: id * 1000,
			ManifestList: fmt.Sprintf("mem://snapshot-history/table/metadata/snap-%d.avro", id),
			SchemaID:     new(schema.ID),
			Summary: &Summary{Operation: OpAppend, Properties: iceberg.Properties{
				"added-data-files": "1", "added-records": "1", "total-records": strconv.FormatInt(id, 10),
			}},
		}
		if index > 0 {
			builder.snapshotList[index].ParentSnapshotID = new(id - 1)
		}
	}
	if count > 0 {
		currentID := int64(count)
		entryID := max(int64(1), currentID/2)
		sequence := entryID
		fileBuilder, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData,
			"mem://snapshot-history/table/data/file.parquet", iceberg.ParquetFile,
			map[int]any{1000: int32(7)}, nil, nil, 1, 1)
		require.NoError(t, err)
		manifestPath := "mem://snapshot-history/table/metadata/manifest.avro"
		var manifestBuffer bytes.Buffer
		manifest, err := iceberg.WriteManifest(manifestPath, &manifestBuffer, 2, spec, schema, currentID,
			[]iceberg.ManifestEntry{iceberg.NewManifestEntry(iceberg.EntryStatusEXISTING,
				&entryID, &sequence, &sequence, fileBuilder.Build())})
		require.NoError(t, err)
		require.NoError(t, fs.WriteFile(manifestPath, manifestBuffer.Bytes()))
		var listBuffer bytes.Buffer
		require.NoError(t, iceberg.WriteManifestList(2, &listBuffer, currentID, nil,
			&currentID, 0, []iceberg.ManifestFile{manifest}))
		require.NoError(t, fs.WriteFile(builder.snapshotList[count-1].ManifestList, listBuffer.Bytes()))
		builder.currentSnapshotID = &currentID
		builder.lastSequenceNumber = &currentID
	}
	built, err := builder.Build()
	require.NoError(t, err)

	return New(Identifier{"db", "tbl"}, built, "metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil }, nil)
}
