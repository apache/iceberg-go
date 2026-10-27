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
	"github.com/stretchr/testify/require"
)

// BenchmarkInspectPartitionsParallelManifests exercises the full metadata
// table read, including manifest decoding and partition aggregation.
// The tracked FileIO adds 10ms latency per manifest open to model remote I/O.
func BenchmarkInspectPartitionsParallelManifests(b *testing.B) {
	for _, count := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("manifests=%d", count), func(b *testing.B) {
			tbl := benchmarkInspectPartitionTable(b, count)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				reader, err := tbl.Inspect().Partitions(b.Context())
				if err != nil {
					b.Fatal(err)
				}
				reader.Release()
			}
		})
	}
}

func benchmarkInspectPartitionTable(b *testing.B, manifestCount int) *Table {
	b.Helper()

	const snapshotID = int64(1)
	sequenceNumber := int64(1)
	spec := *iceberg.UnpartitionedSpec
	schema := iceberg.NewSchema(0)
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "mem://inspect-bench/table", nil)
	require.NoError(b, err)
	builder, err := MetadataBuilderFromBase(meta, "metadata.json")
	require.NoError(b, err)

	fs := iceio.NewMemFS()
	manifests := make([]iceberg.ManifestFile, 0, manifestCount)
	for index := range manifestCount {
		dataPath := fmt.Sprintf("mem://inspect-bench/table/data/file-%d.parquet", index)
		dataFileBuilder, err := iceberg.NewDataFileBuilder(
			spec, iceberg.EntryContentData, dataPath, iceberg.ParquetFile,
			nil, nil, nil, 100, 1_024,
		)
		require.NoError(b, err)

		manifestPath := fmt.Sprintf("mem://inspect-bench/table/metadata/inspect-%d.avro", index)
		var manifestBuf bytes.Buffer
		manifest, err := iceberg.WriteManifest(
			manifestPath, &manifestBuf, 2, spec, schema, snapshotID,
			[]iceberg.ManifestEntry{
				iceberg.NewManifestEntry(iceberg.EntryStatusADDED,
					new(snapshotID), &sequenceNumber, &sequenceNumber, dataFileBuilder.Build()),
			},
		)
		require.NoError(b, err)
		require.NoError(b, fs.WriteFile(context.Background(), manifestPath, manifestBuf.Bytes()))
		manifests = append(manifests, manifest)
	}

	manifestListPath := "mem://inspect-bench/table/metadata/manifest-list.avro"
	var listBuf bytes.Buffer
	require.NoError(b, iceberg.WriteManifestList(
		2, &listBuf, snapshotID, nil, &sequenceNumber, 0, manifests,
	))
	require.NoError(b, fs.WriteFile(context.Background(), manifestListPath, listBuf.Bytes()))

	schemaID := schema.ID
	require.NoError(b, builder.AddSnapshot(&Snapshot{
		SnapshotID:     snapshotID,
		SequenceNumber: sequenceNumber,
		TimestampMs:    meta.LastUpdatedMillis() + 1,
		ManifestList:   manifestListPath,
		SchemaID:       &schemaID,
		Summary:        &Summary{Operation: OpAppend},
	}))
	require.NoError(b, builder.SetSnapshotRef(MainBranch, snapshotID, BranchRef))

	built, err := builder.Build()
	require.NoError(b, err)

	delayed := &inspectPartitionManifestTrackingIO{IO: fs}

	return New(Identifier{"db", "inspect-bench"}, built, "metadata.json",
		func(context.Context) (iceio.IO, error) { return delayed, nil }, nil)
}
