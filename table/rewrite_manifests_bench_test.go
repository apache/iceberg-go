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
)

// BenchmarkManifestMergeModes compares the existing size-only merge with the
// cluster-by path on the same 64 one-entry manifests. Both modes use eight
// output-sized groups and a single merge worker so the benchmark focuses on
// entry routing and manifest writing rather than scheduling.
func BenchmarkManifestMergeModes(b *testing.B) {
	for _, cluster := range []bool{false, true} {
		name := "BySize"
		if cluster {
			name = "ByClusterKey"
		}
		b.Run(name, func(b *testing.B) {
			benchmarkManifestMergeMode(b, cluster)
		})
	}
}

func benchmarkManifestMergeMode(b *testing.B, cluster bool) {
	spec := iceberg.NewPartitionSpec()
	schema := simpleSchema()
	mem := newMemIO(1<<30, errLimitedWrite)
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "table-location", nil)
	if err != nil {
		b.Fatal(err)
	}
	tbl := New(Identifier{"db", "benchmark"}, meta, "metadata.json",
		func(context.Context) (iceio.IO, error) { return mem, nil }, nil)
	txn := tbl.NewTransaction()
	prod := newRewriteManifestsProducer(txn, mem, iceberg.Properties{}, rewriteManifestsCfg{})

	const inputCount = 64
	const clusterCount = 8
	inputSnapshotID := int64(1)
	inputSequenceNumber := int64(1)
	manifests := make([]iceberg.ManifestFile, 0, inputCount)
	clusterKeys := make(map[string]int, inputCount)
	var maxLength int64
	for i := range inputCount {
		path := fmt.Sprintf("file://data-%d.parquet", i)
		builder, err := iceberg.NewDataFileBuilder(
			spec, iceberg.EntryContentData, path, iceberg.ParquetFile,
			nil, nil, nil, 1, 100,
		)
		if err != nil {
			b.Fatal(err)
		}
		entry := iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &inputSnapshotID, &inputSequenceNumber, nil, builder.Build())
		manifestPath := fmt.Sprintf("table-location/metadata/input-%d.avro", i)
		var buf bytes.Buffer
		manifest, err := iceberg.WriteManifest(manifestPath, &buf, 2, spec, schema, inputSnapshotID, []iceberg.ManifestEntry{entry})
		if err != nil {
			b.Fatal(err)
		}
		if err := mem.WriteFile(manifestPath, buf.Bytes()); err != nil {
			b.Fatal(err)
		}
		manifests = append(manifests, manifest)
		clusterKeys[path] = i % clusterCount
		maxLength = max(maxLength, manifest.Length())
	}

	mgr := manifestMergeManager{
		targetSizeBytes:  8 * maxLength,
		mergeEnabled:     true,
		mergeConcurrency: 1,
		snap:             prod,
	}
	if cluster {
		mgr.clusterBy = func(df iceberg.DataFile) any { return clusterKeys[df.FilePath()] }
	}

	b.ResetTimer()
	for range b.N {
		output, err := mgr.mergeManifests(manifests)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		for _, manifest := range output {
			if err := mem.Remove(manifest.FilePath()); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
	}
}

// BenchmarkManifestPruningModes measures the read-side reason for clustering.
// The input manifests interleave 32 partition values. A size-only merge keeps
// that mix in every output group, while cluster-by partition value lets the
// manifest evaluator reject unrelated groups before opening their entries.
func BenchmarkManifestPruningModes(b *testing.B) {
	for _, cluster := range []bool{false, true} {
		name := "BySize"
		if cluster {
			name = "ByClusterKey"
		}
		b.Run(name, func(b *testing.B) {
			benchmarkManifestPruningMode(b, cluster)
		})
	}
}

func benchmarkManifestPruningMode(b *testing.B, cluster bool) {
	b.Helper()

	const (
		inputCount   = 512
		clusterCount = 32
		targetValue  = int32(7)
	)

	spec := partitionedSpec()
	schema := simpleSchema()
	mem := newMemIO(1<<30, errLimitedWrite)
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "table-location", nil)
	if err != nil {
		b.Fatal(err)
	}
	tbl := New(Identifier{"db", "benchmark-pruning"}, meta, "metadata.json",
		func(context.Context) (iceio.IO, error) { return mem, nil }, nil)
	prod := newRewriteManifestsProducer(tbl.NewTransaction(), mem, iceberg.Properties{}, rewriteManifestsCfg{})

	inputSnapshotID := int64(1)
	inputSequenceNumber := int64(1)
	manifests := make([]iceberg.ManifestFile, 0, inputCount)
	for i := range inputCount {
		partitionValue := int32((i / 8) % (clusterCount - 1))
		if partitionValue >= targetValue {
			partitionValue++
		}
		if i%8 == 0 {
			// Put the queried value in every size-only input group. The
			// size-only output therefore cannot prune any group, while the
			// clustered output keeps these entries together.
			partitionValue = targetValue
		}
		builder, err := iceberg.NewDataFileBuilder(
			spec, iceberg.EntryContentData,
			fmt.Sprintf("file://data-%d.parquet", i), iceberg.ParquetFile,
			map[int]any{1000: partitionValue},
			nil, nil, 1, 100,
		)
		if err != nil {
			b.Fatal(err)
		}
		entry := iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED,
			&inputSnapshotID,
			&inputSequenceNumber,
			nil,
			builder.Build(),
		)

		manifestPath := fmt.Sprintf("table-location/metadata/input-%d.avro", i)
		var buf bytes.Buffer
		manifest, err := iceberg.WriteManifest(
			manifestPath, &buf, 2, spec, schema, inputSnapshotID,
			[]iceberg.ManifestEntry{entry},
		)
		if err != nil {
			b.Fatal(err)
		}
		if err := mem.WriteFile(manifestPath, buf.Bytes()); err != nil {
			b.Fatal(err)
		}
		manifests = append(manifests, manifest)
	}

	var maxLength int64
	for _, manifest := range manifests {
		maxLength = max(maxLength, manifest.Length())
	}
	mgr := manifestMergeManager{
		targetSizeBytes:  8 * maxLength,
		mergeEnabled:     true,
		mergeConcurrency: 1,
		snap:             prod,
	}
	if cluster {
		mgr.clusterBy = func(df iceberg.DataFile) any {
			return df.Partition()[1000]
		}
	}
	merged, err := mgr.mergeManifests(manifests)
	if err != nil {
		b.Fatal(err)
	}

	scan := tbl.Scan(WithRowFilter(iceberg.EqualTo(iceberg.Reference("id"), targetValue)))
	partitionFilters := scan.partitionFiltersForSchema(schema)
	filtered, err := scan.filterManifestsWithSchema(merged, schema, &scanMetricsAccumulator{}, partitionFilters)
	if err != nil {
		b.Fatal(err)
	}
	wantSelected := len(filtered)
	if cluster {
		if wantSelected >= len(merged) {
			b.Fatalf("clustered layout selected %d/%d manifests, want pruning", wantSelected, len(merged))
		}
	} else if wantSelected != len(merged) {
		b.Fatalf("size-only layout selected %d/%d manifests, want all", wantSelected, len(merged))
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.ReportMetric(float64(len(merged)), "manifests/op")
	b.ReportMetric(float64(wantSelected), "selected-manifests/op")
	for range b.N {
		filtered, err := scan.filterManifestsWithSchema(merged, schema, &scanMetricsAccumulator{}, partitionFilters)
		if err != nil {
			b.Fatal(err)
		}
		if len(filtered) != wantSelected {
			b.Fatalf("selected %d manifests, want %d", len(filtered), wantSelected)
		}
	}
}
