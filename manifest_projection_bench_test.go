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
	"fmt"
	"iter"
	"testing"

	iceio "github.com/apache/iceberg-go/io"
)

var manifestProjectionBenchmarkSink int

func BenchmarkManifestEntryProjection(b *testing.B) {
	for _, fieldCount := range []int{10, 100, 1_000} {
		b.Run(fmt.Sprintf("stats_fields=%d", fieldCount), func(b *testing.B) {
			fixture := newManifestProjectionBenchmarkFixture(b, fieldCount, 10_000)

			b.Run("full", func(b *testing.B) {
				benchmarkManifestEntryRead(b, fixture, nil)
			})
			b.Run("scan", func(b *testing.B) {
				benchmarkManifestEntryRead(b, fixture, &ManifestEntryProjection{})
			})
			b.Run("scan_with_stats", func(b *testing.B) {
				benchmarkManifestEntryRead(b, fixture, &ManifestEntryProjection{IncludeColumnStats: true})
			})
		})
	}
}

type manifestProjectionBenchmarkFixture struct {
	fs       *iceio.MemFS
	manifest ManifestFile
	count    int
}

func newManifestProjectionBenchmarkFixture(
	b *testing.B,
	fieldCount, entryCount int,
) manifestProjectionBenchmarkFixture {
	b.Helper()

	fields := make([]NestedField, fieldCount)
	valueCounts := make(map[int]int64, fieldCount)
	nullCounts := make(map[int]int64, fieldCount)
	nanCounts := make(map[int]int64, fieldCount)
	lowerBounds := make(map[int][]byte, fieldCount)
	upperBounds := make(map[int][]byte, fieldCount)
	columnSizes := make(map[int]int64, fieldCount)
	distinctCounts := make(map[int]int64, fieldCount)
	for i := range fieldCount {
		id := i + 1
		fields[i] = NestedField{
			ID: id, Name: fmt.Sprintf("field_%d", id), Type: PrimitiveTypes.Int64, Required: true,
		}
		valueCounts[id] = int64(entryCount)
		nullCounts[id] = 0
		nanCounts[id] = 0
		lowerBounds[id] = []byte{0, 0, 0, 0, 0, 0, 0, 0}
		upperBounds[id] = []byte{0, 0, 0, 0, 0, 0, 0, 1}
		columnSizes[id] = 64
		distinctCounts[id] = 1
	}
	schema := NewSchema(1, fields...)
	builder, err := NewDataFileBuilder(
		*UnpartitionedSpec,
		EntryContentData,
		"data.parquet",
		ParquetFile,
		nil,
		nil,
		nil,
		int64(entryCount),
		128,
	)
	if err != nil {
		b.Fatal(err)
	}
	builder.
		ColumnSizes(columnSizes).
		ValueCounts(valueCounts).
		NullValueCounts(nullCounts).
		NaNValueCounts(nanCounts).
		DistinctValueCounts(distinctCounts).
		LowerBoundValues(lowerBounds).
		UpperBoundValues(upperBounds)

	snapshotID := int64(1)
	entries := make([]ManifestEntry, entryCount)
	for i := range entries {
		entries[i] = NewManifestEntry(EntryStatusADDED, &snapshotID, nil, nil, builder.Build())
	}

	manifestPath := fmt.Sprintf("mem://manifest-%d.avro", fieldCount)
	var manifestBytes bytes.Buffer
	manifest, err := WriteManifest(
		manifestPath,
		&manifestBytes,
		2,
		*UnpartitionedSpec,
		schema,
		snapshotID,
		entries,
	)
	if err != nil {
		b.Fatal(err)
	}

	fs := iceio.NewMemFS()
	if err := fs.WriteFile(manifestPath, manifestBytes.Bytes()); err != nil {
		b.Fatal(err)
	}

	return manifestProjectionBenchmarkFixture{fs: fs, manifest: manifest, count: entryCount}
}

func benchmarkManifestEntryRead(
	b *testing.B,
	fixture manifestProjectionBenchmarkFixture,
	projection *ManifestEntryProjection,
) {
	b.Helper()
	b.ReportAllocs()
	b.ReportMetric(float64(fixture.count), "entries/op")
	b.ResetTimer()

	for b.Loop() {
		count := 0
		var entries iter.Seq2[ManifestEntry, error]
		if projection == nil {
			entries = fixture.manifest.Entries(fixture.fs, true)
		} else {
			entries = EntriesWithProjection(fixture.fs, fixture.manifest, true, *projection)
		}
		for entry, err := range entries {
			if err != nil {
				b.Fatal(err)
			}
			count++
			manifestProjectionBenchmarkSink += len(entry.DataFile().FilePath())
		}
		if count != fixture.count {
			b.Fatalf("read %d entries, want %d", count, fixture.count)
		}
	}
}
