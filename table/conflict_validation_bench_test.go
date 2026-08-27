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
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

var canonicalPartitionKeyBenchmarkSink string

func BenchmarkCanonicalPartitionKey(b *testing.B) {
	for _, tc := range []struct {
		name      string
		partition map[int]any
	}{
		{
			name:      "fields=1/int32",
			partition: map[int]any{1: int32(1)},
		},
		{
			name: "fields=2/int32",
			partition: map[int]any{
				1: int32(1), 2: int32(2),
			},
		},
		{
			name: "fields=4/int32",
			partition: map[int]any{
				1: int32(1), 2: int32(2), 3: int32(3), 4: int32(4),
			},
		},
		{
			name: "fields=8/mixed",
			partition: map[int]any{
				1: int32(1), 2: "two", 3: int64(3), 4: "four",
				5: int32(5), 6: "six", 7: int64(7), 8: "eight",
			},
		},
		{
			name: "fields=4/binary-string",
			partition: map[int]any{
				1: []byte("one"), 2: "two", 3: []byte("three"), 4: "four",
			},
		},
		{
			name: "fields=16/mixed",
			partition: map[int]any{
				1: int32(1), 2: "two", 3: int64(3), 4: "four",
				5: int32(5), 6: "six", 7: int64(7), 8: "eight",
				9: int32(9), 10: "ten", 11: int64(11), 12: "twelve",
				13: int32(13), 14: "fourteen", 15: int64(15), 16: "sixteen",
			},
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				key, err := canonicalPartitionKey(7, tc.partition)
				if err != nil {
					b.Fatal(err)
				}
				canonicalPartitionKeyBenchmarkSink = key
			}
		})
	}
}

func BenchmarkConflictValidationSharedManifestReads(b *testing.B) {
	for _, entryCount := range []int{1_000, 10_000} {
		for _, validatorCount := range []int{2, 8} {
			b.Run(fmt.Sprintf("entries=%d/validators=%d", entryCount, validatorCount), func(b *testing.B) {
				baseContext := newConflictValidationBenchmarkContext(b, entryCount)
				fs := baseContext.fs.(*conflictValidationBenchmarkIO)
				visit := func(Snapshot, iceberg.ManifestEntry) error { return nil }

				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					// Each iteration models one commit attempt. Validators
					// within that attempt share the context and its cache.
					ctx := &conflictContext{fs: baseContext.fs, concurrent: baseContext.concurrent}
					for range validatorCount {
						if err := ctx.forEachAddedEntry(iceberg.ManifestContentData, visit); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.ReportMetric(float64(fs.opens)/float64(b.N), "backend-opens/op")
				b.ReportMetric(float64(fs.bytes)/float64(b.N), "backend-bytes/op")
			})
		}
	}
}

type conflictValidationBenchmarkIO struct {
	iceio.IO
	opens int
	bytes int64
}

func (f *conflictValidationBenchmarkIO) Open(name string) (iceio.File, error) {
	f.opens++

	file, err := f.IO.Open(name)
	if err != nil {
		return nil, err
	}

	return &conflictValidationBenchmarkFile{File: file, owner: f}, nil
}

type conflictValidationBenchmarkFile struct {
	iceio.File
	owner *conflictValidationBenchmarkIO
}

func (f *conflictValidationBenchmarkFile) Read(p []byte) (int, error) {
	n, err := f.File.Read(p)
	f.owner.bytes += int64(n)

	return n, err
}

func (f *conflictValidationBenchmarkFile) ReadAt(p []byte, offset int64) (int, error) {
	n, err := f.File.ReadAt(p, offset)
	f.owner.bytes += int64(n)

	return n, err
}

func newConflictValidationBenchmarkContext(b *testing.B, entryCount int) *conflictContext {
	b.Helper()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	spec := *iceberg.UnpartitionedSpec
	snapshotID := int64(2)
	entries := make([]iceberg.ManifestEntry, entryCount)
	for i := range entryCount {
		dataFile, err := iceberg.NewDataFileBuilder(
			spec,
			iceberg.EntryContentData,
			fmt.Sprintf("data-%d.parquet", i),
			iceberg.ParquetFile,
			nil,
			nil,
			nil,
			1,
			1024,
		)
		if err != nil {
			b.Fatal(err)
		}
		entries[i] = iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &snapshotID, dataFile.Build()).
			SequenceNum(1).
			Build()
	}

	manifestPath := "mem://conflict-validation-benchmark/manifest.avro"
	manifestListPath := "mem://conflict-validation-benchmark/manifest-list.avro"
	baseFS := iceio.NewMemFS()

	var manifest bytes.Buffer
	mf, err := iceberg.WriteManifest(manifestPath, &manifest, 2, spec, schema, snapshotID, entries)
	if err != nil {
		b.Fatal(err)
	}
	if err := baseFS.WriteFile(manifestPath, manifest.Bytes()); err != nil {
		b.Fatal(err)
	}

	var manifestList bytes.Buffer
	sequenceNumber := int64(1)
	if err := iceberg.WriteManifestList(2, &manifestList, snapshotID, nil, &sequenceNumber, 0, []iceberg.ManifestFile{mf}); err != nil {
		b.Fatal(err)
	}
	if err := baseFS.WriteFile(manifestListPath, manifestList.Bytes()); err != nil {
		b.Fatal(err)
	}

	return &conflictContext{
		fs: &conflictValidationBenchmarkIO{IO: baseFS},
		concurrent: []Snapshot{{
			SnapshotID:   snapshotID,
			ManifestList: manifestListPath,
		}},
	}
}
