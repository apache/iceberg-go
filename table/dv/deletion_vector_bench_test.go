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

package dv

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/puffin"
)

var benchmarkSerializedDV []byte

func BenchmarkSerializeDV(b *testing.B) {
	for _, tt := range []struct {
		name              string
		positions         int
		stride            uint64
		explicitPositions []uint64
	}{
		{name: "sparse-1k", positions: 1_000, stride: 1_024},
		{name: "sparse-100k", positions: 100_000, stride: 32},
		{name: "sparse-1m", positions: 1_000_000, stride: 4},
		{name: "two-buckets", explicitPositions: []uint64{0, 1 << 32}},
	} {
		b.Run(tt.name, func(b *testing.B) {
			bitmap := NewRoaringPositionBitmap()
			if tt.explicitPositions != nil {
				for _, position := range tt.explicitPositions {
					bitmap.Set(position)
				}
			} else {
				for i := range tt.positions {
					bitmap.Set(uint64(i) * tt.stride)
				}
			}

			// SerializeDV run-length-optimizes the bitmap in place. Warm up once
			// so the timed loop measures steady-state re-serialization.
			sample, err := SerializeDV(bitmap)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.SetBytes(int64(len(sample)))
			for range b.N {
				benchmarkSerializedDV, err = SerializeDV(bitmap)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkReadDVs(b *testing.B) {
	for _, numDVs := range []int{2, 16, 64} {
		b.Run(fmt.Sprintf("dvs=%d", numDVs), func(b *testing.B) {
			files := benchmarkDVFiles(b, numDVs)
			fs := &countingReadIO{base: iceio.LocalFS{}}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				fs.reads = 0
				bitmaps, err := ReadDVs(fs, files)
				if err != nil {
					b.Fatal(err)
				}
				if len(bitmaps) != numDVs {
					b.Fatalf("got %d bitmaps, want %d", len(bitmaps), numDVs)
				}
			}
			b.ReportMetric(float64(fs.reads), "range-reads/op")
			b.StopTimer()
		})
	}
}

func benchmarkDVFiles(b *testing.B, numDVs int) []iceberg.DataFile {
	b.Helper()

	path := filepath.Join(b.TempDir(), "deletion-vectors.puffin")
	f, err := os.Create(path)
	if err != nil {
		b.Fatal(err)
	}

	writer, err := puffin.NewWriter(f)
	if err != nil {
		_ = f.Close()
		b.Fatal(err)
	}

	files := make([]iceberg.DataFile, numDVs)
	for i := range numDVs {
		bitmap := NewRoaringPositionBitmap()
		bitmap.Set(uint64(i))
		data, err := SerializeDV(bitmap)
		if err != nil {
			_ = f.Close()
			b.Fatal(err)
		}

		referencedDataFile := fmt.Sprintf("data-%03d.parquet", i)
		meta, err := writer.AddBlob(puffin.BlobMetadataInput{
			Type:           puffin.BlobTypeDeletionVector,
			SnapshotID:     -1,
			SequenceNumber: -1,
			Fields:         []int32{},
			Properties: map[string]string{
				dvReferencedDataFileProperty: referencedDataFile,
				dvCardinalityProperty:        "1",
			},
		}, data)
		if err != nil {
			_ = f.Close()
			b.Fatal(err)
		}

		offset, size := meta.Offset, meta.Length
		file := newDVTestFile(path, 1, &offset, &size)
		file.referencedDataFile = strPtr(referencedDataFile)
		files[i] = file
	}

	if err := writer.Finish(); err != nil {
		_ = f.Close()
		b.Fatal(err)
	}
	if err := f.Close(); err != nil {
		b.Fatal(err)
	}

	return files
}
