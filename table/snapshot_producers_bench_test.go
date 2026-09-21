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
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// delayedManifestIO approximates the latency of a remote metadata store while
// keeping the benchmark deterministic and independent of external services.
type delayedManifestIO struct {
	*memIO
	delay time.Duration
}

func (io *delayedManifestIO) Open(name string) (iceio.File, error) {
	time.Sleep(io.delay)

	return io.memIO.Open(name)
}

func (io *delayedManifestIO) Create(name string) (iceio.FileWriter, error) {
	time.Sleep(io.delay)

	return io.memIO.Create(name)
}

func BenchmarkOverwriteParentDependentManifests(b *testing.B) {
	const (
		manifestCount = 32
		ioDelay       = time.Millisecond
	)

	for _, concurrency := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("manifests=%d/delay=%s/concurrency=%d", manifestCount, ioDelay, concurrency), func(b *testing.B) {
			fs := &delayedManifestIO{
				memIO: newMemIO(1<<20, nil),
				delay: ioDelay,
			}
			spec := partitionedSpec()
			schema := simpleSchema()
			txn := createTestTransaction(b, fs, spec)
			sp := newOverwriteFilesProducer(OpOverwrite, txn, fs, nil, nil)
			of := sp.producerImpl.(*overwriteFiles)
			of.manifestConcurrency = concurrency

			snapshotID := int64(100)
			sequenceNumber := int64(-1)
			manifestSequenceNumber := int64(42)
			manifests := make([]iceberg.ManifestFile, 0, manifestCount)
			for i := range manifestCount {
				deletedFile := newTestDataFile(b, spec, fmt.Sprintf("file://deleted-%d.parquet", i), nil)
				keptFile := newTestDataFile(b, spec, fmt.Sprintf("file://kept-%d.parquet", i), nil)
				sp.deleteDataFile(deletedFile)

				entries := []iceberg.ManifestEntry{
					iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, &manifestSequenceNumber, nil, deletedFile),
					iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, &manifestSequenceNumber, nil, keptFile),
				}
				path := fmt.Sprintf("table-location/metadata/source-%d.avro", i)
				manifests = append(manifests, writeTestManifestWithEntries(b, fs, spec, schema, snapshotID, path, entries))
			}

			manifestListPath := "table-location/metadata/snap-1.avro"
			var listBuf bytes.Buffer
			err := iceberg.WriteManifestList(2, &listBuf, snapshotID, nil, &sequenceNumber, 0, manifests)
			if err != nil {
				b.Fatal(err)
			}
			if err := fs.WriteFile(manifestListPath, listBuf.Bytes()); err != nil {
				b.Fatal(err)
			}

			snap := Snapshot{
				SnapshotID:     snapshotID,
				SequenceNumber: sequenceNumber,
				ManifestList:   manifestListPath,
			}

			b.ReportAllocs()
			b.ReportMetric(float64(manifestCount), "manifests/op")
			b.ResetTimer()
			for range b.N {
				sp.manifestCount.Store(0)
				if _, err := sp.parentDependentManifests(context.Background(), &snap); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
