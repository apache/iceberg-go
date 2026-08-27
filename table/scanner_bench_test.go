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
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

func BenchmarkOpenManifestPartitionRejectsMostEntries(b *testing.B) {
	const (
		entryCount    = 1_000
		rejectThrough = 950
	)

	spec := partitionedSpec()
	schema := simpleSchema()
	snapshotID := int64(1)
	entries := make([]iceberg.ManifestEntry, entryCount)
	for i := range entries {
		value := int32(i)
		builder, err := iceberg.NewDataFileBuilder(
			spec,
			iceberg.EntryContentData,
			fmt.Sprintf("mem://default/table/data/file-%d.parquet", i),
			iceberg.ParquetFile,
			map[int]any{1000: value},
			nil,
			nil,
			1,
			100,
		)
		if err != nil {
			b.Fatal(err)
		}

		bound := make([]byte, 4)
		binary.LittleEndian.PutUint32(bound, uint32(value))
		entries[i] = iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED,
			&snapshotID,
			nil,
			nil,
			builder.
				LowerBoundValues(map[int][]byte{1: bound}).
				UpperBoundValues(map[int][]byte{1: bound}).
				Build(),
		)
	}

	manifestPath := "mem://default/table/metadata/manifest.avro"
	var manifestBytes bytes.Buffer
	manifest, err := iceberg.WriteManifest(
		manifestPath,
		&manifestBytes,
		2,
		spec,
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

	// This simplified filter isolates skipped metrics evaluation; production
	// scans build the evaluator through buildPartitionEvaluator.
	partitionFilter := func(df iceberg.DataFile) (bool, error) {
		return df.Partition()[1000].(int32) >= rejectThrough, nil
	}
	metricsEval, err := newInclusiveMetricsEvaluator(
		schema,
		iceberg.EqualTo(iceberg.Reference("id"), int32(entryCount-1)),
		true,
		false,
	)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		matched, err := openManifest(fs, manifest, partitionFilter, metricsEval)
		if err != nil {
			b.Fatal(err)
		}
		if len(matched) != 1 {
			b.Fatalf("openManifest returned %d entries, want 1", len(matched))
		}
	}
}
