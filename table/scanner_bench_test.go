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

var effectiveSchemaBenchmarkSink int

func BenchmarkEffectiveSchemaSnapshot(b *testing.B) {
	for _, schemaCount := range []int{1, 16, 128, 1024} {
		for _, fieldCount := range []int{16, 128} {
			b.Run(fmt.Sprintf("schemas=%d/fields=%d", schemaCount, fieldCount), func(b *testing.B) {
				scan := benchmarkHistoricalSchemaScan(b, schemaCount, fieldCount)

				b.Run("before", func(b *testing.B) {
					benchmarkEffectiveSchema(b, scan, false)
				})
				b.Run("after", func(b *testing.B) {
					benchmarkEffectiveSchema(b, scan, true)
				})
			})
		}
	}
}

func benchmarkHistoricalSchemaScan(b *testing.B, schemaCount, fieldCount int) *Scan {
	b.Helper()

	schemas := make([]*iceberg.Schema, schemaCount)
	for schemaID := range schemaCount {
		fields := make([]iceberg.NestedField, fieldCount)
		for fieldID := range fieldCount {
			fields[fieldID] = iceberg.NestedField{
				ID:       fieldID + 1,
				Name:     fmt.Sprintf("field_%d", fieldID),
				Type:     iceberg.PrimitiveTypes.Int64,
				Required: true,
			}
		}
		schemas[schemaID] = iceberg.NewSchema(schemaID, fields...)
	}

	snapshotID := int64(1)
	snapshotSchemaID := schemas[len(schemas)-1].ID
	snapshots := []Snapshot{{SnapshotID: snapshotID, SchemaID: &snapshotSchemaID}}
	metadata := &metadataV2{
		commonMetadata: commonMetadata{
			SchemaList:        schemas,
			CurrentSchemaID:   schemas[0].ID,
			SnapshotList:      snapshots,
			CurrentSnapshotID: &snapshotID,
			snapshotIndex:     buildSnapshotIndex(snapshots),
		},
	}

	return &Scan{
		metadata:   metadata,
		snapshotID: &snapshotID,
	}
}

func benchmarkEffectiveSchema(b *testing.B, scan *Scan, useLookup bool) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		var (
			schema *iceberg.Schema
			err    error
		)
		if useLookup {
			schema, err = scan.effectiveSchema()
		} else {
			schema, err = effectiveSchemaBeforeLookup(scan)
		}
		if err != nil {
			b.Fatal(err)
		}
		effectiveSchemaBenchmarkSink += schema.ID
	}
}

func effectiveSchemaBeforeLookup(scan *Scan) (*iceberg.Schema, error) {
	curSchema := scan.metadata.CurrentSchema()
	if !scan.snapshotSchemaEnabled() {
		return curSchema, nil
	}

	snap, err := scan.ResolveSnapshot()
	if err != nil {
		return nil, err
	}

	if snap.SchemaID == nil {
		return curSchema, nil
	}

	for _, schema := range scan.metadata.Schemas() {
		if schema.ID == *snap.SchemaID {
			return schema, nil
		}
	}

	return nil, fmt.Errorf("schema %d not found", *snap.SchemaID)
}

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
