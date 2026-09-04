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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadTasksUsesIdentityPartitionResidual(t *testing.T) {
	ctx := context.Background()
	const tableLocation = "mem://identity-residual"

	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "tenant_id", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "amount", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 3, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "tenant_id", Transform: iceberg.IdentityTransform{},
	})
	metadata, err := NewMetadata(schema, &spec, UnsortedSortOrder, tableLocation, nil)
	require.NoError(t, err)
	fs := iceio.NewMemFS()
	dataPath := tableLocation + "/data/file.parquet"
	dataSize := writePartitionResidualParquet(t, fs, dataPath, schema, `[
		{"tenant_id":"acme","amount":50,"payload":"low"},
		{"tenant_id":"acme","amount":150,"payload":"keep"},
		{"tenant_id":"acme","amount":250,"payload":"high"}
	]`)

	dataFileBuilder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		dataPath,
		iceberg.ParquetFile,
		map[int]any{1000: "acme"},
		nil,
		nil,
		3,
		dataSize,
	)
	require.NoError(t, err)

	snapshotID := int64(1)
	entry := iceberg.NewManifestEntryBuilder(
		iceberg.EntryStatusADDED,
		&snapshotID,
		dataFileBuilder.Build(),
	).SequenceNum(1).Build()
	manifestPath := tableLocation + "/metadata/manifest.avro"
	var manifestBuffer bytes.Buffer
	manifest, err := iceberg.WriteManifest(
		manifestPath, &manifestBuffer, 2, spec, schema, snapshotID, []iceberg.ManifestEntry{entry},
	)
	require.NoError(t, err)

	manifestListPath := tableLocation + "/metadata/snap-1.avro"
	var manifestListBuffer bytes.Buffer
	sequenceNumber := int64(1)
	require.NoError(t, iceberg.WriteManifestList(
		2, &manifestListBuffer, snapshotID, nil, &sequenceNumber, 0,
		[]iceberg.ManifestFile{manifest},
	))

	require.NoError(t, fs.WriteFile(manifestPath, manifestBuffer.Bytes()))
	require.NoError(t, fs.WriteFile(manifestListPath, manifestListBuffer.Bytes()))

	metadataBuilder, err := MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)
	schemaID := metadata.CurrentSchema().ID
	require.NoError(t, metadataBuilder.AddSnapshot(&Snapshot{
		SnapshotID:     snapshotID,
		SequenceNumber: 1,
		TimestampMs:    metadata.LastUpdatedMillis() + 1,
		ManifestList:   manifestListPath,
		Summary:        &Summary{Operation: OpAppend},
		SchemaID:       &schemaID,
	}))
	require.NoError(t, metadataBuilder.SetSnapshotRef(MainBranch, snapshotID, BranchRef))
	built, err := metadataBuilder.Build()
	require.NoError(t, err)

	tbl := New(
		Identifier{"db", "identity-residual"},
		built,
		tableLocation+"/metadata/metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil },
		nil,
	)
	identityFilter := iceberg.EqualTo(iceberg.Reference("tenant_id"), "acme")
	amountFilter := iceberg.GreaterThan(iceberg.Reference("amount"), int64(100))
	for _, tt := range []struct {
		name     string
		filter   iceberg.BooleanExpression
		residual iceberg.BooleanExpression
		payloads []string
	}{
		{
			name:     "identity only",
			filter:   identityFilter,
			residual: iceberg.AlwaysTrue{},
			payloads: []string{"low", "keep", "high"},
		},
		{
			name:     "mixed",
			filter:   iceberg.NewAnd(identityFilter, amountFilter),
			residual: amountFilter,
			payloads: []string{"keep", "high"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			scan := tbl.Scan(WithRowFilter(tt.filter), WithSelectedFields("payload"))
			tasks, err := scan.PlanFiles(ctx)
			require.NoError(t, err)
			require.Len(t, tasks, 1)
			require.NotNil(t, tasks[0].Residual)

			want, err := iceberg.BindExpr(schema, tt.residual, true)
			require.NoError(t, err)
			assert.True(t, tasks[0].Residual.Equals(want),
				"expected %s, got %s", want, tasks[0].Residual)

			resultSchema, records, err := scan.ReadTasks(ctx, tasks)
			require.NoError(t, err)
			require.Equal(t, 1, resultSchema.NumFields())
			require.Equal(t, "payload", resultSchema.Field(0).Name)

			var payloads []string
			for record, err := range records {
				require.NoError(t, err)
				values := record.Column(0).(*array.String)
				for i := range values.Len() {
					payloads = append(payloads, values.Value(i))
				}
				record.Release()
			}
			assert.Equal(t, tt.payloads, payloads)
		})
	}
}

func writePartitionResidualParquet(
	t *testing.T,
	fs *iceio.MemFS,
	path string,
	schema *iceberg.Schema,
	jsonData string,
) int64 {
	t.Helper()

	arrowSchema, err := SchemaToArrowSchema(schema, nil, true, false)
	require.NoError(t, err)
	record := mustLoadRecordBatchFromJSON(arrowSchema, jsonData)
	defer record.Release()

	tbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{record})
	defer tbl.Release()

	writer, err := fs.Create(path)
	require.NoError(t, err)
	require.NoError(t, pqarrow.WriteTable(tbl, writer, record.NumRows(),
		parquet.NewWriterProperties(parquet.WithStats(true)), pqarrow.DefaultWriterProps()))
	require.NoError(t, writer.Close())

	file, err := fs.Open(path)
	require.NoError(t, err)
	defer file.Close()
	info, err := file.Stat()
	require.NoError(t, err)

	return info.Size()
}
