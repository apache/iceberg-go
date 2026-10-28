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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var tableSchemaSimple = iceberg.NewSchemaWithIdentifiers(1,
	[]int{2},
	iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String},
	iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	iceberg.NestedField{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool},
)

func TestSnapshotSummaryCollector(t *testing.T) {
	var ssc SnapshotSummaryCollector

	assert.Equal(t, iceberg.Properties{}, ssc.build())

	dataFile, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
		iceberg.EntryContentData, "/path/to/file.parquet", iceberg.ParquetFile, nil, nil, nil, 100, 1234)
	require.NoError(t, err)
	require.NoError(t, ssc.addFile(dataFile.Build(), tableSchemaSimple, *iceberg.UnpartitionedSpec))

	assert.Equal(t, iceberg.Properties{
		"added-data-files": "1",
		"added-files-size": "1234",
		"added-records":    "100",
	}, ssc.build())
}

func TestSnapshotSummaryCollectorWithPartition(t *testing.T) {
	var ssc SnapshotSummaryCollector

	assert.Equal(t, iceberg.Properties{}, ssc.build())
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "bool_field", Type: iceberg.PrimitiveTypes.Bool},
		iceberg.NestedField{ID: 2, Name: "string_field", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "int_field", Type: iceberg.PrimitiveTypes.Int32},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		Name: "int_field", SourceIDs: []int{3}, FieldID: 1001, Transform: iceberg.IdentityTransform{},
	})

	dataFile1 := must(iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentData, "/path/to/file1.parquet",
		iceberg.ParquetFile, map[int]any{1001: int32(1)}, nil, nil, 100, 1234)).Build()

	dataFile2 := must(iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentData, "/path/to/file2.parquet",
		iceberg.ParquetFile, map[int]any{1001: int32(2)}, nil, nil, 200, 4321)).Build()

	ssc.addFile(dataFile1, sc, spec)
	ssc.removeFile(dataFile1, sc, spec)
	ssc.removeFile(dataFile2, sc, spec)
	require.Len(t, ssc.partitionPathPlans, 1)

	assert.Equal(t, iceberg.Properties{
		"added-files-size":        "1234",
		"removed-files-size":      "5555",
		"added-data-files":        "1",
		"deleted-data-files":      "2",
		"added-records":           "100",
		"deleted-records":         "300",
		"changed-partition-count": "2",
	}, ssc.build())

	ssc.setPartitionSummaryLimit(10)
	assert.Equal(t, iceberg.Properties{
		"added-files-size":        "1234",
		"removed-files-size":      "5555",
		"added-data-files":        "1",
		"deleted-data-files":      "2",
		"added-records":           "100",
		"deleted-records":         "300",
		"changed-partition-count": "2",
		"partitions.int_field=1":  "added-data-files=1,added-files-size=1234,added-records=100,deleted-data-files=1,deleted-records=100,removed-files-size=1234",
		"partitions.int_field=2":  "deleted-data-files=1,deleted-records=200,removed-files-size=4321",
	}, ssc.build())
}

func TestSnapshotSummaryCollectorPartitionPathParity(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "ts", Type: iceberg.PrimitiveTypes.TimestampTz},
		iceberg.NestedField{ID: 2, Name: "text", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "integer", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 4, Name: "bytes", Type: iceberg.PrimitiveTypes.Binary},
	)
	unknown, err := iceberg.ParseTransform("custom_transform[42]")
	require.NoError(t, err)
	spec := iceberg.NewPartitionSpecID(7,
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "ts /tz", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: "text#part", Transform: iceberg.TruncateTransform{Width: 8}},
		iceberg.PartitionField{SourceIDs: []int{3}, FieldID: 1002, Name: "bucket", Transform: iceberg.BucketTransform{NumBuckets: 16}},
		iceberg.PartitionField{SourceIDs: []int{9}, FieldID: 1003, Name: "missing", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceIDs: []int{3}, FieldID: 1004, Name: "custom", Transform: unknown},
		iceberg.PartitionField{SourceIDs: []int{4}, FieldID: 1005, Name: "binary", Transform: iceberg.IdentityTransform{}},
	)

	partitions := []map[int]any{
		{1000: iceberg.Timestamp(1705314600000000), 1001: "a/b c", 1002: int32(7), 1003: nil, 1004: int32(42), 1005: []byte{0, 1, 255}},
		{1000: iceberg.Timestamp(0), 1001: "", 1002: int32(0), 1003: nil, 1004: nil, 1005: []byte{}},
	}

	var collector SnapshotSummaryCollector
	for _, partition := range partitions {
		want := spec.PartitionToPath(newPartitionRecord(partition, spec.PartitionType(schema)), schema)
		assert.Equal(t, want, collector.partitionPath(partition, schema, spec))
	}
	require.Len(t, collector.partitionPathPlans, 1)

	otherSpec := iceberg.NewPartitionSpecID(8,
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1100, Name: "other#text", Transform: iceberg.IdentityTransform{}},
	)
	otherPartition := map[int]any{1100: "a/b c"}
	want := otherSpec.PartitionToPath(newPartitionRecord(otherPartition, otherSpec.PartitionType(schema)), schema)
	assert.Equal(t, want, collector.partitionPath(otherPartition, schema, otherSpec))
	require.Len(t, collector.partitionPathPlans, 2)

	// Partition spec IDs are table-local. Reusing an ID must not reuse the
	// cached plan when a different spec has been supplied.
	sameIDSpec := iceberg.NewPartitionSpecID(spec.ID(),
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1101, Name: "same-id#text", Transform: iceberg.IdentityTransform{}},
	)
	sameIDPartition := map[int]any{1101: "a/b c"}
	want = sameIDSpec.PartitionToPath(
		newPartitionRecord(sameIDPartition, sameIDSpec.PartitionType(schema)), schema)
	assert.Equal(t, want, collector.partitionPath(sameIDPartition, schema, sameIDSpec))
	require.Len(t, collector.partitionPathPlans, 3)

	// A schema change can also change how an existing spec formats values.
	alternateSchema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp},
		iceberg.NestedField{ID: 2, Name: "text", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "integer", Type: iceberg.PrimitiveTypes.Int32},
		iceberg.NestedField{ID: 4, Name: "bytes", Type: iceberg.PrimitiveTypes.Binary},
	)
	want = spec.PartitionToPath(newPartitionRecord(partitions[0], spec.PartitionType(alternateSchema)), alternateSchema)
	assert.Equal(t, want, collector.partitionPath(partitions[0], alternateSchema, spec))
	require.Len(t, collector.partitionPathPlans, 4)
}

func TestMergeSnapshotSummaries(t *testing.T) {
	tests := []struct {
		sum           Summary
		previous      iceberg.Properties
		truncateTable bool
		expected      Summary
	}{
		{
			Summary{Operation: OpAppend},
			nil, false,
			Summary{Operation: OpAppend, Properties: iceberg.Properties{
				"total-data-files":       "0",
				"total-delete-files":     "0",
				"total-records":          "0",
				"total-files-size":       "0",
				"total-position-deletes": "0",
				"total-equality-deletes": "0",
			}},
		},
		{
			Summary{Operation: OpAppend, Properties: iceberg.Properties{
				"added-data-files":       "1",
				"added-delete-files":     "2",
				"added-equality-deletes": "3",
				"added-files-size":       "4",
				"added-position-deletes": "5",
				"added-records":          "6",
			}},
			nil, false,
			Summary{Operation: OpAppend, Properties: iceberg.Properties{
				"added-data-files":       "1",
				"added-delete-files":     "2",
				"added-equality-deletes": "3",
				"added-files-size":       "4",
				"added-position-deletes": "5",
				"added-records":          "6",
				"total-data-files":       "1",
				"total-delete-files":     "2",
				"total-records":          "6",
				"total-files-size":       "4",
				"total-position-deletes": "5",
				"total-equality-deletes": "3",
			}},
		},
		{
			Summary{Operation: OpReplace},
			nil, false,
			Summary{Operation: OpReplace, Properties: iceberg.Properties{
				"total-data-files":       "0",
				"total-delete-files":     "0",
				"total-records":          "0",
				"total-files-size":       "0",
				"total-position-deletes": "0",
				"total-equality-deletes": "0",
			}},
		},
		{
			Summary{Operation: OpReplace, Properties: iceberg.Properties{
				"added-data-files":       "1",
				"added-delete-files":     "2",
				"added-equality-deletes": "3",
				"added-files-size":       "4",
				"added-position-deletes": "5",
				"added-records":          "6",
			}},
			nil, false,
			Summary{Operation: OpReplace, Properties: iceberg.Properties{
				"added-data-files":       "1",
				"added-delete-files":     "2",
				"added-equality-deletes": "3",
				"added-files-size":       "4",
				"added-position-deletes": "5",
				"added-records":          "6",
				"total-data-files":       "1",
				"total-delete-files":     "2",
				"total-records":          "6",
				"total-files-size":       "4",
				"total-position-deletes": "5",
				"total-equality-deletes": "3",
			}},
		},
		{
			Summary{Operation: OpReplace, Properties: iceberg.Properties{
				"added-data-files":   "1",
				"deleted-data-files": "1",
			}},
			iceberg.Properties{
				"total-data-files":       "10",
				"total-delete-files":     "0",
				"total-records":          "0",
				"total-files-size":       "0",
				"total-position-deletes": "0",
				"total-equality-deletes": "0",
			},
			false,
			Summary{Operation: OpReplace, Properties: iceberg.Properties{
				"added-data-files":       "1",
				"deleted-data-files":     "1",
				"total-data-files":       "10",
				"total-delete-files":     "0",
				"total-records":          "0",
				"total-files-size":       "0",
				"total-position-deletes": "0",
				"total-equality-deletes": "0",
			}},
		},
	}

	for _, tt := range tests {
		result, err := updateSnapshotSummaries(tt.sum, tt.previous)
		require.NoError(t, err)
		assert.Equal(t, tt.expected, result)
	}
}

func TestUpdateSnapshotSummariesUnsupportedOperation(t *testing.T) {
	_, err := updateSnapshotSummaries(Summary{Operation: Operation("scan")}, nil)
	assert.ErrorIs(t, err, iceberg.ErrNotImplemented)
}

func TestUpdateSnapshotSummariesPreservesLargeTotals(t *testing.T) {
	t.Parallel()

	result, err := updateSnapshotSummaries(Summary{Operation: OpAppend, Properties: iceberg.Properties{
		addedRecordsKey:  "1000000000",
		addedFileSizeKey: "1000000000",
	}}, iceberg.Properties{
		totalRecordsKey:  "3000000000",
		totalFileSizeKey: "8000000000",
	})
	require.NoError(t, err)
	assert.Equal(t, "4000000000", result.Properties[totalRecordsKey])
	assert.Equal(t, "9000000000", result.Properties[totalFileSizeKey])
}

func TestSnapshotSummaryCollectorDeletionVectors(t *testing.T) {
	var ssc SnapshotSummaryCollector
	spec := *iceberg.UnpartitionedSpec

	// Two DVs that share one Puffin file, as DVWriter writes them.
	newDV := func(ref string, count, offset, size int64) iceberg.DataFile {
		return must(iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes,
			"/path/to/batch.puffin", iceberg.PuffinFile, nil, nil, nil, count, 1000)).
			ReferencedDataFile(ref).
			ContentOffset(offset).
			ContentSizeInBytes(size).
			Build()
	}
	dvA := newDV("/path/to/data-a.parquet", 2, 4, 300)
	dvB := newDV("/path/to/data-b.parquet", 4, 304, 400)

	oldDV := must(iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes,
		"/path/to/old.puffin", iceberg.PuffinFile, nil, nil, nil, 1, 600)).
		ReferencedDataFile("/path/to/data-a.parquet").
		ContentOffset(4).
		ContentSizeInBytes(250).
		Build()

	posDelete := must(iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes,
		"/path/to/pos-delete.parquet", iceberg.ParquetFile, nil, nil, nil, 3, 800)).Build()

	require.NoError(t, ssc.addFile(dvA, tableSchemaSimple, spec))
	require.NoError(t, ssc.addFile(dvB, tableSchemaSimple, spec))
	require.NoError(t, ssc.addFile(posDelete, tableSchemaSimple, spec))
	require.NoError(t, ssc.removeFile(oldDV, tableSchemaSimple, spec))

	props := ssc.build()

	// DVs are counted on their own...
	assert.Equal(t, "2", props["added-dvs"])
	assert.Equal(t, "1", props["removed-dvs"])
	// ...and are not counted as position delete files.
	assert.Equal(t, "1", props["added-position-delete-files"])
	assert.NotContains(t, props, "removed-position-delete-files")
	// DVs still count as delete files and as position deletes.
	assert.Equal(t, "3", props["added-delete-files"])
	assert.Equal(t, "1", props["removed-delete-files"])
	assert.Equal(t, "9", props["added-position-deletes"]) // 2 + 4 + 3
	assert.Equal(t, "1", props["removed-position-deletes"])
}
