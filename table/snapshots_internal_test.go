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
		Name: "int_field", SourceID: 3, FieldID: 1001, Transform: iceberg.IdentityTransform{},
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
	}

	for _, tt := range tests {
		result, err := updateSnapshotSummaries(tt.sum, tt.previous)
		require.NoError(t, err)
		assert.Equal(t, tt.expected, result)
	}
}

func TestInvalidOperation(t *testing.T) {
	_, err := updateSnapshotSummaries(Summary{Operation: OpReplace}, nil)
	assert.ErrorIs(t, err, iceberg.ErrNotImplemented)
}
