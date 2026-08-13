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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateDataFileName(t *testing.T) {
	tests := []struct {
		name      string
		task      WriteTask
		extension string
		want      string
	}{
		{
			name: "unpartitioned table first file",
			task: WriteTask{
				Uuid:        uuid.MustParse("12345678-1234-1234-1234-123456789abc"),
				ID:          0,
				PartitionID: 0,
				FileCount:   1,
			},
			extension: "parquet",
			want:      "00000-0-12345678-1234-1234-1234-123456789abc-00001.parquet",
		},
		{
			name: "unpartitioned table multiple files",
			task: WriteTask{
				Uuid:        uuid.MustParse("12345678-1234-1234-1234-123456789abc"),
				ID:          0,
				PartitionID: 0,
				FileCount:   42,
			},
			extension: "parquet",
			want:      "00000-0-12345678-1234-1234-1234-123456789abc-00042.parquet",
		},
		{
			name: "partitioned table first partition",
			task: WriteTask{
				Uuid:        uuid.MustParse("87654321-4321-4321-4321-cba987654321"),
				ID:          1,
				PartitionID: 0,
				FileCount:   1,
			},
			extension: "parquet",
			want:      "00000-1-87654321-4321-4321-4321-cba987654321-00001.parquet",
		},
		{
			name: "partitioned table second partition",
			task: WriteTask{
				Uuid:        uuid.MustParse("87654321-4321-4321-4321-cba987654321"),
				ID:          2,
				PartitionID: 1,
				FileCount:   1,
			},
			extension: "parquet",
			want:      "00001-2-87654321-4321-4321-4321-cba987654321-00001.parquet",
		},
		{
			name: "partitioned table multiple files in partition",
			task: WriteTask{
				Uuid:        uuid.MustParse("87654321-4321-4321-4321-cba987654321"),
				ID:          3,
				PartitionID: 2,
				FileCount:   15,
			},
			extension: "parquet",
			want:      "00002-3-87654321-4321-4321-4321-cba987654321-00015.parquet",
		},
		{
			name: "large partition ID",
			task: WriteTask{
				Uuid:        uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
				ID:          100,
				PartitionID: 12345,
				FileCount:   99999,
			},
			extension: "parquet",
			want:      "12345-100-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee-99999.parquet",
		},
		{
			name: "more than 5 digits",
			task: WriteTask{
				Uuid:        uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"),
				ID:          123456,
				PartitionID: 567890,
				FileCount:   123456,
			},
			extension: "parquet",
			want:      "567890-123456-ffffffff-ffff-ffff-ffff-ffffffffffff-123456.parquet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.task.GenerateDataFileName(tt.extension)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGenerateDataFileNameFormat(t *testing.T) {
	// Test that the format matches the Java implementation:
	// {partitionId:05d}-{taskId}-{operationId}-{fileCount:05d}.{extension}

	task := WriteTask{
		Uuid:        uuid.MustParse("12345678-1234-1234-1234-123456789abc"),
		ID:          5,
		PartitionID: 3,
		FileCount:   7,
	}

	filename := task.GenerateDataFileName("parquet")

	// Verify format components
	require.Contains(t, filename, "00003-", "partition ID should be 5-digit padded")
	require.Contains(t, filename, "-5-", "task ID should be present")
	require.Contains(t, filename, "-12345678-1234-1234-1234-123456789abc-", "UUID should be present")
	require.Contains(t, filename, "-00007.parquet", "file count should be 5-digit padded with extension")

	// Verify exact format
	expected := "00003-5-12345678-1234-1234-1234-123456789abc-00007.parquet"
	assert.Equal(t, expected, filename)
}

func TestGenerateDataFileNameUniqueness(t *testing.T) {
	// Test that different tasks generate different filenames
	baseUUID := uuid.MustParse("12345678-1234-1234-1234-123456789abc")

	task1 := WriteTask{Uuid: baseUUID, ID: 0, PartitionID: 0, FileCount: 1}
	task2 := WriteTask{Uuid: baseUUID, ID: 0, PartitionID: 0, FileCount: 2}
	task3 := WriteTask{Uuid: baseUUID, ID: 1, PartitionID: 0, FileCount: 1}
	task4 := WriteTask{Uuid: baseUUID, ID: 0, PartitionID: 1, FileCount: 1}

	file1 := task1.GenerateDataFileName("parquet")
	file2 := task2.GenerateDataFileName("parquet")
	file3 := task3.GenerateDataFileName("parquet")
	file4 := task4.GenerateDataFileName("parquet")

	// All filenames should be unique
	filenames := []string{file1, file2, file3, file4}
	seen := make(map[string]bool)
	for _, f := range filenames {
		assert.False(t, seen[f], "filename %s should be unique", f)
		seen[f] = true
	}
}

// writeFile records a non-zero WriteTask.SortOrderID on the DataFile, nothing
// when the task makes no claim.
func TestWriteFileHonorsExplicitSortOrderID(t *testing.T) {
	t.Parallel()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	metadataBuilder, err := NewMetadataBuilder(2)
	require.NoError(t, err)
	require.NoError(t, metadataBuilder.AddSchema(schema))
	require.NoError(t, metadataBuilder.SetCurrentSchemaID(0))
	unpartitioned := *iceberg.UnpartitionedSpec
	require.NoError(t, metadataBuilder.AddPartitionSpec(&unpartitioned, true))
	require.NoError(t, metadataBuilder.SetDefaultSpecID(0))
	sortOrder, err := NewSortOrder(1, []SortField{{
		SourceIDs: []int{1},
		Direction: SortASC,
		Transform: iceberg.IdentityTransform{},
		NullOrder: NullsFirst,
	}})
	require.NoError(t, err)
	require.NoError(t, metadataBuilder.AddSortOrder(&sortOrder))
	require.NoError(t, metadataBuilder.SetDefaultSortOrderID(-1))

	built, err := metadataBuilder.Build()
	require.NoError(t, err)
	claimedID := built.DefaultSortOrder()
	require.NotZero(t, claimedID, "sanity: sort order id should be non-zero")

	arrowSc, err := SchemaToArrowSchema(schema, nil, true, false)
	require.NoError(t, err)

	writer, err := newDataFileWriter(t.TempDir(), &io.LocalFS{}, metadataBuilder, iceberg.Properties{})
	require.NoError(t, err)

	writeTask := func(id, sortOrderID int) WriteTask {
		rb := mustLoadRecordBatchFromJSON(arrowSc, `[{"id": 2}, {"id": 1}]`)

		return WriteTask{
			Uuid:        uuid.New(),
			ID:          id,
			FileCount:   1,
			Schema:      schema,
			Batches:     []arrow.RecordBatch{rb},
			SortOrderID: sortOrderID,
		}
	}

	t.Run("non-zero claim lands on the data file", func(t *testing.T) {
		df, err := writer.writeFile(t.Context(), nil, writeTask(0, claimedID))
		require.NoError(t, err)
		require.NotNil(t, df.SortOrderID(), "explicit claim must be recorded on the DataFile")
		assert.Equal(t, claimedID, *df.SortOrderID())
	})

	t.Run("zero claim leaves the field absent", func(t *testing.T) {
		df, err := writer.writeFile(t.Context(), nil, writeTask(1, UnsortedSortOrderID))
		require.NoError(t, err)
		assert.Nil(t, df.SortOrderID())
	})
}

// A sort order claim on position delete content is rejected with an error, not
// a panic deep in the file writer's Close().
func TestWriteFileRejectsPosDeleteSortOrderClaim(t *testing.T) {
	t.Parallel()

	metadataBuilder, err := NewMetadataBuilder(2)
	require.NoError(t, err)
	require.NoError(t, metadataBuilder.AddSchema(clonePositionalDeleteSchema()))
	require.NoError(t, metadataBuilder.SetCurrentSchemaID(0))
	unpartitioned := *iceberg.UnpartitionedSpec
	require.NoError(t, metadataBuilder.AddPartitionSpec(&unpartitioned, true))
	require.NoError(t, metadataBuilder.SetDefaultSpecID(0))

	writer, err := newPositionDeleteWriter(t.TempDir(), &io.LocalFS{}, metadataBuilder, iceberg.Properties{})
	require.NoError(t, err)

	rb := mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema,
		`[{"file_path": "file://t/data.parquet", "pos": 0}]`)
	task := WriteTask{
		Uuid:        uuid.New(),
		ID:          0,
		FileCount:   1,
		Schema:      iceberg.PositionalDeleteSchema,
		Batches:     []arrow.RecordBatch{rb},
		SortOrderID: 1,
	}

	df, err := writer.writeFile(t.Context(), nil, task)
	require.Error(t, err, "a sort order claim on position delete content must be rejected")
	assert.Nil(t, df)
	assert.Contains(t, err.Error(), "position delete")
}
