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

package codec_test

import (
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/codec"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeFileScanTaskRoundTrip(t *testing.T) {
	for _, version := range []int{1, 2, 3} {
		t.Run("v"+strconv.Itoa(version), func(t *testing.T) {
			spec, schema, original := fullyPopulatedFileScanTask(t, version)

			bytes, err := codec.EncodeFileScanTask(original, spec, schema, version)
			require.NoError(t, err)
			require.NotEmpty(t, bytes)

			decoded, err := codec.DecodeFileScanTask(bytes, spec, schema, version)
			require.NoError(t, err)

			require.Equal(t, original.File.FilePath(), decoded.File.FilePath())
			require.Equal(t, original.File.Count(), decoded.File.Count())
			require.Equal(t, original.File.Partition(), decoded.File.Partition())

			require.Len(t, decoded.DeleteFiles, len(original.DeleteFiles))
			for i := range original.DeleteFiles {
				require.Equal(t, original.DeleteFiles[i].FilePath(), decoded.DeleteFiles[i].FilePath())
			}
			require.Len(t, decoded.EqualityDeleteFiles, len(original.EqualityDeleteFiles))
			for i := range original.EqualityDeleteFiles {
				require.Equal(t, original.EqualityDeleteFiles[i].FilePath(), decoded.EqualityDeleteFiles[i].FilePath())
			}
			require.Len(t, decoded.DeletionVectorFiles, len(original.DeletionVectorFiles))
			for i := range original.DeletionVectorFiles {
				require.Equal(t, original.DeletionVectorFiles[i].FilePath(), decoded.DeletionVectorFiles[i].FilePath(),
					"DV file path must round-trip")
				require.Equal(t, original.DeletionVectorFiles[i].ReferencedDataFile(), decoded.DeletionVectorFiles[i].ReferencedDataFile(),
					"DV file must remember the data file it deletes from")
				require.Equal(t, original.DeletionVectorFiles[i].ContentOffset(), decoded.DeletionVectorFiles[i].ContentOffset(),
					"DV file content offset (puffin blob offset) must round-trip")
				require.Equal(t, original.DeletionVectorFiles[i].ContentSizeInBytes(), decoded.DeletionVectorFiles[i].ContentSizeInBytes(),
					"DV file content size must round-trip")
				require.Equal(t, original.DeletionVectorFiles[i].FileFormat(), decoded.DeletionVectorFiles[i].FileFormat(),
					"DV file format must round-trip")
			}

			require.Equal(t, original.Start, decoded.Start)
			require.Equal(t, original.Length, decoded.Length)
			require.Equal(t, original.FirstRowID, decoded.FirstRowID)
			require.Equal(t, original.DataSequenceNumber, decoded.DataSequenceNumber)
		})
	}
}

func TestEncodeFileScanTaskRejectsMismatchedDeleteFileSpec(t *testing.T) {
	schema := iceberg.NewSchema(123,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
	)
	specMain := iceberg.NewPartitionSpecID(7,
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_part", Transform: iceberg.IdentityTransform{}},
	)
	specOther := iceberg.NewPartitionSpecID(99,
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_part", Transform: iceberg.IdentityTransform{}},
	)

	mainFile := newScanTaskDataFile(t, specMain, "s3://bucket/main.parquet",
		iceberg.EntryContentData, iceberg.ParquetFile, "", 2)
	mismatchedDelete := newScanTaskDataFile(t, specOther, "s3://bucket/del.parquet",
		iceberg.EntryContentPosDeletes, iceberg.ParquetFile, "", 2)

	task := table.FileScanTask{
		File:        mainFile,
		DeleteFiles: []iceberg.DataFile{mismatchedDelete},
	}
	_, err := codec.EncodeFileScanTask(task, specMain, schema, 2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "spec id",
		"error must call out the spec-id mismatch")
	require.Contains(t, err.Error(), "99",
		"error must include the offending delete file's spec id")
	require.Contains(t, err.Error(), "7",
		"error must include the codec's spec id")
}

func TestEncodeFileScanTaskEmptyDeleteLists(t *testing.T) {
	spec, schema, task := fullyPopulatedFileScanTask(t, 2)
	task.DeleteFiles = nil
	task.EqualityDeleteFiles = nil
	task.DeletionVectorFiles = nil

	bytes, err := codec.EncodeFileScanTask(task, spec, schema, 2)
	require.NoError(t, err)

	decoded, err := codec.DecodeFileScanTask(bytes, spec, schema, 2)
	require.NoError(t, err)
	require.Empty(t, decoded.DeleteFiles)
	require.Empty(t, decoded.EqualityDeleteFiles)
	require.Empty(t, decoded.DeletionVectorFiles)
}

func fullyPopulatedFileScanTask(t *testing.T, version int) (iceberg.PartitionSpec, *iceberg.Schema, table.FileScanTask) {
	t.Helper()
	schema := iceberg.NewSchema(123,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
	)
	spec := iceberg.NewPartitionSpecID(7,
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "id_part", Transform: iceberg.IdentityTransform{}},
	)

	file := newScanTaskDataFile(t, spec, "s3://bucket/ns/tbl/data/part-0000.parquet", iceberg.EntryContentData, iceberg.ParquetFile, "", version)
	delete1 := newScanTaskDataFile(t, spec, "s3://bucket/ns/tbl/data/del-0001.parquet", iceberg.EntryContentPosDeletes, iceberg.ParquetFile, "", version)

	firstRow := int64(0)
	dataSeq := int64(17)

	task := table.FileScanTask{
		File:               file,
		DeleteFiles:        []iceberg.DataFile{delete1},
		Start:              0,
		Length:             1024 * 1024,
		FirstRowID:         &firstRow,
		DataSequenceNumber: &dataSeq,
	}

	if version >= 2 {
		eq1 := newScanTaskDataFile(t, spec, "s3://bucket/ns/tbl/data/eq-0001.parquet", iceberg.EntryContentEqDeletes, iceberg.ParquetFile, "", version)
		task.EqualityDeleteFiles = []iceberg.DataFile{eq1}
	}

	if version >= 3 {
		// Deletion vectors are v3's first-class delete mechanism: a
		// Puffin blob that references the data file whose rows it
		// deletes. The round-trip test asserts the DV-specific fields
		// (file format, ReferencedDataFile, ContentOffset, ContentSize
		// In Bytes) so a regression in the DV encode path surfaces.
		dv := newScanTaskDataFile(t, spec, "s3://bucket/ns/tbl/data/dv-0001.puffin",
			iceberg.EntryContentPosDeletes, iceberg.PuffinFile, file.FilePath(), version)
		task.DeletionVectorFiles = []iceberg.DataFile{dv}
	}

	return spec, schema, task
}

func newScanTaskDataFile(t *testing.T, spec iceberg.PartitionSpec, path string, content iceberg.ManifestEntryContent, format iceberg.FileFormat, referencedDataFile string, version int) iceberg.DataFile {
	t.Helper()
	builder, err := iceberg.NewDataFileBuilder(
		spec,
		content,
		path,
		format,
		map[int]any{1000: int64(42)},
		map[int]string{},
		map[int]int{},
		1024,
		1024*1024,
	)
	require.NoError(t, err)
	builder.
		ColumnSizes(map[int]int64{1: 512}).
		ValueCounts(map[int]int64{1: 1024}).
		NullValueCounts(map[int]int64{1: 0}).
		LowerBoundValues(map[int][]byte{1: {0x01}}).
		UpperBoundValues(map[int][]byte{1: {0xff}})
	if content == iceberg.EntryContentEqDeletes {
		builder.EqualityFieldIDs([]int{1})
	}
	if version >= 3 {
		builder.FirstRowID(0)
	}
	if referencedDataFile != "" {
		builder.ReferencedDataFile(referencedDataFile).
			ContentOffset(128).
			ContentSizeInBytes(2048)
	}

	return builder.Build()
}
