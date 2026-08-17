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

package table_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newReplaceFilesTestTable(t *testing.T) *table.Table {
	return newReplaceFilesTestTableVersion(t, 2)
}

func newReplaceFilesTestTableVersion(t *testing.T, version int) *table.Table {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
		table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: strconv.Itoa(version)})
	require.NoError(t, err)

	return table.New(
		table.Identifier{"db", "replace_files_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(ctx context.Context) (iceio.IO, error) {
			return iceio.LocalFS{}, nil
		},
		&rowDeltaCatalog{metadata: meta},
	)
}

func newPartitionedReplaceFilesTestTable(t *testing.T) (*table.Table, iceberg.PartitionSpec) {
	t.Helper()

	location := filepath.ToSlash(t.TempDir())
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
	})
	meta, err := table.NewMetadata(schema, &spec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	return table.New(
		table.Identifier{"db", "partitioned_replace_files_test"},
		meta, location+"/metadata/v1.metadata.json",
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		&rowDeltaCatalog{metadata: meta},
	), spec
}

func newRewriteDeletionVector(t *testing.T, path, ref string, offset, length *int64) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		path, iceberg.PuffinFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	if ref != "" {
		builder.ReferencedDataFile(ref)
	}
	if offset != nil {
		builder.ContentOffset(*offset)
	}
	if length != nil {
		builder.ContentSizeInBytes(*length)
	}

	return builder.Build()
}

func TestReplaceFiles_DataAndDeleteFiles(t *testing.T) {
	tbl := newReplaceFilesTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	// Step 1: Write and commit a data file with 3 rows
	dataPath := tbl.Location() + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[
		{"id": 1, "data": "alpha"},
		{"id": 2, "data": "beta"},
		{"id": 3, "data": "gamma"}
	]`)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	assertRowCount(t, tbl, 3)

	// Step 2: Add a position delete file via RowDelta
	posDelPath := tbl.Location() + "/data/pos-del-001.parquet"
	writeParquetFile(t, posDelPath, table.PositionalDeleteArrowSchema,
		fmt.Sprintf(`[{"file_path": "%s", "pos": 1}]`, dataPath))

	posDelBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		posDelPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	posDelFile := posDelBuilder.Build()

	tx2 := tbl.NewTransaction()
	rd := tx2.NewRowDelta(nil)
	rd.AddDeletes(posDelFile)
	require.NoError(t, rd.Commit(t.Context()))
	tbl, err = tx2.Commit(t.Context())
	require.NoError(t, err)
	assertRowCount(t, tbl, 2) // beta deleted

	// Step 3: Get existing data + delete files from scan tasks
	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	oldDataFile := tasks[0].File
	var deleteFilesToRemove []iceberg.DataFile
	deleteFilesToRemove = append(deleteFilesToRemove, tasks[0].DeleteFiles...)
	require.Len(t, deleteFilesToRemove, 1)

	// Step 4: Write a compacted data file (without deleted row)
	newDataPath := tbl.Location() + "/data/data-compacted.parquet"
	writeParquetFile(t, newDataPath, arrowSc, `[
		{"id": 1, "data": "alpha"},
		{"id": 3, "data": "gamma"}
	]`)

	// Build new DataFile directly (not via AddFiles which would commit it)
	newDataFileBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		newDataPath, iceberg.ParquetFile, nil, nil, nil, 2, 512)
	require.NoError(t, err)
	newDataFile := newDataFileBuilder.Build()

	// Step 5: ReplaceFiles — swap old data + remove delete file
	tx3 := tbl.NewTransaction()
	err = tx3.ReplaceFiles(t.Context(),
		[]iceberg.DataFile{oldDataFile},
		[]iceberg.DataFile{newDataFile},
		deleteFilesToRemove,
		nil,
	)
	require.NoError(t, err)

	tbl, err = tx3.Commit(t.Context())
	require.NoError(t, err)

	// Verify: 2 rows, snapshot committed
	assertRowCount(t, tbl, 2)

	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)
	assert.Equal(t, table.OpOverwrite, snap.Summary.Operation)
}

func TestReplaceFilesWithDeleteFilesPreservesDataSequenceNumber(t *testing.T) {
	tbl := newReplaceFilesTestTable(t)
	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	dataPath := tbl.Location() + "/data/data.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[{"id":1,"data":"a"},{"id":2,"data":"b"}]`)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	oldDeletePath := tbl.Location() + "/data/old-pos-delete.parquet"
	writeParquetFile(t, oldDeletePath, table.PositionalDeleteArrowSchema,
		fmt.Sprintf(`[{"file_path":%q,"pos":0}]`, dataPath))
	oldDeleteBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		oldDeletePath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)
	tx = tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(oldDeleteBuilder.Build()).Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	oldDelete := oldDeleteBuilder.Build()
	oldSequence := deleteFileSequence(t, tbl, oldDelete.FilePath())
	newDeletePath := tbl.Location() + "/data/new-pos-delete.parquet"
	writeParquetFile(t, newDeletePath, table.PositionalDeleteArrowSchema,
		fmt.Sprintf(`[{"file_path":%q,"pos":0}]`, dataPath))
	newDeleteBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		newDeletePath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
	require.NoError(t, err)

	tx = tbl.NewTransaction()
	require.NoError(t, tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
		[]iceberg.DataFile{oldDelete},
		[]table.DeleteFileAddition{{
			File:               newDeleteBuilder.Build(),
			DataSequenceNumber: oldSequence,
		}}, nil))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	assert.Equal(t, oldSequence, deleteFileSequence(t, tbl, newDeletePath),
		"rewritten delete files must retain the replaced data sequence number")
	assert.NotEqual(t, tbl.CurrentSnapshot().SequenceNumber, oldSequence,
		"the new snapshot sequence must not replace the delete data sequence")
	assert.Equal(t, []int64{2}, scanIDs(t, tbl),
		"the replacement must preserve delete applicability")
}

func TestReplaceFilesWithDeleteFilesRejectsDVOnV2(t *testing.T) {
	tbl := newReplaceFilesTestTable(t)
	ref := tbl.Location() + "/data/data.parquet"
	dvBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		tbl.Location()+"/data/delete-vector.puffin", iceberg.PuffinFile,
		nil, nil, nil, 1, 1)
	require.NoError(t, err)
	dv := dvBuilder.ReferencedDataFile(ref).Build()
	oldDelete := newPosDeleteFile(t, tbl.Location()+"/data/old-pos-delete.parquet")

	tx := tbl.NewTransaction()
	err = tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
		[]iceberg.DataFile{oldDelete},
		[]table.DeleteFileAddition{{File: dv, DataSequenceNumber: 0}}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires table format version >= 3")
}

func TestReplaceFilesWithDeleteFilesRejectsPositionDeleteOnV3(t *testing.T) {
	tbl := newReplaceFilesTestTableVersion(t, 3)
	tx := tbl.NewTransaction()
	err := tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
		[]iceberg.DataFile{newPosDeleteFile(t, "old-pos-delete.parquet")},
		[]table.DeleteFileAddition{{
			File:               newPosDeleteFile(t, "new-pos-delete.parquet"),
			DataSequenceNumber: 0,
		}}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be a deletion vector for v3 table")
}

func TestReplaceFilesWithDeleteFilesValidatesDeletionVectorMetadata(t *testing.T) {
	tbl := newReplaceFilesTestTableVersion(t, 3)
	oldDelete := newPosDeleteFile(t, "old-pos-delete.parquet")
	ref := "data.parquet"
	offset := int64(8)
	length := int64(16)
	negativeOffset := int64(-1)
	zeroLength := int64(0)

	tests := []struct {
		name      string
		file      iceberg.DataFile
		errSubstr string
	}{
		{
			name:      "missing referenced data file",
			file:      newRewriteDeletionVector(t, "missing-ref.puffin", "", &offset, &length),
			errSubstr: "missing referenced_data_file",
		},
		{
			name:      "missing content offset",
			file:      newRewriteDeletionVector(t, "missing-offset.puffin", ref, nil, &length),
			errSubstr: "missing content_offset",
		},
		{
			name:      "negative content offset",
			file:      newRewriteDeletionVector(t, "negative-offset.puffin", ref, &negativeOffset, &length),
			errSubstr: "invalid content_offset -1",
		},
		{
			name:      "missing content size",
			file:      newRewriteDeletionVector(t, "missing-size.puffin", ref, &offset, nil),
			errSubstr: "missing content_size_in_bytes",
		},
		{
			name:      "nonpositive content size",
			file:      newRewriteDeletionVector(t, "zero-size.puffin", ref, &offset, &zeroLength),
			errSubstr: "invalid content_size_in_bytes 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := tbl.NewTransaction()
			err := tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
				[]iceberg.DataFile{oldDelete},
				[]table.DeleteFileAddition{{File: tt.file, DataSequenceNumber: 0}}, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errSubstr)
		})
	}
}

func TestReplaceFilesWithDeleteFilesValidatesDeletionVectorIdentity(t *testing.T) {
	tbl := newReplaceFilesTestTableVersion(t, 3)
	oldDelete := newPosDeleteFile(t, "old-pos-delete.parquet")
	offsetA, offsetB := int64(8), int64(24)
	length := int64(16)

	t.Run("duplicate referenced data file", func(t *testing.T) {
		tx := tbl.NewTransaction()
		err := tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
			[]iceberg.DataFile{oldDelete},
			[]table.DeleteFileAddition{
				{File: newRewriteDeletionVector(t, "a.puffin", "data.parquet", &offsetA, &length), DataSequenceNumber: 0},
				{File: newRewriteDeletionVector(t, "b.puffin", "data.parquet", &offsetB, &length), DataSequenceNumber: 0},
			}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must reference distinct data files")
	})

	t.Run("duplicate blob identity", func(t *testing.T) {
		tx := tbl.NewTransaction()
		err := tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
			[]iceberg.DataFile{oldDelete},
			[]table.DeleteFileAddition{
				{File: newRewriteDeletionVector(t, "shared.puffin", "data-a.parquet", &offsetA, &length), DataSequenceNumber: 0},
				{File: newRewriteDeletionVector(t, "shared.puffin", "data-b.parquet", &offsetA, &length), DataSequenceNumber: 0},
			}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "blob identity must be unique")
	})

	t.Run("distinct blobs may share a Puffin path", func(t *testing.T) {
		sharedTbl := appendTwoDataFiles(t, newReplaceFilesTestTable(t))
		sourceDelete := newPosDeleteFile(t, sharedTbl.Location()+"/data/source-pos-delete.parquet")
		tx := sharedTbl.NewTransaction()
		require.NoError(t, tx.NewRowDelta(nil).AddDeletes(sourceDelete).Commit(t.Context()))
		sharedTbl, err := tx.Commit(t.Context())
		require.NoError(t, err)
		sequence := deleteFileSequence(t, sharedTbl, sourceDelete.FilePath())

		tx = sharedTbl.NewTransaction()
		require.NoError(t, tx.UpgradeFormatVersion(3))
		sharedTbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
		tasks, err := sharedTbl.Scan().PlanFiles(t.Context())
		require.NoError(t, err)
		require.Len(t, tasks, 2)

		sharedPath := sharedTbl.Location() + "/data/shared.puffin"
		tx = sharedTbl.NewTransaction()
		err = tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
			[]iceberg.DataFile{sourceDelete},
			[]table.DeleteFileAddition{
				{File: newRewriteDeletionVector(t, sharedPath, tasks[0].File.FilePath(), &offsetA, &length), DataSequenceNumber: sequence},
				{File: newRewriteDeletionVector(t, sharedPath, tasks[1].File.FilePath(), &offsetB, &length), DataSequenceNumber: sequence},
			}, nil)
		require.NoError(t, err, "distinct DV blobs in one Puffin container must be accepted")
	})
}

func TestReplaceFilesWithDeleteFilesRejectsPartialPositionDeleteToDVRewrite(t *testing.T) {
	tbl := newReplaceFilesTestTable(t)
	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	dataPath := tbl.Location() + "/data/data.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[{"id":1,"data":"a"},{"id":2,"data":"b"}]`)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	filePathField, ok := iceberg.PositionalDeleteSchema.FindFieldByName("file_path")
	require.True(t, ok)
	bound, err := iceberg.StringLiteral(dataPath).MarshalBinary()
	require.NoError(t, err)
	oldDeletes := make([]iceberg.DataFile, 0, 2)
	for i := range 2 {
		builder, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
			fmt.Sprintf("%s/data/old-pos-delete-%d.parquet", tbl.Location(), i),
			iceberg.ParquetFile, nil, nil, nil, 1, 128)
		require.NoError(t, err)
		if i == 0 {
			builder.
				LowerBoundValues(map[int][]byte{filePathField.ID: bound}).
				UpperBoundValues(map[int][]byte{filePathField.ID: bound})
		}
		oldDeletes = append(oldDeletes, builder.Build())
	}
	tx = tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(oldDeletes...).Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tx = tbl.NewTransaction()
	require.NoError(t, tx.UpgradeFormatVersion(3))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	sequence := deleteFileSequence(t, tbl, oldDeletes[0].FilePath())
	offset, length := int64(8), int64(16)
	replacement := newRewriteDeletionVector(t, tbl.Location()+"/data/replacement.puffin", dataPath, &offset, &length)

	tx = tbl.NewTransaction()
	err = tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
		[]iceberg.DataFile{oldDeletes[0]},
		[]table.DeleteFileAddition{{File: replacement, DataSequenceNumber: sequence}}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires replacing all applicable position-delete files")
	assert.Contains(t, err.Error(), oldDeletes[1].FilePath())
	assert.Contains(t, err.Error(), "partition-scoped")

	tx = tbl.NewTransaction()
	require.NoError(t, tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
		oldDeletes,
		[]table.DeleteFileAddition{{File: replacement, DataSequenceNumber: sequence}}, nil),
		"the DV rewrite should be accepted once every applicable position delete is replaced")
}

func TestReplaceFilesWithDeleteFilesRejectsDeletionVectorPartitionMismatch(t *testing.T) {
	tbl, spec := newPartitionedReplaceFilesTestTable(t)
	dataPath := tbl.Location() + "/data/data.parquet"
	dataPartition := map[int]any{1000: int64(10)}
	dataBuilder, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentData, dataPath, iceberg.ParquetFile,
		dataPartition, nil, nil, 1, 128)
	require.NoError(t, err)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFiles(t.Context(), []iceberg.DataFile{dataBuilder.Build()}, nil))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	oldDeletePath := tbl.Location() + "/data/old-pos-delete.parquet"
	oldDeleteBuilder, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentPosDeletes, oldDeletePath, iceberg.ParquetFile,
		dataPartition, nil, nil, 1, 128)
	require.NoError(t, err)
	oldDelete := oldDeleteBuilder.Build()
	tx = tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(oldDelete).Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	deleteSequence := fileDataSequence(t, tbl, oldDeletePath)

	tx = tbl.NewTransaction()
	require.NoError(t, tx.UpgradeFormatVersion(3))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	offset, length := int64(8), int64(16)
	dvBuilder, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentPosDeletes,
		tbl.Location()+"/data/replacement-dv.puffin", iceberg.PuffinFile,
		map[int]any{1000: int64(11)}, nil, nil, 1, 128)
	require.NoError(t, err)
	replacement := dvBuilder.
		ReferencedDataFile(dataPath).
		ContentOffset(offset).
		ContentSizeInBytes(length).
		Build()

	tx = tbl.NewTransaction()
	err = tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
		[]iceberg.DataFile{oldDelete},
		[]table.DeleteFileAddition{{File: replacement, DataSequenceNumber: deleteSequence}}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "partition spec or values")
	assert.Contains(t, err.Error(), dataPath)
}

func TestReplaceFilesWithDeleteFilesIgnoresOlderSurvivingPositionDelete(t *testing.T) {
	tbl, spec := newPartitionedReplaceFilesTestTable(t)
	partition := map[int]any{1000: int64(10)}

	dataAPath := tbl.Location() + "/data/data-a.parquet"
	dataABuilder, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentData, dataAPath, iceberg.ParquetFile,
		partition, nil, nil, 1, 128)
	require.NoError(t, err)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFiles(t.Context(), []iceberg.DataFile{dataABuilder.Build()}, nil))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	oldPartitionDeletePath := tbl.Location() + "/data/old-partition-delete.parquet"
	oldPartitionDeleteBuilder, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentPosDeletes, oldPartitionDeletePath, iceberg.ParquetFile,
		partition, nil, nil, 1, 128)
	require.NoError(t, err)
	oldPartitionDelete := oldPartitionDeleteBuilder.Build()
	tx = tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(oldPartitionDelete).Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	oldPartitionDeleteSequence := fileDataSequence(t, tbl, oldPartitionDeletePath)

	dataBPath := tbl.Location() + "/data/data-b.parquet"
	dataBBuilder, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentData, dataBPath, iceberg.ParquetFile,
		partition, nil, nil, 1, 128)
	require.NoError(t, err)
	tx = tbl.NewTransaction()
	require.NoError(t, tx.AddDataFiles(t.Context(), []iceberg.DataFile{dataBBuilder.Build()}, nil))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	dataBSequence := fileDataSequence(t, tbl, dataBPath)
	require.Greater(t, dataBSequence, oldPartitionDeleteSequence)

	filePathField, ok := iceberg.PositionalDeleteSchema.FindFieldByName("file_path")
	require.True(t, ok)
	bound, err := iceberg.StringLiteral(dataBPath).MarshalBinary()
	require.NoError(t, err)
	newPositionDeletePath := tbl.Location() + "/data/new-position-delete.parquet"
	newPositionDeleteBuilder, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentPosDeletes, newPositionDeletePath, iceberg.ParquetFile,
		partition, nil, nil, 1, 128)
	require.NoError(t, err)
	newPositionDelete := newPositionDeleteBuilder.
		LowerBoundValues(map[int][]byte{filePathField.ID: bound}).
		UpperBoundValues(map[int][]byte{filePathField.ID: bound}).
		Build()
	tx = tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(newPositionDelete).Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	newPositionDeleteSequence := fileDataSequence(t, tbl, newPositionDeletePath)

	tx = tbl.NewTransaction()
	require.NoError(t, tx.UpgradeFormatVersion(3))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	offset, length := int64(8), int64(16)
	dvBuilder, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentPosDeletes,
		tbl.Location()+"/data/replacement-dv.puffin", iceberg.PuffinFile,
		partition, nil, nil, 1, 128)
	require.NoError(t, err)
	replacement := dvBuilder.
		ReferencedDataFile(dataBPath).
		ContentOffset(offset).
		ContentSizeInBytes(length).
		Build()

	tx = tbl.NewTransaction()
	require.NoError(t, tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
		[]iceberg.DataFile{newPositionDelete},
		[]table.DeleteFileAddition{{File: replacement, DataSequenceNumber: newPositionDeleteSequence}}, nil),
		"an older position delete cannot apply to the newer target data file")
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, newPositionDeleteSequence, fileDataSequence(t, tbl, replacement.FilePath()))
}

func TestReplaceFilesWithDeleteFilesRejectsSurvivingDeletionVector(t *testing.T) {
	tbl, target := seedV3TableWithDV(t)
	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)

	var sibling string
	for _, task := range tasks {
		if task.File.FilePath() != target {
			sibling = task.File.FilePath()

			break
		}
	}
	require.NotEmpty(t, sibling)

	writer := dv.NewDVWriter(iceio.LocalFS{}, unpartitionedSpecByID)
	require.NoError(t, writer.Add(sibling, []int64{0}, 0, nil))
	siblingDVs, err := writer.Flush(t.Context(), tbl.Location()+"/data/sibling-dv.puffin")
	require.NoError(t, err)
	require.Len(t, siblingDVs, 1)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(siblingDVs...).Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	offset, length := int64(8), int64(16)
	replacement := newRewriteDeletionVector(t, tbl.Location()+"/data/replacement-dv.puffin", target, &offset, &length)
	sequence := deleteFileSequence(t, tbl, siblingDVs[0].FilePath())
	tx = tbl.NewTransaction()
	err = tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
		[]iceberg.DataFile{siblingDVs[0]},
		[]table.DeleteFileAddition{{File: replacement, DataSequenceNumber: sequence}}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists and must be replaced")
}

func TestReplaceFilesWithDeleteFilesAllowsDroppedEqualityField(t *testing.T) {
	tbl := newReplaceFilesTestTable(t)
	oldDeleteBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		tbl.Location()+"/data/old-equality-delete.parquet", iceberg.ParquetFile,
		nil, nil, nil, 1, 128)
	require.NoError(t, err)
	oldDelete := oldDeleteBuilder.EqualityFieldIDs([]int{2}).Build()

	tx := tbl.NewTransaction()
	require.NoError(t, tx.NewRowDelta(nil).AddDeletes(oldDelete).Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	sequence := deleteFileSequence(t, tbl, oldDelete.FilePath())

	tx = tbl.NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, false).DeleteColumn([]string{"data"}).Commit())
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	_, currentHasDroppedField := tbl.Schema().FindFieldByID(2)
	require.False(t, currentHasDroppedField)

	newDeleteBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		tbl.Location()+"/data/new-equality-delete.parquet", iceberg.ParquetFile,
		nil, nil, nil, 1, 128)
	require.NoError(t, err)
	newDelete := newDeleteBuilder.EqualityFieldIDs([]int{2}).Build()
	tx = tbl.NewTransaction()
	require.NoError(t, tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
		[]iceberg.DataFile{oldDelete},
		[]table.DeleteFileAddition{{File: newDelete, DataSequenceNumber: sequence}}, nil))
}

func deleteFileSequence(t *testing.T, tbl *table.Table, path string) int64 {
	return fileDataSequence(t, tbl, path)
}

func fileDataSequence(t *testing.T, tbl *table.Table, path string) int64 {
	t.Helper()
	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)
	manifests, err := snap.Manifests(iceio.LocalFS{})
	require.NoError(t, err)
	for _, manifest := range manifests {
		for entry, err := range manifest.Entries(iceio.LocalFS{}, false) {
			require.NoError(t, err)
			if entry.DataFile().FilePath() == path {
				return entry.SequenceNum()
			}
		}
	}
	t.Fatalf("file %q not found in current snapshot", path)

	return -1
}

func scanIDs(t *testing.T, tbl *table.Table) []int64 {
	t.Helper()
	_, records, err := tbl.Scan().ToArrowRecords(t.Context())
	require.NoError(t, err)
	var ids []int64
	for record, err := range records {
		require.NoError(t, err)
		values := record.Column(record.Schema().FieldIndices("id")[0]).(*array.Int64)
		for i := range values.Len() {
			ids = append(ids, values.Value(i))
		}
		record.Release()
	}

	return ids
}

func TestReplaceFiles_DelegatesToReplaceDataFilesWhenNoDeleteFiles(t *testing.T) {
	tbl := newReplaceFilesTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	dataPath := tbl.Location() + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[{"id": 1, "data": "hello"}]`)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	oldDataFile := tasks[0].File

	newDataPath := tbl.Location() + "/data/data-new.parquet"
	writeParquetFile(t, newDataPath, arrowSc, `[{"id": 1, "data": "hello"}]`)

	newBuilder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		newDataPath, iceberg.ParquetFile, nil, nil, nil, 1, 256)
	require.NoError(t, err)

	tx2 := tbl.NewTransaction()
	err = tx2.ReplaceFiles(t.Context(),
		[]iceberg.DataFile{oldDataFile},
		[]iceberg.DataFile{newBuilder.Build()},
		nil, // no delete files
		nil,
	)
	require.NoError(t, err)

	tbl, err = tx2.Commit(t.Context())
	require.NoError(t, err)
	assertRowCount(t, tbl, 1)
}

func TestReplaceFiles_ValidationErrors(t *testing.T) {
	tbl := newReplaceFilesTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	dataPath := tbl.Location() + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[{"id": 1, "data": "hello"}]`)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	t.Run("nil delete file", func(t *testing.T) {
		tx := tbl.NewTransaction()
		err := tx.ReplaceFiles(t.Context(),
			nil, nil,
			[]iceberg.DataFile{nil},
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil delete file")
	})

	t.Run("duplicate delete file paths", func(t *testing.T) {
		posDelBuilder, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
			"s3://bucket/del.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 128)
		require.NoError(t, err)
		df := posDelBuilder.Build()

		tx := tbl.NewTransaction()
		err = tx.ReplaceFiles(t.Context(),
			nil, nil,
			[]iceberg.DataFile{df, df},
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unique")
	})

	t.Run("delete file not in table", func(t *testing.T) {
		posDelBuilder, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
			"s3://bucket/nonexistent-del.parquet", iceberg.ParquetFile, nil, nil, nil, 1, 128)
		require.NoError(t, err)

		tx := tbl.NewTransaction()
		err = tx.ReplaceFiles(t.Context(),
			nil, nil,
			[]iceberg.DataFile{posDelBuilder.Build()},
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot remove delete files")
	})

	newDV := func(path, ref string) iceberg.DataFile {
		b, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
			path, iceberg.PuffinFile, nil, nil, nil, 1, 128)
		require.NoError(t, err)
		if ref != "" {
			b.ReferencedDataFile(ref)
		}

		return b.Build()
	}

	t.Run("deletion vector missing referenced_data_file", func(t *testing.T) {
		tx := tbl.NewTransaction()
		err := tx.ReplaceFiles(t.Context(),
			nil, nil,
			[]iceberg.DataFile{newDV("s3://bucket/dv-0001.puffin", "")},
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing referenced_data_file")
	})

	t.Run("deletion vectors referencing the same data file", func(t *testing.T) {
		tx := tbl.NewTransaction()
		err := tx.ReplaceFiles(t.Context(),
			nil, nil,
			[]iceberg.DataFile{
				newDV("s3://bucket/dv-a.puffin", "s3://bucket/data-001.parquet"),
				newDV("s3://bucket/dv-b.puffin", "s3://bucket/data-001.parquet"),
			},
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "distinct data files")
	})

	t.Run("deletion vector not in table", func(t *testing.T) {
		tx := tbl.NewTransaction()
		err := tx.ReplaceFiles(t.Context(),
			nil, nil,
			[]iceberg.DataFile{newDV("s3://bucket/dv-0001.puffin", "s3://bucket/nonexistent-data.parquet")},
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot remove deletion vectors that do not belong to the table")
	})
}
