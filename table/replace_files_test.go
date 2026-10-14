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
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
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
	oldSequence := currentManifestEntry(t, tbl, oldDelete.FilePath()).SequenceNum()
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

	assert.Equal(t, oldSequence, currentManifestEntry(t, tbl, newDeletePath).SequenceNum(),
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

	t.Run("distinct blobs may share a new or existing Puffin path", func(t *testing.T) {
		sharedTbl := appendTwoDataFiles(t, newReplaceFilesTestTable(t))
		sourceDelete := newPosDeleteFile(t, sharedTbl.Location()+"/data/source-pos-delete.parquet")
		tx := sharedTbl.NewTransaction()
		require.NoError(t, tx.NewRowDelta(nil).AddDeletes(sourceDelete).Commit(t.Context()))
		sharedTbl, err := tx.Commit(t.Context())
		require.NoError(t, err)
		sequence := currentManifestEntry(t, sharedTbl, sourceDelete.FilePath()).SequenceNum()

		tx = sharedTbl.NewTransaction()
		require.NoError(t, tx.UpgradeFormatVersion(3))
		sharedTbl, err = tx.Commit(t.Context())
		require.NoError(t, err)
		tasks, err := sharedTbl.Scan().PlanFiles(t.Context())
		require.NoError(t, err)
		require.Len(t, tasks, 2)

		sharedPath := sharedTbl.Location() + "/data/shared.puffin"
		vectorA := newRewriteDeletionVector(t, sharedPath, tasks[0].File.FilePath(), &offsetA, &length)
		vectorB := newRewriteDeletionVector(t, sharedPath, tasks[1].File.FilePath(), &offsetB, &length)
		tx = sharedTbl.NewTransaction()
		err = tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
			[]iceberg.DataFile{sourceDelete},
			[]table.DeleteFileAddition{
				{File: vectorA, DataSequenceNumber: sequence},
				{File: vectorB, DataSequenceNumber: sequence},
			}, nil)
		require.NoError(t, err, "distinct DV blobs in one Puffin container must be accepted")
		sharedTbl, err = tx.Commit(t.Context())
		require.NoError(t, err)

		offsetC := offsetB + length
		tx = sharedTbl.NewTransaction()
		err = tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
			[]iceberg.DataFile{vectorA},
			[]table.DeleteFileAddition{{
				File:               newRewriteDeletionVector(t, sharedPath, tasks[0].File.FilePath(), &offsetC, &length),
				DataSequenceNumber: sequence,
			}}, nil)
		require.NoError(t, err, "a new DV blob may reuse an existing container path while a sibling survives")
	})
}

func TestReplaceFilesWithDeleteFilesRejectsDuplicatePaths(t *testing.T) {
	const path = "shared-delete-file"
	offset, length := int64(8), int64(16)
	pos := newPosDeleteFile(t, path)
	eq := newEqDeleteFile(t, path)
	vector := newRewriteDeletionVector(t, path, "data.parquet", &offset, &length)
	conflictError := "delete file path " + path +
		" cannot identify both a deletion vector container and a regular delete file for ReplaceFiles"

	for _, tt := range []struct {
		name      string
		version   int
		files     []iceberg.DataFile
		wantError string
	}{
		{"position deletes", 2, []iceberg.DataFile{pos, newPosDeleteFile(t, path)}, "add delete file paths must be unique for ReplaceFiles"},
		{"equality deletes v2", 2, []iceberg.DataFile{eq, newEqDeleteFile(t, path)}, "add delete file paths must be unique for ReplaceFiles"},
		{"equality deletes v3", 3, []iceberg.DataFile{eq, newEqDeleteFile(t, path)}, "add delete file paths must be unique for ReplaceFiles"},
		{"position then equality", 2, []iceberg.DataFile{pos, eq}, "add delete file paths must be unique for ReplaceFiles"},
		{"equality then position", 2, []iceberg.DataFile{eq, pos}, "add delete file paths must be unique for ReplaceFiles"},
		{"deletion vector then equality", 3, []iceberg.DataFile{vector, eq}, conflictError},
		{"equality then deletion vector", 3, []iceberg.DataFile{eq, vector}, conflictError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tbl := newReplaceFilesTestTableVersion(t, tt.version)
			additions := make([]table.DeleteFileAddition, len(tt.files))
			for i, file := range tt.files {
				additions[i] = table.DeleteFileAddition{File: file, DataSequenceNumber: 0}
			}

			err := tbl.NewTransaction().ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
				[]iceberg.DataFile{newEqDeleteFile(t, "old-equality-delete.parquet")}, additions, nil)
			require.EqualError(t, err, tt.wantError)
		})
	}
}

func TestReplaceFilesWithDeleteFilesValidatesExistingPaths(t *testing.T) {
	for _, version := range []int{2, 3} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			tbl := newReplaceFilesTestTable(t)
			data := newDataFile(t, tbl.Location()+"/data/data.parquet")
			source := newEqDeleteFile(t, tbl.Location()+"/data/source.parquet")
			other := newEqDeleteFile(t, tbl.Location()+"/data/other.parquet")

			tx := tbl.NewTransaction()
			require.NoError(t, tx.AddDataFiles(t.Context(), []iceberg.DataFile{data}, nil))
			tbl, err := tx.Commit(t.Context())
			require.NoError(t, err)
			tx = tbl.NewTransaction()
			require.NoError(t, tx.NewRowDelta(nil).AddDeletes(source).AddDeletes(other).Commit(t.Context()))
			if version == 3 {
				require.NoError(t, tx.UpgradeFormatVersion(3))
			}
			tbl, err = tx.Commit(t.Context())
			require.NoError(t, err)
			sequence := currentManifestEntry(t, tbl, source.FilePath()).SequenceNum()

			for _, tt := range []struct {
				name string
				path string
			}{
				{"existing data file", data.FilePath()},
				{"surviving delete file", other.FilePath()},
				{"delete file being replaced", source.FilePath()},
				{"new delete file", tbl.Location() + "/data/replacement.parquet"},
			} {
				t.Run(tt.name, func(t *testing.T) {
					tx := tbl.NewTransaction()
					err := tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
						[]iceberg.DataFile{source},
						[]table.DeleteFileAddition{{
							File:               newEqDeleteFile(t, tt.path),
							DataSequenceNumber: sequence,
						}}, nil)
					if tt.name == "new delete file" {
						require.NoError(t, err)

						return
					}
					require.EqualError(t, err, "cannot add files that are already referenced by table, files: "+tt.path)
				})
			}
		})
	}
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

	sequence := currentManifestEntry(t, tbl, oldDeletes[0].FilePath()).SequenceNum()
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
	deleteSequence := currentManifestEntry(t, tbl, oldDeletePath).SequenceNum()

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
	oldPartitionDeleteSequence := currentManifestEntry(t, tbl, oldPartitionDeletePath).SequenceNum()

	dataBPath := tbl.Location() + "/data/data-b.parquet"
	dataBBuilder, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentData, dataBPath, iceberg.ParquetFile,
		partition, nil, nil, 1, 128)
	require.NoError(t, err)
	tx = tbl.NewTransaction()
	require.NoError(t, tx.AddDataFiles(t.Context(), []iceberg.DataFile{dataBBuilder.Build()}, nil))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	dataBSequence := currentManifestEntry(t, tbl, dataBPath).SequenceNum()
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
	newPositionDeleteSequence := currentManifestEntry(t, tbl, newPositionDeletePath).SequenceNum()

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
	assert.Equal(t, newPositionDeleteSequence, currentManifestEntry(t, tbl, replacement.FilePath()).SequenceNum())
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
	sequence := currentManifestEntry(t, tbl, siblingDVs[0].FilePath()).SequenceNum()
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
	sequence := currentManifestEntry(t, tbl, oldDelete.FilePath()).SequenceNum()

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

// currentManifestEntry returns the manifest entry for path in the current
// snapshot, including DELETED entries.
func currentManifestEntry(t *testing.T, tbl *table.Table, path string) iceberg.ManifestEntry {
	t.Helper()
	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)
	manifests, err := snap.Manifests(iceio.LocalFS{})
	require.NoError(t, err)
	for _, manifest := range manifests {
		for entry, err := range manifest.Entries(iceio.LocalFS{}, false) {
			require.NoError(t, err)
			if entry.DataFile().FilePath() == path {
				return entry
			}
		}
	}
	t.Fatalf("file %q not found in current snapshot", path)

	return nil
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

// seedDataFilesWithPositionDeletes creates two v2 data files (ids 1-3 and 4-6)
// each with a position delete on a different row. Tasks are sorted by path.
func seedDataFilesWithPositionDeletes(t *testing.T) (*table.Table, []table.FileScanTask) {
	t.Helper()

	tbl := newReplaceFilesTestTable(t)
	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	files := []struct {
		rows      string
		deletePos int64
	}{
		{
			rows:      `[{"id":1,"data":"a"}, {"id":2,"data":"b"}, {"id":3,"data":"c"}]`,
			deletePos: 0,
		},
		{
			rows:      `[{"id":4,"data":"d"}, {"id":5,"data":"e"}, {"id":6,"data":"f"}]`,
			deletePos: 1,
		},
	}
	dataPaths := make([]string, len(files))
	for i, f := range files {
		dataPaths[i] = fmt.Sprintf("%s/data/data-%d.parquet", tbl.Location(), i)
		writeParquetFile(t, dataPaths[i], arrowSc, f.rows)
	}

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), dataPaths, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	filePathField, ok := iceberg.PositionalDeleteSchema.FindFieldByName("file_path")
	require.True(t, ok)
	tx = tbl.NewTransaction()
	rowDelta := tx.NewRowDelta(nil)
	for i, f := range files {
		deletePath := fmt.Sprintf("%s/data/pos-delete-%d.parquet", tbl.Location(), i)
		row := fmt.Sprintf(`[{"file_path":%q,"pos":%d}]`, dataPaths[i], f.deletePos)

		writeParquetFile(t, deletePath, table.PositionalDeleteArrowSchema, row)
		bound, err := iceberg.StringLiteral(dataPaths[i]).MarshalBinary()
		require.NoError(t, err)

		builder, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
			deletePath, iceberg.ParquetFile, nil, nil, nil, 1, 128,
		)
		require.NoError(t, err)
		rowDelta.AddDeletes(builder.
			LowerBoundValues(map[int][]byte{filePathField.ID: bound}).
			UpperBoundValues(map[int][]byte{filePathField.ID: bound}).
			Build())
	}

	require.NoError(t, rowDelta.Commit(t.Context()))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3, 4, 6}, idsInTable(t, tbl))

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, len(files))
	slices.SortFunc(tasks, func(a, b table.FileScanTask) int {
		return strings.Compare(a.File.FilePath(), b.File.FilePath())
	})
	for _, task := range tasks {
		require.Len(t, task.DeleteFiles, 1, "each data file must carry only its own position delete")
	}

	return tbl, tasks
}

// seedReplaceFilesTableWithDelete returns a table with one data file and one delete on it:
// a position delete on v2, a deletion vector on v3.
func seedReplaceFilesTableWithDelete(t *testing.T, version int) (*table.Table, iceberg.DataFile, iceberg.DataFile) {
	t.Helper()

	switch version {
	case 2:
		tbl, tasks := seedDataFilesWithPositionDeletes(t)

		return tbl, tasks[0].File, tasks[0].DeleteFiles[0]
	case 3:
		tbl, target := seedV3TableWithDV(t)
		tasks, err := tbl.Scan().PlanFiles(t.Context())
		require.NoError(t, err)

		for _, task := range tasks {
			if task.File.FilePath() == target {
				require.Len(t, task.DeletionVectorFiles, 1)

				return tbl, task.File, task.DeletionVectorFiles[0]
			}
		}
		t.Fatalf("deletion vector target %q not found in scan tasks", target)
	default:
		t.Fatalf("unsupported format version %d", version)
	}

	return nil, nil, nil
}

// seedSupersededDeletionVector deletes twice from one v3 data file.
// The first DV gets replaced and stays only as a DELETED entry.
// It returns that old DV and the scan task holding the new, live DV.
func seedSupersededDeletionVector(t *testing.T) (*table.Table, table.FileScanTask, iceberg.DataFile) {
	t.Helper()

	tbl := newMergeOnReadTestTableVersion(t, "3")
	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSc, []string{
		`[{"id":1,"data":"a"},{"id":2,"data":"b"},
		{"id":3,"data":"c"},{"id":4,"data":"d"},
		{"id":5,"data":"e"}]`,
	})
	require.NoError(t, err)
	defer data.Release()

	tbl, err = tbl.Append(t.Context(), array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	var superseded iceberg.DataFile
	for _, id := range []int64{2, 4} {
		tbl, err = tbl.Delete(t.Context(), iceberg.EqualTo(iceberg.Reference("id"), id), nil)
		require.NoError(t, err)

		if superseded == nil {
			tasks, err := tbl.Scan().PlanFiles(t.Context())
			require.NoError(t, err)
			require.Len(t, tasks, 1)
			require.Len(t, tasks[0].DeletionVectorFiles, 1)
			superseded = tasks[0].DeletionVectorFiles[0]
		}
	}
	require.Equal(t, 1, liveDVCount(t, tbl))
	require.Equal(t, iceberg.EntryStatusDELETED, currentManifestEntry(t, tbl, superseded.FilePath()).Status(),
		"the second delete must keep the first DV only as a DELETED entry")

	tasks, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Len(t, tasks[0].DeletionVectorFiles, 1)
	require.Equal(t, superseded.ReferencedDataFile(), tasks[0].DeletionVectorFiles[0].ReferencedDataFile())
	require.NotEqual(t, superseded.FilePath(), tasks[0].DeletionVectorFiles[0].FilePath())

	return tbl, tasks[0], superseded
}

func newCompactedDataFile(t *testing.T, tbl *table.Table, path string, recordCount int64) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		path, iceberg.ParquetFile, nil, nil, nil, recordCount, 512,
	)
	require.NoError(t, err)
	if tbl.Metadata().Version() >= 3 {
		builder.FirstRowID(tbl.Metadata().NextRowID())
	}

	return builder.Build()
}

// compactWithDeletes swaps data for compacted and removes dels, either through
// ReplaceFiles or, when automatic is set, through RewriteFiles.
func compactWithDeletes(ctx context.Context, tx *table.Transaction, automatic bool, data, compacted, dels []iceberg.DataFile) error {
	if !automatic {
		return tx.ReplaceFiles(ctx, data, compacted, dels, nil)
	}

	result := table.CompactionGroupResult{OldDataFiles: data, NewDataFiles: compacted}
	for _, del := range dels {
		if table.IsDeletionVector(del) {
			result.SafeDeletionVectors = append(result.SafeDeletionVectors, del)
		} else {
			result.SafePosDeletes = append(result.SafePosDeletes, del)
		}
	}

	return tx.NewRewrite(nil).ApplyResult(result).Commit(ctx)
}

// metadataFileCount counts files in the table's metadata folder.
// Staging a commit writes files there, so an unchanged count means nothing was written.
func metadataFileCount(t *testing.T, tbl *table.Table) int {
	t.Helper()

	entries, err := os.ReadDir(filepath.FromSlash(tbl.Location() + "/metadata"))
	require.NoError(t, err)

	return len(entries)
}

// A compaction planned before another writer deleted one of its data files must fail.
// Committing it would bring the deleted rows back.
func TestReplaceFilesRejectsDataFileDeletedByCurrentSnapshot(t *testing.T) {
	tbl, tasks := seedDataFilesWithPositionDeletes(t)
	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	compactedPath := tbl.Location() + "/data/compacted-0.parquet"

	jsonData := `[{"id":2,"data":"b"},{"id":3,"data":"c"}]`
	writeParquetFile(t, compactedPath, arrowSc, jsonData)

	path := tbl.Location() + "/data/compacted-1.parquet"
	compacted := []iceberg.DataFile{
		newCompactedDataFile(t, tbl, compactedPath, 2),
		newCompactedDataFile(t, tbl, path, 2),
	}

	tx := tbl.NewTransaction()
	require.NoError(t, tx.Delete(t.Context(), iceberg.GreaterThanEqual(iceberg.Reference("id"), int64(4)), nil))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, []int64{2, 3}, idsInTable(t, tbl))
	require.Equal(t, iceberg.EntryStatusDELETED, currentManifestEntry(t, tbl, tasks[1].File.FilePath()).Status())
	require.NotEqual(t, iceberg.EntryStatusDELETED, currentManifestEntry(t, tbl, tasks[0].File.FilePath()).Status())

	before := metadataFileCount(t, tbl)
	tx = tbl.NewTransaction()
	err = tx.ReplaceFiles(t.Context(),
		[]iceberg.DataFile{tasks[0].File, tasks[1].File}, compacted,
		[]iceberg.DataFile{tasks[0].DeleteFiles[0], tasks[1].DeleteFiles[0]}, nil)
	assert.ErrorContains(t, err, "cannot delete data files that do not belong to the table")
	assert.Equal(t, before, metadataFileCount(t, tbl), "a rejected replace must not write manifests")

	tx = tbl.NewTransaction()
	require.NoError(t, tx.ReplaceFiles(t.Context(), []iceberg.DataFile{tasks[0].File}, compacted[:1], tasks[0].DeleteFiles, nil),
		"the data file the delete left live must still compact")
	assert.Greater(t, metadataFileCount(t, tbl), before, "staging a replace must write manifests")

	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []int64{2, 3}, idsInTable(t, tbl))
}

func TestReplaceFilesRejectsDeleteFileRemovedByCurrentSnapshot(t *testing.T) {
	for _, tt := range []struct {
		name      string
		version   int
		automatic bool
		wantErr   string
	}{
		{
			name:      "position delete",
			version:   2,
			automatic: false,
			wantErr:   "cannot remove delete files that do not belong to the table",
		},
		{
			name:      "automatic position delete",
			version:   2,
			automatic: true,
			wantErr:   "cannot remove automatic delete files that do not belong to the table",
		},
		{
			name:      "deletion vector",
			version:   3,
			automatic: false,
			wantErr:   "cannot remove deletion vectors that do not belong to the table",
		},
		{
			name:      "automatic deletion vector",
			version:   3,
			automatic: true,
			wantErr:   "cannot remove deletion vectors that do not belong to the table",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tbl, data, del := seedReplaceFilesTableWithDelete(t, tt.version)

			tx := tbl.NewTransaction()
			require.NoError(t, tx.ReplaceFiles(t.Context(), nil, nil, []iceberg.DataFile{del}, nil))
			tbl, err := tx.Commit(t.Context())
			require.NoError(t, err)
			require.Equal(t, iceberg.EntryStatusDELETED, currentManifestEntry(t, tbl, del.FilePath()).Status())
			require.NotEqual(t, iceberg.EntryStatusDELETED, currentManifestEntry(t, tbl, data.FilePath()).Status())

			compacted := newCompactedDataFile(t, tbl, tbl.Location()+"/data/compacted.parquet", 1)
			before := metadataFileCount(t, tbl)
			err = compactWithDeletes(t.Context(), tbl.NewTransaction(), tt.automatic,
				[]iceberg.DataFile{data}, []iceberg.DataFile{compacted}, []iceberg.DataFile{del})
			assert.ErrorContains(t, err, tt.wantErr)
			assert.Equal(t, before, metadataFileCount(t, tbl), "a rejected replace must not write manifests")
		})
	}
}

// A rewrite planned with an old DV must fail once a newer DV has replaced it,
// because the newer DV has deletes the rewrite never saw.
func TestReplaceFilesRejectsDeletionVectorSupersededByCurrentSnapshot(t *testing.T) {
	type stageFunc func(t *testing.T, tx *table.Transaction, tbl *table.Table, task table.FileScanTask, superseded iceberg.DataFile) error
	compaction := func(automatic bool) stageFunc {
		return func(t *testing.T, tx *table.Transaction, tbl *table.Table, task table.FileScanTask, superseded iceberg.DataFile) error {
			compacted := newCompactedDataFile(t, tbl, tbl.Location()+"/data/compacted.parquet", 4)

			return compactWithDeletes(t.Context(), tx, automatic,
				[]iceberg.DataFile{task.File}, []iceberg.DataFile{compacted}, []iceberg.DataFile{superseded})
		}
	}

	for _, tt := range []struct {
		name  string
		stage stageFunc
	}{
		{
			name:  "compaction",
			stage: compaction(false),
		},
		{
			name:  "automatic compaction",
			stage: compaction(true),
		},
		{
			name: "deletion vector rewrite",
			stage: func(t *testing.T, tx *table.Transaction, tbl *table.Table, task table.FileScanTask, superseded iceberg.DataFile) error {
				writer := dv.NewDVWriter(iceio.LocalFS{}, unpartitionedSpecByID)
				require.NoError(t, writer.Add(task.File.FilePath(), []int64{1}, 0, nil))
				rewritten, err := writer.Flush(t.Context(), tbl.Location()+"/data/rewritten-dv.puffin")
				require.NoError(t, err)
				require.Len(t, rewritten, 1)

				return tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
					[]iceberg.DataFile{superseded},
					[]table.DeleteFileAddition{{
						File:               rewritten[0],
						DataSequenceNumber: currentManifestEntry(t, tbl, superseded.FilePath()).SequenceNum(),
					}}, nil)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tbl, task, superseded := seedSupersededDeletionVector(t)

			before := metadataFileCount(t, tbl)
			err := tt.stage(t, tbl.NewTransaction(), tbl, task, superseded)
			assert.ErrorContains(t, err, "cannot remove deletion vectors that do not belong to the table")
			assert.Equal(t, before, metadataFileCount(t, tbl), "a rejected replace must not write manifests")
		})
	}
}

// Removing the current DV must remove that DV, not the old replaced one that
// is still listed as a DELETED entry.
func TestReplaceFilesRemovesLiveDeletionVectorNotSupersededEntry(t *testing.T) {
	t.Run("compaction", func(t *testing.T) {
		tbl, task, _ := seedSupersededDeletionVector(t)
		arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
		require.NoError(t, err)

		compactedPath := tbl.Location() + "/data/compacted.parquet"
		jsonData := `[{"id":1,"data":"a"},{"id":3,"data":"c"},{"id":5,"data":"e"}]`
		writeParquetFile(t, compactedPath, arrowSc, jsonData)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.ReplaceFiles(t.Context(),
			[]iceberg.DataFile{task.File},
			[]iceberg.DataFile{newCompactedDataFile(t, tbl, compactedPath, 3)},
			task.DeletionVectorFiles, nil))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)

		assert.Zero(t, liveDVCount(t, tbl), "the live deletion vector must be removed with its data file")
		assert.Equal(t, []int64{1, 3, 5}, idsInTable(t, tbl))
	})

	t.Run("deletion vector rewrite", func(t *testing.T) {
		tbl, task, _ := seedSupersededDeletionVector(t)
		liveDV := task.DeletionVectorFiles[0]
		writer := dv.NewDVWriter(iceio.LocalFS{}, unpartitionedSpecByID)
		require.NoError(t, writer.Add(task.File.FilePath(), []int64{1, 3}, 0, nil))

		rewritten, err := writer.Flush(t.Context(), tbl.Location()+"/data/rewritten-dv.puffin")
		require.NoError(t, err)
		require.Len(t, rewritten, 1)

		tx := tbl.NewTransaction()
		require.NoError(t, tx.ReplaceFilesWithDeleteFiles(t.Context(), nil, nil,
			[]iceberg.DataFile{liveDV},
			[]table.DeleteFileAddition{{
				File:               rewritten[0],
				DataSequenceNumber: currentManifestEntry(t, tbl, liveDV.FilePath()).SequenceNum(),
			}}, nil))
		tbl, err = tx.Commit(t.Context())
		require.NoError(t, err)

		assert.Equal(t, 1, liveDVCount(t, tbl), "a data file must keep exactly one deletion vector")
		assert.Equal(t, iceberg.EntryStatusDELETED, currentManifestEntry(t, tbl, liveDV.FilePath()).Status())
		assert.Equal(t, []int64{1, 3, 5}, idsInTable(t, tbl))
	})
}
