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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestRewriteFilesRejectsConcurrentEqualityDeleteForNewSpec(t *testing.T) {
	tbl, _ := newConcurrentRewriteTestTableVersion(t, 2)
	arrowSchema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	oldPath := tbl.Location() + "/data/old.parquet"
	writeParquetFile(t, oldPath, arrowSchema, `[{"id":7,"data":"a"}]`)
	seed := tbl.NewTransaction()
	require.NoError(t, seed.AddDataFilesByPath(t.Context(), []string{oldPath}, nil, false))
	tbl, err = seed.Commit(t.Context())
	require.NoError(t, err)
	files, err := tbl.Scan().PlanFiles(t.Context())
	require.NoError(t, err)
	require.Len(t, files, 1)
	oldFile := files[0].File
	oldSequence := currentManifestEntry(t, tbl, oldPath).DataSequenceNumber()
	require.Equal(t, int32(0), oldFile.SpecID())

	evolve := tbl.NewTransaction()
	require.NoError(t, evolve.UpdateSpec(true).AddIdentity("id").Commit())
	tbl, err = evolve.Commit(t.Context())
	require.NoError(t, err)
	spec := tbl.Spec()
	require.Equal(t, 1, spec.ID())
	partition := map[int]any{spec.Field(0).FieldID: int64(7)}

	outputPath := tbl.Location() + "/data/new.parquet"
	writeParquetFile(t, outputPath, arrowSchema, `[{"id":7,"data":"a"}]`)
	outputBuilder, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData,
		outputPath, iceberg.ParquetFile, partition, nil, nil, 1, 256)
	require.NoError(t, err)
	leader := tbl.NewTransaction()
	rewrite := leader.NewRewrite(nil).DeleteFile(oldFile).AddDataFile(outputBuilder.Build()).DataSequenceNumber(oldSequence)
	require.NoError(t, rewrite.Commit(t.Context()))

	deleteSchema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	deleteArrowSchema, err := table.SchemaToArrowSchema(deleteSchema, nil, true, false)
	require.NoError(t, err)
	deletePath := tbl.Location() + "/data/delete.parquet"
	writeParquetFile(t, deletePath, deleteArrowSchema, `[{"id":7}]`)
	deleteBuilder, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentEqDeletes,
		deletePath, iceberg.ParquetFile, partition, nil, nil, 1, 256)
	require.NoError(t, err)
	peer := tbl.NewTransaction()
	require.NoError(t, peer.NewRowDelta(nil).AddDeletes(deleteBuilder.EqualityFieldIDs([]int{1}).Build()).Commit(t.Context()))
	peerTable, err := peer.Commit(t.Context())
	require.NoError(t, err)
	// The delete uses the new spec and does not apply to the old-spec file.
	require.Equal(t, []int64{7}, scanIDs(t, peerTable))

	// With the old data sequence, the delete would apply to the replacement
	// file after replay. The rewrite must not make the visible row disappear.
	_, err = leader.Commit(t.Context())
	require.ErrorIs(t, err, table.ErrConflictingDeleteFiles)
}
