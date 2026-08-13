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

// End-to-end regression test for issue #1309: AddFiles() after a prior Delete()
// produces a manifest list whose `partitions` field serializes as Avro null,
// which Redshift Spectrum rejects ("Wrong type in Avro file. Field: partitions.
// Expected: 12 (union). Got: 7 (null)").
//
// The trigger is the inheritance path: Delete() uses the mergeOverwrite producer
// and the following AddFiles() uses fastAppend, which inherits the delete
// snapshot's manifests unchanged. When the manifest list is rewritten, an
// inherited manifest whose `partitions` summaries are present-but-empty (an
// unpartitioned table has zero field summaries) round-trips as a non-nil pointer
// to a nil slice, which the writer previously encoded as null instead of a
// present empty array.
//
// The fix (ensurePartitionList in manifest.go) normalizes a present-but-nil
// summary list back to an empty array so the union resolves to the array branch.
// This test asserts every record in the final manifest list encodes `partitions`
// as a present array, never null.

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
	"github.com/twmb/avro/ocf"
)

// manifestListRecordV2 mirrors the version-2 manifest-list entry schema
// (internal/avro_schemas.go, "manifest_list_file_v2"). Only PartitionList is
// inspected, but the full record is declared so the reader schema derived from
// these struct tags matches the writer schema field-for-field. PartitionList is
// decoded as `any` so a null union member surfaces as a nil interface and a
// present array surfaces as a non-nil []any.
type manifestListRecordV2 struct {
	Path               string `avro:"manifest_path"`
	Len                int64  `avro:"manifest_length"`
	SpecID             int32  `avro:"partition_spec_id"`
	Content            int32  `avro:"content"`
	SeqNumber          int64  `avro:"sequence_number"`
	MinSeqNumber       int64  `avro:"min_sequence_number"`
	AddedSnapshotID    int64  `avro:"added_snapshot_id"`
	AddedFilesCount    int32  `avro:"added_files_count"`
	ExistingFilesCount int32  `avro:"existing_files_count"`
	DeletedFilesCount  int32  `avro:"deleted_files_count"`
	PartitionList      any    `avro:"partitions"`
	AddedRowsCount     int64  `avro:"added_rows_count"`
	ExistingRowsCount  int64  `avro:"existing_rows_count"`
	DeletedRowsCount   int64  `avro:"deleted_rows_count"`
	Key                []byte `avro:"key_metadata"`
}

// readManifestListPartitionFields raw-decodes the manifest-list Avro file at
// manifestListPath and returns the `partitions` union value for every record, in
// file order. Reading the raw union (rather than the normalized ManifestFile) is
// what lets the test distinguish a present empty array from null.
func readManifestListPartitionFields(t *testing.T, fs iceio.IO, manifestListPath string) []any {
	t.Helper()

	f, err := fs.Open(manifestListPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	rd, err := ocf.NewReader(f)
	require.NoError(t, err)
	defer func() { require.NoError(t, rd.Close()) }()

	var partitions []any
	for {
		var rec manifestListRecordV2
		err := rd.Decode(&rec)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		partitions = append(partitions, rec.PartitionList)
	}

	return partitions
}

// TestManifestListPartitionsPresentAfterDeleteThenAddFiles reproduces the #1309
// sequence (append -> delete -> add-files) on an unpartitioned v2 table and
// asserts the final manifest list never encodes `partitions` as null.
func TestManifestListPartitionsPresentAfterDeleteThenAddFiles(t *testing.T) {
	ctx := context.Background()
	location := filepath.ToSlash(t.TempDir())
	fs := iceio.LocalFS{}

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	// Default (copy-on-write) delete mode is what routes Delete() through the
	// mergeOverwrite producer, so leave WriteDeleteMode unset.
	meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)

	metaLoc := location + "/metadata/v1.metadata.json"
	fsF := func(context.Context) (iceio.IO, error) { return fs, nil }
	cat := &concurrentTestCatalog{metadata: meta, location: metaLoc, fsF: fsF}
	tbl := table.New(table.Identifier{"db", "list_partitions_1309"}, meta, metaLoc, fsF, cat)

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	// Step 1 — append initial data so the later Delete has a file to rewrite.
	data, err := array.TableFromJSON(memory.DefaultAllocator, arrowSchema, []string{
		`[{"id":1},{"id":2},{"id":3}]`,
	})
	require.NoError(t, err)
	defer data.Release()

	tbl, err = tbl.Append(ctx, array.NewTableReader(data, -1), nil)
	require.NoError(t, err)

	// Step 2 — Delete (copy-on-write / mergeOverwrite). id==2 matches a subset of
	// the file's rows, forcing a rewrite and an overwrite snapshot whose manifest
	// is inherited by the next commit.
	tbl, err = tbl.Delete(ctx, iceberg.EqualTo(iceberg.Reference("id"), int64(2)), nil)
	require.NoError(t, err)

	// Step 3 — AddFiles (fastAppend) inherits the delete snapshot's manifests.
	// This is the commit whose manifest list #1309 reports as unreadable.
	extraPath := location + "/data-extra.parquet"
	writeInt64Parquet(t, fs, extraPath, arrowSchema, []int64{4, 5})

	txn := tbl.NewTransaction()
	require.NoError(t, txn.AddFiles(ctx, []string{extraPath}, nil, false))
	tbl, err = txn.Commit(ctx)
	require.NoError(t, err)

	// The final manifest list references the inherited (delete) manifest plus the
	// newly added one. Every record's `partitions` must be a present array; a nil
	// (null) value is the #1309 regression.
	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)
	require.NotEmpty(t, snap.ManifestList, "final snapshot must have a manifest list")

	partitionFields := readManifestListPartitionFields(t, fs, snap.ManifestList)
	require.GreaterOrEqual(t, len(partitionFields), 2,
		"final manifest list should reference the inherited delete manifest and the added manifest")
	for i, p := range partitionFields {
		require.NotNilf(t, p,
			"manifest-list record %d encoded `partitions` as Avro null; Redshift Spectrum "+
				"rejects this (issue #1309) — it must be a present (empty) array", i)
		require.IsTypef(t, []any{}, p,
			"manifest-list record %d `partitions` should decode as an array", i)
	}

	// Local data-integrity smoke test (independent of the manifest-list fix
	// above): iceberg-go's own reader round-trips the appended values — 1 and 3
	// survive the delete of id==2, and 4 and 5 are added. This only exercises
	// this library's read path; it does not reproduce or explain the issue's
	// secondary Athena "NULL columns" symptom, which involves an external engine
	// and is out of scope here.
	require.Equal(t, []int64{1, 3, 4, 5}, idsInTable(t, tbl),
		"iceberg-go must read the appended rows back with their real values")
}

// writeInt64Parquet writes a single-column ("id") Parquet file so AddFiles has a
// real file to ingest.
func writeInt64Parquet(t *testing.T, fs iceio.LocalFS, path string, arrowSchema *arrow.Schema, ids []int64) {
	t.Helper()

	bldr := array.NewInt64Builder(memory.DefaultAllocator)
	defer bldr.Release()

	bldr.AppendValues(ids, nil)
	col := bldr.NewArray()
	defer col.Release()

	rec := array.NewRecordBatch(arrowSchema, []arrow.Array{col}, int64(len(ids)))
	defer rec.Release()

	arrTbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
	defer arrTbl.Release()

	fo, err := fs.Create(path)
	require.NoError(t, err)

	require.NoError(t, pqarrow.WriteTable(arrTbl, fo, arrTbl.NumRows(),
		nil, pqarrow.DefaultWriterProps()))
}
