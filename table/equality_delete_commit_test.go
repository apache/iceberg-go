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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// idKeyArrowSchema builds the Arrow schema for an equality delete file
// keyed on the "id" column alone.
func idKeyArrowSchema(t *testing.T) *arrow.Schema {
	t.Helper()

	sc, err := table.SchemaToArrowSchema(
		iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true}),
		nil, true, false)
	require.NoError(t, err)

	return sc
}

// TestCommitEqualityDeletesUnpartitioned exercises the delete-only
// convenience API end to end on an unpartitioned table: append rows,
// commit equality deletes by key, then scan and confirm the keyed rows
// are gone and the snapshot summary reflects a single OpDelete.
func TestCommitEqualityDeletesUnpartitioned(t *testing.T) {
	tbl := newEqDeleteReadTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Metadata().CurrentSchema(), nil, false, false)
	require.NoError(t, err)

	dataPath := tbl.Location() + "/data/data-001.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[
		{"id": 1, "data": "alpha"},
		{"id": 2, "data": "beta"},
		{"id": 3, "data": "gamma"},
		{"id": 4, "data": "delta"},
		{"id": 5, "data": "epsilon"}
	]`)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)
	assertRowCount(t, tbl, 5)

	records, release := makeEqDeleteRecords(t, idKeyArrowSchema(t), `[{"id": 2}, {"id": 4}]`)
	defer release()

	tx2 := tbl.NewTransaction()
	require.NoError(t, tx2.CommitEqualityDeletes(t.Context(), []int{1}, records,
		iceberg.Properties{"cdc-source": "orders"}))
	tbl, err = tx2.Commit(t.Context())
	require.NoError(t, err)

	assertRowCount(t, tbl, 3)

	_, itr, err := tbl.Scan(table.WithSelectedFields("id")).ToArrowRecords(t.Context())
	require.NoError(t, err)

	var ids []int64
	for rec, err := range itr {
		require.NoError(t, err)
		col := rec.Column(0).(*array.Int64)
		for i := 0; i < col.Len(); i++ {
			ids = append(ids, col.Value(i))
		}
		rec.Release()
	}
	assert.Equal(t, []int64{1, 3, 5}, ids, "rows id=2 and id=4 must be deleted")

	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)
	assert.Equal(t, table.OpDelete, snap.Summary.Operation)
	assert.Equal(t, "1", snap.Summary.Properties["added-equality-delete-files"])
	assert.Equal(t, "orders", snap.Summary.Properties["cdc-source"],
		"snapshot props passed to the convenience API must land in the summary")
}

// TestCommitEqualityDeletesPartitioned confirms the convenience API
// routes records to one delete file per partition and commits them as a
// single OpDelete snapshot, inheriting the partitioned fanout behavior of
// WriteEqualityDeletes.
func TestCommitEqualityDeletesPartitioned(t *testing.T) {
	tbl := newPartitionedEqDeleteTestTable(t)

	delSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String, Required: true},
	)
	delArrowSc, err := table.SchemaToArrowSchema(delSchema, nil, true, false)
	require.NoError(t, err)

	records, release := makeEqDeleteRecords(t, delArrowSc,
		`[{"id": 10, "category": "books"}, {"id": 20, "category": "music"}, {"id": 30, "category": "books"}]`)
	defer release()

	tx := tbl.NewTransaction()
	require.NoError(t, tx.CommitEqualityDeletes(t.Context(), []int{1}, records, nil))
	committed, err := tx.Commit(t.Context())
	require.NoError(t, err)

	snap := committed.CurrentSnapshot()
	require.NotNil(t, snap)
	assert.Equal(t, table.OpDelete, snap.Summary.Operation)
	assert.Equal(t, "2", snap.Summary.Properties["added-equality-delete-files"],
		"records spanning two partitions must yield two equality delete files")
}

// TestCommitEqualityDeletesRejectsV1 proves the format-version gate is
// inherited from WriteEqualityDeletes without a second validation path.
func TestCommitEqualityDeletesRejectsV1(t *testing.T) {
	tbl := newEqDeleteTestTable(t, "1")

	records := func(yield func(arrow.RecordBatch, error) bool) {}

	tx := tbl.NewTransaction()
	err := tx.CommitEqualityDeletes(t.Context(), []int{1}, records, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "format version >= 2")
}

// TestCommitEqualityDeletesRejectsEmptyFieldIDs proves the empty-field-ID
// guard is inherited from WriteEqualityDeletes.
func TestCommitEqualityDeletesRejectsEmptyFieldIDs(t *testing.T) {
	tbl := newEqDeleteTestTable(t, "2")

	records := func(yield func(arrow.RecordBatch, error) bool) {}

	tx := tbl.NewTransaction()
	err := tx.CommitEqualityDeletes(t.Context(), nil, records, nil)
	require.ErrorIs(t, err, table.ErrEmptyEqualityFieldIDs)
}

// TestCommitEqualityDeletesEmptyRecordsIsNoOp confirms that an empty
// record stream produces no delete files and commits no snapshot, rather
// than surfacing RowDelta's "at least one file" error.
func TestCommitEqualityDeletesEmptyRecordsIsNoOp(t *testing.T) {
	tbl := newEqDeleteTestTable(t, "2")

	records := func(yield func(arrow.RecordBatch, error) bool) {}

	tx := tbl.NewTransaction()
	require.NoError(t, tx.CommitEqualityDeletes(t.Context(), []int{1}, records, nil))

	committed, err := tx.Commit(t.Context())
	require.NoError(t, err)
	assert.Nil(t, committed.CurrentSnapshot(),
		"empty delete stream must not produce a snapshot")
}

// TestCommitEqualityDeletesConcurrentAppendConflict pins the safety
// property that matters for continuous CDC writers: under the default
// serializable isolation, a concurrent append committed after the leader's
// base snapshot must make the convenience commit fail with a delete
// conflict, proving it inherits RowDelta's stale-snapshot conflict
// detection rather than committing optimistically.
func TestCommitEqualityDeletesConcurrentAppendConflict(t *testing.T) {
	tbl, cat := newConcurrentRewriteTestTable(t)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	dataPath := tbl.Location() + "/data/seed.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[
		{"id": 1, "data": "a"},
		{"id": 2, "data": "b"},
		{"id": 3, "data": "c"}
	]`)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	// Leader captures its base before the peer commits.
	leaderTxn := tbl.NewTransaction()
	records, release := makeEqDeleteRecords(t, idKeyArrowSchema(t), `[{"id": 2}]`)
	defer release()
	require.NoError(t, leaderTxn.CommitEqualityDeletes(t.Context(), []int{1}, records, nil))

	// A concurrent writer appends new data and commits first, advancing
	// the catalog past the leader's base.
	peerPath := tbl.Location() + "/data/peer.parquet"
	writeParquetFile(t, peerPath, arrowSc, `[{"id": 4, "data": "d"}]`)
	peerTxn := tbl.NewTransaction()
	require.NoError(t, peerTxn.AddFiles(t.Context(), []string{peerPath}, nil, false))
	_, err = peerTxn.Commit(t.Context())
	require.NoError(t, err)

	beforeLeader := cat.attempts.Load()
	_, err = leaderTxn.Commit(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, table.ErrConflictingDataFiles,
		"serializable isolation must reject an equality delete racing a concurrent append")
	assert.GreaterOrEqual(t, cat.attempts.Load()-beforeLeader, int32(1),
		"the stale-assertion attempt must reach the catalog before the validator rejects the refreshed context")
}

// TestCommitEqualityDeletesSnapshotIsolationAllowsConcurrentAppend is the
// companion to the conflict test: switching write.delete.isolation-level
// to snapshot must let the same concurrent-append scenario commit, proving
// the isolation knob is honored through the convenience API.
func TestCommitEqualityDeletesSnapshotIsolationAllowsConcurrentAppend(t *testing.T) {
	tbl, _ := newConcurrentRewriteTestTable(t)

	txProps := tbl.NewTransaction()
	require.NoError(t, txProps.SetProperties(iceberg.Properties{
		table.WriteDeleteIsolationLevelKey: string(table.IsolationSnapshot),
	}))
	tbl, err := txProps.Commit(t.Context())
	require.NoError(t, err)

	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	dataPath := tbl.Location() + "/data/seed.parquet"
	writeParquetFile(t, dataPath, arrowSc, `[
		{"id": 1, "data": "a"},
		{"id": 2, "data": "b"}
	]`)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddFiles(t.Context(), []string{dataPath}, nil, false))
	tbl, err = tx.Commit(t.Context())
	require.NoError(t, err)

	leaderTxn := tbl.NewTransaction()
	records, release := makeEqDeleteRecords(t, idKeyArrowSchema(t), `[{"id": 2}]`)
	defer release()
	require.NoError(t, leaderTxn.CommitEqualityDeletes(t.Context(), []int{1}, records, nil))

	peerPath := tbl.Location() + "/data/peer.parquet"
	writeParquetFile(t, peerPath, arrowSc, `[{"id": 3, "data": "c"}]`)
	peerTxn := tbl.NewTransaction()
	require.NoError(t, peerTxn.AddFiles(t.Context(), []string{peerPath}, nil, false))
	_, err = peerTxn.Commit(t.Context())
	require.NoError(t, err)

	_, err = leaderTxn.Commit(t.Context())
	require.NoError(t, err,
		"snapshot isolation must permit an equality delete racing a concurrent append")
}
