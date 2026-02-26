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
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud"
	"github.com/stretchr/testify/require"
)

var errLimitedWrite = errors.New("write limit exceeded")

type limitedWriteCloser struct {
	limit   int
	written int
	err     error
}

func (w *limitedWriteCloser) Write(p []byte) (int, error) {
	if w.written+len(p) > w.limit {
		return 0, w.err
	}
	w.written += len(p)

	return len(p), nil
}

func (w *limitedWriteCloser) Close() error {
	return nil
}

func (w *limitedWriteCloser) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(w, r)
}

type memIO struct {
	limit int
	err   error
	files map[string][]byte
}

func newMemIO(limit int, err error) *memIO {
	return &memIO{
		limit: limit,
		err:   err,
		files: make(map[string][]byte),
	}
}

func (m *memIO) Open(name string) (iceio.File, error) {
	data, ok := m.files[name]
	if !ok {
		return nil, fs.ErrNotExist
	}

	return &internal.MockFile{Contents: bytes.NewReader(data)}, nil
}

func (m *memIO) Create(name string) (iceio.FileWriter, error) {
	return &limitedWriteCloser{limit: m.limit, err: m.err}, nil
}

func (m *memIO) WriteFile(name string, content []byte) error {
	m.files[name] = append([]byte(nil), content...)

	return nil
}

func (m *memIO) Remove(name string) error {
	delete(m.files, name)

	return nil
}

// createTestTransactionWithMemIO creates a transaction using the io package's mem blob FS
// so that Create() output is persisted and can be read back (e.g. for sequential commits).
func createTestTransactionWithMemIO(t *testing.T, spec iceberg.PartitionSpec) (*Transaction, iceio.WriteFileIO) {
	t.Helper()
	ctx := context.Background()
	fs, err := iceio.LoadFS(ctx, nil, "mem://default/table-location")
	require.NoError(t, err, "LoadFS mem")
	wfs := fs.(iceio.WriteFileIO)
	schema := simpleSchema()
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "mem://default/table-location", nil)
	require.NoError(t, err, "new metadata")
	tbl := New(Identifier{"db", "tbl"}, meta, "metadata.json", func(context.Context) (iceio.IO, error) { return fs, nil }, nil)

	return tbl.NewTransaction(), wfs
}

func manifestHeaderSize(t *testing.T, version int, spec iceberg.PartitionSpec, schema *iceberg.Schema) int {
	t.Helper()

	var buf bytes.Buffer
	writer, err := iceberg.NewManifestWriter(version, &buf, spec, schema, 1)
	require.NoError(t, err, "new manifest writer")
	_ = writer.Close()

	return buf.Len()
}

func manifestSize(t *testing.T, version int, spec iceberg.PartitionSpec, schema *iceberg.Schema, snapshotID int64, entries []iceberg.ManifestEntry) int {
	t.Helper()

	var buf bytes.Buffer
	_, err := iceberg.WriteManifest("size.avro", &buf, version, spec, schema, snapshotID, entries)
	require.NoError(t, err, "write manifest for size")

	return buf.Len()
}

func newTestDataFile(t *testing.T, spec iceberg.PartitionSpec, path string, partition map[int]any) iceberg.DataFile {
	return newTestDataFileWithCount(t, spec, path, partition, 1)
}

func newTestDataFileWithCount(t *testing.T, spec iceberg.PartitionSpec, path string, partition map[int]any, count int64) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		path,
		iceberg.ParquetFile,
		partition,
		nil,
		nil,
		count,
		count,
	)
	require.NoError(t, err, "new data file builder")

	return builder.Build()
}

func simpleSchema() *iceberg.Schema {
	return iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	})
}

func partitionedSpec() iceberg.PartitionSpec {
	return iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceID: 1, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
	})
}

func createTestTransaction(t *testing.T, io iceio.IO, spec iceberg.PartitionSpec) *Transaction {
	schema := simpleSchema()
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "table-location", nil)
	require.NoError(t, err, "new metadata")

	tbl := New(Identifier{"db", "tbl"}, meta, "metadata.json", func(context.Context) (iceio.IO, error) {
		return io, nil
	}, nil)

	return tbl.NewTransaction()
}

// TestCommitV3RowLineage ensures v3 snapshot commits set FirstRowID and AddedRows
// on the snapshot for row lineage, and that applying updates advances next-row-id correctly.
func TestCommitV3RowLineage(t *testing.T) {
	trackIO := newTrackingIO()
	spec := iceberg.NewPartitionSpec()
	txn := createTestTransaction(t, trackIO, spec)
	txn.meta.formatVersion = 3

	// Single data file with record count 1 (newTestDataFile uses 1, 1 for record count and file size).
	const expectedAddedRows = 1
	sp := newFastAppendFilesProducer(OpAppend, txn, trackIO, nil, nil)
	df := newTestDataFile(t, spec, "file://data.parquet", nil)
	sp.appendDataFile(df)

	updates, reqs, err := sp.commit()
	require.NoError(t, err, "commit should succeed")
	require.Len(t, updates, 2, "expected AddSnapshot and SetSnapshotRef updates")
	addSnap, ok := updates[0].(*addSnapshotUpdate)
	require.True(t, ok, "first update must be AddSnapshot")

	// Exact snapshot lineage: first-row-id 0 for new table, added-rows matches appended file(s).
	require.NotNil(t, addSnap.Snapshot.FirstRowID, "v3 snapshot must have first-row-id")
	require.NotNil(t, addSnap.Snapshot.AddedRows, "v3 snapshot must have added-rows")
	require.Equal(t, int64(0), *addSnap.Snapshot.FirstRowID, "first-row-id should be table next-row-id at commit")
	require.Equal(t, int64(expectedAddedRows), *addSnap.Snapshot.AddedRows, "added-rows should match appended data file record count")

	// Apply updates and verify metadata next-row-id advances monotonically.
	err = txn.apply(updates, reqs)
	require.NoError(t, err, "apply should succeed")
	meta, err := txn.meta.Build()
	require.NoError(t, err, "build metadata")
	require.Equal(t, int64(expectedAddedRows), meta.NextRowID(), "next-row-id should equal first-row-id + added-rows")
}

// TestCommitV3RowLineageTwoSequentialCommits runs two commits and asserts monotonic,
// gap-free first-row-id / next-row-id progression.
func TestCommitV3RowLineageTwoSequentialCommits(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	ident := Identifier{"db", "tbl"}
	txn, memIO := createTestTransactionWithMemIO(t, spec)
	txn.meta.formatVersion = 3

	// First commit: new table, append one file (1 row).
	sp1 := newFastAppendFilesProducer(OpAppend, txn, memIO, nil, nil)
	sp1.appendDataFile(newTestDataFile(t, spec, "file://data-1.parquet", nil))
	updates1, reqs1, err := sp1.commit()
	require.NoError(t, err, "first commit should succeed")
	addSnap1, ok := updates1[0].(*addSnapshotUpdate)
	require.True(t, ok)
	require.Equal(t, int64(0), *addSnap1.Snapshot.FirstRowID, "first snapshot first-row-id")
	require.Equal(t, int64(1), *addSnap1.Snapshot.AddedRows, "first snapshot added-rows")
	err = txn.apply(updates1, reqs1)
	require.NoError(t, err, "first apply should succeed")
	meta1, err := txn.meta.Build()
	require.NoError(t, err)
	require.Equal(t, int64(1), meta1.NextRowID(), "next-row-id after first commit")

	// Second commit: fast append one more file. Carried manifest already has first_row_id, so only new manifest gets row IDs; delta = 1.
	tbl2 := New(ident, meta1, "metadata.json", func(context.Context) (iceio.IO, error) { return memIO, nil }, nil)
	txn2 := tbl2.NewTransaction()
	txn2.meta.formatVersion = 3
	sp2 := newFastAppendFilesProducer(OpAppend, txn2, memIO, nil, nil)
	sp2.appendDataFile(newTestDataFile(t, spec, "file://data-2.parquet", nil))
	updates2, reqs2, err := sp2.commit()
	require.NoError(t, err, "second commit should succeed")
	addSnap2, ok := updates2[0].(*addSnapshotUpdate)
	require.True(t, ok)
	require.Equal(t, int64(1), *addSnap2.Snapshot.FirstRowID, "second snapshot first-row-id continues from first next-row-id")
	require.Equal(t, int64(1), *addSnap2.Snapshot.AddedRows, "only new manifest gets row IDs assigned")

	err = txn2.apply(updates2, reqs2)
	require.NoError(t, err, "second apply should succeed")
	meta2, err := txn2.meta.Build()
	require.NoError(t, err)
	require.Equal(t, int64(2), meta2.NextRowID(), "next-row-id = 1 + 1 (gap-free)")
}

// TestCommitV3RowLineageDeltaIncludesExistingRows uses merge append so one manifest
// has both existing and added rows; verifies assigned delta includes ExistingRowsCount
// and metadata next-row-id matches.
func TestCommitV3RowLineageDeltaIncludesExistingRows(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	ident := Identifier{"db", "tbl"}
	txn, memIO := createTestTransactionWithMemIO(t, spec)
	txn.meta.formatVersion = 3

	// First commit: one file (1 row).
	sp1 := newFastAppendFilesProducer(OpAppend, txn, memIO, nil, nil)
	sp1.appendDataFile(newTestDataFile(t, spec, "file://data-1.parquet", nil))
	updates1, reqs1, err := sp1.commit()
	require.NoError(t, err, "first commit should succeed")
	err = txn.apply(updates1, reqs1)
	require.NoError(t, err)
	meta1, err := txn.meta.Build()
	require.NoError(t, err)
	require.Equal(t, int64(1), meta1.NextRowID())

	// Second commit: merge append so the two data manifests (existing + new) are merged into one with 1 existing + 1 added row.
	tbl2 := New(ident, meta1, "metadata.json", func(context.Context) (iceio.IO, error) { return memIO, nil }, nil)
	txn2 := tbl2.NewTransaction()
	txn2.meta.formatVersion = 3
	if txn2.meta.props == nil {
		txn2.meta.props = make(iceberg.Properties)
	}
	txn2.meta.props[ManifestMergeEnabledKey] = "true"
	txn2.meta.props[ManifestMinMergeCountKey] = "2"
	sp2 := newMergeAppendFilesProducer(OpAppend, txn2, memIO, nil, nil)
	sp2.appendDataFile(newTestDataFile(t, spec, "file://data-2.parquet", nil))
	updates2, reqs2, err := sp2.commit()
	require.NoError(t, err, "second commit (merge) should succeed")
	addSnap2, ok := updates2[0].(*addSnapshotUpdate)
	require.True(t, ok)
	require.Equal(t, int64(1), *addSnap2.Snapshot.FirstRowID, "first-row-id continues from first commit")
	require.Equal(t, int64(2), *addSnap2.Snapshot.AddedRows, "assigned delta = existing (1) + added (1) in merged manifest")

	err = txn2.apply(updates2, reqs2)
	require.NoError(t, err)
	meta2, err := txn2.meta.Build()
	require.NoError(t, err)
	require.Equal(t, int64(3), meta2.NextRowID(), "next-row-id = first-row-id + assigned delta (1+2)")
}

func readManifestListFromPath(t *testing.T, fs iceio.IO, path string) []iceberg.ManifestFile {
	t.Helper()

	f, err := fs.Open(path)
	require.NoError(t, err, "open manifest list: %s", path)
	defer f.Close()

	list, err := iceberg.ReadManifestList(f)
	require.NoError(t, err, "read manifest list: %s", path)

	return list
}

func manifestFirstRowIDForSnapshot(t *testing.T, manifests []iceberg.ManifestFile, snapshotID int64) int64 {
	t.Helper()

	type manifestRowLineage struct {
		AddedSnapshotID int64  `json:"AddedSnapshotID"`
		FirstRowID      *int64 `json:"FirstRowId"`
	}

	for _, manifest := range manifests {
		raw, err := json.Marshal(manifest)
		require.NoError(t, err, "marshal manifest")

		var decoded manifestRowLineage
		require.NoError(t, json.Unmarshal(raw, &decoded), "unmarshal manifest row-lineage fields")

		if decoded.AddedSnapshotID == snapshotID {
			require.NotNil(t, decoded.FirstRowID, "first_row_id must be persisted for v3 data manifests")

			return *decoded.FirstRowID
		}
	}

	require.Failf(t, "missing manifest for snapshot", "snapshot-id=%d", snapshotID)

	return 0
}

// TestCommitV3RowLineagePersistsManifestFirstRowID verifies that snapshot producer
// writes first_row_id to manifest list entries using the snapshot's start row-id.
func TestCommitV3RowLineagePersistsManifestFirstRowID(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	ident := Identifier{"db", "tbl"}
	txn, memIO := createTestTransactionWithMemIO(t, spec)
	txn.meta.formatVersion = 3

	// Use multi-row files to make row-range starts obvious.
	sp1 := newFastAppendFilesProducer(OpAppend, txn, memIO, nil, nil)
	sp1.appendDataFile(newTestDataFileWithCount(t, spec, "file://data-1.parquet", nil, 3))
	updates1, reqs1, err := sp1.commit()
	require.NoError(t, err, "first commit should succeed")
	addSnap1, ok := updates1[0].(*addSnapshotUpdate)
	require.True(t, ok, "first update must be AddSnapshot")
	require.Equal(t, int64(0), *addSnap1.Snapshot.FirstRowID, "snapshot first-row-id for commit 1")

	manifests1 := readManifestListFromPath(t, memIO, addSnap1.Snapshot.ManifestList)
	currentManifestFirstRowID1 := manifestFirstRowIDForSnapshot(t, manifests1, addSnap1.Snapshot.SnapshotID)
	require.Equal(t, *addSnap1.Snapshot.FirstRowID, currentManifestFirstRowID1,
		"persisted manifest first_row_id must match snapshot first-row-id for current commit")

	err = txn.apply(updates1, reqs1)
	require.NoError(t, err, "first apply should succeed")
	meta1, err := txn.meta.Build()
	require.NoError(t, err)
	require.Equal(t, int64(3), meta1.NextRowID())

	tbl2 := New(ident, meta1, "metadata.json", func(context.Context) (iceio.IO, error) { return memIO, nil }, nil)
	txn2 := tbl2.NewTransaction()
	txn2.meta.formatVersion = 3
	sp2 := newFastAppendFilesProducer(OpAppend, txn2, memIO, nil, nil)
	sp2.appendDataFile(newTestDataFileWithCount(t, spec, "file://data-2.parquet", nil, 5))
	updates2, _, err := sp2.commit()
	require.NoError(t, err, "second commit should succeed")
	addSnap2, ok := updates2[0].(*addSnapshotUpdate)
	require.True(t, ok, "first update must be AddSnapshot")
	require.Equal(t, int64(3), *addSnap2.Snapshot.FirstRowID, "snapshot first-row-id for commit 2")

	manifests2 := readManifestListFromPath(t, memIO, addSnap2.Snapshot.ManifestList)
	currentManifestFirstRowID2 := manifestFirstRowIDForSnapshot(t, manifests2, addSnap2.Snapshot.SnapshotID)
	require.Equal(t, *addSnap2.Snapshot.FirstRowID, currentManifestFirstRowID2,
		"persisted manifest first_row_id must match snapshot first-row-id for current commit")
}

func TestSnapshotProducerManifestsClosesWriterOnError(t *testing.T) {
	spec := partitionedSpec()
	schema := simpleSchema()
	mem := newMemIO(manifestHeaderSize(t, 2, spec, schema), errLimitedWrite)
	txn := createTestTransaction(t, mem, spec)

	sp := newFastAppendFilesProducer(OpAppend, txn, mem, nil, nil)
	validPartition := map[int]any{1000: int32(1)}
	sp.appendDataFile(newTestDataFile(t, spec, "file://data-1.parquet", validPartition))
	sp.appendDataFile(newTestDataFile(t, spec, "file://data-2.parquet", nil))

	_, err := sp.manifests()
	require.ErrorIs(t, err, errLimitedWrite)
}

func TestManifestMergeManagerClosesWriterOnError(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	schema := simpleSchema()
	mem := newMemIO(manifestHeaderSize(t, 2, spec, schema), errLimitedWrite)
	txn := createTestTransaction(t, mem, spec)

	sp := newFastAppendFilesProducer(OpAppend, txn, mem, nil, nil)
	df := newTestDataFile(t, spec, "file://data-1.parquet", nil)
	entries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &sp.snapshotID, nil, nil, df),
	}

	manifestPath := "table-location/metadata/manifest-1.avro"
	var manifestBuf bytes.Buffer
	manifestFile, err := iceberg.WriteManifest(manifestPath, &manifestBuf, 2, spec, schema, sp.snapshotID, entries)
	require.NoError(t, err, "write manifest")
	require.NoError(t, mem.WriteFile(manifestPath, manifestBuf.Bytes()))

	missingManifest := iceberg.NewManifestFile(2, "table-location/metadata/missing.avro", 1, int32(spec.ID()), sp.snapshotID).
		Build()

	mgr := manifestMergeManager{snap: sp}
	_, err = mgr.createManifest(spec.ID(), []iceberg.ManifestFile{
		manifestFile,
		missingManifest,
	})
	require.ErrorIs(t, err, errLimitedWrite)
}

func TestOverwriteFilesExistingManifestsClosesWriterOnError(t *testing.T) {
	spec := partitionedSpec()
	schema := simpleSchema()

	snapshotID := int64(100)
	seqNum := int64(-1)
	validSeq := int64(42)
	manifestPath := "table-location/metadata/manifest-1.avro"
	manifestListPath := "table-location/metadata/snap-1.avro"

	validPartition := map[int]any{1000: int32(1)}
	validFile := newTestDataFile(t, spec, "file://valid.parquet", validPartition)
	sizeEntries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusEXISTING, &snapshotID, &validSeq, nil, validFile),
	}
	headerLen := manifestHeaderSize(t, 2, spec, schema)
	manifestLen := manifestSize(t, 2, spec, schema, snapshotID, sizeEntries)
	require.Greater(t, manifestLen, headerLen, "manifest size")

	mem := newMemIO(manifestLen-1, errLimitedWrite)
	txn := createTestTransaction(t, mem, spec)

	deletedFile := newTestDataFile(t, spec, "file://deleted.parquet", validPartition)
	invalidFile := newTestDataFile(t, spec, "file://invalid.parquet", validPartition)
	entries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, &validSeq, nil, deletedFile),
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, &validSeq, nil, validFile),
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, invalidFile),
	}

	var manifestBuf bytes.Buffer
	manifestFile, err := iceberg.WriteManifest(manifestPath, &manifestBuf, 2, spec, schema, snapshotID, entries)
	require.NoError(t, err, "write manifest")
	require.NoError(t, mem.WriteFile(manifestPath, manifestBuf.Bytes()))

	var listBuf bytes.Buffer
	_, err = iceberg.WriteManifestList(2, &listBuf, snapshotID, nil, &seqNum, 0, []iceberg.ManifestFile{manifestFile})
	require.NoError(t, err, "write manifest list")
	require.NoError(t, mem.WriteFile(manifestListPath, listBuf.Bytes()))

	snap := Snapshot{
		SnapshotID:     snapshotID,
		SequenceNumber: seqNum,
		TimestampMs:    time.Now().UnixMilli(),
		ManifestList:   manifestListPath,
	}
	txn.meta.snapshotList = []Snapshot{snap}
	txn.meta.currentSnapshotID = &snapshotID

	sp := newOverwriteFilesProducer(OpOverwrite, txn, mem, nil, nil)
	sp.deleteDataFile(deletedFile)

	_, err = sp.existingManifests()
	require.ErrorIs(t, err, errLimitedWrite)
}

// trackingWriteCloser wraps a bytes.Buffer and tracks if Close was called.
type trackingWriteCloser struct {
	buf     *bytes.Buffer
	closed  bool
	closeMu sync.Mutex
}

func newTrackingWriteCloser() *trackingWriteCloser {
	return &trackingWriteCloser{buf: &bytes.Buffer{}}
}

func (t *trackingWriteCloser) Write(p []byte) (int, error) {
	return t.buf.Write(p)
}

func (t *trackingWriteCloser) Close() error {
	t.closeMu.Lock()
	defer t.closeMu.Unlock()
	t.closed = true

	return nil
}

func (t *trackingWriteCloser) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(struct{ io.Writer }{t.buf}, r)
}

func (t *trackingWriteCloser) IsClosed() bool {
	t.closeMu.Lock()
	defer t.closeMu.Unlock()

	return t.closed
}

// trackingIO is an IO implementation that tracks file writer closure.
type trackingIO struct {
	files     map[string][]byte
	writers   map[string]*trackingWriteCloser
	writersMu sync.Mutex
}

func newTrackingIO() *trackingIO {
	return &trackingIO{
		files:   make(map[string][]byte),
		writers: make(map[string]*trackingWriteCloser),
	}
}

func (t *trackingIO) Open(name string) (iceio.File, error) {
	data, ok := t.files[name]
	if !ok {
		return nil, fs.ErrNotExist
	}

	return &internal.MockFile{Contents: bytes.NewReader(data)}, nil
}

func (t *trackingIO) Create(name string) (iceio.FileWriter, error) {
	t.writersMu.Lock()
	defer t.writersMu.Unlock()

	tw := newTrackingWriteCloser()
	t.writers[name] = tw

	return tw, nil
}

func (t *trackingIO) WriteFile(name string, content []byte) error {
	t.files[name] = append([]byte(nil), content...)

	return nil
}

func (t *trackingIO) Remove(name string) error {
	delete(t.files, name)

	return nil
}

func (t *trackingIO) GetUnclosedWriters() []string {
	t.writersMu.Lock()
	defer t.writersMu.Unlock()

	var unclosed []string
	for name, writer := range t.writers {
		if !writer.IsClosed() {
			unclosed = append(unclosed, name)
		}
	}

	return unclosed
}

func (t *trackingIO) GetWriterCount() int {
	t.writersMu.Lock()
	defer t.writersMu.Unlock()

	return len(t.writers)
}

// TestManifestWriterClosesUnderlyingFile tests that when using newManifestWriter,
// the underlying file writer is properly closed. This test is related to issue #644 and #681
// where blob.Writer was never closed, causing table corruption.
func TestManifestWriterClosesUnderlyingFile(t *testing.T) {
	trackIO := newTrackingIO()
	spec := iceberg.NewPartitionSpec()
	txn := createTestTransaction(t, trackIO, spec)

	sp := newFastAppendFilesProducer(OpAppend, txn, trackIO, nil, nil)
	df := newTestDataFile(t, spec, "file://data-1.parquet", nil)
	sp.appendDataFile(df)

	manifests, err := sp.manifests()
	require.NoError(t, err, "manifests should succeed")
	require.Len(t, manifests, 1, "should have one manifest")

	unclosed := trackIO.GetUnclosedWriters()
	require.Empty(t, unclosed, "all file writerFactory should be closed, but these are still open: %v", unclosed)
}

// TestCreateManifestClosesUnderlyingFile tests that createManifest properly
// closes the underlying file writer. This is related to issue #644 and #681.
func TestCreateManifestClosesUnderlyingFile(t *testing.T) {
	trackIO := newTrackingIO()
	spec := iceberg.NewPartitionSpec()
	txn := createTestTransaction(t, trackIO, spec)
	schema := simpleSchema()

	sp := newFastAppendFilesProducer(OpAppend, txn, trackIO, nil, nil)
	df := newTestDataFile(t, spec, "file://data-1.parquet", nil)
	entries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &sp.snapshotID, nil, nil, df),
	}

	manifestPath := "table-location/metadata/manifest-1.avro"
	var manifestBuf bytes.Buffer
	manifestFile, err := iceberg.WriteManifest(manifestPath, &manifestBuf, 2, spec, schema, sp.snapshotID, entries)
	require.NoError(t, err, "write manifest")
	require.NoError(t, trackIO.WriteFile(manifestPath, manifestBuf.Bytes()))

	trackIO.writers = make(map[string]*trackingWriteCloser)

	mgr := manifestMergeManager{snap: sp}
	_, err = mgr.createManifest(spec.ID(), []iceberg.ManifestFile{manifestFile})
	require.NoError(t, err, "createManifest should succeed")

	unclosed := trackIO.GetUnclosedWriters()
	require.Empty(t, unclosed, "all file writerFactory should be closed after createManifest, but these are still open: %v", unclosed)
}

// TestOverwriteExistingManifestsClosesUnderlyingFile tests that existingManifests
// in overwriteFiles properly closes the underlying file writer. This is related to issue #644 and #681.
func TestOverwriteExistingManifestsClosesUnderlyingFile(t *testing.T) {
	trackIO := newTrackingIO()
	spec := partitionedSpec()
	txn := createTestTransaction(t, trackIO, spec)
	schema := simpleSchema()

	snapshotID := int64(100)
	seqNum := int64(-1)
	validSeq := int64(42)
	manifestPath := "table-location/metadata/manifest-1.avro"
	manifestListPath := "table-location/metadata/snap-1.avro"

	validPartition := map[int]any{1000: int32(1)}
	deletedFile := newTestDataFile(t, spec, "file://deleted.parquet", validPartition)
	validFile := newTestDataFile(t, spec, "file://valid.parquet", validPartition)
	entries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, &validSeq, nil, deletedFile),
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, &validSeq, nil, validFile),
	}

	var manifestBuf bytes.Buffer
	manifestFile, err := iceberg.WriteManifest(manifestPath, &manifestBuf, 2, spec, schema, snapshotID, entries)
	require.NoError(t, err, "write manifest")
	require.NoError(t, trackIO.WriteFile(manifestPath, manifestBuf.Bytes()))

	var listBuf bytes.Buffer
	_, err = iceberg.WriteManifestList(2, &listBuf, snapshotID, nil, &seqNum, 0, []iceberg.ManifestFile{manifestFile})
	require.NoError(t, err, "write manifest list")
	require.NoError(t, trackIO.WriteFile(manifestListPath, listBuf.Bytes()))

	snap := Snapshot{
		SnapshotID:     snapshotID,
		SequenceNumber: seqNum,
		ManifestList:   manifestListPath,
	}
	txn.meta.snapshotList = []Snapshot{snap}
	txn.meta.currentSnapshotID = &snapshotID

	sp := newOverwriteFilesProducer(OpOverwrite, txn, trackIO, nil, nil)
	sp.deleteDataFile(deletedFile)

	trackIO.writers = make(map[string]*trackingWriteCloser)

	_, err = sp.existingManifests()
	require.NoError(t, err, "existingManifests should succeed")

	unclosed := trackIO.GetUnclosedWriters()
	require.Empty(t, unclosed, "all file writerFactory should be closed after existingManifests, but these are still open: %v", unclosed)
}

// errorOnDeletedEntries is a producerImpl that returns an error from deletedEntries()
// to test that file writerFactory are properly closed even when deletedEntries fails.
type errorOnDeletedEntries struct {
	base                *snapshotProducer
	err                 error
	waitForWriter       <-chan struct{} // optional signal before returning error
	cancelWaitForWriter <-chan struct{} // optional cancel channel
}

func (e *errorOnDeletedEntries) processManifests(manifests []iceberg.ManifestFile) ([]iceberg.ManifestFile, error) {
	return manifests, nil
}

func (e *errorOnDeletedEntries) existingManifests() ([]iceberg.ManifestFile, error) {
	return nil, nil
}

func (e *errorOnDeletedEntries) deletedEntries() ([]iceberg.ManifestEntry, error) {
	if e.waitForWriter != nil {
		select {
		case <-e.waitForWriter:
		case <-e.cancelWaitForWriter:
			return nil, e.err
		}
	}

	return nil, e.err
}

// blockingTrackingIO extends trackingIO to signal when a writer is created.
type blockingTrackingIO struct {
	*trackingIO
	writerCreated chan struct{}
	signalOnce    sync.Once
}

func newBlockingTrackingIO() *blockingTrackingIO {
	return &blockingTrackingIO{
		trackingIO:    newTrackingIO(),
		writerCreated: make(chan struct{}),
	}
}

func (b *blockingTrackingIO) Create(name string) (iceio.FileWriter, error) {
	writer, err := b.trackingIO.Create(name)
	b.signalOnce.Do(func() {
		close(b.writerCreated)
	})

	return writer, err
}

// This test verifies that NO writerFactory are created when deletedEntries() fails,
// because the error should be returned before any goroutines start.
func TestManifestsClosesWriterWhenDeletedEntriesFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	blockingIO := newBlockingTrackingIO()
	spec := iceberg.NewPartitionSpec()
	txn := createTestTransaction(t, blockingIO, spec)

	sp := createSnapshotProducer(OpAppend, txn, blockingIO, nil, nil)
	errDeletedEntries := errors.New("simulated deletedEntries error")
	sp.producerImpl = &errorOnDeletedEntries{
		base:                sp,
		err:                 errDeletedEntries,
		waitForWriter:       blockingIO.writerCreated,
		cancelWaitForWriter: ctx.Done(),
	}

	df := newTestDataFile(t, spec, "file://data-1.parquet", nil)
	sp.appendDataFile(df)

	done := make(chan struct{})
	var manifestsErr error
	go func() {
		_, manifestsErr = sp.manifests()
		close(done)
	}()

	select {
	case <-done:
		require.ErrorIs(t, manifestsErr, errDeletedEntries)
		writerCount := blockingIO.GetWriterCount()
		require.NotZero(t, writerCount, "test setup error: expected writer to be created")
		require.Fail(t, "goroutine started before deletedEntries() check, creating a writer that could be orphaned")

	case <-time.After(100 * time.Millisecond):
		writerCount := blockingIO.GetWriterCount()
		require.Zero(t, writerCount, "expected no writerFactory to be created when deletedEntries is called first")
	}
}
