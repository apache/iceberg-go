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
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

var errLimitedWrite = errors.New("write limit exceeded")

type limitedWriteCloser struct {
	limit   int
	written int
	err     error

	// parent and path, when both set, cause Close() to persist the
	// successfully-written payload back into parent.files[path]. This makes
	// the orphan/cleanup tests exercise the real write→delete cycle (R3)
	// instead of pre-populating wfs.files with a placeholder.
	parent *memIO
	path   string
	buf    bytes.Buffer
}

func (w *limitedWriteCloser) Write(p []byte) (int, error) {
	if w.written+len(p) > w.limit {
		return 0, w.err
	}
	w.written += len(p)
	if w.parent != nil {
		w.buf.Write(p)
	}

	return len(p), nil
}

func (w *limitedWriteCloser) Close() error {
	if w.parent == nil || w.path == "" {
		return nil
	}
	w.parent.mu.Lock()
	defer w.parent.mu.Unlock()
	w.parent.files[w.path] = append([]byte(nil), w.buf.Bytes()...)

	return nil
}

func (w *limitedWriteCloser) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(w, r)
}

type memIO struct {
	limit int
	err   error
	mu    sync.Mutex
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
	m.mu.Lock()
	data, ok := m.files[name]
	m.mu.Unlock()
	if !ok {
		return nil, fs.ErrNotExist
	}

	return &internal.MockFile{Contents: bytes.NewReader(data)}, nil
}

func (m *memIO) Create(name string) (iceio.FileWriter, error) {
	return &limitedWriteCloser{limit: m.limit, err: m.err, parent: m, path: name}, nil
}

func (m *memIO) WriteFile(name string, content []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[name] = append([]byte(nil), content...)

	return nil
}

func (m *memIO) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, name)

	return nil
}

type blockingCreateIO struct {
	*memIO

	targetActive int
	reached      chan struct{}
	release      chan struct{}

	mu          sync.Mutex
	active      int
	maxActive   int
	once        sync.Once
	releaseOnce sync.Once
}

func newBlockingCreateIO(limit int, err error, targetActive int) *blockingCreateIO {
	return &blockingCreateIO{
		memIO:        newMemIO(limit, err),
		targetActive: targetActive,
		reached:      make(chan struct{}),
		release:      make(chan struct{}),
	}
}

func (b *blockingCreateIO) Create(name string) (iceio.FileWriter, error) {
	b.mu.Lock()
	b.active++
	if b.active > b.maxActive {
		b.maxActive = b.active
	}
	if b.active == b.targetActive {
		b.once.Do(func() { close(b.reached) })
	}
	b.mu.Unlock()

	<-b.release

	b.mu.Lock()
	b.active--
	b.mu.Unlock()

	return b.memIO.Create(name)
}

func (b *blockingCreateIO) MaxActive() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.maxActive
}

func (b *blockingCreateIO) Release() {
	b.releaseOnce.Do(func() { close(b.release) })
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

func writeTestManifestFile(
	t *testing.T,
	fs iceio.WriteFileIO,
	spec iceberg.PartitionSpec,
	schema *iceberg.Schema,
	snapshotID int64,
	index int,
) iceberg.ManifestFile {
	t.Helper()

	df := newTestDataFile(t, spec, "file://data-"+strconv.Itoa(index)+".parquet", nil)
	entries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, df),
	}

	path := "table-location/metadata/source-" + strconv.Itoa(index) + ".avro"
	var buf bytes.Buffer
	manifestFile, err := iceberg.WriteManifest(path, &buf, 2, spec, schema, snapshotID, entries)
	require.NoError(t, err, "write manifest")
	require.NoError(t, fs.WriteFile(path, buf.Bytes()))

	return manifestFile
}

func newTestPosDeleteFileForSpec(
	t *testing.T,
	spec iceberg.PartitionSpec,
	path string,
	partition map[int]any,
	referencedDataFile string,
) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentPosDeletes,
		path,
		iceberg.ParquetFile,
		partition,
		nil,
		nil,
		1,
		1,
	)
	require.NoError(t, err, "new position delete file builder")

	return builder.ReferencedDataFile(referencedDataFile).Build()
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
		SourceIDs: []int{1}, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
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

	updates, reqs, err := sp.commit(context.Background())
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
	updates1, reqs1, err := sp1.commit(context.Background())
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
	updates2, reqs2, err := sp2.commit(context.Background())
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
	updates1, reqs1, err := sp1.commit(context.Background())
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
	updates2, reqs2, err := sp2.commit(context.Background())
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
		FirstRowID      *int64 `json:"FirstRowIDValue"`
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
	updates1, reqs1, err := sp1.commit(context.Background())
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
	updates2, _, err := sp2.commit(context.Background())
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

	_, err := sp.manifests(context.Background())
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
	err = iceberg.WriteManifestList(2, &listBuf, snapshotID, nil, &seqNum, 0, []iceberg.ManifestFile{manifestFile})
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

	_, err = sp.existingManifests(&snap)
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

	manifests, err := sp.manifests(context.Background())
	require.NoError(t, err, "manifests should succeed")
	require.Len(t, manifests, 1, "should have one manifest")

	unclosed := trackIO.GetUnclosedWriters()
	require.Empty(t, unclosed, "all file writerFactory should be closed, but these are still open: %v", unclosed)
}

func TestAddedDeleteManifestsUseDeleteFileSpecID(t *testing.T) {
	oldSpec := iceberg.NewPartitionSpec()
	txn, wfs := createTestTransactionWithMemIO(t, oldSpec)

	newSpec := partitionedSpec()
	require.NoError(t, txn.meta.AddPartitionSpec(&newSpec, false))
	require.NoError(t, txn.meta.SetDefaultSpecID(-1))
	currentSpec, err := txn.meta.CurrentSpec()
	require.NoError(t, err)
	require.NotNil(t, currentSpec)
	require.Equal(t, 1, currentSpec.ID(), "test setup should evolve the current spec")

	sp := newFastAppendFilesProducer(OpDelete, txn, wfs, nil, nil)
	oldSpecDelete := newTestPosDeleteFileForSpec(
		t,
		oldSpec,
		"mem://default/table-location/delete/old-spec-pos-delete.parquet",
		nil,
		"mem://default/table-location/data/old-spec-data.parquet",
	)
	currentSpecDelete := newTestPosDeleteFileForSpec(
		t,
		*currentSpec,
		"mem://default/table-location/delete/current-spec-pos-delete.parquet",
		map[int]any{1000: int32(7)},
		"mem://default/table-location/data/current-spec-data.parquet",
	)
	sp.appendDeleteFile(oldSpecDelete)
	sp.appendDeleteFile(currentSpecDelete)

	manifests, err := sp.manifests(context.Background())
	require.NoError(t, err)

	deleteManifestSpecIDs := make([]int32, 0, len(manifests))
	for _, manifest := range manifests {
		if manifest.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}

		deleteManifestSpecIDs = append(deleteManifestSpecIDs, manifest.PartitionSpecID())
		for entry, err := range manifest.Entries(wfs, false) {
			require.NoError(t, err)
			require.Equal(t, manifest.PartitionSpecID(), entry.DataFile().SpecID())
		}
	}

	require.ElementsMatch(t, []int32{0, 1}, deleteManifestSpecIDs)
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

func TestManifestMergeMaxConcurrencyProperty(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	io := newMemIO(1<<20, nil)
	txn := createTestTransaction(t, io, spec)
	txn.meta.props[ManifestMergeMaxConcurrencyKey] = "3"

	sp := newMergeAppendFilesProducer(OpAppend, txn, io, nil, nil)
	merge := sp.producerImpl.(*mergeAppendFiles)
	require.Equal(t, 3, merge.mergeConcurrency)
}

func TestManifestMergeMaxConcurrencyDefaultsForNonPositiveValues(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	io := newMemIO(1<<20, nil)
	txn := createTestTransaction(t, io, spec)
	txn.meta.props[ManifestMergeMaxConcurrencyKey] = "0"

	sp := newMergeAppendFilesProducer(OpAppend, txn, io, nil, nil)
	merge := sp.producerImpl.(*mergeAppendFiles)
	require.Equal(t, runtime.GOMAXPROCS(0), merge.mergeConcurrency)
}

func TestManifestMergeGroupLimitsConcurrentBins(t *testing.T) {
	const mergeConcurrency = 2

	spec := iceberg.NewPartitionSpec()
	schema := simpleSchema()
	blockingIO := newBlockingCreateIO(1<<20, nil, mergeConcurrency)
	defer blockingIO.Release()
	txn := createTestTransaction(t, blockingIO, spec)

	sp := newFastAppendFilesProducer(OpAppend, txn, blockingIO, nil, nil)

	manifests := make([]iceberg.ManifestFile, 0, 8)
	for i := 0; i < cap(manifests); i++ {
		manifests = append(manifests, writeTestManifestFile(t, blockingIO, spec, schema, sp.snapshotID, i))
	}

	mgr := manifestMergeManager{
		targetSizeBytes:  int(manifests[0].Length() * 2),
		minCountToMerge:  1,
		mergeEnabled:     true,
		mergeConcurrency: mergeConcurrency,
		snap:             sp,
	}

	done := make(chan error, 1)
	go func() {
		_, err := mgr.mergeGroup(manifests[0], spec.ID(), manifests)
		done <- err
	}()

	select {
	case <-blockingIO.reached:
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for merge workers")
	}

	// Give any incorrectly-unbounded workers a chance to enter Create before
	// releasing the blocked merge outputs.
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, mergeConcurrency, blockingIO.MaxActive())

	blockingIO.Release()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for manifest merge")
	}

	require.LessOrEqual(t, blockingIO.MaxActive(), mergeConcurrency)
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
	err = iceberg.WriteManifestList(2, &listBuf, snapshotID, nil, &seqNum, 0, []iceberg.ManifestFile{manifestFile})
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

	_, err = sp.existingManifests(&snap)
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

func (e *errorOnDeletedEntries) existingManifests(_ *Snapshot) ([]iceberg.ManifestFile, error) {
	return nil, nil
}

func (e *errorOnDeletedEntries) deletedEntries(_ context.Context, _ *Snapshot) ([]iceberg.ManifestEntry, error) {
	if e.waitForWriter != nil {
		select {
		case <-e.waitForWriter:
		case <-e.cancelWaitForWriter:
			return nil, e.err
		}
	}

	return nil, e.err
}

func (e *errorOnDeletedEntries) validate(_ *conflictContext) error {
	return nil
}

func (e *errorOnDeletedEntries) needsValidation() bool { return true }

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
		_, manifestsErr = sp.manifests(context.Background())
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

// TestFastAppendInheritsZeroCountManifests verifies that fastAppendFiles.existingManifests
// includes manifests with added_files_count=0 and existing_files_count=0. This is the
// standard Iceberg v2 "inherited manifest" representation written by Athena and other
// external writers. The previous filter (HasAddedFiles || HasExistingFiles) silently
// dropped these manifests, causing data written by Athena to disappear after any
// iceberg-go fast-append.
func TestFastAppendInheritsZeroCountManifests(t *testing.T) {
	spec := iceberg.NewPartitionSpec()

	// Use the mem blob FS so that files written via Create() can be read back.
	txn, wfs := createTestTransactionWithMemIO(t, spec)

	// Snapshot 1: a snapshot whose manifest list contains two manifest entries
	// with added_files_count=0 and existing_files_count=0, simulating what
	// Athena (and other Iceberg v2 writers) produce.
	snap1ID := int64(1001)
	seqNum1 := int64(1)

	// These paths must be under the table location so the mem FS can locate them.
	athenaManifest1 := iceberg.NewManifestFile(2,
		"mem://default/table-location/metadata/athena-m0.avro", 512, 0, snap1ID).Build()
	athenaManifest2 := iceberg.NewManifestFile(2,
		"mem://default/table-location/metadata/athena-m1.avro", 256, 0, snap1ID).Build()

	// Sanity check: both manifests have zero counts, so the old filter would drop them.
	require.False(t, athenaManifest1.HasAddedFiles(), "test setup: manifest1 must have zero added count")
	require.False(t, athenaManifest1.HasExistingFiles(), "test setup: manifest1 must have zero existing count")
	require.False(t, athenaManifest2.HasAddedFiles(), "test setup: manifest2 must have zero added count")
	require.False(t, athenaManifest2.HasExistingFiles(), "test setup: manifest2 must have zero existing count")

	snap1ListPath := "mem://default/table-location/metadata/snap-1001.avro"

	var listBuf bytes.Buffer
	err := iceberg.WriteManifestList(2, &listBuf, snap1ID, nil, &seqNum1, 0,
		[]iceberg.ManifestFile{athenaManifest1, athenaManifest2})
	require.NoError(t, err, "write manifest list for snap1")
	require.NoError(t, wfs.WriteFile(snap1ListPath, listBuf.Bytes()))

	// Inject snap1 as the current snapshot in the transaction metadata.
	txn.meta.snapshotList = []Snapshot{
		{
			SnapshotID:     snap1ID,
			SequenceNumber: seqNum1,
			TimestampMs:    time.Now().UnixMilli(),
			ManifestList:   snap1ListPath,
			Summary:        &Summary{Operation: OpAppend},
		},
	}
	txn.meta.currentSnapshotID = &snap1ID

	// Snapshot 2: fast-append one new data file on top of snap1.
	sp := newFastAppendFilesProducer(OpAppend, txn, wfs, nil, nil)
	df := newTestDataFile(t, spec, "file://new-data.parquet", nil)
	sp.appendDataFile(df)

	updates, reqs, err := sp.commit(context.Background())
	require.NoError(t, err, "fast-append commit must succeed")
	require.NotEmpty(t, updates, "must produce updates")
	require.NotEmpty(t, reqs, "must produce requirements")

	addSnap, ok := updates[0].(*addSnapshotUpdate)
	require.True(t, ok, "first update must be AddSnapshot")

	// Read back the new manifest list and verify it contains all three manifests:
	// the two Athena-written zero-count manifests plus the new one.
	snap2Manifests := readManifestListFromPath(t, wfs, addSnap.Snapshot.ManifestList)

	// Collect the manifest paths in the new snapshot.
	paths := make([]string, 0, len(snap2Manifests))
	for _, m := range snap2Manifests {
		paths = append(paths, m.FilePath())
	}

	require.Contains(t, paths, athenaManifest1.FilePath(),
		"new snapshot must carry forward the first Athena manifest (zero added_files_count)")
	require.Contains(t, paths, athenaManifest2.FilePath(),
		"new snapshot must carry forward the second Athena manifest (zero existing_files_count)")
	require.Len(t, snap2Manifests, 3,
		"new snapshot must have exactly 3 manifests: 2 inherited + 1 newly written")

	// Verify the newly written manifest belongs to snap2.
	snap2ID := addSnap.Snapshot.SnapshotID
	var newManifestFound bool
	for _, m := range snap2Manifests {
		if m.SnapshotID() == snap2ID {
			newManifestFound = true

			break
		}
	}
	require.True(t, newManifestFound, "new snapshot must include a manifest written by snap2")
}

// TestParentDependentManifests_NilParent verifies that with no parent snapshot
// there is nothing to inherit or delete, so the parent-dependent manifest set
// is empty.
func TestParentDependentManifests_NilParent(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	io := newMemIO(1<<20, nil)
	txn := createTestTransaction(t, io, spec)
	sp := newFastAppendFilesProducer(OpAppend, txn, io, nil, nil)

	got, err := sp.parentDependentManifests(context.Background(), nil)
	require.NoError(t, err, "nil parent: parentDependentManifests must not error")
	require.Empty(t, got, "nil parent: nothing to inherit or delete")
}

// TestSummary_SnapshotByIDError verifies that summary computation fails when
// the parent snapshot cannot be found, instead of silently treating totals as
// starting from zero.
func TestSummary_SnapshotByIDError(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	io := newMemIO(1<<20, nil)
	txn := createTestTransaction(t, io, spec)
	sp := newFastAppendFilesProducer(OpAppend, txn, io, nil, nil)
	sp.parentSnapshotID = 9999 // no such snapshot in metadata
	sp.appendDataFile(newTestDataFile(t, spec, "file://data.parquet", nil))

	_, err := sp.summary(nil)
	require.Error(t, err, "unknown parent snapshot ID: summary must return error, not silent fallback")
	require.ErrorIs(t, err, ErrSnapshotNotFound,
		"production wraps ErrSnapshotNotFound; pin meaning via errors.Is")
	require.ErrorContains(t, err, "summary: lookup parent snapshot 9999",
		"error must identify the summary code path and parent snapshot ID")
}

// TestSummary_InheritsPreviousSnapshotTotals verifies that incremental totals
// are computed from the parent snapshot summary when the parent exists.
func TestSummary_InheritsPreviousSnapshotTotals(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	io := newMemIO(1<<20, nil)
	txn := createTestTransaction(t, io, spec)

	const parentID = int64(42)
	txn.meta.snapshotList = append(txn.meta.snapshotList, Snapshot{
		SnapshotID: parentID,
		Summary: &Summary{
			Operation: OpAppend,
			Properties: iceberg.Properties{
				totalDataFilesKey: "5",
			},
		},
	})

	sp := newFastAppendFilesProducer(OpAppend, txn, io, nil, nil)
	sp.parentSnapshotID = parentID
	sp.appendDataFile(newTestDataFile(t, spec, "file://data.parquet", nil))

	sum, err := sp.summary(nil)
	require.NoError(t, err)
	require.Equal(t, "6", sum.Properties[totalDataFilesKey],
		"total-data-files must inherit parent total (5) plus one added file")
	require.Equal(t, "1", sum.Properties[addedDataFilesKey])
}

// TestSummary_ParentSnapshotWithoutSummary verifies that summary computation
// does not panic when the parent snapshot exists but has no summary (V1 /
// omitempty). Totals fall back to the zero baseline like a new table.
func TestSummary_ParentSnapshotWithoutSummary(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	io := newMemIO(1<<20, nil)
	txn := createTestTransaction(t, io, spec)

	const parentID = int64(42)
	txn.meta.snapshotList = append(txn.meta.snapshotList, Snapshot{
		SnapshotID: parentID,
		Summary:    nil,
	})

	sp := newFastAppendFilesProducer(OpAppend, txn, io, nil, nil)
	sp.parentSnapshotID = parentID
	sp.appendDataFile(newTestDataFile(t, spec, "file://data.parquet", nil))

	sum, err := sp.summary(nil)
	require.NoError(t, err)
	require.Equal(t, "1", sum.Properties[totalDataFilesKey],
		"nil parent summary must use zero baseline, not panic")
	require.Equal(t, "1", sum.Properties[addedDataFilesKey])
}

// TestParentSnapshot_SnapshotByIDError verifies that when the parent snapshot
// cannot be found an error is returned instead of a silent nil parent — which
// would drop all inherited data from the new snapshot.
func TestParentSnapshot_SnapshotByIDError(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	io := newMemIO(1<<20, nil)
	txn := createTestTransaction(t, io, spec)
	sp := newFastAppendFilesProducer(OpAppend, txn, io, nil, nil)
	sp.parentSnapshotID = 9999 // no such snapshot in metadata

	got, err := sp.parentSnapshot()
	require.Error(t, err, "unknown parent snapshot ID: must return error, not silent fallback")
	require.ErrorIs(t, err, ErrSnapshotNotFound,
		"production wraps ErrSnapshotNotFound; pin meaning via errors.Is so a regression "+
			"that swallows the lookup error would fail this test")
	require.Nil(t, got, "error path must return nil snapshot")
}

// TestParentDependentManifests_IOError verifies that when the parent snapshot's
// manifest list file cannot be read an error is surfaced instead of a silent
// empty inherited set (which would drop the parent's data on an OCC retry).
func TestParentDependentManifests_IOError(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	io := newMemIO(1<<20, nil)
	txn := createTestTransaction(t, io, spec)
	sp := newFastAppendFilesProducer(OpAppend, txn, io, nil, nil)

	// A parent whose manifest list path does not exist in the IO: Manifests()
	// fails with fs.ErrNotExist when it tries to open it.
	ghostParent := &Snapshot{
		SnapshotID:   42,
		ManifestList: "mem://default/table-location/metadata/ghost-manifest-list.avro",
		Summary:      &Summary{Operation: OpAppend},
	}

	got, err := sp.parentDependentManifests(context.Background(), ghostParent)
	require.Error(t, err, "IO error reading parent manifests: must surface, not silent empty set")
	require.ErrorIs(t, err, fs.ErrNotExist,
		"production propagates the underlying IO error; pin meaning via errors.Is")
	require.Nil(t, got, "error path must return nil manifest slice")
}

func newTestDeletionVectorForRef(t *testing.T, spec iceberg.PartitionSpec, path, referencedDataFile string) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentPosDeletes, path, iceberg.PuffinFile,
		nil, nil, nil, 1, 1)
	require.NoError(t, err, "new deletion vector builder")

	return builder.ReferencedDataFile(referencedDataFile).Build()
}

// TestSummaryOnRetry_SkipsAlreadyRemovedDeleteFile is the regression for the
// retry summary undercount: when a peer already removed the delete file this
// producer also intends to remove, the fresh parent no longer counts it, so the
// retry summary must NOT subtract it again (which would drive total-delete-files
// below the real value).
func TestSummaryOnRetry_SkipsAlreadyRemovedDeleteFile(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	txn, wfs := createTestTransactionWithMemIO(t, spec)
	sp := newOverwriteFilesProducer(OpOverwrite, txn, wfs, nil, nil)

	posDel := newTestPosDeleteFileForSpec(t, spec,
		"mem://default/table-location/data/pos-del.parquet", nil,
		"mem://default/table-location/data/data.parquet")
	sp.removeDeleteFile(posDel)

	// Fresh parent: a peer already dropped the pos-delete. Empty manifest list,
	// summary counts zero delete files.
	parentManifestList := "mem://default/table-location/metadata/fresh-parent-skip.avro"
	out, err := wfs.Create(parentManifestList)
	require.NoError(t, err)
	require.NoError(t, iceberg.WriteManifestList(2, out, 77, nil, ptr(int64(0)), 0, nil))
	require.NoError(t, out.Close())

	freshParent := &Snapshot{
		SnapshotID:   77,
		ManifestList: parentManifestList,
		Summary: &Summary{Operation: OpAppend, Properties: iceberg.Properties{
			"total-delete-files":     "0",
			"total-position-deletes": "0",
			"total-equality-deletes": "0",
			"total-data-files":       "1",
			"total-records":          "1",
			"total-files-size":       "1",
		}},
	}

	present, err := sp.checkRemovedFiles(freshParent)
	require.NoError(t, err)
	require.NotContains(t, present.deleteFiles, posDel.FilePath(),
		"a pos-delete absent from the fresh parent must not be marked present")

	s, err := sp.summaryOnRetry(nil, freshParent, present)
	require.NoError(t, err)
	require.Equal(t, "0", s.Properties["total-delete-files"],
		"must not subtract a delete file the peer already removed")
	require.Equal(t, "0", s.Properties["total-position-deletes"],
		"must not subtract position deletes for an already-removed file")
}

// TestSummaryOnRetry_SubtractsPresentDeleteFile is the companion guard: when the
// delete file the producer removes is still present on the fresh parent, the
// retry summary MUST subtract it (the gate does not silently disable removals).
func TestSummaryOnRetry_SubtractsPresentDeleteFile(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	txn, wfs := createTestTransactionWithMemIO(t, spec)
	sp := newOverwriteFilesProducer(OpOverwrite, txn, wfs, nil, nil)

	posDel := newTestPosDeleteFileForSpec(t, spec,
		"mem://default/table-location/data/pos-del.parquet", nil,
		"mem://default/table-location/data/data.parquet")
	sp.removeDeleteFile(posDel)

	freshParent := &Snapshot{
		SnapshotID: 88,
		Summary: &Summary{Operation: OpAppend, Properties: iceberg.Properties{
			"total-delete-files":     "1",
			"total-position-deletes": "1",
			"total-equality-deletes": "0",
			"total-data-files":       "1",
			"total-records":          "1",
			"total-files-size":       "1",
		}},
	}

	// The pos-delete is still live on the fresh parent.
	present := &removedFilePresence{
		deleteFiles: map[string]struct{}{posDel.FilePath(): {}},
		dvRefs:      map[string]struct{}{},
	}

	s, err := sp.summaryOnRetry(nil, freshParent, present)
	require.NoError(t, err)
	require.Equal(t, "0", s.Properties["total-delete-files"],
		"a delete file still present on the fresh parent must be subtracted (1→0)")
}

// TestRemovedFilePresenceCounts pins the gate's matching rules: pos/eq deletes
// match by path, deletion vectors by referenced data file.
func TestRemovedFilePresenceCounts(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	present := &removedFilePresence{
		deleteFiles: map[string]struct{}{"file://present-del.parquet": {}},
		dvRefs:      map[string]struct{}{"file://present-ref.parquet": {}},
	}

	presentDel := newTestPosDeleteFileForSpec(t, spec, "file://present-del.parquet", nil, "file://x.parquet")
	absentDel := newTestPosDeleteFileForSpec(t, spec, "file://absent-del.parquet", nil, "file://x.parquet")
	require.True(t, present.counts(presentDel), "pos-delete present by path")
	require.False(t, present.counts(absentDel), "pos-delete absent by path")

	presentDV := newTestDeletionVectorForRef(t, spec, "file://dv.puffin", "file://present-ref.parquet")
	absentDV := newTestDeletionVectorForRef(t, spec, "file://dv.puffin", "file://absent-ref.parquet")
	require.True(t, present.counts(presentDV), "DV present by referenced data file")
	require.False(t, present.counts(absentDV), "DV absent by referenced data file")
}

// TestCheckRemovedFiles_AbortsWhenRewriteTargetMissing pins the terminal abort:
// when a data file this producer intends to rewrite is no longer on the fresh
// parent (a peer already removed it), checkRemovedFiles must fail with
// ErrCommitDiverged rather than resurrecting our replacement beside the peer's
// result.
func TestCheckRemovedFiles_AbortsWhenRewriteTargetMissing(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	txn, wfs := createTestTransactionWithMemIO(t, spec)
	sp := newOverwriteFilesProducer(OpOverwrite, txn, wfs, nil, nil)

	target := newTestDataFile(t, spec, "mem://default/table-location/data/gone.parquet", nil)
	sp.deleteDataFile(target)

	// Fresh parent no longer contains the data file (empty manifest list).
	parentManifestList := "mem://default/table-location/metadata/fresh-parent-diverged.avro"
	out, err := wfs.Create(parentManifestList)
	require.NoError(t, err)
	require.NoError(t, iceberg.WriteManifestList(2, out, 99, nil, ptr(int64(0)), 0, nil))
	require.NoError(t, out.Close())

	freshParent := &Snapshot{SnapshotID: 99, ManifestList: parentManifestList}

	present, err := sp.checkRemovedFiles(freshParent)
	require.ErrorIs(t, err, ErrCommitDiverged)
	require.ErrorContains(t, err, target.FilePath())
	require.Nil(t, present, "no presence set is returned on a diverged abort")
}

// TestCheckRemovedFiles_ShortCircuitsWithNothingToRemove guards the append
// fast-path: a producer that removes nothing must not scan the parent's
// manifests at all, so a nil parent is fine and an empty presence set is
// returned.
func TestCheckRemovedFiles_ShortCircuitsWithNothingToRemove(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	txn, wfs := createTestTransactionWithMemIO(t, spec)
	sp := newFastAppendFilesProducer(OpAppend, txn, wfs, nil, nil)

	present, err := sp.checkRemovedFiles(nil)
	require.NoError(t, err)
	require.Empty(t, present.deleteFiles)
	require.Empty(t, present.dvRefs)
}

// TestDeleteFileRemoved_DVMatchesByRefAndPath pins the DV expunge identity:
// a deletion vector matches a captured removal only when both its referenced
// data file and its path match. Ref-only matching would drop a peer's
// replacement DV on an OCC retry; path-only matching would drop Puffin
// siblings sharing the file.
func TestDeleteFileRemoved_DVMatchesByRefAndPath(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	txn, wfs := createTestTransactionWithMemIO(t, spec)
	sp := newOverwriteFilesProducer(OpOverwrite, txn, wfs, nil, nil)

	dataPath := "mem://default/table-location/data/d.parquet"
	dv := newTestDeletionVectorForRef(t, spec, "mem://default/table-location/data/dv-old.puffin", dataPath)
	sp.removeDeletionVector(dv)

	require.True(t, sp.deleteFileRemoved(dv), "exact (ref, path) pair matches")

	replacement := newTestDeletionVectorForRef(t, spec, "mem://default/table-location/data/dv-new.puffin", dataPath)
	require.False(t, sp.deleteFileRemoved(replacement),
		"a peer's replacement DV (same ref, new path) must survive")

	sibling := newTestDeletionVectorForRef(t, spec, dv.FilePath(), "mem://default/table-location/data/other.parquet")
	require.False(t, sp.deleteFileRemoved(sibling),
		"a Puffin sibling (same path, other ref) must survive")
}

// TestCheckRemovedFiles_SupersededDVReadsAsAbsent covers the concurrent DV
// replacement race: a peer removed the DV this producer intends to expunge and
// added a new one for the same data file. The replacement must not read as
// "our DV is still present" — otherwise the retry summary subtracts a removal
// the fresh parent no longer counts.
func TestCheckRemovedFiles_SupersededDVReadsAsAbsent(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	txn, wfs := createTestTransactionWithMemIO(t, spec)
	sp := newOverwriteFilesProducer(OpOverwrite, txn, wfs, nil, nil)

	dataPath := "mem://default/table-location/data/d.parquet"
	staleDV := newTestDeletionVectorForRef(t, spec, "mem://default/table-location/data/dv-old.puffin", dataPath)
	sp.removeDeletionVector(staleDV)

	peerDV := newTestDeletionVectorForRef(t, spec, "mem://default/table-location/data/dv-new.puffin", dataPath)
	parent := writeParentSnapshotWithDeletesManifest(t, wfs, spec, 91, "superseded", peerDV)

	present, err := sp.checkRemovedFiles(parent)
	require.NoError(t, err)
	require.Empty(t, present.dvRefs,
		"a superseded DV (same ref, different path) must not read as present")

	s, err := sp.summaryOnRetry(nil, parent, present)
	require.NoError(t, err)
	require.Equal(t, "1", s.Properties["total-delete-files"],
		"the peer's replacement DV stays counted on the retry summary")
	require.Equal(t, "1", s.Properties["total-position-deletes"],
		"the peer's position deletes stay counted on the retry summary")
}

// TestCheckRemovedFiles_ExactDVPresentAndSubtracted is the companion guard:
// when the exact DV this producer removes is still live on the fresh parent,
// it must be marked present and the retry summary must subtract it.
func TestCheckRemovedFiles_ExactDVPresentAndSubtracted(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	txn, wfs := createTestTransactionWithMemIO(t, spec)
	sp := newOverwriteFilesProducer(OpOverwrite, txn, wfs, nil, nil)

	dataPath := "mem://default/table-location/data/d.parquet"
	staleDV := newTestDeletionVectorForRef(t, spec, "mem://default/table-location/data/dv-old.puffin", dataPath)
	sp.removeDeletionVector(staleDV)

	parent := writeParentSnapshotWithDeletesManifest(t, wfs, spec, 92, "exact", staleDV)

	present, err := sp.checkRemovedFiles(parent)
	require.NoError(t, err)
	require.Contains(t, present.dvRefs, dataPath, "the exact DV is still live on parent")

	s, err := sp.summaryOnRetry(nil, parent, present)
	require.NoError(t, err)
	require.Equal(t, "0", s.Properties["total-delete-files"],
		"a DV still present on the fresh parent must be subtracted (1→0)")
}

// TestExistingManifests_SupersededDVSurvivesRetry pins the manifest-filter half
// of the pair-matching rule: on an OCC retry the fresh parent may carry a
// peer's replacement DV for the data file whose DV this producer removes. The
// replacement must be inherited untouched and not tombstoned — ref-only
// matching would expunge it and resurrect the rows the peer deleted.
func TestExistingManifests_SupersededDVSurvivesRetry(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	txn, wfs := createTestTransactionWithMemIO(t, spec)
	sp := newOverwriteFilesProducer(OpOverwrite, txn, wfs, nil, nil)

	dataPath := "mem://default/table-location/data/d.parquet"
	staleDV := newTestDeletionVectorForRef(t, spec, "mem://default/table-location/data/dv-old.puffin", dataPath)
	sp.removeDeletionVector(staleDV)

	peerDV := newTestDeletionVectorForRef(t, spec, "mem://default/table-location/data/dv-new.puffin", dataPath)
	parent := writeParentSnapshotWithDeletesManifest(t, wfs, spec, 93, "survives", peerDV)

	parentManifests, err := parent.Manifests(wfs)
	require.NoError(t, err)
	require.Len(t, parentManifests, 1)

	got, err := sp.existingManifests(parent)
	require.NoError(t, err)
	require.Len(t, got, 1, "the peer's deletes manifest must be inherited")
	require.Equal(t, parentManifests[0].FilePath(), got[0].FilePath(),
		"nothing matched, so the manifest is passed through untouched")

	tombstones, err := sp.deletedEntries(context.Background(), parent)
	require.NoError(t, err)
	require.Empty(t, tombstones, "no DELETE tombstone for the peer's replacement DV")
}

// TestExistingManifests_ExactDVStillExpunged is the companion guard: the exact
// captured DV is still filtered from the inherited manifests and tombstoned.
func TestExistingManifests_ExactDVStillExpunged(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	txn, wfs := createTestTransactionWithMemIO(t, spec)
	sp := newOverwriteFilesProducer(OpOverwrite, txn, wfs, nil, nil)

	dataPath := "mem://default/table-location/data/d.parquet"
	staleDV := newTestDeletionVectorForRef(t, spec, "mem://default/table-location/data/dv-old.puffin", dataPath)
	sp.removeDeletionVector(staleDV)

	parent := writeParentSnapshotWithDeletesManifest(t, wfs, spec, 94, "expunged", staleDV)

	got, err := sp.existingManifests(parent)
	require.NoError(t, err)
	require.Empty(t, got, "a manifest whose only entry is the removed DV is dropped")

	tombstones, err := sp.deletedEntries(context.Background(), parent)
	require.NoError(t, err)
	require.Len(t, tombstones, 1)
	require.Equal(t, staleDV.FilePath(), tombstones[0].DataFile().FilePath())
}

// writeParentSnapshotWithDeletesManifest writes a v3 deletes manifest holding
// the given delete files plus its manifest list into fs and returns a parent
// snapshot pointing at it. v3 because referenced_data_file only serializes in
// the v3 data_file schema. The summary counts one delete file so retry-summary
// assertions can observe whether a removal was subtracted.
func writeParentSnapshotWithDeletesManifest(t *testing.T, fs iceio.WriteFileIO, spec iceberg.PartitionSpec, snapshotID int64, tag string, deleteFiles ...iceberg.DataFile) *Snapshot {
	t.Helper()

	manifestPath := "mem://default/table-location/metadata/deletes-" + tag + ".avro"
	var buf bytes.Buffer
	wr, err := iceberg.NewManifestWriter(3, &buf, spec, simpleSchema(), snapshotID,
		iceberg.WithManifestWriterContent(iceberg.ManifestContentDeletes))
	require.NoError(t, err)
	for _, df := range deleteFiles {
		require.NoError(t, wr.Add(iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapshotID, nil, nil, df)))
	}
	require.NoError(t, wr.Close())
	mf, err := wr.ToManifestFile(manifestPath, int64(buf.Len()),
		iceberg.WithManifestFileContent(iceberg.ManifestContentDeletes))
	require.NoError(t, err)

	out, err := fs.Create(manifestPath)
	require.NoError(t, err)
	_, err = out.Write(buf.Bytes())
	require.NoError(t, err)
	require.NoError(t, out.Close())

	listPath := "mem://default/table-location/metadata/snap-" + tag + ".avro"
	lout, err := fs.Create(listPath)
	require.NoError(t, err)
	seq := int64(1)
	require.NoError(t, iceberg.WriteManifestList(3, lout, snapshotID, nil, &seq, 0, []iceberg.ManifestFile{mf}))
	require.NoError(t, lout.Close())

	return &Snapshot{
		SnapshotID:     snapshotID,
		SequenceNumber: seq,
		ManifestList:   listPath,
		Summary: &Summary{Operation: OpAppend, Properties: iceberg.Properties{
			"total-delete-files":     "1",
			"total-position-deletes": "1",
			"total-equality-deletes": "0",
			"total-data-files":       "1",
			"total-records":          "1",
			"total-files-size":       "1",
		}},
	}
}

func TestAddDataFilesV3RejectsWithoutFirstRowID(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	txn, _ := createTestTransactionWithMemIO(t, spec)
	txn.meta.formatVersion = 3

	df := newTestDataFile(t, spec, "file://data.parquet", nil)

	err := txn.AddDataFiles(context.Background(), []iceberg.DataFile{df}, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "missing first_row_id")
	require.ErrorContains(t, err, "required for v3 tables")
}

func TestAddDataFilesV3SucceedsWithFirstRowID(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	txn, _ := createTestTransactionWithMemIO(t, spec)
	txn.meta.formatVersion = 3

	builder, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentData, "file://data.parquet",
		iceberg.ParquetFile, nil, nil, nil, 5, 100,
	)
	require.NoError(t, err)
	df := builder.FirstRowID(0).Build()

	err = txn.AddDataFiles(context.Background(), []iceberg.DataFile{df}, nil)
	require.NoError(t, err)

	meta, err := txn.meta.Build()
	require.NoError(t, err)
	require.Equal(t, int64(5), meta.NextRowID(), "next-row-id should advance by record count")
}

func TestAddDataFilesV2SucceedsWithoutFirstRowID(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	txn, _ := createTestTransactionWithMemIO(t, spec)
	txn.meta.formatVersion = 2

	df := newTestDataFile(t, spec, "file://data.parquet", nil)

	err := txn.AddDataFiles(context.Background(), []iceberg.DataFile{df}, nil)
	require.NoError(t, err)
}
