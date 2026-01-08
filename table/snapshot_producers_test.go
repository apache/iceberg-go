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
	"errors"
	"io"
	"io/fs"
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
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		path,
		iceberg.ParquetFile,
		partition,
		nil,
		nil,
		1,
		1,
	)
	require.NoError(t, err, "new data file builder")

	return builder.Build()
}

func TestSnapshotProducerManifestsClosesWriterOnError(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceID: 1, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
	})

	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "table-location", nil)
	require.NoError(t, err, "new metadata")
	spec = meta.PartitionSpec()
	schema = meta.CurrentSchema()
	fieldID := 0
	for field := range spec.Fields() {
		fieldID = field.FieldID

		break
	}
	require.NotZero(t, fieldID, "partition field id")

	mem := newMemIO(manifestHeaderSize(t, 2, spec, schema), errLimitedWrite)
	tbl := New(Identifier{"db", "tbl"}, meta, "metadata.json", func(context.Context) (iceio.IO, error) {
		return mem, nil
	}, nil)
	txn := tbl.NewTransaction()

	sp := newFastAppendFilesProducer(OpAppend, txn, mem, nil, nil)
	validPartition := map[int]any{fieldID: int32(1)}
	sp.appendDataFile(newTestDataFile(t, spec, "file://data-1.parquet", validPartition))
	sp.appendDataFile(newTestDataFile(t, spec, "file://data-2.parquet", nil))

	_, err = sp.manifests()
	require.ErrorIs(t, err, errLimitedWrite)
}

func TestManifestMergeManagerClosesWriterOnError(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	})
	spec := iceberg.NewPartitionSpec()

	mem := newMemIO(manifestHeaderSize(t, 2, spec, schema), errLimitedWrite)
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "table-location", nil)
	require.NoError(t, err, "new metadata")

	tbl := New(Identifier{"db", "tbl"}, meta, "metadata.json", func(context.Context) (iceio.IO, error) {
		return mem, nil
	}, nil)
	txn := tbl.NewTransaction()

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
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true,
	})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceID: 1, FieldID: 1000, Name: "id", Transform: iceberg.IdentityTransform{},
	})

	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "table-location", nil)
	require.NoError(t, err, "new metadata")

	spec = meta.PartitionSpec()
	schema = meta.CurrentSchema()
	fieldID := 0
	for field := range spec.Fields() {
		fieldID = field.FieldID

		break
	}
	require.NotZero(t, fieldID, "partition field id")

	snapshotID := int64(100)
	seqNum := int64(-1)
	validSeq := int64(42)
	manifestPath := "table-location/metadata/manifest-1.avro"
	manifestListPath := "table-location/metadata/snap-1.avro"

	validPartition := map[int]any{fieldID: int32(1)}
	validFile := newTestDataFile(t, spec, "file://valid.parquet", validPartition)
	sizeEntries := []iceberg.ManifestEntry{
		iceberg.NewManifestEntry(iceberg.EntryStatusEXISTING, &snapshotID, &validSeq, nil, validFile),
	}
	headerLen := manifestHeaderSize(t, 2, spec, schema)
	manifestLen := manifestSize(t, 2, spec, schema, snapshotID, sizeEntries)
	require.Greater(t, manifestLen, headerLen, "manifest size")

	mem := newMemIO(manifestLen-1, errLimitedWrite)
	tbl := New(Identifier{"db", "tbl"}, meta, "metadata.json", func(context.Context) (iceio.IO, error) {
		return mem, nil
	}, nil)
	txn := tbl.NewTransaction()

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

	_, err = sp.existingManifests()
	require.ErrorIs(t, err, errLimitedWrite)
}
