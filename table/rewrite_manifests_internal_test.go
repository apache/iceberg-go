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
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

// writeManifestWithEntries writes a real data manifest holding n added entries
// to dir and returns its on-disk path and length.
func writeManifestWithEntries(t *testing.T, fs iceio.WriteFileIO, dir string, n int) (string, int64) {
	t.Helper()

	sc := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)

	var entries []iceberg.ManifestEntry
	snapID := int64(1)
	for i := range n {
		df, err := iceberg.NewDataFileBuilder(
			*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
			dir+"/data/file-"+string(rune('a'+i))+".parquet", iceberg.ParquetFile,
			nil, nil, nil, 1, 100,
		)
		require.NoError(t, err)
		entries = append(entries,
			iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, nil, nil, df.Build()))
	}

	path := dir + "/metadata/m-v1.avro"
	out, err := fs.Create(path)
	require.NoError(t, err)
	mf, err := iceberg.WriteManifest(path, out, 2, *iceberg.UnpartitionedSpec, sc, snapID, entries)
	require.NoError(t, err)
	require.NoError(t, out.Close())

	return path, mf.Length()
}

// TestManifestActiveFilesCountsEntriesWhenHeaderCountsAbsent covers the V1
// fallback: when a manifest's header counts are absent (reported as -1, as Java
// V1 manifests are), manifestActiveFiles reads the live entries and counts them
// directly instead of silently skipping the guard.
func TestManifestActiveFilesCountsEntriesWhenHeaderCountsAbsent(t *testing.T) {
	dir := t.TempDir()
	fs := iceio.LocalFS{}

	const n = 3
	path, length := writeManifestWithEntries(t, fs, dir, n)

	// A manifest reporting unknown (-1) added/existing counts, as a manifest
	// written without the count fields would (V1's toFile maps absent counts to
	// -1), but pointing at the real file so the fallback has entries to read.
	unknown := iceberg.NewManifestFile(2, path, length, 0, 1).
		AddedFiles(-1).ExistingFiles(-1).DeletedFiles(-1).Build()
	require.EqualValues(t, -1, unknown.AddedDataFiles(), "precondition: counts must read as unknown")

	got, err := manifestActiveFiles(fs, []iceberg.ManifestFile{unknown})
	require.NoError(t, err)
	require.EqualValues(t, n, got, "fallback must count the live entries, not skip the manifest")

	// And the header-count path still wins when the counts are present.
	known := iceberg.NewManifestFile(2, path, length, 0, 1).
		AddedFiles(n).ExistingFiles(0).DeletedFiles(0).Build()
	got, err = manifestActiveFiles(fs, []iceberg.ManifestFile{known})
	require.NoError(t, err)
	require.EqualValues(t, n, got)
}

// memKeys snapshots the set of paths a memIO currently holds.
func memKeys(m *memIO) map[string]struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]struct{}, len(m.files))
	for k := range m.files {
		out[k] = struct{}{}
	}

	return out
}

// TestRewriteManifestsCleansOrphanOnValidationError asserts that when the
// file-count guard rejects a merge, the merged manifest already written to
// storage is deleted rather than leaked as an orphan.
func TestRewriteManifestsCleansOrphanOnValidationError(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	schema := simpleSchema()
	mem := newMemIO(1<<20, errLimitedWrite) // large limit: every write succeeds
	txn := createTestTransaction(t, mem, spec)

	prod := newRewriteManifestsProducer(txn, mem, iceberg.Properties{}, rewriteManifestsCfg{targetSizeBytes: 8 << 20})
	r := prod.producerImpl.(*rewriteManifests)

	// Two real manifests, each holding one entry. A two-manifest bin forces a
	// real merge; a one-manifest bin would pass through untouched.
	inputs := make([]iceberg.ManifestFile, 2)
	for i := range inputs {
		df := newTestDataFile(t, spec, "file://data-"+string(rune('a'+i))+".parquet", nil)
		entries := []iceberg.ManifestEntry{
			iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &prod.snapshotID, nil, nil, df),
		}
		path := "table-location/metadata/input-" + string(rune('a'+i)) + ".avro"
		var buf bytes.Buffer
		_, err := iceberg.WriteManifest(path, &buf, 2, spec, schema, prod.snapshotID, entries)
		require.NoError(t, err)
		require.NoError(t, mem.WriteFile(path, buf.Bytes()))

		// A header count that lies: the merge re-counts the one real entry and
		// disagrees, tripping the file-count guard after the merged file is
		// already on disk.
		inputs[i] = iceberg.NewManifestFile(2, path, int64(buf.Len()), int32(spec.ID()), prod.snapshotID).
			AddedFiles(99).Content(iceberg.ManifestContentData).Build()
	}

	before := memKeys(mem)
	_, err := r.processManifests(inputs)
	require.Error(t, err, "lying header counts must trip the file-count guard")

	for path := range memKeys(mem) {
		_, preexisting := before[path]
		require.Truef(t, preexisting, "merged manifest %s written before validation must be cleaned up", path)
	}
}

// failSecondCreateIO fails every Create after the first, so a concurrent merge
// has one bin write and persist its manifest while a sibling bin fails.
type failSecondCreateIO struct {
	*memIO
	mu    sync.Mutex
	count int
}

func (f *failSecondCreateIO) Create(name string) (iceio.FileWriter, error) {
	f.mu.Lock()
	first := f.count == 0
	f.count++
	f.mu.Unlock()
	if first {
		return f.memIO.Create(name)
	}

	return &limitedWriteCloser{limit: 0, err: errLimitedWrite}, nil
}

// TestRewriteManifestsCleansOrphansOnMergeFailure covers zeroshade's mid-merge
// leak: mergeManifests writes bins concurrently, so when one bin fails the ones
// that already wrote their .avro files must be deleted, not orphaned.
func TestRewriteManifestsCleansOrphansOnMergeFailure(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	schema := simpleSchema()
	mem := &failSecondCreateIO{memIO: newMemIO(1<<20, errLimitedWrite)}
	txn := createTestTransaction(t, mem, spec)

	prod := newRewriteManifestsProducer(txn, mem, iceberg.Properties{}, rewriteManifestsCfg{})
	r := prod.producerImpl.(*rewriteManifests)

	// Four one-entry manifests. A small target size packs them into two bins of
	// two, each a real merge, so the merge issues two concurrent Create calls.
	inputs := make([]iceberg.ManifestFile, 4)
	var maxLen int64
	for i := range inputs {
		df := newTestDataFile(t, spec, "file://data-"+string(rune('a'+i))+".parquet", nil)
		entries := []iceberg.ManifestEntry{
			iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &prod.snapshotID, nil, nil, df),
		}
		path := "table-location/metadata/input-" + string(rune('a'+i)) + ".avro"
		var buf bytes.Buffer
		_, err := iceberg.WriteManifest(path, &buf, 2, spec, schema, prod.snapshotID, entries)
		require.NoError(t, err)
		require.NoError(t, mem.WriteFile(path, buf.Bytes()))

		inputs[i] = iceberg.NewManifestFile(2, path, int64(buf.Len()), int32(spec.ID()), prod.snapshotID).
			AddedFiles(1).Content(iceberg.ManifestContentData).Build()
		maxLen = max(maxLen, int64(buf.Len()))
	}
	r.cfg.targetSizeBytes = 2 * maxLen

	before := memKeys(mem.memIO)
	_, err := r.processManifests(inputs)
	require.ErrorIs(t, err, errLimitedWrite, "a failing bin must surface the write error")

	for path := range memKeys(mem.memIO) {
		_, preexisting := before[path]
		require.Truef(t, preexisting, "merged manifest %s from the bin that succeeded must be cleaned up", path)
	}
}
