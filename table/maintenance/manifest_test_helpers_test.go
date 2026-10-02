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

package maintenance

import (
	"bytes"
	"fmt"
	stdfs "io/fs"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

// memFile serves manifest and manifest-list bytes staged in trackingIO.
// A *bytes.Reader already satisfies Read/Seek/ReadAt; memFile adds the
// Stat/Close that iceio.File additionally requires. Stat returns a nil
// FileInfo because the manifest readers only ever read the stream.
type memFile struct {
	*bytes.Reader
}

func (memFile) Stat() (stdfs.FileInfo, error) { return nil, nil }
func (memFile) Close() error                  { return nil }

// trackingIO is a minimal in-memory IO for the orphan-cleanup reference-set
// tests. WriteFile stages file bytes in a map and Open serves them back, so a
// manifest or manifest list can be written once and read back by
// getReferencedFiles. It implements only the Open/Remove surface iceio.IO
// requires, plus WriteFile for the manifest helpers.
type trackingIO struct {
	files map[string][]byte
}

func newTrackingIO() *trackingIO {
	return &trackingIO{files: make(map[string][]byte)}
}

func (t *trackingIO) Open(name string) (iceio.File, error) {
	data, ok := t.files[name]
	if !ok {
		return nil, stdfs.ErrNotExist
	}

	return memFile{bytes.NewReader(data)}, nil
}

func (t *trackingIO) WriteFile(name string, content []byte) error {
	t.files[name] = append([]byte(nil), content...)

	return nil
}

func (t *trackingIO) Remove(name string) error {
	delete(t.files, name)

	return nil
}

// trackingCallsIO wraps trackingIO to count Open and Remove calls per path.
type trackingCallsIO struct {
	*trackingIO
	mu          sync.Mutex
	openCount   map[string]int
	removeCount map[string]int
}

func newTrackingCallsIO() *trackingCallsIO {
	return &trackingCallsIO{
		trackingIO:  newTrackingIO(),
		openCount:   make(map[string]int),
		removeCount: make(map[string]int),
	}
}

func (c *trackingCallsIO) Open(name string) (iceio.File, error) {
	c.mu.Lock()
	c.openCount[name]++
	c.mu.Unlock()

	return c.trackingIO.Open(name)
}

func (c *trackingCallsIO) Remove(name string) error {
	c.mu.Lock()
	c.removeCount[name]++
	c.mu.Unlock()

	return c.trackingIO.Remove(name)
}

// writeManifest writes a v2 data manifest with a single ADDED
// entry pointing at dataPath into tio.files at manifestPath, and returns a
// ManifestFile descriptor with seqNum pre-assigned so the same descriptor
// can be referenced from multiple manifest lists.
func writeManifest(t testing.TB, tio *trackingIO, snapshotID, seqNum int64, manifestPath, dataPath string) iceberg.ManifestFile {
	t.Helper()

	dataSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	spec := iceberg.NewPartitionSpec()

	df, err := iceberg.NewDataFileBuilder(
		spec, iceberg.EntryContentData, dataPath, iceberg.ParquetFile,
		nil, nil, nil, 1, 1024,
	)
	require.NoError(t, err)

	entry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &snapshotID, df.Build()).
		SequenceNum(seqNum).
		Build()

	var buf bytes.Buffer
	_, err = iceberg.WriteManifest(manifestPath, &buf, 2, spec, dataSchema, snapshotID, []iceberg.ManifestEntry{entry})
	require.NoError(t, err)
	require.NoError(t, tio.WriteFile(manifestPath, buf.Bytes()))

	return iceberg.NewManifestFile(2, manifestPath, int64(buf.Len()), 0, snapshotID).
		SequenceNum(seqNum, seqNum).
		AddedFiles(1).
		AddedRows(1).
		Build()
}

// writeManifestList writes a v2 manifest list referencing the
// given manifests into tio.files at listPath.
func writeManifestList(t testing.TB, tio *trackingIO, snapshotID int64, listPath string, manifests []iceberg.ManifestFile) {
	t.Helper()

	var buf bytes.Buffer
	seqNum := int64(1)
	require.NoError(t, iceberg.WriteManifestList(2, &buf, snapshotID, nil, &seqNum, 0, manifests))
	require.NoError(t, tio.WriteFile(listPath, buf.Bytes()))
}

// metaJSONOpts configures the metadata document built by buildMetaJSON.
type metaJSONOpts struct {
	snapshots           string
	statistics          string
	partitionStatistics string
}

// buildMetaJSON returns the minimal v2 metadata document that ParseMetadataString will accept
func buildMetaJSON(o metaJSONOpts) string {
	return fmt.Sprintf(`{
	  "format-version": 2,
	  "table-uuid": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
	  "location": "s3://bucket/table",
	  "last-sequence-number": 0,
	  "last-updated-ms": 1000,
	  "last-column-id": 1,
	  "current-schema-id": 0,
	  "schemas": [{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"x","required":true,"type":"long"}]}],
	  "default-spec-id": 0,
	  "partition-specs": [{"spec-id":0,"fields":[]}],
	  "last-partition-id": 0,
	  "default-sort-order-id": 0,
	  "sort-orders": [{"order-id":0,"fields":[]}],
	  "snapshots": [%s],
	  "statistics": [%s],
	  "partition-statistics": [%s]
	}`, o.snapshots, o.statistics, o.partitionStatistics)
}

// manifestTrackingIO wraps an IO to record the peak number of concurrently
// open files, so tests can assert that manifest-list reads run concurrently
// yet stay within the configured worker limit.
type manifestTrackingIO struct {
	iceio.IO
	mu      sync.Mutex
	open    int
	maxOpen int
	delay   time.Duration
}

func (fs *manifestTrackingIO) Open(name string) (iceio.File, error) {
	f, err := fs.IO.Open(name)
	if err != nil {
		return nil, err
	}

	fs.mu.Lock()
	fs.open++
	if fs.open > fs.maxOpen {
		fs.maxOpen = fs.open
	}
	fs.mu.Unlock()
	time.Sleep(fs.delay)

	return &manifestTrackingFile{File: f, onClose: func() {
		fs.mu.Lock()
		fs.open--
		fs.mu.Unlock()
	}}, nil
}

type manifestTrackingFile struct {
	iceio.File
	onClose func()
	once    sync.Once
}

func (f *manifestTrackingFile) Close() error {
	f.once.Do(f.onClose)

	return f.File.Close()
}

// benchmarkDelayIO wraps an IO and adds a fixed per-Open delay, modelling the
// round trip to a remote object store in the manifest-list benchmark.
type benchmarkDelayIO struct {
	iceio.IO
	delay time.Duration
}

func (fs *benchmarkDelayIO) Open(name string) (iceio.File, error) {
	f, err := fs.IO.Open(name)
	if err != nil {
		return nil, err
	}
	time.Sleep(fs.delay)

	return f, nil
}
