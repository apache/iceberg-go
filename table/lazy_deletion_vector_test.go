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
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLazyDeletionVectorLoaderLoadsSharedPuffinGroupsOnDemand(t *testing.T) {
	files := writeSharedDVPuffinFixture(t, 2)
	refs := make([]string, len(files))
	for i, file := range files {
		require.NotNil(t, file.ReferencedDataFile())
		refs[i] = *file.ReferencedDataFile()
	}
	tasks := []FileScanTask{
		{DeletionVectorFiles: []iceberg.DataFile{files[0]}},
		{DeletionVectorFiles: []iceberg.DataFile{files[1]}},
	}
	fs := &countingDVOpenIO{}

	loader, err := newLazyDeletionVectorLoader(fs, tasks)
	require.NoError(t, err)
	require.Len(t, loader.groups, 1)
	assert.Zero(t, fs.opens.Load(), "indexing must not open a Puffin file")

	bitmap, err := loader.load(t.Context(), refs[0])
	require.NoError(t, err)
	require.NotNil(t, bitmap)
	assert.True(t, bitmap.Contains(0))
	assert.True(t, bitmap.Contains(2))
	assert.Equal(t, int64(1), fs.opens.Load())

	bitmap, err = loader.load(t.Context(), refs[1])
	require.NoError(t, err)
	require.NotNil(t, bitmap)
	assert.True(t, bitmap.Contains(1))
	assert.True(t, bitmap.Contains(3))
	assert.Equal(t, int64(1), fs.opens.Load(), "a shared Puffin group must be loaded once")
}

func TestLazyDeletionVectorLoaderSingleflightsConcurrentGroupLoads(t *testing.T) {
	files := writeSharedDVPuffinFixture(t, 2)
	refs := make([]string, len(files))
	for i, file := range files {
		require.NotNil(t, file.ReferencedDataFile())
		refs[i] = *file.ReferencedDataFile()
	}
	tasks := []FileScanTask{{DeletionVectorFiles: files}}
	fs := &countingDVOpenIO{}
	loader, err := newLazyDeletionVectorLoader(fs, tasks)
	require.NoError(t, err)

	const callers = 32
	bitmaps := make([]*dv.RoaringPositionBitmap, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := range callers {
		go func(i int) {
			defer wg.Done()
			bitmaps[i], errs[i] = loader.load(t.Context(), refs[i%len(refs)])
		}(i)
	}
	wg.Wait()

	assert.Equal(t, int64(1), fs.opens.Load(), "concurrent callers must share one group read")
	for i := range callers {
		require.NoError(t, errs[i])
		require.NotNil(t, bitmaps[i])
	}
}

type failingLazyDeletionVectorIO struct {
	opens atomic.Int64
	err   error
}

func (f *failingLazyDeletionVectorIO) Open(string) (iceio.File, error) {
	f.opens.Add(1)

	return nil, f.err
}

func (f *failingLazyDeletionVectorIO) Remove(string) error { return nil }

func TestLazyDeletionVectorLoaderCachesGroupErrors(t *testing.T) {
	const dataFilePath = "file:///table/data/missing.parquet"
	offset, size := int64(0), int64(1)
	dvFile := newDVMockDataFile("missing.puffin", dataFilePath, offset, size, 1)
	fs := &failingLazyDeletionVectorIO{err: errors.New("boom")}
	loader, err := newLazyDeletionVectorLoader(fs,
		[]FileScanTask{{DeletionVectorFiles: []iceberg.DataFile{dvFile}}})
	require.NoError(t, err)

	const callers = 8
	errs := make([]error, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := range callers {
		go func(i int) {
			defer wg.Done()
			_, errs[i] = loader.load(t.Context(), dataFilePath)
		}(i)
	}
	wg.Wait()

	assert.Equal(t, int64(1), fs.opens.Load())
	for _, loadErr := range errs {
		require.Error(t, loadErr)
		assert.ErrorContains(t, loadErr, "read deletion vectors from missing.puffin")
		assert.ErrorContains(t, loadErr, "boom")
	}
}

type countingPuffinOpenIO struct {
	base  iceio.IO
	opens atomic.Int64
}

func (f *countingPuffinOpenIO) Open(name string) (iceio.File, error) {
	file, err := f.base.Open(name)
	if err == nil && strings.HasSuffix(name, ".puffin") {
		f.opens.Add(1)
	}

	return file, err
}

func (f *countingPuffinOpenIO) Remove(name string) error { return f.base.Remove(name) }

func TestReadTasksDoesNotLoadDeletionVectorsBeforeIteration(t *testing.T) {
	baseFS := iceio.LocalFS{}
	countingFS := &countingPuffinOpenIO{base: baseFS}
	tmp := t.TempDir()
	tbl := buildDVScanTestTable(t, countingFS, tmp)

	dataPath := filepath.Join(tmp, "data.parquet")
	dataFile := writeIntParquetWithFieldID(t, baseFS, dataPath, 0, 5)
	puffinPath, offset, length, card := writeDVPuffinFixture(t, []uint64{1}, dataPath)
	dvFile := newDVMockDataFile(puffinPath, dataPath, offset, length, card)

	_, records, err := tbl.Scan().ReadTasks(t.Context(), []FileScanTask{{
		File:                dataFile,
		DeletionVectorFiles: []iceberg.DataFile{dvFile},
	}})
	require.NoError(t, err)
	assert.Zero(t, countingFS.opens.Load(), "GetRecords must not open Puffin files")

	for record, recordErr := range records {
		require.NoError(t, recordErr)
		require.NotNil(t, record)
		record.Release()
	}
	assert.Equal(t, int64(1), countingFS.opens.Load(), "iteration should load the DV once")
}

func TestReadTasksSurfacesLazyDeletionVectorErrorsDuringIteration(t *testing.T) {
	baseFS := iceio.LocalFS{}
	countingFS := &countingPuffinOpenIO{base: baseFS}
	tmp := t.TempDir()
	tbl := buildDVScanTestTable(t, countingFS, tmp)

	dataPath := filepath.Join(tmp, "data.parquet")
	dataFile := writeIntParquetWithFieldID(t, baseFS, dataPath, 0, 1)
	offset, size := int64(0), int64(1)
	dvFile := newDVMockDataFile(filepath.Join(tmp, "missing.puffin"), dataPath, offset, size, 1)

	_, records, err := tbl.Scan().ReadTasks(t.Context(), []FileScanTask{{
		File:                dataFile,
		DeletionVectorFiles: []iceberg.DataFile{dvFile},
	}})
	require.NoError(t, err)

	var loadErr error
	for record, recordErr := range records {
		if record != nil {
			record.Release()
		}
		if recordErr != nil {
			loadErr = recordErr

			break
		}
	}
	require.Error(t, loadErr)
	assert.ErrorContains(t, loadErr, "open DV file")
}
