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
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenManifestWithProjectionDropsStatsAfterFiltering(t *testing.T) {
	spec := partitionedSpec()
	schema := simpleSchema()
	snapshotID := int64(1)
	builder, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		"mem://default/table/data/file.parquet",
		iceberg.ParquetFile,
		map[int]any{1000: int32(7)},
		nil,
		nil,
		1,
		100,
	)
	require.NoError(t, err)
	builder.
		ValueCounts(map[int]int64{1: 1}).
		NullValueCounts(map[int]int64{1: 0}).
		NaNValueCounts(map[int]int64{1: 0}).
		LowerBoundValues(map[int][]byte{1: {0x01}}).
		UpperBoundValues(map[int][]byte{1: {0x01}})

	manifestPath := "mem://default/table/metadata/manifest.avro"
	var manifestBytes bytes.Buffer
	manifest, err := iceberg.WriteManifest(
		manifestPath,
		&manifestBytes,
		2,
		spec,
		schema,
		snapshotID,
		[]iceberg.ManifestEntry{iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, &snapshotID, nil, nil, builder.Build(),
		)},
	)
	require.NoError(t, err)

	fs := iceio.NewMemFS()
	require.NoError(t, fs.WriteFile(manifestPath, manifestBytes.Bytes()))

	projection := iceberg.ManifestEntryProjection{IncludeColumnStats: true}
	entries, err := openManifestWithProjection(
		fs,
		manifest,
		func(file iceberg.DataFile) (bool, error) {
			assert.Equal(t, int32(7), file.Partition()[1000])

			return true, nil
		},
		func(file iceberg.DataFile) (bool, error) {
			assert.Equal(t, map[int]int64{1: 1}, file.ValueCounts())
			assert.Equal(t, map[int][]byte{1: {0x01}}, file.LowerBoundValues())

			return true, nil
		},
		&projection,
		true,
	)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "mem://default/table/data/file.parquet", entries[0].DataFile().FilePath())
	assert.Empty(t, entries[0].DataFile().ValueCounts())
	assert.Empty(t, entries[0].DataFile().LowerBoundValues())
}

func TestManifestProjectionRetainsDataFileStatsForDeleteScans(t *testing.T) {
	scan := &Scan{rowFilter: iceberg.AlwaysTrue{}}
	dataManifest := iceberg.NewManifestFile(
		2, "data-manifest.avro", 100, 0, 1,
	).Build()
	deleteManifest := iceberg.NewManifestFile(
		2, "delete-manifest.avro", 100, 0, 1,
	).Content(iceberg.ManifestContentDeletes).Build()

	projection, dropStats := scan.manifestProjectionForManifest(dataManifest, false)
	assert.False(t, projection.IncludeColumnStats)
	assert.False(t, dropStats)

	retainDataStats := scan.manifestProjectionRetainsDataStats(
		[]iceberg.ManifestFile{dataManifest, deleteManifest})
	assert.True(t, retainDataStats)

	projection, dropStats = scan.manifestProjectionForManifest(dataManifest, retainDataStats)
	assert.True(t, projection.IncludeColumnStats)
	assert.False(t, dropStats,
		"data-file stats must survive planning for equality-delete pruning")

	projection, dropStats = scan.manifestProjectionForManifest(deleteManifest, retainDataStats)
	assert.True(t, projection.IncludeColumnStats)
	assert.False(t, dropStats)
}
