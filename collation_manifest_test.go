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

package iceberg

import (
	"bytes"
	"testing"

	"github.com/apache/iceberg-go/collation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCollationBoundsAvroRoundTrip writes a v3 manifest whose data file carries
// collation-aware bounds, reads it back through the real Avro path, and verifies
// the bounds survive — i.e. the data_file.collation_bounds field is a working
// reference for the proposed manifest/spec change.
func TestCollationBoundsAvroRoundTrip(t *testing.T) {
	spec := collation.MustParse("en_US-ci").WithVersion("v1")
	schema := NewSchema(
		0,
		NestedField{ID: 1, Name: "id", Type: PrimitiveTypes.Int64, Required: true},
		NestedField{ID: 2, Name: "name", Type: StringTypeWithCollation(spec), Required: true},
	)
	pspec := NewPartitionSpecID(0) // unpartitioned

	entry, ok := ComputeCollatedBounds(spec, []string{"Banana", "apple", "Cherry"})
	require.True(t, ok)

	snapshotID := int64(123)
	entries := []ManifestEntry{
		&manifestEntry{
			EntryStatus: EntryStatusADDED,
			Snapshot:    &snapshotID,
			Data: &dataFile{
				Content:             EntryContentData,
				Path:                "/data/file.parquet",
				Format:              ParquetFile,
				PartitionData:       map[string]any{},
				RecordCount:         3,
				FileSize:            1000,
				BlockSizeInBytes:    64 * 1024,
				CollationBoundsData: mapToAvroColMap(map[int]CollationBoundEntry{2: entry}),
			},
		},
	}

	var buf bytes.Buffer
	_, err := WriteManifest("/manifest.avro", &buf, 3, pspec, schema, snapshotID, entries)
	require.NoError(t, err)

	mf := &manifestFile{version: 3, Path: "/manifest.avro", Content: ManifestContentData}
	got, err := ReadManifest(mf, bytes.NewReader(buf.Bytes()), false)
	require.NoError(t, err)
	require.Len(t, got, 1)

	provider, ok := got[0].DataFile().(CollationBoundsProvider)
	require.True(t, ok, "decoded data file should expose collation bounds")

	bounds := provider.CollationBounds()
	require.Contains(t, bounds, 2)
	assert.Equal(t, "en_US-ci", bounds[2].Collation)
	assert.Equal(t, "v1", bounds[2].Version)
	assert.Equal(t, []byte("apple"), bounds[2].Lower, "lower bound is the original value, not a sort key")
	assert.Equal(t, []byte("Cherry"), bounds[2].Upper)

	// And the round-tripped bound is usable for pruning under a matching version.
	assert.True(t, bounds[2].ValidFor(spec))
}

// TestManifestWithoutCollationBoundsRoundTrip confirms the new optional field is
// backward compatible: a data file with no collation bounds round-trips with an
// empty collation-bounds map and no error.
func TestManifestWithoutCollationBoundsRoundTrip(t *testing.T) {
	schema := NewSchema(
		0,
		NestedField{ID: 1, Name: "id", Type: PrimitiveTypes.Int64, Required: true},
	)
	pspec := NewPartitionSpecID(0)

	snapshotID := int64(7)
	entries := []ManifestEntry{
		&manifestEntry{
			EntryStatus: EntryStatusADDED,
			Snapshot:    &snapshotID,
			Data: &dataFile{
				Content:          EntryContentData,
				Path:             "/data/file.parquet",
				Format:           ParquetFile,
				PartitionData:    map[string]any{},
				RecordCount:      1,
				FileSize:         10,
				BlockSizeInBytes: 64 * 1024,
			},
		},
	}

	var buf bytes.Buffer
	_, err := WriteManifest("/manifest.avro", &buf, 3, pspec, schema, snapshotID, entries)
	require.NoError(t, err)

	mf := &manifestFile{version: 3, Path: "/manifest.avro", Content: ManifestContentData}
	got, err := ReadManifest(mf, bytes.NewReader(buf.Bytes()), false)
	require.NoError(t, err)
	require.Len(t, got, 1)

	provider, ok := got[0].DataFile().(CollationBoundsProvider)
	require.True(t, ok)
	assert.Empty(t, provider.CollationBounds())
}
