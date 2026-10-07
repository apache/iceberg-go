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
// multi-version collation-aware bounds, reads it back through the real Avro path,
// and verifies they survive under the active collation bounds layout. It checks
// only layout-neutral properties, so it holds for any layout behind the seam.
func TestCollationBoundsAvroRoundTrip(t *testing.T) {
	v1 := collation.MustParse("en_US-ci").WithVersion("v1")
	v2 := v1.WithVersion("v2")
	other := v1.WithVersion("v3")
	schema := NewSchema(
		0,
		NestedField{ID: 1, Name: "id", Type: PrimitiveTypes.Int64, Required: true},
		NestedField{ID: 2, Name: "name", Type: StringTypeWithCollation(v1), Required: true},
	)
	pspec := NewPartitionSpecID(0) // unpartitioned

	values := []string{"Banana", "apple", "Cherry"}
	e1, ok := ComputeCollatedBounds(v1, values)
	require.True(t, ok)
	e2, ok := ComputeCollatedBounds(v2, values)
	require.True(t, ok)

	b, err := NewDataFileBuilder(pspec, EntryContentData, "/data/file.parquet", ParquetFile,
		nil, nil, nil, 3, 1000)
	require.NoError(t, err)
	df := b.WithCollationBounds(map[int]CollationColumnStats{
		2: {Bounds: []CollationBoundEntry{e1, e2}},
	}).Build()

	snapshotID := int64(123)
	entries := []ManifestEntry{NewManifestEntry(EntryStatusADDED, &snapshotID, nil, nil, df)}

	var buf bytes.Buffer
	_, err = WriteManifest("/manifest.avro", &buf, 3, pspec, schema, snapshotID, entries)
	require.NoError(t, err)

	mf := &manifestFile{version: 3, Path: "/manifest.avro", Content: ManifestContentData}
	got, err := ReadManifest(mf, bytes.NewReader(buf.Bytes()), false)
	require.NoError(t, err)
	require.Len(t, got, 1)

	provider, ok := got[0].DataFile().(CollationBoundsProvider)
	require.True(t, ok, "decoded data file should expose collation bounds")

	stats := provider.CollationBounds()
	require.Contains(t, stats, 2)
	require.Len(t, stats[2].Bounds, 2, "one bound per collation version")

	for _, spec := range []*collation.Spec{v1, v2} {
		bound, ok := stats[2].BoundFor(spec)
		require.True(t, ok, "bound for version %s must be usable", spec.Version())
		assert.Equal(t, spec.Version(), bound.Version)
		assert.Equal(t, []byte("apple"), bound.Lower, "lower bound is the original value, not a sort key")
		assert.Equal(t, []byte("Cherry"), bound.Upper)
	}

	_, ok = stats[2].BoundFor(other)
	assert.False(t, ok, "a version not present in the file must not be usable")
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
