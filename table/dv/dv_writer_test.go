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

package dv

import (
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerializeDVRoundTrip(t *testing.T) {
	bm := NewRoaringPositionBitmap()
	bm.Set(1)
	bm.Set(3)
	bm.Set(5)
	bm.Set(7)
	bm.Set(9)

	data, err := SerializeDV(bm)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	decoded, err := DeserializeDV(data, 5)
	require.NoError(t, err)

	assert.Equal(t, int64(5), decoded.Cardinality())
	assert.True(t, decoded.Contains(1))
	assert.True(t, decoded.Contains(3))
	assert.True(t, decoded.Contains(5))
	assert.True(t, decoded.Contains(7))
	assert.True(t, decoded.Contains(9))
	assert.False(t, decoded.Contains(2))
	assert.False(t, decoded.Contains(4))
}

func TestSerializeDVEmpty(t *testing.T) {
	bm := NewRoaringPositionBitmap()

	data, err := SerializeDV(bm)
	require.NoError(t, err)

	decoded, err := DeserializeDV(data, 0)
	require.NoError(t, err)
	assert.True(t, decoded.IsEmpty())
}

func TestSerializeDVLargePositions(t *testing.T) {
	bm := NewRoaringPositionBitmap()
	bm.Set(100)
	bm.Set(101)
	bm.Set(2147483747)
	bm.Set(2147483748)
	bm.Set((uint64(1) << 32) | 42)

	data, err := SerializeDV(bm)
	require.NoError(t, err)

	decoded, err := DeserializeDV(data, 5)
	require.NoError(t, err)

	assert.Equal(t, int64(5), decoded.Cardinality())
	assert.True(t, decoded.Contains(100))
	assert.True(t, decoded.Contains(101))
	assert.True(t, decoded.Contains(2147483747))
	assert.True(t, decoded.Contains(2147483748))
	assert.True(t, decoded.Contains((uint64(1)<<32)|42))
}

func newTestFS() *iceio.MemFS {
	return iceio.NewMemFS()
}

// unpartitionedResolver returns the canonical UnpartitionedSpec for id 0 and
// nil for everything else. Used by tests that exercise only the unpartitioned
// path; the Flush-side unknown-id error path is covered by a dedicated test.
func unpartitionedResolver() SpecResolver {
	return func(id int32) *iceberg.PartitionSpec {
		if id == 0 {
			return iceberg.UnpartitionedSpec
		}

		return nil
	}
}

// specMapResolver builds a resolver over a fixed set of specs keyed by id.
// Used by partitioned tests that exercise multiple specs in one Flush.
func specMapResolver(specs ...iceberg.PartitionSpec) SpecResolver {
	m := make(map[int32]*iceberg.PartitionSpec, len(specs))
	for i := range specs {
		m[int32(specs[i].ID())] = &specs[i]
	}

	return func(id int32) *iceberg.PartitionSpec { return m[id] }
}

func TestDVWriterFlushEmpty(t *testing.T) {
	fs := newTestFS()
	w := NewDVWriter(fs, unpartitionedResolver())

	dataFiles, err := w.Flush(context.Background(), "mem://test/dv.puffin")
	require.NoError(t, err)
	assert.Nil(t, dataFiles)
}

func TestDVWriterSingleDataFile(t *testing.T) {
	fs := newTestFS()
	w := NewDVWriter(fs, unpartitionedResolver())

	dataPath := "s3://bucket/data/file-001.parquet"
	require.NoError(t, w.Add(dataPath, []int64{1, 3, 5, 7, 9}, 0, nil))

	dataFiles, err := w.Flush(context.Background(), "mem://test/dv.puffin")
	require.NoError(t, err)
	require.Len(t, dataFiles, 1)

	df := dataFiles[0]
	assert.Equal(t, iceberg.EntryContentPosDeletes, df.ContentType())
	assert.Equal(t, "mem://test/dv.puffin", df.FilePath())
	assert.Equal(t, iceberg.PuffinFile, df.FileFormat())
	assert.Equal(t, int64(5), df.Count())
	assert.NotNil(t, df.ReferencedDataFile())
	assert.Equal(t, dataPath, *df.ReferencedDataFile())
	assert.NotNil(t, df.ContentOffset())
	assert.NotNil(t, df.ContentSizeInBytes())
	assert.Empty(t, df.Partition(),
		"unpartitioned DV manifest entry carries no partition record")
	assert.Equal(t, int32(0), df.SpecID(),
		"unpartitioned spec has id 0")

	verifyDVReadBack(t, fs, df)
}

func TestDVWriterMultipleDataFiles(t *testing.T) {
	fs := newTestFS()
	w := NewDVWriter(fs, unpartitionedResolver())

	path1 := "s3://bucket/data/file-001.parquet"
	path2 := "s3://bucket/data/file-002.parquet"
	require.NoError(t, w.Add(path1, []int64{0, 10, 20}, 0, nil))
	require.NoError(t, w.Add(path2, []int64{5, 15, 25, 35}, 0, nil))

	dataFiles, err := w.Flush(context.Background(), "mem://test/multi-dv.puffin")
	require.NoError(t, err)
	require.Len(t, dataFiles, 2)

	assert.Equal(t, int64(3), dataFiles[0].Count())
	assert.Equal(t, path1, *dataFiles[0].ReferencedDataFile())

	assert.Equal(t, int64(4), dataFiles[1].Count())
	assert.Equal(t, path2, *dataFiles[1].ReferencedDataFile())

	for _, df := range dataFiles {
		assert.Equal(t, "mem://test/multi-dv.puffin", df.FilePath())
		verifyDVReadBack(t, fs, df)
	}
}

func TestDVWriterDeduplicatesPositions(t *testing.T) {
	fs := newTestFS()
	w := NewDVWriter(fs, unpartitionedResolver())

	dataPath := "s3://bucket/data/file-001.parquet"
	require.NoError(t, w.Add(dataPath, []int64{1, 3, 5}, 0, nil))
	require.NoError(t, w.Add(dataPath, []int64{3, 5, 7}, 0, nil))

	dataFiles, err := w.Flush(context.Background(), "mem://test/dedup.puffin")
	require.NoError(t, err)
	require.Len(t, dataFiles, 1)

	assert.Equal(t, int64(4), dataFiles[0].Count())

	verifyDVReadBack(t, fs, dataFiles[0])
}

func TestDVWriterAddRejectsNegativePositions(t *testing.T) {
	fs := newTestFS()
	w := NewDVWriter(fs, unpartitionedResolver())

	dataPath := "s3://bucket/data/file-001.parquet"
	require.NoError(t, w.Add(dataPath, []int64{1, 3}, 0, nil))

	err := w.Add(dataPath, []int64{5, -1, 7}, 0, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)

	dataFiles, err := w.Flush(context.Background(), "mem://test/negative-pos.puffin")
	require.NoError(t, err)
	require.Len(t, dataFiles, 1)
	assert.Equal(t, int64(2), dataFiles[0].Count(), "negative Add should be rejected without mutating prior valid state")

	bm, err := ReadDV(fs, dataFiles[0])
	require.NoError(t, err)
	assert.True(t, bm.Contains(1))
	assert.True(t, bm.Contains(3))
	assert.False(t, bm.Contains(5))
	assert.False(t, bm.Contains(7))

	assert.Equal(t, dataFiles[0].Count(), bm.Cardinality())
}

func TestDVWriterAddNegativeFirstCreatesNoEntry(t *testing.T) {
	fs := newTestFS()
	w := NewDVWriter(fs, unpartitionedResolver())

	dataPath := "s3://bucket/data/file-001.parquet"
	require.ErrorIs(t, w.Add(dataPath, []int64{-1}, 0, nil), iceberg.ErrInvalidArgument)

	dataFiles, err := w.Flush(context.Background(), "mem://test/negative-first.puffin")
	require.NoError(t, err)
	assert.Nil(t, dataFiles)
}

func TestDVWriterResetsAfterFlush(t *testing.T) {
	fs := newTestFS()
	w := NewDVWriter(fs, unpartitionedResolver())

	require.NoError(t, w.Add("s3://bucket/data/file-001.parquet", []int64{1, 2, 3}, 0, nil))
	_, err := w.Flush(context.Background(), "mem://test/first.puffin")
	require.NoError(t, err)

	dataFiles, err := w.Flush(context.Background(), "mem://test/second.puffin")
	require.NoError(t, err)
	assert.Nil(t, dataFiles)
}

// TestDVWriterPartitionedSingleFile verifies that a DV manifest entry for a
// data file in a partitioned table carries the data file's partition record
// and spec id. The partition propagation is the load-bearing piece for
// partitioned-DV write support (#1135 PR 2 lifts the unpartitioned-only gate
// in arrow_utils.go and starts threading real specID+partition through here).
func TestDVWriterPartitionedSingleFile(t *testing.T) {
	fs := newTestFS()
	spec := iceberg.NewPartitionSpecID(7, iceberg.PartitionField{
		SourceIDs: []int{2},
		FieldID:   1000,
		Name:      "tenant_id",
		Transform: iceberg.IdentityTransform{},
	})
	w := NewDVWriter(fs, specMapResolver(spec))

	dataPath := "s3://bucket/tenant=42/file-001.parquet"
	require.NoError(t, w.Add(dataPath, []int64{1, 2, 3}, 7, map[int]any{1000: int32(42)}))

	dataFiles, err := w.Flush(context.Background(), "mem://test/partitioned.puffin")
	require.NoError(t, err)
	require.Len(t, dataFiles, 1)

	df := dataFiles[0]
	assert.Equal(t, iceberg.EntryContentPosDeletes, df.ContentType(),
		"DV manifest entries must declare position-delete content type")
	assert.Equal(t, int32(7), df.SpecID(),
		"DV manifest entry must carry the data file's spec id")
	assert.Equal(t, map[int]any{1000: int32(42)}, df.Partition(),
		"DV manifest entry must carry the data file's partition record")
	assert.Equal(t, dataPath, *df.ReferencedDataFile())

	verifyDVReadBack(t, fs, df)
}

// TestDVWriterPartitionedMultipleFiles pins per-data-file partition storage:
// each data file path gets its own DataFile with the partition values from
// its Add. Two data files in two distinct partitions land in the same Puffin
// file but produce two manifest entries with distinct partition records and
// the same spec id.
func TestDVWriterPartitionedMultipleFiles(t *testing.T) {
	fs := newTestFS()
	spec := iceberg.NewPartitionSpecID(3, iceberg.PartitionField{
		SourceIDs: []int{2},
		FieldID:   1000,
		Name:      "region",
		Transform: iceberg.IdentityTransform{},
	})
	w := NewDVWriter(fs, specMapResolver(spec))

	pathEU := "s3://bucket/region=EU/file-eu.parquet"
	pathUS := "s3://bucket/region=US/file-us.parquet"

	require.NoError(t, w.Add(pathEU, []int64{1, 2}, 3, map[int]any{1000: "EU"}))
	require.NoError(t, w.Add(pathUS, []int64{3, 4, 5}, 3, map[int]any{1000: "US"}))

	dataFiles, err := w.Flush(context.Background(), "mem://test/multi-partition.puffin")
	require.NoError(t, err)
	require.Len(t, dataFiles, 2)

	byRef := make(map[string]iceberg.DataFile, len(dataFiles))
	for _, df := range dataFiles {
		require.NotNil(t, df.ReferencedDataFile())
		byRef[*df.ReferencedDataFile()] = df
	}

	require.Contains(t, byRef, pathEU)
	require.Contains(t, byRef, pathUS)

	assert.Equal(t, map[int]any{1000: "EU"}, byRef[pathEU].Partition(),
		"EU file's DV manifest entry must carry the EU partition record")
	assert.Equal(t, map[int]any{1000: "US"}, byRef[pathUS].Partition(),
		"US file's DV manifest entry must carry the US partition record")
	assert.Equal(t, int32(3), byRef[pathEU].SpecID())
	assert.Equal(t, int32(3), byRef[pathUS].SpecID())
	assert.Equal(t, iceberg.EntryContentPosDeletes, byRef[pathEU].ContentType())
	assert.Equal(t, iceberg.EntryContentPosDeletes, byRef[pathUS].ContentType())
}

// TestDVWriterPartitionCapturedOnFirstAdd pins the capture invariant: Add
// captures specID and partition on first call for a given data file path;
// subsequent Adds contribute positions only. Callers must not pass conflicting
// partition data on follow-up Adds (one data file has one partition by
// construction), but if they do, the first-Add capture is the one that lands
// on the output DataFile. Matches Java's BaseDVFileWriter.deletesByPath
// computeIfAbsent semantics.
//
// Two subtests cover the two dimensions of the captured pair so a future
// implementation that stops capturing one of them on first Add is detected.
func TestDVWriterPartitionCapturedOnFirstAdd(t *testing.T) {
	spec0 := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
		SourceIDs: []int{2},
		FieldID:   1000,
		Name:      "region",
		Transform: iceberg.IdentityTransform{},
	})
	spec5 := iceberg.NewPartitionSpecID(5, iceberg.PartitionField{
		SourceIDs: []int{2},
		FieldID:   1000,
		Name:      "region",
		Transform: iceberg.IdentityTransform{},
	})
	dataPath := "s3://bucket/region=EU/file.parquet"

	t.Run("partitionData captured on first Add", func(t *testing.T) {
		fs := newTestFS()
		w := NewDVWriter(fs, specMapResolver(spec0))

		require.NoError(t, w.Add(dataPath, []int64{1}, 0, map[int]any{1000: "EU"}))
		// Second Add carries a different partition value; capture is unaffected.
		require.NoError(t, w.Add(dataPath, []int64{2}, 0, map[int]any{1000: "US"}))

		dataFiles, err := w.Flush(context.Background(), "mem://test/capture-partition.puffin")
		require.NoError(t, err)
		require.Len(t, dataFiles, 1)
		assert.Equal(t, map[int]any{1000: "EU"}, dataFiles[0].Partition())
	})

	t.Run("specID captured on first Add", func(t *testing.T) {
		fs := newTestFS()
		w := NewDVWriter(fs, specMapResolver(spec0, spec5))

		require.NoError(t, w.Add(dataPath, []int64{1}, 0, map[int]any{1000: "EU"}))
		// Second Add carries a different spec id; capture is unaffected.
		require.NoError(t, w.Add(dataPath, []int64{2}, 5, map[int]any{1000: "EU"}))

		dataFiles, err := w.Flush(context.Background(), "mem://test/capture-spec.puffin")
		require.NoError(t, err)
		require.Len(t, dataFiles, 1)
		assert.Equal(t, int32(0), dataFiles[0].SpecID(),
			"spec id captured on first Add; later Adds with a different spec id "+
				"do not overwrite the entry's specID")
	})
}

// TestDVWriterAddDefensiveCopies pins that Add takes a defensive copy of the
// partition data map at capture, so a caller that mutates the map between Add
// and Flush does not silently corrupt the entry. Matches Java's
// StructLikeUtil.copy(partition) in BaseDVFileWriter.Deletes.
func TestDVWriterAddDefensiveCopies(t *testing.T) {
	fs := newTestFS()
	spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
		SourceIDs: []int{2},
		FieldID:   1000,
		Name:      "region",
		Transform: iceberg.IdentityTransform{},
	})
	w := NewDVWriter(fs, specMapResolver(spec))
	partition := map[int]any{1000: "EU"}

	require.NoError(t, w.Add("s3://bucket/region=EU/file.parquet", []int64{1}, 0, partition))
	// Mutate the caller's map after Add. A reference-storing writer would
	// produce a DataFile carrying "MUTATED" on Flush; a defensive-copy
	// implementation keeps the original captured value.
	partition[1000] = "MUTATED"

	dataFiles, err := w.Flush(context.Background(), "mem://test/defensive-copy.puffin")
	require.NoError(t, err)
	require.Len(t, dataFiles, 1)
	assert.Equal(t, map[int]any{1000: "EU"}, dataFiles[0].Partition(),
		"Add must defensively copy partitionData so post-Add caller mutations "+
			"do not corrupt the captured entry")
}

// TestDVWriterFlushMixedSpecIDs covers the partition-evolution case: two data
// files in one Flush carry different spec IDs (because their referenced data
// files were written under different specs). Today the writer accepts this
// silently and emits one DataFile per entry with each carrying its own spec
// id. Manifest assembly downstream is expected to group by spec id; this
// test pins that the writer does not flatten or drop a spec id at this layer.
func TestDVWriterFlushMixedSpecIDs(t *testing.T) {
	fs := newTestFS()
	specOld := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Name: "region",
		Transform: iceberg.IdentityTransform{},
	})
	specNew := iceberg.NewPartitionSpecID(1, iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1001, Name: "region_bucket",
		Transform: iceberg.IdentityTransform{},
	})
	w := NewDVWriter(fs, specMapResolver(specOld, specNew))

	pathOld := "s3://bucket/region=EU/file.parquet"
	pathNew := "s3://bucket/region_bucket=0/file.parquet"

	require.NoError(t, w.Add(pathOld, []int64{1}, 0, map[int]any{1000: "EU"}))
	require.NoError(t, w.Add(pathNew, []int64{2}, 1, map[int]any{1001: int32(0)}))

	dataFiles, err := w.Flush(context.Background(), "mem://test/mixed-spec.puffin")
	require.NoError(t, err)
	require.Len(t, dataFiles, 2)

	byRef := make(map[string]iceberg.DataFile, len(dataFiles))
	for _, df := range dataFiles {
		byRef[*df.ReferencedDataFile()] = df
	}

	assert.Equal(t, int32(0), byRef[pathOld].SpecID(),
		"each DV DataFile must carry the spec id captured at its Add — "+
			"flushing a mixed-spec batch must not flatten spec ids")
	assert.Equal(t, int32(1), byRef[pathNew].SpecID())
}

// TestDVWriterFlushUnknownSpecID pins that a specID with no corresponding
// spec in the resolver is surfaced as a clean error at Flush, not a
// malformed DataFile. This is the failure mode for a caller bug like
// passing a stale specID after partition evolution rewrote the metadata.
func TestDVWriterFlushUnknownSpecID(t *testing.T) {
	fs := newTestFS()
	w := NewDVWriter(fs, unpartitionedResolver())

	// specID 99 is not registered with the resolver.
	require.NoError(t, w.Add("s3://bucket/file.parquet", []int64{1}, 99, nil))

	_, err := w.Flush(context.Background(), "mem://test/unknown-spec.puffin")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown partition spec id 99")
}

func verifyDVReadBack(t *testing.T, fs iceio.IO, df iceberg.DataFile) {
	t.Helper()

	bm, err := ReadDV(fs, df)
	require.NoError(t, err)
	assert.Equal(t, df.Count(), bm.Cardinality())
}
