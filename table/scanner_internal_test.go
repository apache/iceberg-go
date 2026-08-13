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
	"encoding/binary"
	"errors"
	"math"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/DataDog/iceberg-go"
	iceio "github.com/DataDog/iceberg-go/io"
	"github.com/DataDog/iceberg-go/table/internal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDataManifest(minSeqNum int64) iceberg.ManifestFile {
	return iceberg.NewManifestFile(2, "/path/to/manifest.avro", 1000, 0, 1).
		Content(iceberg.ManifestContentData).
		SequenceNum(minSeqNum, minSeqNum).
		Build()
}

func newDeleteManifest(minSeqNum int64) iceberg.ManifestFile {
	return iceberg.NewManifestFile(2, "/path/to/manifest.avro", 1000, 0, 1).
		Content(iceberg.ManifestContentDeletes).
		SequenceNum(minSeqNum, minSeqNum).
		Build()
}

func newEqualityDeleteIndexTestEntry(
	path string,
	specID int32,
	partition map[int]any,
	sequenceNumber int64,
) iceberg.ManifestEntry {
	return iceberg.NewManifestEntry(
		iceberg.EntryStatusADDED,
		nil,
		&sequenceNumber,
		nil,
		&mockDataFile{path: path, specid: specID, partition: partition},
	)
}

type equalityDeleteIndexTestSpecLookup map[int]iceberg.PartitionSpec

func (s equalityDeleteIndexTestSpecLookup) PartitionSpecByID(id int) *iceberg.PartitionSpec {
	spec, ok := s[id]
	if !ok {
		return nil
	}

	return &spec
}

func equalityDeleteIndexTestSpecs() equalityDeleteIndexTestSpecLookup {
	partitionedField := func() iceberg.PartitionField {
		return iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000, Name: "partition",
			Transform: iceberg.IdentityTransform{},
		}
	}

	return equalityDeleteIndexTestSpecLookup{
		0: iceberg.NewPartitionSpecID(0),
		1: iceberg.NewPartitionSpecID(1, partitionedField()),
		2: iceberg.NewPartitionSpecID(2, partitionedField()),
		3: iceberg.NewPartitionSpecID(3, iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000, Name: "void",
			Transform: iceberg.VoidTransform{},
		}),
	}
}

func TestEqualityDeleteIndexMatchesPartitionAndSequence(t *testing.T) {
	partition := map[int]any{1000: []byte{0xde, 0xad}}
	deleteEntries := []iceberg.ManifestEntry{
		newEqualityDeleteIndexTestEntry("same-sequence.parquet", 1, partition, 5),
		newEqualityDeleteIndexTestEntry("matching-newer.parquet", 1,
			map[int]any{1000: append([]byte(nil), 0xde, 0xad)}, 7),
		newEqualityDeleteIndexTestEntry("matching-older.parquet", 1, partition, 3),
		newEqualityDeleteIndexTestEntry("different-partition.parquet", 1,
			map[int]any{1000: []byte{0xbe, 0xef}}, 8),
		newEqualityDeleteIndexTestEntry("different-spec.parquet", 2, partition, 8),
		newEqualityDeleteIndexTestEntry("global-newer.parquet", 0, nil, 6),
		newEqualityDeleteIndexTestEntry("global-same-sequence.parquet", 0, nil, 5),
	}

	idx, err := buildEqualityDeleteIndex(deleteEntries, equalityDeleteIndexTestSpecs())
	require.NoError(t, err)

	dataEntry := newEqualityDeleteIndexTestEntry("data.parquet", 1, partition, 5)
	matched, err := idx.forDataFile(dataEntry)
	require.NoError(t, err)
	require.Len(t, matched, 2)
	assert.Equal(t, []string{"global-newer.parquet", "matching-newer.parquet"},
		[]string{matched[0].FilePath(), matched[1].FilePath()})
}

func TestEqualityDeleteIndexKeepsPartitionedDeletesScoped(t *testing.T) {
	partitionedDelete := newEqualityDeleteIndexTestEntry(
		"partitioned-delete.parquet", 1, map[int]any{1000: "a"}, 3)
	globalDelete := newEqualityDeleteIndexTestEntry("global-delete.parquet", 0, nil, 3)

	idx, err := buildEqualityDeleteIndex(
		[]iceberg.ManifestEntry{partitionedDelete, globalDelete}, equalityDeleteIndexTestSpecs())
	require.NoError(t, err)

	unpartitionedData := newEqualityDeleteIndexTestEntry("data.parquet", 0, nil, 1)
	matched, err := idx.forDataFile(unpartitionedData)
	require.NoError(t, err)
	require.Len(t, matched, 1)
	assert.Equal(t, "global-delete.parquet", matched[0].FilePath())
}

func TestEqualityDeleteIndexTreatsVoidSpecAsGlobal(t *testing.T) {
	voidDelete := newEqualityDeleteIndexTestEntry(
		"void-delete.parquet", 3, map[int]any{1000: nil}, 3)

	idx, err := buildEqualityDeleteIndex(
		[]iceberg.ManifestEntry{voidDelete}, equalityDeleteIndexTestSpecs())
	require.NoError(t, err)

	unpartitionedData := newEqualityDeleteIndexTestEntry("data.parquet", 0, nil, 1)
	matched, err := idx.forDataFile(unpartitionedData)
	require.NoError(t, err)
	require.Len(t, matched, 1)
	assert.Equal(t, "void-delete.parquet", matched[0].FilePath())
}

func TestEqualityDeleteIndexRejectsUnknownPartitionSpec(t *testing.T) {
	deleteEntry := newEqualityDeleteIndexTestEntry("delete.parquet", 99, nil, 2)

	_, err := buildEqualityDeleteIndex(
		[]iceberg.ManifestEntry{deleteEntry}, equalityDeleteIndexTestSpecs())
	require.ErrorIs(t, err, ErrPartitionSpecNotFound)
	assert.ErrorContains(t, err, "delete.parquet")
}

func TestEqualityDeleteIndexSkipsPartitionKeysWithoutPartitionedDeletes(t *testing.T) {
	dataEntry := newEqualityDeleteIndexTestEntry(
		"data.parquet", 1, map[int]any{1000: struct{}{}}, 1)

	for _, entries := range [][]iceberg.ManifestEntry{
		nil,
		{newEqualityDeleteIndexTestEntry("global-delete.parquet", 0, nil, 2)},
	} {
		idx, err := buildEqualityDeleteIndex(entries, equalityDeleteIndexTestSpecs())
		require.NoError(t, err)

		matched, err := idx.forDataFile(dataEntry)
		require.NoError(t, err)
		assert.Len(t, matched, len(entries))
	}
}

func TestEqualityDeleteIndexSortsBySequence(t *testing.T) {
	partition := map[int]any{1000: "a"}
	deleteEntries := []iceberg.ManifestEntry{
		newEqualityDeleteIndexTestEntry("sequence-4.parquet", 1, partition, 4),
		newEqualityDeleteIndexTestEntry("sequence-2-a.parquet", 1, partition, 2),
		newEqualityDeleteIndexTestEntry("sequence-3.parquet", 1, partition, 3),
		newEqualityDeleteIndexTestEntry("sequence-2-b.parquet", 1, partition, 2),
	}

	idx, err := buildEqualityDeleteIndex(deleteEntries, equalityDeleteIndexTestSpecs())
	require.NoError(t, err)

	dataEntry := newEqualityDeleteIndexTestEntry("data.parquet", 1, partition, 1)
	matched, err := idx.forDataFile(dataEntry)
	require.NoError(t, err)
	paths := make([]string, len(matched))
	for i, file := range matched {
		paths[i] = file.FilePath()
	}
	assert.Equal(t, []string{
		"sequence-2-a.parquet",
		"sequence-2-b.parquet",
		"sequence-3.parquet",
		"sequence-4.parquet",
	}, paths)
}

func TestEqualityDeletePartitionKeyNormalizesValues(t *testing.T) {
	sampleUUID := uuid.MustParse("12345678-9abc-def0-1234-56789abcdef0")
	tests := []struct {
		name  string
		left  any
		right any
	}{
		{name: "integer widths", left: int32(7), right: int64(7)},
		{name: "date and integer", left: iceberg.Date(7), right: int32(7)},
		{name: "float widths", left: float32(1.5), right: float64(1.5)},
		{name: "NaN", left: math.Float32frombits(0x7fc00000), right: math.NaN()},
		{name: "UUID and bytes", left: sampleUUID, right: sampleUUID[:]},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			left, err := newEqualityDeletePartitionKey(1, map[int]any{1000: tt.left})
			require.NoError(t, err)
			right, err := newEqualityDeletePartitionKey(1, map[int]any{1000: tt.right})
			require.NoError(t, err)
			assert.Equal(t, left, right)
		})
	}

	base, err := newEqualityDeletePartitionKey(1, map[int]any{1000: int32(7)})
	require.NoError(t, err)
	differentSpec, err := newEqualityDeletePartitionKey(2, map[int]any{1000: int32(7)})
	require.NoError(t, err)
	differentField, err := newEqualityDeletePartitionKey(1, map[int]any{1001: int32(7)})
	require.NoError(t, err)
	assert.NotEqual(t, base, differentSpec)
	assert.NotEqual(t, base, differentField)
	multiField, err := newEqualityDeletePartitionKey(1, map[int]any{
		1000: int32(7),
		1001: "region",
	})
	require.NoError(t, err)
	multiFieldReordered, err := newEqualityDeletePartitionKey(1, map[int]any{
		1001: "region",
		1000: int64(7),
	})
	require.NoError(t, err)
	assert.Equal(t, multiField, multiFieldReordered)

	_, err = newEqualityDeletePartitionKey(1, map[int]any{1000: struct{}{}})
	assert.ErrorContains(t, err, "unsupported partition value type struct {}")
}

func TestEqualityDeletePartitionKeyDistinguishesSignedZero(t *testing.T) {
	negativeZero := math.Copysign(0, -1)

	negative, err := newEqualityDeletePartitionKey(1, map[int]any{1000: negativeZero})
	require.NoError(t, err)
	positive, err := newEqualityDeletePartitionKey(1, map[int]any{1000: float64(0)})
	require.NoError(t, err)

	assert.NotEqual(t, negative, positive)
}

func TestMinSequenceNum(t *testing.T) {
	tests := []struct {
		name      string
		manifests []iceberg.ManifestFile
		expected  int64
	}{
		{
			name:      "empty list returns 0",
			manifests: []iceberg.ManifestFile{},
			expected:  0,
		},
		{
			name: "single data manifest returns its sequence number",
			manifests: []iceberg.ManifestFile{
				newDataManifest(5),
			},
			expected: 5,
		},
		{
			name: "multiple data manifests returns minimum",
			manifests: []iceberg.ManifestFile{
				newDataManifest(10),
				newDataManifest(3),
				newDataManifest(7),
			},
			expected: 3,
		},
		{
			name: "only delete manifests returns 0",
			manifests: []iceberg.ManifestFile{
				newDeleteManifest(5),
				newDeleteManifest(10),
			},
			expected: 0,
		},
		{
			name: "mixed manifests only considers data manifests",
			manifests: []iceberg.ManifestFile{
				newDeleteManifest(1), // should be ignored
				newDataManifest(8),
				newDeleteManifest(2), // should be ignored
				newDataManifest(5),
			},
			expected: 5,
		},
		{
			name: "data manifest with sequence 0 is handled correctly",
			manifests: []iceberg.ManifestFile{
				newDataManifest(0),
				newDataManifest(5),
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := minSequenceNum(tt.manifests)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSplitLineageMetadataFields(t *testing.T) {
	tests := []struct {
		name          string
		selected      []string
		caseSensitive bool
		wantUser      []string
		wantLineage   []iceberg.NestedField
	}{
		{
			name:          "case-insensitive matches mixed-case lineage columns",
			selected:      []string{"id", "_ROW_ID", "_Last_Updated_Sequence_Number"},
			caseSensitive: false,
			wantUser:      []string{"id"},
			wantLineage:   []iceberg.NestedField{iceberg.RowID(), iceberg.LastUpdatedSequenceNumber()},
		},
		{
			name:          "case-sensitive leaves mixed-case names as user fields",
			selected:      []string{"id", "_ROW_ID", "_Last_Updated_Sequence_Number"},
			caseSensitive: true,
			wantUser:      []string{"id", "_ROW_ID", "_Last_Updated_Sequence_Number"},
			wantLineage:   nil,
		},
		{
			name:          "case-sensitive matches exact lineage column names",
			selected:      []string{"id", iceberg.RowIDColumnName, iceberg.LastUpdatedSequenceNumberColumnName},
			caseSensitive: true,
			wantUser:      []string{"id"},
			wantLineage:   []iceberg.NestedField{iceberg.RowID(), iceberg.LastUpdatedSequenceNumber()},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			userFields, lineageFields := splitLineageMetadataFields(tt.selected, tt.caseSensitive)
			assert.Equal(t, tt.wantUser, userFields)
			assert.Equal(t, tt.wantLineage, lineageFields)
		})
	}
}

func TestKeyDefaultMapRaceCondition(t *testing.T) {
	var factoryCallCount atomic.Int64
	factory := func(key string) int {
		factoryCallCount.Add(1)
		runtime.Gosched() // to widen the race window

		return 42
	}

	kdm := newKeyDefaultMap(factory)

	var wg sync.WaitGroup
	start := make(chan struct{})

	numGoroutines := 1000
	for range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = kdm.Get("same-key")
		}()
	}

	close(start)
	wg.Wait()

	callCount := factoryCallCount.Load()
	assert.Equal(t, int64(1), callCount,
		"factory should be called exactly once per key, but was called %d times", callCount)
}

func TestKeyDefaultMapWrapErrCachesError(t *testing.T) {
	var factoryCallCount atomic.Int64
	expectedErr := errors.New("boom")
	kdm := newKeyDefaultMapWrapErr(func(key string) (int, error) {
		factoryCallCount.Add(1)

		return 0, expectedErr
	})

	value, err := kdm.Get("same-key")
	require.ErrorIs(t, err, expectedErr)
	assert.Zero(t, value)

	value, err = kdm.Get("same-key")
	require.ErrorIs(t, err, expectedErr)
	assert.Zero(t, value)

	assert.Equal(t, int64(1), factoryCallCount.Load(),
		"factory should be called exactly once per key even when it fails")
}

func TestBuildPartitionProjectionWithInvalidSpecID(t *testing.T) {
	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{
			ID: 1, Name: "id",
			Type: iceberg.PrimitiveTypes.Int64, Required: true,
		},
	)

	metadata, err := NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"s3://test-bucket/test_table",
		iceberg.Properties{},
	)
	require.NoError(t, err)

	expr, err := buildPartitionProjection(999, metadata, metadata.CurrentSchema(), iceberg.AlwaysTrue{}, true)
	require.Error(t, err)
	assert.Nil(t, expr)
	assert.ErrorIs(t, err, ErrPartitionSpecNotFound)
	assert.ErrorContains(t, err, "id 999")
}

func TestFetchPartitionSpecFilteredManifests_PropagatesEvalError(t *testing.T) {
	spec := partitionedSpec()
	txn, memIO := createTestTransactionWithMemIO(t, spec)

	const (
		snapshotID       = int64(1)
		manifestListPath = "mem://default/table-location/metadata/snap-1-manifest-list.avro"
	)

	// Invalid int32 bounds trigger a LiteralFromBytes error during manifest eval.
	badBounds := []byte{0x00, 0x01}
	mf := iceberg.NewManifestFile(2, "mem://default/table-location/metadata/manifest.avro", 100, int32(spec.ID()), snapshotID).
		Partitions([]iceberg.FieldSummary{
			{LowerBound: &badBounds, UpperBound: &badBounds, ContainsNull: false},
		}).Build()

	var listBuf bytes.Buffer
	seqNum := int64(1)
	require.NoError(t, iceberg.WriteManifestList(2, &listBuf, snapshotID, nil, &seqNum, 0, []iceberg.ManifestFile{mf}))
	require.NoError(t, memIO.WriteFile(manifestListPath, listBuf.Bytes()))

	snapID := snapshotID
	txn.meta.snapshotList = []Snapshot{{
		SnapshotID:     snapshotID,
		ManifestList:   manifestListPath,
		SequenceNumber: seqNum,
	}}
	txn.meta.currentSnapshotID = &snapID

	built, err := txn.meta.Build()
	require.NoError(t, err)

	tbl := New(Identifier{"db", "tbl"}, built, "metadata.json", func(context.Context) (iceio.IO, error) {
		return memIO, nil
	}, nil)

	scan := tbl.Scan(WithRowFilter(iceberg.EqualTo(iceberg.Reference("id"), int32(5))))
	pio := &countingPlanIO{}
	scan.planIO = mustPlanIOState(t, pio)
	state := scan.planIO

	_, err = scan.fetchPartitionSpecFilteredManifests(context.Background())
	require.Error(t, err, "manifest eval errors must propagate instead of silently dropping manifests")
	require.ErrorContains(t, err, "manifest")

	_, err = scan.PlanFiles(context.Background())
	require.Error(t, err, "PlanFiles must fail when manifest filtering errors")
	assert.Same(t, state, scan.planIO)
	assert.Equal(t, 0, pio.closeCalls)
}

func TestFetchPartitionSpecFilteredManifests_InvalidSpecIDDoesNotPanic(t *testing.T) {
	spec := partitionedSpec()
	txn, memIO := createTestTransactionWithMemIO(t, spec)

	const (
		snapshotID       = int64(1)
		manifestListPath = "mem://default/table-location/metadata/snap-1-manifest-list.avro"
	)

	mf := iceberg.NewManifestFile(2, "mem://default/table-location/metadata/manifest.avro", 100, 999, snapshotID).Build()

	var listBuf bytes.Buffer
	seqNum := int64(1)
	require.NoError(t, iceberg.WriteManifestList(2, &listBuf, snapshotID, nil, &seqNum, 0, []iceberg.ManifestFile{mf}))
	require.NoError(t, memIO.WriteFile(manifestListPath, listBuf.Bytes()))

	snapID := snapshotID
	txn.meta.snapshotList = []Snapshot{{
		SnapshotID:     snapshotID,
		ManifestList:   manifestListPath,
		SequenceNumber: seqNum,
	}}
	txn.meta.currentSnapshotID = &snapID

	built, err := txn.meta.Build()
	require.NoError(t, err)

	tbl := New(Identifier{"db", "tbl"}, built, "metadata.json", func(context.Context) (iceio.IO, error) {
		return memIO, nil
	}, nil)

	scan := tbl.Scan()

	_, err = scan.fetchPartitionSpecFilteredManifests(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPartitionSpecNotFound)
	require.ErrorContains(t, err, "999")

	_, err = scan.PlanFiles(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPartitionSpecNotFound)
}

// TestFetchManifestCountersWithRealSnapshot verifies the scanned/skipped/total
// manifest accounting against a real snapshot with a mix of data and delete
// manifests. Partition-spec pruning skips a non-overlapping data manifest, so
// the scanned and skipped counters are both exercised (an empty-metadata test
// leaves every counter at zero and would not catch a swapped scanned/skipped or
// data/delete branch).
func TestFetchManifestCountersWithRealSnapshot(t *testing.T) {
	spec := partitionedSpec()
	txn, memIO := createTestTransactionWithMemIO(t, spec)

	// Identity partition on the int32 "id" column; encode partition bounds as
	// 4-byte little-endian int32 so the manifest evaluator can decode them.
	enc := func(v int32) *[]byte {
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, uint32(v))

		return &b
	}
	match := enc(5)    // overlaps the id == 5 filter
	noMatch := enc(10) // does not overlap

	const (
		snapshotID       = int64(1)
		manifestListPath = "mem://default/table-location/metadata/snap-1-manifest-list.avro"
	)
	specID := int32(spec.ID())

	dataScanned := iceberg.NewManifestFile(2, "mem://default/table-location/metadata/data-scanned.avro", 100, specID, snapshotID).
		Content(iceberg.ManifestContentData).
		Partitions([]iceberg.FieldSummary{{ContainsNull: false, LowerBound: match, UpperBound: match}}).
		Build()
	dataSkipped := iceberg.NewManifestFile(2, "mem://default/table-location/metadata/data-skipped.avro", 100, specID, snapshotID).
		Content(iceberg.ManifestContentData).
		Partitions([]iceberg.FieldSummary{{ContainsNull: false, LowerBound: noMatch, UpperBound: noMatch}}).
		Build()
	deleteScanned := iceberg.NewManifestFile(2, "mem://default/table-location/metadata/delete-scanned.avro", 100, specID, snapshotID).
		Content(iceberg.ManifestContentDeletes).
		SequenceNum(1, 1).
		Partitions([]iceberg.FieldSummary{{ContainsNull: false, LowerBound: match, UpperBound: match}}).
		Build()

	var listBuf bytes.Buffer
	seqNum := int64(1)
	require.NoError(t, iceberg.WriteManifestList(2, &listBuf, snapshotID, nil, &seqNum, 0,
		[]iceberg.ManifestFile{dataScanned, dataSkipped, deleteScanned}))
	require.NoError(t, memIO.WriteFile(manifestListPath, listBuf.Bytes()))

	snapID := snapshotID
	txn.meta.snapshotList = []Snapshot{{
		SnapshotID:     snapshotID,
		ManifestList:   manifestListPath,
		SequenceNumber: seqNum,
	}}
	txn.meta.currentSnapshotID = &snapID

	built, err := txn.meta.Build()
	require.NoError(t, err)

	tbl := New(Identifier{"db", "tbl"}, built, "metadata.json", func(context.Context) (iceio.IO, error) {
		return memIO, nil
	}, nil)

	scan := tbl.Scan(WithRowFilter(iceberg.EqualTo(iceberg.Reference("id"), int32(5))))

	schema, err := scan.effectiveSchema()
	require.NoError(t, err)

	var acc scanMetricsAccumulator
	filtered, err := scan.fetchPartitionSpecFilteredManifestsWithSchema(context.Background(), schema, &acc)
	require.NoError(t, err)

	// Two data manifests, one delete manifest.
	assert.Equal(t, int64(2), acc.totalDataManifests)
	assert.Equal(t, int64(1), acc.totalDeleteManifests)
	// The overlapping data and delete manifests are scanned; the non-overlapping
	// data manifest is skipped. Delete manifests must land in the delete counters.
	assert.Equal(t, int64(1), acc.scannedDataManifests)
	assert.Equal(t, int64(1), acc.skippedDataManifests)
	assert.Equal(t, int64(1), acc.scannedDeleteManifests)
	assert.Equal(t, int64(0), acc.skippedDeleteManifests)
	// Invariant: scanned + skipped == total, per content type.
	assert.Equal(t, acc.totalDataManifests, acc.scannedDataManifests+acc.skippedDataManifests)
	assert.Equal(t, acc.totalDeleteManifests, acc.scannedDeleteManifests+acc.skippedDeleteManifests)
	assert.Len(t, filtered, 2, "only the overlapping manifests survive partition-spec filtering")
}

func TestBuildManifestEvaluatorWithInvalidSpecID(t *testing.T) {
	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{
			ID: 1, Name: "id",
			Type: iceberg.PrimitiveTypes.Int64, Required: true,
		},
	)

	metadata, err := NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"s3://test-bucket/test_table",
		iceberg.Properties{},
	)
	require.NoError(t, err)

	rowFilter := iceberg.AlwaysTrue{}
	partitionFilters := newKeyDefaultMapWrapErr(func(specID int) (iceberg.BooleanExpression, error) {
		return buildPartitionProjection(specID, metadata, metadata.CurrentSchema(), rowFilter, true)
	})

	evaluator, err := buildManifestEvaluator(999, metadata, metadata.CurrentSchema(), partitionFilters, true)
	require.Error(t, err)
	assert.Nil(t, evaluator)
	assert.ErrorIs(t, err, ErrPartitionSpecNotFound)
	assert.ErrorContains(t, err, "id 999")
}

func TestBuildPartitionEvaluatorWithInvalidSpecID(t *testing.T) {
	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{
			ID: 1, Name: "id",
			Type: iceberg.PrimitiveTypes.Int64, Required: true,
		},
	)

	metadata, err := NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"s3://test-bucket/test_table",
		iceberg.Properties{},
	)
	require.NoError(t, err)

	rowFilter := iceberg.AlwaysTrue{}
	partitionFilters := newKeyDefaultMapWrapErr(func(specID int) (iceberg.BooleanExpression, error) {
		return buildPartitionProjection(specID, metadata, metadata.CurrentSchema(), rowFilter, true)
	})

	evaluator, err := buildPartitionEvaluator(999, metadata, metadata.CurrentSchema(), partitionFilters, true)
	require.Error(t, err)
	assert.Nil(t, evaluator)
	assert.ErrorIs(t, err, ErrPartitionSpecNotFound)
	assert.ErrorContains(t, err, "id 999")
}

func TestTimeTravelManifestPruningUsesSnapshotSchema(t *testing.T) {
	spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
		SourceIDs: []int{1},
		FieldID:   1000,
		Name:      "id",
		Transform: iceberg.IdentityTransform{},
	})
	memIO := iceio.NewMemFS()
	manifestListPath := "mem://default/table/metadata/snap-7.avro"
	scan, _, snapshotID := newSchemaEvolutionScanWithSnapshot(t, &spec, memIO, manifestListPath, nil)

	lower := int32Bound(10)
	upper := int32Bound(20)
	manifest := iceberg.NewManifestFile(2, "mem://default/table/metadata/manifest.avro", 100, int32(spec.ID()), snapshotID).
		Partitions([]iceberg.FieldSummary{{
			ContainsNull: false,
			LowerBound:   &lower,
			UpperBound:   &upper,
		}}).
		Build()

	var listBytes bytes.Buffer
	seqNum := int64(1)
	err := iceberg.WriteManifestList(2, &listBytes, snapshotID, nil, &seqNum, 0, []iceberg.ManifestFile{manifest})
	require.NoError(t, err)
	require.NoError(t, memIO.WriteFile(manifestListPath, listBytes.Bytes()))

	tasks, err := scan.PlanFiles(context.Background())
	require.NoError(t, err)
	assert.Empty(t, tasks, "old snapshot manifest bounds outside the filter should be pruned")
}

func TestTimeTravelMetricsPruningUsesSnapshotSchema(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	memIO := iceio.NewMemFS()
	scan, oldSchema, snapshotID := newSchemaEvolutionScan(t, &spec, memIO)

	lower := int32Bound(10)
	upper := int32Bound(20)
	dataFile, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		"mem://default/table/data.parquet",
		iceberg.ParquetFile,
		nil,
		nil,
		nil,
		1,
		1024,
	)
	require.NoError(t, err)
	entry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &snapshotID,
		dataFile.LowerBoundValues(map[int][]byte{1: lower}).
			UpperBoundValues(map[int][]byte{1: upper}).
			Build(),
	).SequenceNum(1).Build()

	manifestPath := "mem://default/table/metadata/manifest.avro"
	var manifestBytes bytes.Buffer
	manifest, err := iceberg.WriteManifest(manifestPath, &manifestBytes, 2, spec, oldSchema, snapshotID, []iceberg.ManifestEntry{entry})
	require.NoError(t, err)
	require.NoError(t, memIO.WriteFile(manifestPath, manifestBytes.Bytes()))

	entries, err := scan.collectManifestEntries(context.Background(), []iceberg.ManifestFile{manifest})
	require.NoError(t, err)
	assert.Empty(t, entries.dataEntries, "old snapshot file metrics outside the filter should be pruned")
}

func newSchemaEvolutionScan(t *testing.T, spec *iceberg.PartitionSpec, fs iceio.IO) (*Scan, *iceberg.Schema, int64) {
	return newSchemaEvolutionScanWithSnapshot(t, spec, fs, "", nil)
}

func newSchemaEvolutionScanWithSnapshot(t *testing.T, spec *iceberg.PartitionSpec, fs iceio.IO, manifestList string, snapshotSchemaID *int) (*Scan, *iceberg.Schema, int64) {
	t.Helper()

	oldSchema := iceberg.NewSchema(0, iceberg.NestedField{
		ID:   1,
		Name: "id",
		Type: iceberg.PrimitiveTypes.Int32,
	})
	currentSchema := iceberg.NewSchema(1, iceberg.NestedField{
		ID:   2,
		Name: "id",
		Type: iceberg.PrimitiveTypes.Int32,
	})
	meta, err := NewMetadata(
		oldSchema,
		spec,
		UnsortedSortOrder,
		"mem://default/table",
		iceberg.Properties{PropertyFormatVersion: "2"},
	)
	require.NoError(t, err)

	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	require.NoError(t, builder.AddSchema(currentSchema))
	require.NoError(t, builder.SetCurrentSchemaID(currentSchema.ID))

	snapshotID := int64(7)
	oldSchemaID := oldSchema.ID
	if snapshotSchemaID == nil {
		snapshotSchemaID = &oldSchemaID
	}
	snapshot := &Snapshot{
		SnapshotID:     snapshotID,
		SequenceNumber: 1,
		TimestampMs:    meta.LastUpdatedMillis() + 1,
		ManifestList:   manifestList,
		Summary:        &Summary{Operation: OpAppend},
		SchemaID:       snapshotSchemaID,
	}
	require.NoError(t, builder.AddSnapshot(snapshot))
	require.NoError(t, builder.SetSnapshotRef(MainBranch, snapshotID, BranchRef))

	built, err := builder.Build()
	require.NoError(t, err)

	scan := &Scan{
		metadata:       built,
		ioF:            func(context.Context) (iceio.IO, error) { return fs, nil },
		planningMode:   ScanPlanningLocal,
		rowFilter:      iceberg.EqualTo(iceberg.Reference("id"), int32(5)),
		selectedFields: []string{"*"},
		caseSensitive:  true,
		snapshotID:     &snapshotID,
		options:        iceberg.Properties{},
		limit:          ScanNoLimit,
		concurrency:    1,
	}

	return scan, oldSchema, snapshotID
}

func TestTimeTravelUnknownSnapshotSchemaIDErrors(t *testing.T) {
	spec := iceberg.NewPartitionSpec()
	missingSchemaID := 999
	scan, _, snapshotID := newSchemaEvolutionScanWithSnapshot(t, &spec, iceio.NewMemFS(), "", &missingSchemaID)

	_, err := scan.Projection()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidMetadata)
	assert.ErrorContains(t, err, strconv.FormatInt(snapshotID, 10))
	assert.ErrorContains(t, err, strconv.Itoa(missingSchemaID))
}

func int32Bound(v int32) []byte {
	out := make([]byte, 4)
	binary.LittleEndian.PutUint32(out, uint32(v))

	return out
}

// TestSynthesizeRowLineageColumns verifies that _row_id and _last_updated_sequence_number
// are filled from task constants when those columns are present and null.
func TestSynthesizeRowLineageColumns(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	defer mem.AssertSize(t, 0)
	firstRowID := int64(1000)
	dataSeqNum := int64(5)
	task := FileScanTask{FirstRowID: &firstRowID, DataSequenceNumber: &dataSeqNum}
	cursor := (&rowPositionSource{}).cursor()

	// Build a batch with a data column plus _row_id and _last_updated_sequence_number (all nulls).
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			lineageArrowField(iceberg.RowIDColumnName, iceberg.RowIDFieldID),
			lineageArrowField(iceberg.LastUpdatedSequenceNumberColumnName, iceberg.LastUpdatedSequenceNumberFieldID),
		},
		nil,
	)
	const nrows = 3
	xBldr := array.NewInt64Builder(mem)
	defer xBldr.Release()
	xBldr.AppendValues([]int64{1, 2, 3}, nil)
	rowIDBldr := array.NewInt64Builder(mem)
	defer rowIDBldr.Release()
	rowIDBldr.AppendNulls(nrows)
	seqBldr := array.NewInt64Builder(mem)
	defer seqBldr.Release()
	seqBldr.AppendNulls(nrows)

	xArr := xBldr.NewArray()
	rowIDArr := rowIDBldr.NewArray()
	seqArr := seqBldr.NewArray()
	batch := array.NewRecordBatch(schema, []arrow.Array{xArr, rowIDArr, seqArr}, nrows)
	xArr.Release()
	rowIDArr.Release()
	seqArr.Release()
	defer batch.Release()

	out, err := synthesizeRowLineageColumns(ctx, cursor, task, batch, true, true)
	require.NoError(t, err)
	defer out.Release()

	// _row_id should be 1000, 1001, 1002
	rowIDCol := out.Column(1).(*array.Int64)
	require.Equal(t, nrows, rowIDCol.Len())
	for i := 0; i < nrows; i++ {
		assert.False(t, rowIDCol.IsNull(i), "row %d", i)
		assert.EqualValues(t, 1000+int64(i), rowIDCol.Value(i), "row %d", i)
	}
	// _last_updated_sequence_number should be 5 for all
	seqCol := out.Column(2).(*array.Int64)
	for i := 0; i < nrows; i++ {
		assert.False(t, seqCol.IsNull(i), "row %d", i)
		assert.EqualValues(t, 5, seqCol.Value(i), "row %d", i)
	}
	assert.EqualValues(t, 3, cursor.consumed)
}

// TestSynthesizeRowLineageColumnsPreservesExplicit covers the spec's null-
// coalescing rule: if a row already has an explicit (non-null) value in the
// source file, that value MUST be preserved; only null entries inherit the
// file-level constants. This is the case that arises when rewriting a file
// that already carries explicit lineage from a prior rewrite.
func TestSynthesizeRowLineageColumnsPreservesExplicit(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	defer mem.AssertSize(t, 0)

	firstRowID := int64(1000)
	dataSeqNum := int64(5)
	task := FileScanTask{FirstRowID: &firstRowID, DataSequenceNumber: &dataSeqNum}
	cursor := (&rowPositionSource{}).cursor()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			lineageArrowField(iceberg.RowIDColumnName, iceberg.RowIDFieldID),
			lineageArrowField(iceberg.LastUpdatedSequenceNumberColumnName, iceberg.LastUpdatedSequenceNumberFieldID),
		},
		nil,
	)
	const nrows = 3

	xBldr := array.NewInt64Builder(mem)
	defer xBldr.Release()
	xBldr.AppendValues([]int64{1, 2, 3}, nil)

	// Mixed _row_id: explicit 42, null, explicit 99. Non-null values must
	// survive untouched. Null gets firstRowID + row position = 1000 + 1.
	rowIDBldr := array.NewInt64Builder(mem)
	defer rowIDBldr.Release()
	rowIDBldr.AppendValues([]int64{42, 0, 99}, []bool{true, false, true})

	// Mixed _last_updated_sequence_number: explicit 7, null, explicit 9.
	// Null gets task.DataSequenceNumber = 5; others survive.
	seqBldr := array.NewInt64Builder(mem)
	defer seqBldr.Release()
	seqBldr.AppendValues([]int64{7, 0, 9}, []bool{true, false, true})

	xArr := xBldr.NewArray()
	rowIDArr := rowIDBldr.NewArray()
	seqArr := seqBldr.NewArray()
	batch := array.NewRecordBatch(schema, []arrow.Array{xArr, rowIDArr, seqArr}, nrows)
	xArr.Release()
	rowIDArr.Release()
	seqArr.Release()
	defer batch.Release()

	out, err := synthesizeRowLineageColumns(ctx, cursor, task, batch, true, true)
	require.NoError(t, err)
	defer out.Release()

	rowIDCol := out.Column(1).(*array.Int64)
	require.Equal(t, nrows, rowIDCol.Len())
	assert.False(t, rowIDCol.IsNull(0))
	assert.EqualValues(t, 42, rowIDCol.Value(0), "explicit _row_id must survive")
	assert.False(t, rowIDCol.IsNull(1))
	assert.EqualValues(t, 1001, rowIDCol.Value(1), "null _row_id must inherit firstRowID + position")
	assert.False(t, rowIDCol.IsNull(2))
	assert.EqualValues(t, 99, rowIDCol.Value(2), "explicit _row_id must survive even with a different value")

	seqCol := out.Column(2).(*array.Int64)
	require.Equal(t, nrows, seqCol.Len())
	assert.False(t, seqCol.IsNull(0))
	assert.EqualValues(t, 7, seqCol.Value(0), "explicit seq must survive")
	assert.False(t, seqCol.IsNull(1))
	assert.EqualValues(t, 5, seqCol.Value(1), "null seq must inherit task.DataSequenceNumber")
	assert.False(t, seqCol.IsNull(2))
	assert.EqualValues(t, 9, seqCol.Value(2), "explicit seq must survive even with a different value")

	assert.EqualValues(t, 3, cursor.consumed)
}

// TestSynthesizeRowLineageColumnsAppendsMissing covers the path where the
// lineage columns are absent from the batch: they must be appended (in _row_id,
// _last_updated_sequence_number order) and the input schema's metadata must
// survive the rebuild.
func TestSynthesizeRowLineageColumnsAppendsMissing(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	defer mem.AssertSize(t, 0)

	firstRowID := int64(1000)
	dataSeqNum := int64(5)
	task := FileScanTask{FirstRowID: &firstRowID, DataSequenceNumber: &dataSeqNum}
	cursor := (&rowPositionSource{}).cursor()

	md := arrow.NewMetadata([]string{"k"}, []string{"v"})
	schema := arrow.NewSchema(
		[]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, &md)
	const nrows = 3
	xBldr := array.NewInt64Builder(mem)
	defer xBldr.Release()
	xBldr.AppendValues([]int64{1, 2, 3}, nil)
	xArr := xBldr.NewArray()
	batch := array.NewRecordBatch(schema, []arrow.Array{xArr}, nrows)
	xArr.Release()
	defer batch.Release()

	out, err := synthesizeRowLineageColumns(ctx, cursor, task, batch, true, true)
	require.NoError(t, err)
	defer out.Release()

	require.EqualValues(t, 3, out.NumCols())
	assert.Equal(t, iceberg.RowIDColumnName, out.Schema().Field(1).Name)
	assert.Equal(t, iceberg.LastUpdatedSequenceNumberColumnName, out.Schema().Field(2).Name)

	rowIDCol := out.Column(1).(*array.Int64)
	seqCol := out.Column(2).(*array.Int64)
	for i := range nrows {
		assert.EqualValues(t, 1000+int64(i), rowIDCol.Value(i), "row %d", i)
		assert.EqualValues(t, 5, seqCol.Value(i), "row %d", i)
	}

	got, ok := out.Schema().Metadata().GetValue("k")
	assert.True(t, ok, "input schema metadata must be preserved")
	assert.Equal(t, "v", got)
}

// TestSynthesizeRowLineageColumnsRejectsWrongType guards against a panic: an
// existing _row_id column of the wrong type would otherwise leave the field's
// declared type and the replacement int64 array inconsistent.
func TestSynthesizeRowLineageColumnsRejectsWrongType(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	ctx := compute.WithAllocator(t.Context(), mem)
	defer mem.AssertSize(t, 0)

	firstRowID := int64(1000)
	task := FileScanTask{FirstRowID: &firstRowID}
	cursor := (&rowPositionSource{}).cursor()

	wrongField := lineageArrowField(iceberg.RowIDColumnName, iceberg.RowIDFieldID)
	wrongField.Type = arrow.BinaryTypes.String
	schema := arrow.NewSchema([]arrow.Field{wrongField}, nil)

	bldr := array.NewStringBuilder(mem)
	defer bldr.Release()
	bldr.AppendValues([]string{"a", "b"}, nil)
	arr := bldr.NewArray()
	batch := array.NewRecordBatch(schema, []arrow.Array{arr}, 2)
	arr.Release()
	defer batch.Release()

	_, err := synthesizeRowLineageColumns(ctx, cursor, task, batch, true, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "want int64")
}

// TestRowPositionCursor covers the position reconstruction that lets row-group
// pruning stay enabled: surviving spans map emitted rows back to their original
// file positions, jumping across the gaps left by pruned groups even when a
// single sequence of calls crosses a span seam.
func TestRowPositionCursor(t *testing.T) {
	tests := []struct {
		name  string
		spans []internal.RowGroupSpan
		calls int
		want  []int64
	}{
		{
			name:  "no spans falls back to contiguous from zero",
			spans: nil,
			calls: 4,
			want:  []int64{0, 1, 2, 3},
		},
		{
			name:  "pruned leading group starts at its file position",
			spans: []internal.RowGroupSpan{{FirstRowPos: 5, NumRows: 5}},
			calls: 5,
			want:  []int64{5, 6, 7, 8, 9},
		},
		{
			name:  "gap between surviving groups is skipped across the seam",
			spans: []internal.RowGroupSpan{{FirstRowPos: 0, NumRows: 5}, {FirstRowPos: 10, NumRows: 5}},
			calls: 10,
			want:  []int64{0, 1, 2, 3, 4, 10, 11, 12, 13, 14},
		},
		{
			name:  "empty leading span is stepped over",
			spans: []internal.RowGroupSpan{{FirstRowPos: 0, NumRows: 0}, {FirstRowPos: 7, NumRows: 3}},
			calls: 3,
			want:  []int64{7, 8, 9},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor := (&rowPositionSource{spans: tt.spans}).cursor()
			got := make([]int64, tt.calls)
			for i := range got {
				got[i] = cursor.next()
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

// lineageArrowField builds an Int64 Arrow field tagged with the reserved
// row-lineage field id, mirroring how the Parquet reader tags columns.
func lineageArrowField(name string, fieldID int) arrow.Field {
	return arrow.Field{
		Name:     name,
		Type:     arrow.PrimitiveTypes.Int64,
		Nullable: true,
		Metadata: arrow.NewMetadata([]string{ArrowParquetFieldIDKey}, []string{strconv.Itoa(fieldID)}),
	}
}

// TestProjectionV3SelectRowLineageColumns verifies that explicitly selecting
// _row_id (and _last_updated_sequence_number) on a v3 table yields a projection
// containing those metadata columns, even though they are not declared in the
// user schema. This is the legacy "Select(_row_id)" escape hatch.
func TestProjectionV3SelectRowLineageColumns(t *testing.T) {
	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	metadata, err := NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"s3://test-bucket/test_table",
		iceberg.Properties{"format-version": "3"},
	)
	require.NoError(t, err)
	assert.Equal(t, 3, metadata.Version(), "sanity: must be v3")

	scan := &Scan{
		metadata:       metadata,
		selectedFields: []string{"id", "payload", iceberg.RowIDColumnName, iceberg.LastUpdatedSequenceNumberColumnName},
		caseSensitive:  true,
	}

	proj, err := scan.Projection()
	require.NoError(t, err, "Projection must accept lineage column names on v3")
	require.NotNil(t, proj)

	fields := proj.Fields()
	require.Len(t, fields, 4, "projected schema must contain id, payload, _row_id, _last_updated_sequence_number")

	fieldByName := make(map[string]iceberg.NestedField, len(fields))
	for _, f := range fields {
		fieldByName[f.Name] = f
	}

	rowIDField, ok := fieldByName[iceberg.RowIDColumnName]
	require.True(t, ok, "_row_id must be in projection")
	assert.Equal(t, iceberg.RowIDFieldID, rowIDField.ID)
	assert.False(t, rowIDField.Required, "_row_id must be optional")

	seqField, ok := fieldByName[iceberg.LastUpdatedSequenceNumberColumnName]
	require.True(t, ok, "_last_updated_sequence_number must be in projection")
	assert.Equal(t, iceberg.LastUpdatedSequenceNumberFieldID, seqField.ID)
	assert.False(t, seqField.Required, "_last_updated_sequence_number must be optional")
}

// TestProjectionRowLineageRejectedOnV1V2 asserts that requesting a row-lineage
// metadata column via Select on a v1 or v2 table is an error (those format
// versions do not support row lineage).
func TestProjectionRowLineageRejectedOnV1V2(t *testing.T) {
	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	for _, tc := range []struct {
		name string
		ver  string
	}{
		{name: "v1", ver: "1"},
		{name: "v2", ver: "2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata, err := NewMetadata(
				schema,
				iceberg.UnpartitionedSpec,
				UnsortedSortOrder,
				"s3://test-bucket/test_table",
				iceberg.Properties{PropertyFormatVersion: tc.ver},
			)
			require.NoError(t, err)

			scan := &Scan{
				metadata:       metadata,
				selectedFields: []string{"id", iceberg.RowIDColumnName},
				caseSensitive:  true,
			}

			_, err = scan.Projection()
			require.Error(t, err, "Projection must reject lineage columns on pre-v3 tables")
			assert.ErrorIs(t, err, ErrInvalidOperation)
			assert.ErrorContains(t, err, iceberg.RowIDColumnName)
			assert.ErrorContains(t, err, "format version")
		})
	}
}

// TestProjectionWithRowLineageRequiresV3 asserts that the WithRowLineage scan
// option errors out on Projection() when the table format version is below 3.
func TestProjectionWithRowLineageRequiresV3(t *testing.T) {
	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	metadata, err := NewMetadata(
		schema,
		iceberg.UnpartitionedSpec,
		UnsortedSortOrder,
		"s3://test-bucket/test_table",
		iceberg.Properties{PropertyFormatVersion: "2"},
	)
	require.NoError(t, err)

	scan := &Scan{
		metadata:          metadata,
		selectedFields:    []string{"*"},
		caseSensitive:     true,
		includeRowLineage: true,
	}

	_, err = scan.Projection()
	require.Error(t, err, "WithRowLineage on a v2 table must be an explicit error")
	assert.ErrorContains(t, err, "row lineage")
}

// TestProjectionV3SchemaAlreadyHasRowID covers a v3 table whose stored schema
// already declares _row_id at its reserved field ID. NewMetadata now rejects
// such user schemas, but tables loaded from a catalog (e.g. written by Java) can
// legitimately carry the reserved ID, so the metadata is parsed directly here to
// bypass the creation-time validator. The end-to-end projection must be
// idempotent and not duplicate the reserved column.
func TestProjectionV3SchemaAlreadyHasRowID(t *testing.T) {
	metadataJSON := `{
		"format-version": 3,
		"table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
		"location": "s3://test-bucket/test_table",
		"last-sequence-number": 0,
		"last-updated-ms": 1602638573590,
		"last-column-id": 2,
		"next-row-id": 0,
		"current-schema-id": 0,
		"schemas": [
			{
				"type": "struct",
				"schema-id": 0,
				"fields": [
					{"id": 1, "name": "id", "required": true, "type": "long"},
					{"id": 2, "name": "payload", "required": false, "type": "string"},
					{"id": 2147483540, "name": "_row_id", "required": false, "type": "long"}
				]
			}
		],
		"default-spec-id": 0,
		"partition-specs": [{"spec-id": 0, "fields": []}],
		"last-partition-id": 999,
		"default-sort-order-id": 0,
		"sort-orders": [{"order-id": 0, "fields": []}],
		"properties": {},
		"current-snapshot-id": -1,
		"snapshots": []
	}`

	metadata, err := ParseMetadataString(metadataJSON)
	require.NoError(t, err)
	assert.Equal(t, 3, metadata.Version(), "sanity: must be v3")

	scan := &Scan{
		metadata:          metadata,
		selectedFields:    []string{"*"},
		caseSensitive:     true,
		includeRowLineage: true,
	}

	require.NotPanics(t, func() {
		proj, perr := scan.Projection()
		require.NoError(t, perr)
		require.NotNil(t, proj)

		seen := make(map[int]string, len(proj.Fields()))
		for _, f := range proj.Fields() {
			if prev, dup := seen[f.ID]; dup {
				t.Fatalf("duplicate field id %d: %q and %q", f.ID, prev, f.Name)
			}
			seen[f.ID] = f.Name
		}
		assert.Contains(t, seen, iceberg.RowIDFieldID, "_row_id must survive projection")
	})
}

// A filter on the source column of an unknown partition field must not prune:
// the same manifest summary would prune under an identity transform.
func TestPlanFilesUnknownTransformDoesNotPrune(t *testing.T) {
	unknown, err := iceberg.ParseTransform("custom_transform[42]")
	require.NoError(t, err)

	lower, upper := int32Bound(10), int32Bound(20)
	dataPath := "mem://default/table/data.parquet"
	manifestPath := "mem://default/table/metadata/manifest.avro"
	manifestListPath := "mem://default/table/metadata/snap-7.avro"

	plan := func(t *testing.T, transform iceberg.Transform) []FileScanTask {
		t.Helper()

		spec := iceberg.NewPartitionSpecID(0, iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000, Name: "id_part", Transform: transform,
		})
		memIO := iceio.NewMemFS()
		scan, oldSchema, snapshotID := newSchemaEvolutionScanWithSnapshot(t, &spec, memIO, manifestListPath, nil)

		// Metrics bounds cover the filter value so only partition pruning is
		// in play.
		dataFile, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, dataPath,
			iceberg.ParquetFile, nil, nil, nil, 1, 1024)
		require.NoError(t, err)
		entry := iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &snapshotID,
			dataFile.LowerBoundValues(map[int][]byte{1: int32Bound(5)}).
				UpperBoundValues(map[int][]byte{1: int32Bound(5)}).
				Build(),
		).SequenceNum(1).Build()

		var manifestBytes bytes.Buffer
		_, err = iceberg.WriteManifest(manifestPath, &manifestBytes, 2, spec, oldSchema, snapshotID,
			[]iceberg.ManifestEntry{entry})
		require.NoError(t, err)
		require.NoError(t, memIO.WriteFile(manifestPath, manifestBytes.Bytes()))

		// Partition summary excludes the filter value (id == 5).
		manifest := iceberg.NewManifestFile(2, manifestPath, int64(manifestBytes.Len()), int32(spec.ID()), snapshotID).
			Partitions([]iceberg.FieldSummary{{ContainsNull: false, LowerBound: &lower, UpperBound: &upper}}).
			SequenceNum(1, 1).
			Build()

		var listBytes bytes.Buffer
		seqNum := int64(1)
		require.NoError(t, iceberg.WriteManifestList(2, &listBytes, snapshotID, nil, &seqNum, 0,
			[]iceberg.ManifestFile{manifest}))
		require.NoError(t, memIO.WriteFile(manifestListPath, listBytes.Bytes()))

		tasks, err := scan.PlanFiles(context.Background())
		require.NoError(t, err)

		return tasks
	}

	t.Run("identity prunes", func(t *testing.T) {
		assert.Empty(t, plan(t, iceberg.IdentityTransform{}))
	})

	t.Run("unknown does not prune", func(t *testing.T) {
		assert.Len(t, plan(t, unknown), 1)
	})
}

func TestArrowScanFiltersMissingColumnInitialDefault(t *testing.T) {
	tbl := buildV3TableWithRows(t, `[{"id":1,"data":"a"},{"id":2,"data":"b"}]`)
	decimalDefault := iceberg.Decimal{Val: decimal128.FromI64(1234), Scale: 2}

	txn := tbl.NewTransaction()
	require.NoError(t, txn.UpdateSchema(true, false).
		AddColumn(
			[]string{"new_col"},
			iceberg.PrimitiveTypes.Int32,
			"",
			false,
			iceberg.Int32Literal(42),
		).
		AddColumn(
			[]string{"new_decimal"},
			iceberg.DecimalTypeOf(9, 2),
			"",
			false,
			iceberg.DecimalLiteral(decimalDefault),
		).
		Commit())
	var err error
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	tests := []struct {
		name     string
		filter   iceberg.BooleanExpression
		expected int64
	}{
		{
			name:     "matching equality",
			filter:   iceberg.EqualTo(iceberg.Reference("new_col"), int32(42)),
			expected: 2,
		},
		{
			name:     "mismatching equality",
			filter:   iceberg.EqualTo(iceberg.Reference("new_col"), int32(7)),
			expected: 0,
		},
		{
			name:     "is null",
			filter:   iceberg.IsNull(iceberg.Reference("new_col")),
			expected: 0,
		},
		{
			name:     "not null",
			filter:   iceberg.NotNull(iceberg.Reference("new_col")),
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scan := tbl.Scan(
				WithSelectedFields("id", "new_col"),
				WithRowFilter(tt.filter),
			)
			tasks, err := scan.PlanFiles(t.Context())
			require.NoError(t, err)
			require.Len(t, tasks, 1, "manifest planning must retain the old file")

			result, err := scan.ToArrowTable(t.Context())
			require.NoError(t, err)
			defer result.Release()
			require.Equal(t, tt.expected, result.NumRows())

			for _, chunk := range result.Column(1).Data().Chunks() {
				defaults := chunk.(*array.Int32)
				for i := range defaults.Len() {
					require.Equal(t, int32(42), defaults.Value(i))
				}
			}
		})
	}

	t.Run("matching decimal", func(t *testing.T) {
		scan := tbl.Scan(
			WithSelectedFields("id", "new_decimal"),
			WithRowFilter(iceberg.EqualTo(
				iceberg.Reference("new_decimal"),
				decimalDefault,
			)),
		)
		tasks, err := scan.PlanFiles(t.Context())
		require.NoError(t, err)
		require.Len(t, tasks, 1, "manifest planning must retain the old file")

		result, err := scan.ToArrowTable(t.Context())
		require.NoError(t, err)
		defer result.Release()
		require.Equal(t, int64(2), result.NumRows())

		for _, chunk := range result.Column(1).Data().Chunks() {
			defaults := chunk.(*array.Decimal128)
			for i := range defaults.Len() {
				require.Equal(t, decimal128.FromI64(1234), defaults.Value(i))
			}
		}
	})
}
