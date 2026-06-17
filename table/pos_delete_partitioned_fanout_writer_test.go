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
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/DataDog/iceberg-go"
	"github.com/DataDog/iceberg-go/internal"
	"github.com/DataDog/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPositionDeletePartitionedFanoutWriterProcessBatch(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                   string
		pathToPartitionContext map[string]partitionContext
		ctx                    context.Context
		input                  arrow.RecordBatch
		expectedDataFile       iceberg.DataFile
		expectedErr            error
	}{
		{
			name:             "empty batch",
			input:            mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema, `[]`),
			expectedDataFile: nil,
		},
		{
			name:        "error on missing required path to partition data",
			input:       mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema, `[{"file_path": "file://test_path.parquet", "pos": 0}]`),
			expectedErr: errors.New("unexpected missing partition context"),
		},
		{
			name:        "abort on context already done",
			ctx:         onlyContext(context.WithDeadline(context.Background(), time.UnixMilli(0))),
			input:       mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema, `[]`),
			expectedErr: errors.New("context deadline exceeded"),
		},
		{
			name:                   "error on partition context pointing to unknown partition spec",
			pathToPartitionContext: map[string]partitionContext{"file://namespace/age_bucket=1/test.parquet": {partitionData: map[int]any{iceberg.PartitionDataIDStart: 1}, specID: 200}},
			input:                  mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema, `[{"file_path": "file://namespace/age_bucket=1/test.parquet", "pos": 100}]`),
			expectedErr:            errors.New("unexpected missing partition spec"),
		},
		{
			name:                   "success",
			pathToPartitionContext: map[string]partitionContext{"file://namespace/age_bucket=1/test.parquet": {partitionData: map[int]any{iceberg.PartitionDataIDStart: 1}, specID: 0}},
			input:                  mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema, `[{"file_path": "file://namespace/age_bucket=1/test.parquet", "pos": 100}]`),
			expectedDataFile:       &mockDataFile{columnSizes: map[int]int64{2147483545: 88, 2147483546: 174}, format: iceberg.ParquetFile, partition: map[int]any{iceberg.PartitionDataIDStart: 1}, count: 1, specid: 0, contentType: iceberg.EntryContentPosDeletes, sortOrderID: ptr(1)},
		},
		// This test case illustrates how the positionDeletePartitionedFanoutWriter does not validate that all records
		// in a batch have the same file path. Doing so would be prohibitive in the current implementation and
		// the usage of the positionDeletePartitionedFanoutWriter is expected to ensure batches all have the same
		// file_path value.
		{
			name:                   "batch with records having different file paths",
			pathToPartitionContext: map[string]partitionContext{"file://namespace/age_bucket=1/test.parquet": {partitionData: map[int]any{iceberg.PartitionDataIDStart: 1}, specID: 0}},
			input:                  mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema, `[{"file_path": "file://namespace/age_bucket=1/test.parquet", "pos": 100}, {"file_path": "file://namespace/age_bucket=0/test.parquet", "pos": 10}]`),
			expectedDataFile:       &mockDataFile{columnSizes: map[int]int64{2147483545: 96, 2147483546: 187}, format: iceberg.ParquetFile, partition: map[int]any{iceberg.PartitionDataIDStart: 1}, count: 2, specid: 0, contentType: iceberg.EntryContentPosDeletes, sortOrderID: ptr(1)},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := tc.ctx
			if ctx == nil {
				ctx = t.Context()
			}

			partitionSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{2},
				Name:      "age_bucket",
				Transform: iceberg.BucketTransform{
					NumBuckets: 2,
				},
			})

			metadataBuilder, err := NewMetadataBuilder(2)
			require.NoError(t, err)
			err = metadataBuilder.AddSchema(iceberg.NewSchema(0, append(iceberg.PositionalDeleteSchema.Fields(), iceberg.NestedField{Name: "age", ID: 2, Type: iceberg.Int64Type{}})...))
			require.NoError(t, err)
			err = metadataBuilder.SetCurrentSchemaID(0)
			require.NoError(t, err)
			err = metadataBuilder.AddPartitionSpec(&partitionSpec, true)
			require.NoError(t, err)
			err = metadataBuilder.SetDefaultSpecID(0)
			require.NoError(t, err)
			sortOrder, err := NewSortOrder(1, []SortField{{
				SourceIDs: []int{2},
				Direction: SortASC,
				Transform: iceberg.IdentityTransform{},
				NullOrder: NullsFirst,
			}})
			require.NoError(t, err)
			err = metadataBuilder.AddSortOrder(&sortOrder)
			require.NoError(t, err)
			err = metadataBuilder.SetDefaultSortOrderID(1)
			require.NoError(t, err)
			latestMeta, err := metadataBuilder.Build()
			require.NoError(t, err)

			writeUUID := uuid.New()
			cw := newConcurrentDataFileWriter(func(rootLocation string, fs io.WriteFileIO, meta *MetadataBuilder, props iceberg.Properties, opts ...dataFileWriterOption) (dataFileWriter, error) {
				return newPositionDeleteWriter(rootLocation, fs, meta, props, opts...)
			})
			factory, err := newWriterFactory(t.TempDir(), recordWritingArgs{
				fs:        &io.LocalFS{},
				sc:        PositionalDeleteArrowSchema,
				writeUUID: &writeUUID,
				counter:   internal.Counter(0),
			}, metadataBuilder, iceberg.PositionalDeleteSchema, 1024*1024,
				withContentType(iceberg.EntryContentPosDeletes),
				withFactoryFileSchema(iceberg.PositionalDeleteSchema))
			require.NoError(t, err)
			writer := newPositionDeletePartitionedFanoutWriter(latestMeta, cw, tc.pathToPartitionContext, nil, factory)

			dataFileCh := make(chan iceberg.DataFile, 10)
			err = writer.processBatch(ctx, tc.input, dataFileCh)
			if tc.expectedErr != nil {
				require.ErrorContains(t, err, tc.expectedErr.Error())

				return
			}
			require.NoError(t, err)

			err = factory.closeAll()
			require.NoError(t, err)

			close(dataFileCh)

			actualDataFile := <-dataFileCh
			assert.NoError(t, equalsDataFile(tc.expectedDataFile, actualDataFile, defaultPositionDeleteMatching...))
		})
	}
}

func TestPositionDeletePartitionedFanoutWriterPartitionPathIsDeterministic(t *testing.T) {
	t.Parallel()

	partitionSpec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			FieldID:   1000,
			SourceIDs: []int{2147483546}, // file_path
			Name:      "file_path",
			Transform: iceberg.IdentityTransform{},
		},
		iceberg.PartitionField{
			FieldID:   1001,
			SourceIDs: []int{2147483545}, // pos
			Name:      "pos",
			Transform: iceberg.IdentityTransform{},
		},
		iceberg.PartitionField{
			FieldID:   1002,
			SourceIDs: []int{2147483545}, // pos
			Name:      "pos_bucket",
			Transform: iceberg.BucketTransform{
				NumBuckets: 128,
			},
		},
	)

	metadataBuilder, err := NewMetadataBuilder(2)
	require.NoError(t, err)
	err = metadataBuilder.AddSchema(iceberg.PositionalDeleteSchema)
	require.NoError(t, err)
	err = metadataBuilder.SetCurrentSchemaID(0)
	require.NoError(t, err)
	err = metadataBuilder.AddPartitionSpec(&partitionSpec, true)
	require.NoError(t, err)
	err = metadataBuilder.SetDefaultSpecID(0)
	require.NoError(t, err)
	sortOrder, err := NewSortOrder(1, []SortField{{
		SourceIDs: []int{2147483546},
		Direction: SortASC,
		Transform: iceberg.IdentityTransform{},
		NullOrder: NullsFirst,
	}})
	require.NoError(t, err)
	err = metadataBuilder.AddSortOrder(&sortOrder)
	require.NoError(t, err)
	err = metadataBuilder.SetDefaultSortOrderID(1)
	require.NoError(t, err)

	latestMeta, err := metadataBuilder.Build()
	require.NoError(t, err)

	writer := &positionDeletePartitionedFanoutWriter{
		metadata: latestMeta,
	}

	ctx := partitionContext{
		specID: 0,
		partitionData: map[int]any{
			1000: "file://ns/data-file.parquet",
			1001: int64(42),
			1002: int32(7),
		},
	}

	expectedPath := partitionSpec.PartitionToPath(partitionRecord{
		ctx.partitionData[1000],
		ctx.partitionData[1001],
		ctx.partitionData[1002],
	}, latestMeta.CurrentSchema())

	// run multiple times to ensure it consistently
	// produces the same output for the same input context
	seen := make(map[string]struct{})
	for range 1024 {
		path, err := writer.partitionPath(ctx)
		require.NoError(t, err)
		seen[path] = struct{}{}
	}

	require.Lenf(t, seen, 1, "partition path must be stable for the same input map, got paths: %v", slices.Collect(maps.Keys(seen)))
	require.Contains(t, seen, expectedPath)
}

// TestPositionDeletePartitionedFanoutWriterPartitionPathUsesTableSchema is a
// regression test for https://github.com/apache/iceberg-go/issues/1082.
//
// The partitioned position-delete writer used to derive partition paths from
// iceberg.PositionalDeleteSchema. That schema only contains file_path/pos, so
// any partition field whose source column lives on the table data schema was
// silently dropped by PartitionSpec.PartitionType, collapsing distinct target
// partitions into a single empty partition path. Downstream, the rolling
// writer factory keys writers by that path, so position-delete rows for
// different target partitions ended up in the same delete file.
//
// This test pins the fix: when the partition spec references a real table
// data column, two partitionContexts with different partition values must
// produce distinct, non-empty partition paths.
func TestPositionDeletePartitionedFanoutWriterPartitionPathUsesTableSchema(t *testing.T) {
	t.Parallel()

	// Table schema: id int + data string. Partition identity on `id`.
	tableSchema := iceberg.NewSchema(
		0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	partitionSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		FieldID:   1000,
		SourceIDs: []int{1},
		Name:      "id",
		Transform: iceberg.IdentityTransform{},
	})

	metadataBuilder, err := NewMetadataBuilder(2)
	require.NoError(t, err)
	require.NoError(t, metadataBuilder.AddSchema(tableSchema))
	require.NoError(t, metadataBuilder.SetCurrentSchemaID(0))
	require.NoError(t, metadataBuilder.AddPartitionSpec(&partitionSpec, true))
	require.NoError(t, metadataBuilder.SetDefaultSpecID(0))
	sortOrder, err := NewSortOrder(1, []SortField{{
		SourceIDs: []int{1},
		Direction: SortASC,
		Transform: iceberg.IdentityTransform{},
		NullOrder: NullsFirst,
	}})
	require.NoError(t, err)
	require.NoError(t, metadataBuilder.AddSortOrder(&sortOrder))
	require.NoError(t, metadataBuilder.SetDefaultSortOrderID(1))

	latestMeta, err := metadataBuilder.Build()
	require.NoError(t, err)

	writer := &positionDeletePartitionedFanoutWriter{
		metadata: latestMeta,
	}

	ctxA := partitionContext{specID: 0, partitionData: map[int]any{1000: int32(1)}}
	ctxB := partitionContext{specID: 0, partitionData: map[int]any{1000: int32(2)}}

	pathA, err := writer.partitionPath(ctxA)
	require.NoError(t, err)
	pathB, err := writer.partitionPath(ctxB)
	require.NoError(t, err)

	// Distinct, non-empty, table-schema-derived paths. If the writer reverts to using
	// PositionalDeleteSchema, both calls collapse to "" and the rolling-writer factory
	// keys them onto the same writer — the bug from #1082.
	assert.Equal(t, "id=1", pathA)
	assert.Equal(t, "id=2", pathB)
}

// TestPositionDeletePartitionedFanoutWriterRoutesPartitionsIndependently is the end-to-end
// regression for https://github.com/apache/iceberg-go/issues/1082. It drives processBatch
// with two batches whose target data files live in distinct partitions and asserts that
// the writer factory produced two separate delete files, each tagged with the right
// partition. Pre-fix, both batches collapsed onto one writer (one output file, wrong
// partition metadata for the second batch's rows).
func TestPositionDeletePartitionedFanoutWriterRoutesPartitionsIndependently(t *testing.T) {
	t.Parallel()

	tableSchema := iceberg.NewSchema(
		0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	partitionSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		FieldID:   1000,
		SourceIDs: []int{1},
		Name:      "id",
		Transform: iceberg.IdentityTransform{},
	})

	metadataBuilder, err := NewMetadataBuilder(2)
	require.NoError(t, err)
	require.NoError(t, metadataBuilder.AddSchema(tableSchema))
	require.NoError(t, metadataBuilder.SetCurrentSchemaID(0))
	require.NoError(t, metadataBuilder.AddPartitionSpec(&partitionSpec, true))
	require.NoError(t, metadataBuilder.SetDefaultSpecID(0))
	sortOrder, err := NewSortOrder(1, []SortField{{
		SourceIDs: []int{1},
		Direction: SortASC,
		Transform: iceberg.IdentityTransform{},
		NullOrder: NullsFirst,
	}})
	require.NoError(t, err)
	require.NoError(t, metadataBuilder.AddSortOrder(&sortOrder))
	require.NoError(t, metadataBuilder.SetDefaultSortOrderID(1))
	latestMeta, err := metadataBuilder.Build()
	require.NoError(t, err)

	const (
		pathA = "file://t/id=1/a.parquet"
		pathB = "file://t/id=2/b.parquet"
	)
	pathToCtx := map[string]partitionContext{
		pathA: {partitionData: map[int]any{1000: int32(1)}, specID: 0},
		pathB: {partitionData: map[int]any{1000: int32(2)}, specID: 0},
	}

	writeUUID := uuid.New()
	cw := newConcurrentDataFileWriter(func(rootLocation string, fs io.WriteFileIO, meta *MetadataBuilder, props iceberg.Properties, opts ...dataFileWriterOption) (dataFileWriter, error) {
		return newPositionDeleteWriter(rootLocation, fs, meta, props, opts...)
	})
	factory, err := newWriterFactory(t.TempDir(), recordWritingArgs{
		fs:        &io.LocalFS{},
		sc:        PositionalDeleteArrowSchema,
		writeUUID: &writeUUID,
		counter:   internal.Counter(0),
	}, metadataBuilder, iceberg.PositionalDeleteSchema, 1024*1024,
		withContentType(iceberg.EntryContentPosDeletes),
		withFactoryFileSchema(iceberg.PositionalDeleteSchema))
	require.NoError(t, err)
	writer := newPositionDeletePartitionedFanoutWriter(latestMeta, cw, pathToCtx, nil, factory)

	dataFileCh := make(chan iceberg.DataFile, 4)

	batchA := mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema,
		fmt.Sprintf(`[{"file_path": %q, "pos": 0},{"file_path": %q, "pos": 1}]`, pathA, pathA))
	batchB := mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema,
		fmt.Sprintf(`[{"file_path": %q, "pos": 5}]`, pathB))

	require.NoError(t, writer.processBatch(t.Context(), batchA, dataFileCh))
	require.NoError(t, writer.processBatch(t.Context(), batchB, dataFileCh))
	require.NoError(t, factory.closeAll())
	close(dataFileCh)

	var files []iceberg.DataFile
	for df := range dataFileCh {
		files = append(files, df)
	}
	require.Len(t, files, 2, "expected one delete file per target partition; pre-fix collapse produces only 1")

	byPart := make(map[int32]iceberg.DataFile, 2)
	for _, df := range files {
		part, ok := df.Partition()[1000].(int32)
		require.True(t, ok, "DataFile.Partition()[1000] must be int32, got %T", df.Partition()[1000])
		byPart[part] = df
	}
	require.Contains(t, byPart, int32(1))
	require.Contains(t, byPart, int32(2))
	assert.Equal(t, int64(2), byPart[1].Count(), "id=1 delete file must contain only the two rows targeting pathA")
	assert.Equal(t, int64(1), byPart[2].Count(), "id=2 delete file must contain only the one row targeting pathB")
}

func TestPositionDeletePartitionedNoGoroutineLeak(t *testing.T) {
	t.Parallel()

	partitionSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2},
		Name:      "age_bucket",
		Transform: iceberg.BucketTransform{
			NumBuckets: 2,
		},
	})

	metadataBuilder, err := NewMetadataBuilder(2)
	require.NoError(t, err)
	err = metadataBuilder.AddSchema(iceberg.NewSchema(0, append(iceberg.PositionalDeleteSchema.Fields(), iceberg.NestedField{Name: "age", ID: 2, Type: iceberg.Int64Type{}})...))
	require.NoError(t, err)
	err = metadataBuilder.SetCurrentSchemaID(0)
	require.NoError(t, err)
	err = metadataBuilder.AddPartitionSpec(&partitionSpec, true)
	require.NoError(t, err)
	err = metadataBuilder.SetDefaultSpecID(0)
	require.NoError(t, err)
	sortOrder, err := NewSortOrder(1, []SortField{{
		SourceIDs: []int{2},
		Direction: SortASC,
		Transform: iceberg.IdentityTransform{},
		NullOrder: NullsFirst,
	}})
	require.NoError(t, err)
	err = metadataBuilder.AddSortOrder(&sortOrder)
	require.NoError(t, err)
	err = metadataBuilder.SetDefaultSortOrderID(1)
	require.NoError(t, err)

	tmpDir := t.TempDir()

	// Allow goroutines from prior tests to settle.
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	before := runtime.NumGoroutine()

	iterations := 50
	for range iterations {
		writeUUID := uuid.New()
		emptyItr := func(yield func(arrow.RecordBatch, error) bool) {}

		itr := positionDeleteRecordsToDataFiles(t.Context(), tmpDir, metadataBuilder,
			map[string]partitionContext{}, recordWritingArgs{
				sc:        PositionalDeleteArrowSchema,
				itr:       emptyItr,
				fs:        &io.LocalFS{},
				writeUUID: &writeUUID,
				counter:   internal.Counter(0),
			})

		for range itr {
		}
	}

	// Allow leaked goroutines to appear.
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()

	// Before the fix, each iteration leaked a goroutine from iter.Pull(args.counter)
	// being called unconditionally but stopCount never invoked in the partitioned path.
	// Allow a small margin for background runtime goroutines.
	assert.LessOrEqual(t, after, before+5,
		"expected no goroutine growth after %d iterations, got %d -> %d (delta: %d)",
		iterations, before, after, after-before)
}

func onlyContext(ctx context.Context, _ func()) context.Context {
	return ctx
}

// dataFileMatcher implements a custom "matcher" that compares DataFiles. Because not all fields are always
// important to validate, the dataFileMatcher can be passed in a number of matching options to define
// which of its fields are compared during matching.
type dataFileMatcher struct {
	matchers   []fieldMatcher
	formatters []formatter
}

type (
	dataFileMatcherOption func(m *dataFileMatcher)
	fieldMatcher          func(expected iceberg.DataFile, actual iceberg.DataFile) bool
	formatter             func(val iceberg.DataFile) string
)

func withFileFormatMatching() dataFileMatcherOption {
	return func(m *dataFileMatcher) {
		m.matchers = append(m.matchers, func(expected iceberg.DataFile, actual iceberg.DataFile) bool {
			return expected.FileFormat() == actual.FileFormat()
		})
		m.formatters = append(m.formatters, func(val iceberg.DataFile) string {
			return fmt.Sprintf("FileFormat: %s", val.FileFormat())
		})
	}
}

func withPathMatching() dataFileMatcherOption {
	return func(m *dataFileMatcher) {
		m.matchers = append(m.matchers, func(expected iceberg.DataFile, actual iceberg.DataFile) bool {
			return expected.FilePath() == actual.FilePath()
		})
		m.formatters = append(m.formatters, func(val iceberg.DataFile) string {
			return "FilePath: " + val.FilePath()
		})
	}
}

func withSpecIDMatching() dataFileMatcherOption {
	return func(m *dataFileMatcher) {
		m.matchers = append(m.matchers, func(expected iceberg.DataFile, actual iceberg.DataFile) bool {
			return expected.SpecID() == actual.SpecID()
		})
		m.formatters = append(m.formatters, func(val iceberg.DataFile) string {
			return fmt.Sprintf("SpecID: %d", val.SpecID())
		})
	}
}

func withPartitionMatching() dataFileMatcherOption {
	return func(m *dataFileMatcher) {
		m.matchers = append(m.matchers, func(expected iceberg.DataFile, actual iceberg.DataFile) bool {
			return maps.Equal(expected.Partition(), actual.Partition())
		})
		m.formatters = append(m.formatters, func(val iceberg.DataFile) string {
			return fmt.Sprintf("Partition: %v", val.Partition())
		})
	}
}

func withContentTypeMatching() dataFileMatcherOption {
	return func(m *dataFileMatcher) {
		m.matchers = append(m.matchers, func(expected iceberg.DataFile, actual iceberg.DataFile) bool {
			return expected.ContentType() == actual.ContentType()
		})
		m.formatters = append(m.formatters, func(val iceberg.DataFile) string {
			return fmt.Sprintf("ContentType: %s", val.ContentType())
		})
	}
}

func withCountMatching() dataFileMatcherOption {
	return func(m *dataFileMatcher) {
		m.matchers = append(m.matchers, func(expected iceberg.DataFile, actual iceberg.DataFile) bool {
			return expected.Count() == actual.Count()
		})
		m.formatters = append(m.formatters, func(val iceberg.DataFile) string {
			return fmt.Sprintf("Count: %d", val.Count())
		})
	}
}

func withColumnSizesMatching() dataFileMatcherOption {
	return func(m *dataFileMatcher) {
		m.matchers = append(m.matchers, func(expected iceberg.DataFile, actual iceberg.DataFile) bool {
			return maps.Equal(expected.ColumnSizes(), actual.ColumnSizes())
		})
		m.formatters = append(m.formatters, func(val iceberg.DataFile) string {
			return fmt.Sprintf("ColumnSizes: %v", val.ColumnSizes())
		})
	}
}

func withContentOffsetMatching() dataFileMatcherOption {
	return func(m *dataFileMatcher) {
		m.matchers = append(m.matchers, func(expected iceberg.DataFile, actual iceberg.DataFile) bool {
			return comparePointerAndValue(expected.ContentOffset(), actual.ContentOffset())
		})
		m.formatters = append(m.formatters, func(val iceberg.DataFile) string {
			if val.ContentSizeInBytes() == nil {
				return "ContentOffset: nil"
			}

			return fmt.Sprintf("ContentOffset: %d", *val.ContentOffset())
		})
	}
}

func withContentSizeInBytesMatching() dataFileMatcherOption {
	return func(m *dataFileMatcher) {
		m.matchers = append(m.matchers, func(expected iceberg.DataFile, actual iceberg.DataFile) bool {
			return comparePointerAndValue(expected.ContentSizeInBytes(), actual.ContentSizeInBytes())
		})
		m.formatters = append(m.formatters, func(val iceberg.DataFile) string {
			if val.ContentSizeInBytes() == nil {
				return "ContentSizeInBytes: nil"
			}

			return fmt.Sprintf("ContentSizeInBytes: %d", *val.ContentSizeInBytes())
		})
	}
}

func withSortOrderIDMatching() dataFileMatcherOption {
	return func(m *dataFileMatcher) {
		m.matchers = append(m.matchers, func(expected iceberg.DataFile, actual iceberg.DataFile) bool {
			return comparePointerAndValue(expected.SortOrderID(), actual.SortOrderID())
		})
		m.formatters = append(m.formatters, func(val iceberg.DataFile) string {
			if val.SortOrderID() == nil {
				return "SortOrderID: nil"
			}

			return fmt.Sprintf("SortOrderID: %d", *val.SortOrderID())
		})
	}
}

func comparePointerAndValue[T comparable](left *T, right *T) bool {
	if left == nil && right == nil {
		return true
	}
	if left == nil {
		return false
	}
	if right == nil {
		return false
	}

	return *left == *right
}

func (m *dataFileMatcher) Matches(expected iceberg.DataFile, actual iceberg.DataFile) bool {
	if expected == nil && actual == nil {
		return true
	}
	if expected == nil {
		return false
	}
	if actual == nil {
		return false
	}
	for _, m := range m.matchers {
		if !m(expected, actual) {
			return false
		}
	}

	return true
}

func (m *dataFileMatcher) Format(val iceberg.DataFile) string {
	if val == nil {
		return "nil"
	}
	values := make([]string, 0, len(m.formatters))
	for _, format := range m.formatters {
		values = append(values, format(val))
	}

	return fmt.Sprintf("{%s}", strings.Join(values, ", "))
}

// defaultPositionDeleteMatching is a convenience preset for the options we want to match for position delete matching
var defaultPositionDeleteMatching = []dataFileMatcherOption{withContentTypeMatching(), withColumnSizesMatching(), withCountMatching(), withFileFormatMatching(), withSpecIDMatching(), withPartitionMatching(), withCountMatching(), withSortOrderIDMatching()}

// ptr returns a pointer to v. A generic stand-in for the various one-off
// ptr-helpers previously sprinkled across the internal package tests.
func ptr[T any](v T) *T { return &v }

// TestPositionDeleteUnpartitionedSortOrderID covers the unpartitioned branch
// of positionDeleteRecordsToDataFiles: the resulting DataFiles must carry
// the table's default sort order id exactly like the partitioned branch does.
func TestPositionDeleteUnpartitionedSortOrderID(t *testing.T) {
	t.Parallel()

	metadataBuilder, err := NewMetadataBuilder(2)
	require.NoError(t, err)
	require.NoError(t, metadataBuilder.AddSchema(iceberg.PositionalDeleteSchema))
	require.NoError(t, metadataBuilder.SetCurrentSchemaID(0))
	unpartitioned := *iceberg.UnpartitionedSpec
	require.NoError(t, metadataBuilder.AddPartitionSpec(&unpartitioned, true))
	require.NoError(t, metadataBuilder.SetDefaultSpecID(0))
	sortOrder, err := NewSortOrder(1, []SortField{{
		SourceIDs: []int{2147483546}, // file_path
		Direction: SortASC,
		Transform: iceberg.IdentityTransform{},
		NullOrder: NullsFirst,
	}})
	require.NoError(t, err)
	require.NoError(t, metadataBuilder.AddSortOrder(&sortOrder))
	require.NoError(t, metadataBuilder.SetDefaultSortOrderID(-1))

	built, err := metadataBuilder.Build()
	require.NoError(t, err)
	expectedID := built.DefaultSortOrder()
	require.NotZero(t, expectedID, "sanity: sort order id should be non-zero")

	writeUUID := uuid.New()
	rb := mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema, `[{"file_path": "file://unpartitioned/test.parquet", "pos": 0}]`)
	itr := func(yield func(arrow.RecordBatch, error) bool) {
		rb.Retain()
		yield(rb, nil)
	}
	seq := positionDeleteRecordsToDataFiles(t.Context(), t.TempDir(), metadataBuilder, nil, recordWritingArgs{
		sc:        PositionalDeleteArrowSchema,
		itr:       itr,
		fs:        &io.LocalFS{},
		writeUUID: &writeUUID,
		counter:   internal.Counter(0),
	})

	var files []iceberg.DataFile
	for df, err := range seq {
		require.NoError(t, err)
		files = append(files, df)
	}
	require.NotEmpty(t, files, "expected at least one data file")
	for _, df := range files {
		require.NotNil(t, df.SortOrderID(), "unpartitioned pos-delete DataFile must carry sort order id")
		assert.Equal(t, expectedID, *df.SortOrderID())
	}
}

// TestEqualityDeleteUnpartitionedSortOrderID covers the unpartitioned branch
// of equalityDeleteRecordsToDataFiles: the resulting DataFiles must carry
// the table's default sort order id.
func TestEqualityDeleteUnpartitionedSortOrderID(t *testing.T) {
	t.Parallel()

	delSchema := iceberg.NewSchema(
		0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	metadataBuilder, err := NewMetadataBuilder(2)
	require.NoError(t, err)
	require.NoError(t, metadataBuilder.AddSchema(delSchema))
	require.NoError(t, metadataBuilder.SetCurrentSchemaID(0))
	unpartitioned := *iceberg.UnpartitionedSpec
	require.NoError(t, metadataBuilder.AddPartitionSpec(&unpartitioned, true))
	require.NoError(t, metadataBuilder.SetDefaultSpecID(0))
	sortOrder, err := NewSortOrder(1, []SortField{{
		SourceIDs: []int{1},
		Direction: SortASC,
		Transform: iceberg.IdentityTransform{},
		NullOrder: NullsFirst,
	}})
	require.NoError(t, err)
	require.NoError(t, metadataBuilder.AddSortOrder(&sortOrder))
	require.NoError(t, metadataBuilder.SetDefaultSortOrderID(-1))

	built, err := metadataBuilder.Build()
	require.NoError(t, err)
	expectedID := built.DefaultSortOrder()
	require.NotZero(t, expectedID, "sanity: sort order id should be non-zero")

	delArrowSc, err := SchemaToArrowSchema(delSchema, nil, true, false)
	require.NoError(t, err)

	writeUUID := uuid.New()
	rb := mustLoadRecordBatchFromJSON(delArrowSc, `[{"id": 2}, {"id": 4}]`)
	itr := func(yield func(arrow.RecordBatch, error) bool) {
		rb.Retain()
		yield(rb, nil)
	}
	seq, err := equalityDeleteRecordsToDataFiles(t.Context(), t.TempDir(), metadataBuilder, delSchema, []int{1}, recordWritingArgs{
		sc:        delArrowSc,
		itr:       itr,
		fs:        &io.LocalFS{},
		writeUUID: &writeUUID,
		counter:   internal.Counter(0),
	})
	require.NoError(t, err)

	var files []iceberg.DataFile
	for df, err := range seq {
		require.NoError(t, err)
		files = append(files, df)
	}
	require.NotEmpty(t, files, "expected at least one data file")
	for _, df := range files {
		require.NotNil(t, df.SortOrderID(), "unpartitioned eq-delete DataFile must carry sort order id")
		assert.Equal(t, expectedID, *df.SortOrderID())
	}
}

// equalsDataFile invokes a dataFileMatcher with the specified matching options and compares two DataFile values.
// Its return value is nil if both values are equal and an error with a meaningful formatted message to help
// show the mismatch in case they are not. This is meant to be used with testify like:
//
//	assert.NoError(t, equalsDataFile(expected, actual))
func equalsDataFile(expected iceberg.DataFile, actual iceberg.DataFile, opts ...dataFileMatcherOption) (err error) {
	matcher := &dataFileMatcher{}
	for _, apply := range opts {
		apply(matcher)
	}
	if !matcher.Matches(expected, actual) {
		return fmt.Errorf("Expected: %s\nActual:   %s", matcher.Format(expected), matcher.Format(actual))
	}

	return nil
}
