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
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	"github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPositionDeletePartitionedFanoutWriterProcessBatch(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                string
		pathToPartitionData map[string]map[int]any
		ctx                 context.Context
		input               arrow.RecordBatch
		expectedDataFile    iceberg.DataFile
		expectedErr         error
	}{
		{
			name:             "empty batch",
			input:            mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema, `[]`),
			expectedDataFile: nil,
		},
		{
			name:        "error on missing required path to partition data",
			input:       mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema, `[{"file_path": "file://test_path.parquet", "pos": 0}]`),
			expectedErr: errors.New("unexpected missing partition values for path"),
		},
		{
			name:        "abort on context already done",
			ctx:         onlyContext(context.WithDeadline(context.Background(), time.UnixMilli(0))),
			input:       mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema, `[]`),
			expectedErr: errors.New("context deadline exceeded"),
		},
		{
			name:                "success",
			pathToPartitionData: map[string]map[int]any{"file://namespace/age_bucket=1/test.parquet": {iceberg.PartitionDataIDStart: 1}},
			input:               mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema, `[{"file_path": "file://namespace/age_bucket=1/test.parquet", "pos": 100}]`),
			expectedDataFile:    &mockDataFile{columnSizes: map[int]int64{2147483545: 88, 2147483546: 174}, format: iceberg.ParquetFile, partition: map[int]any{iceberg.PartitionDataIDStart: 1}, count: 1, specid: 0, contentType: iceberg.EntryContentPosDeletes},
		},
		// This test case illustrates how the positionDeletePartitionedFanoutWriter does not validate that all records
		// in a batch have the same file path. Doing so would be prohibitive in the current implementation and
		// the usage of the positionDeletePartitionedFanoutWriter is expected to ensure batches all have the same
		// file_path value.
		{
			name:                "batch with records having different file paths",
			pathToPartitionData: map[string]map[int]any{"file://namespace/age_bucket=1/test.parquet": {iceberg.PartitionDataIDStart: 1}},
			input:               mustLoadRecordBatchFromJSON(PositionalDeleteArrowSchema, `[{"file_path": "file://namespace/age_bucket=1/test.parquet", "pos": 100}, {"file_path": "file://namespace/age_bucket=0/test.parquet", "pos": 10}]`),
			expectedDataFile:    &mockDataFile{columnSizes: map[int]int64{2147483545: 96, 2147483546: 187}, format: iceberg.ParquetFile, partition: map[int]any{iceberg.PartitionDataIDStart: 1}, count: 2, specid: 0, contentType: iceberg.EntryContentPosDeletes},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := tc.ctx
			if ctx == nil {
				ctx = t.Context()
			}

			partitionSpec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceID: 2,
				Name:     "age_bucket",
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

			writeUUID := uuid.New()
			cw := newConcurrentDataFileWriter(func(rootLocation string, fs io.WriteFileIO, meta *MetadataBuilder, props iceberg.Properties, opts ...dataFileWriterOption) (dataFileWriter, error) {
				return newPositionDeleteWriter(rootLocation, fs, meta, props, opts...)
			})
			writerFactory := NewWriterFactory(t.TempDir(), recordWritingArgs{
				fs:        &io.LocalFS{},
				sc:        PositionalDeleteArrowSchema,
				writeUUID: &writeUUID,
				counter:   internal.Counter(0),
			}, metadataBuilder, iceberg.PositionalDeleteSchema, 1024*1024)
			writer := newPositionDeletePartitionedFanoutWriter(partitionSpec, cw, tc.pathToPartitionData, nil, &writerFactory)
			require.NoError(t, err)

			dataFileCh := make(chan iceberg.DataFile, 10)
			err = writer.processBatch(ctx, tc.input, dataFileCh)
			if tc.expectedErr != nil {
				require.ErrorContains(t, err, tc.expectedErr.Error())

				return
			}
			require.NoError(t, err)

			err = writerFactory.closeAll()
			require.NoError(t, err)

			close(dataFileCh)

			actualDataFile := <-dataFileCh
			assert.NoError(t, equalsDataFile(tc.expectedDataFile, actualDataFile, defaultPositionDeleteMatching...))
		})
	}
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
var defaultPositionDeleteMatching = []dataFileMatcherOption{withContentTypeMatching(), withColumnSizesMatching(), withCountMatching(), withFileFormatMatching(), withSpecIDMatching(), withPartitionMatching(), withCountMatching()}

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
