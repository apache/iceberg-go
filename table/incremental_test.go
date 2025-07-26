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
	"io"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testUUID = uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

// testFS is a minimal implementation for testing
type testFS struct{}

func (t *testFS) Open(name string) (iceio.File, error)        { return nil, nil }
func (t *testFS) CreateWithContent(name string, content []byte) error { return nil }
func (t *testFS) Remove(name string) error                    { return nil }
func (t *testFS) ReadFile(name string) ([]byte, error)        { return nil, nil }
func (t *testFS) WriteFile(name string, data []byte) error    { return nil }
func (t *testFS) Size(name string) (int64, error)             { return 0, nil }

func (t *testFS) OpenWrite(name string) (io.WriteCloser, error) { return nil, nil }

func TestIncrementalScanInterfaces(t *testing.T) {
	// Create a minimal metadata for testing
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	metadata := &metadataV2{
		commonMetadata: commonMetadata{
			FormatVersion:   2,
			UUID:            testUUID,
			Loc:             "test-location",
			LastUpdatedMS:   12345,
			LastColumnId:    2,
			SchemaList:      []*iceberg.Schema{schema},
			CurrentSchemaID: 1,
			Specs:           []iceberg.PartitionSpec{iceberg.NewPartitionSpec()},
			DefaultSpecID:   0,
			Props:           iceberg.Properties{},
			SortOrderList:   []SortOrder{{}},
		},
	}

	testIO := func(ctx context.Context) (iceio.IO, error) {
		return &testFS{}, nil
	}

	t.Run("BaseIncrementalScan", func(t *testing.T) {
		scan := newBaseIncrementalScan(metadata, testIO)
		
		// Test interface compliance
		var _ IncrementalScan = scan
		
		// Test basic functionality
		result := scan.UseStartSnapshotID(123)
		assert.Equal(t, int64(123), *result.StartSnapshotID())
		
		result = result.UseEndSnapshotID(456)
		assert.Equal(t, int64(456), *result.EndSnapshotID())
		
		result = result.UseStartSnapshotExclusive(true)
		assert.True(t, result.IsStartSnapshotExclusive())
		
		// Test schema projection
		schema, err := result.Schema()
		require.NoError(t, err)
		assert.Equal(t, 2, len(schema.Fields()))
	})

	t.Run("IncrementalAppendScan", func(t *testing.T) {
		scan := newIncrementalAppendScan(metadata, testIO)
		
		// Test interface compliance
		var _ IncrementalAppendScan = scan
		var _ IncrementalScan = scan
		
		// Test append-specific methods
		result := scan.FromSnapshotInclusive(100)
		assert.Equal(t, int64(100), *result.StartSnapshotID())
		assert.False(t, result.IsStartSnapshotExclusive())
		
		result = scan.FromSnapshotExclusive(200)
		assert.Equal(t, int64(200), *result.StartSnapshotID())
		assert.True(t, result.IsStartSnapshotExclusive())
		
		result = scan.ToSnapshot(300)
		assert.Equal(t, int64(300), *result.EndSnapshotID())
	})

	t.Run("IncrementalChangelogScan", func(t *testing.T) {
		scan := newIncrementalChangelogScan(metadata, testIO)
		
		// Test interface compliance
		var _ IncrementalChangelogScan = scan
		var _ IncrementalScan = scan
		
		// Test changelog-specific methods
		result := scan.FromSnapshotInclusive(400)
		assert.Equal(t, int64(400), *result.StartSnapshotID())
		assert.False(t, result.IsStartSnapshotExclusive())
		
		result = scan.FromSnapshotExclusive(500)
		assert.Equal(t, int64(500), *result.StartSnapshotID())
		assert.True(t, result.IsStartSnapshotExclusive())
		
		result = scan.ToSnapshot(600)
		assert.Equal(t, int64(600), *result.EndSnapshotID())
	})
}

func TestTableIncrementalMethods(t *testing.T) {
	// Create a minimal table for testing
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	metadata := &metadataV2{
		commonMetadata: commonMetadata{
			FormatVersion:   2,
			UUID:            testUUID,
			Loc:             "test-location",
			LastUpdatedMS:   12345,
			LastColumnId:    2,
			SchemaList:      []*iceberg.Schema{schema},
			CurrentSchemaID: 1,
			Specs:           []iceberg.PartitionSpec{iceberg.NewPartitionSpec()},
			DefaultSpecID:   0,
			Props:           iceberg.Properties{},
			SortOrderList:   []SortOrder{{}},
		},
	}

	testIO := func(ctx context.Context) (iceio.IO, error) {
		return &testFS{}, nil
	}

	table := Table{
		metadata: metadata,
		fsF:      testIO,
	}

	t.Run("NewIncrementalScan", func(t *testing.T) {
		scan := table.NewIncrementalScan()
		assert.NotNil(t, scan)
		
		// Test that it's a base incremental scan
		baseScan, ok := scan.(*baseIncrementalScan)
		assert.True(t, ok)
		assert.Equal(t, metadata, baseScan.metadata)
	})

	t.Run("NewIncrementalAppendScan", func(t *testing.T) {
		scan := table.NewIncrementalAppendScan()
		assert.NotNil(t, scan)
		
		// Test that it's an incremental append scan
		appendScan, ok := scan.(*incrementalAppendScan)
		assert.True(t, ok)
		assert.Equal(t, metadata, appendScan.metadata)
	})

	t.Run("NewIncrementalChangelogScan", func(t *testing.T) {
		scan := table.NewIncrementalChangelogScan()
		assert.NotNil(t, scan)
		
		// Test that it's an incremental changelog scan
		changelogScan, ok := scan.(*incrementalChangelogScan)
		assert.True(t, ok)
		assert.Equal(t, metadata, changelogScan.metadata)
	})
}

func TestScanIncrementalMethods(t *testing.T) {
	// Create a minimal scan for testing
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	metadata := &metadataV2{
		commonMetadata: commonMetadata{
			FormatVersion:   2,
			UUID:            testUUID,
			Loc:             "test-location",
			LastUpdatedMS:   12345,
			LastColumnId:    2,
			SchemaList:      []*iceberg.Schema{schema},
			CurrentSchemaID: 1,
			Specs:           []iceberg.PartitionSpec{iceberg.NewPartitionSpec()},
			DefaultSpecID:   0,
			Props:           iceberg.Properties{},
			SortOrderList:   []SortOrder{{}},
			SnapshotList: []Snapshot{
				{SnapshotID: 100, TimestampMs: 1000},
				{SnapshotID: 200, TimestampMs: 2000},
				{SnapshotID: 300, TimestampMs: 3000},
			},
			CurrentSnapshotID: &[]int64{300}[0],
		},
	}

	testIO := func(ctx context.Context) (iceio.IO, error) {
		return &testFS{}, nil
	}

	scan := &Scan{
		metadata: metadata,
		ioF:      testIO,
	}

	t.Run("AppendsBetween", func(t *testing.T) {
		result := scan.AppendsBetween(100, 200)
		assert.NotNil(t, result)
		
		appendScan, ok := result.(*incrementalAppendScan)
		assert.True(t, ok)
		assert.Equal(t, int64(100), *appendScan.StartSnapshotID())
		assert.Equal(t, int64(200), *appendScan.EndSnapshotID())
		assert.True(t, appendScan.IsStartSnapshotExclusive())
	})

	t.Run("AppendsAfter", func(t *testing.T) {
		result := scan.AppendsAfter(100)
		assert.NotNil(t, result)
		
		appendScan, ok := result.(*incrementalAppendScan)
		assert.True(t, ok)
		assert.Equal(t, int64(100), *appendScan.StartSnapshotID())
		assert.Equal(t, int64(300), *appendScan.EndSnapshotID()) // Should use current snapshot
		assert.True(t, appendScan.IsStartSnapshotExclusive())
	})
}

func TestIncrementalScanOptions(t *testing.T) {
	metadata := &metadataV2{
		commonMetadata: commonMetadata{
			FormatVersion: 2,
			UUID:          testUUID,
			Loc:           "test-location",
			SchemaList: []*iceberg.Schema{
				iceberg.NewSchema(1,
					iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
				),
			},
			CurrentSchemaID: 1,
			Specs:           []iceberg.PartitionSpec{iceberg.NewPartitionSpec()},
			DefaultSpecID:   0,
			SortOrderList:   []SortOrder{{}},
		},
	}

	testIO := func(ctx context.Context) (iceio.IO, error) {
		return &testFS{}, nil
	}

	t.Run("FilterAndSelection", func(t *testing.T) {
		scan := newBaseIncrementalScan(metadata, testIO)
		
		// Test filter
		expr := iceberg.GreaterThan(iceberg.Reference("id"), int64(10))
		result := scan.Filter(expr)
		assert.Equal(t, expr, result.(*baseIncrementalScan).rowFilter)
		
		// Test column selection
		result = result.Select("id")
		assert.Equal(t, []string{"id"}, result.(*baseIncrementalScan).selectedFields)
		
		// Test case sensitivity
		result = result.CaseSensitive(false)
		assert.False(t, result.(*baseIncrementalScan).caseSensitive)
		
		// Test options
		result = result.Option("key", "value")
		assert.Equal(t, "value", result.(*baseIncrementalScan).options["key"])
	})
} 