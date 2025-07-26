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
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test helpers

func createTestTable(t *testing.T) (*Table, iceio.IO) {
	mockFS := &mockTestFS{}
	
	// Create a simple mock metadata
	mockMeta := &mockMetadata{
		currentSnap: &mockSnapshot{
			id: 1,
		},
	}
	
	table := &Table{
		metadata:         mockMeta,
		metadataLocation: "s3://test-bucket/test-table/metadata/1.json",
		fsF: func(ctx context.Context) (iceio.IO, error) {
			return mockFS, nil
		},
	}
	
	return table, mockFS
}

// testMockDataFile implements iceberg.DataFile for testing
type testMockDataFile struct {
	path        string
	count       int64
	contentType iceberg.ManifestEntryContent
}

func createMockDataFile(path string, count int64, contentType iceberg.ManifestEntryContent) *testMockDataFile {
	return &testMockDataFile{
		path:        path,
		count:       count,
		contentType: contentType,
	}
}

func (m *testMockDataFile) Location() string                             { return m.path }
func (m *testMockDataFile) FilePath() string                             { return m.path }
func (m *testMockDataFile) Count() int64                                 { return m.count }
func (m *testMockDataFile) FileSizeBytes() int64                         { return 1000 }
func (m *testMockDataFile) ContentType() iceberg.ManifestEntryContent    { return m.contentType }
func (m *testMockDataFile) Format() iceberg.FileFormat                   { return iceberg.ParquetFile }
func (m *testMockDataFile) FileFormat() iceberg.FileFormat               { return iceberg.ParquetFile }
func (m *testMockDataFile) Partition() map[int]any                       { return nil }
func (m *testMockDataFile) Splits() []uint64                             { return []uint64{} }
func (m *testMockDataFile) SortOrderID() *int                            { return nil }
func (m *testMockDataFile) ColumnSizes() map[int]int64                   { return nil }
func (m *testMockDataFile) ValueCounts() map[int]int64                   { return nil }
func (m *testMockDataFile) NullValueCounts() map[int]int64               { return nil }
func (m *testMockDataFile) NaNValueCounts() map[int]int64                { return nil }
func (m *testMockDataFile) LowerBounds() map[int][]byte                  { return nil }
func (m *testMockDataFile) UpperBounds() map[int][]byte                  { return nil }
func (m *testMockDataFile) KeyMetadata() []byte                          { return nil }
func (m *testMockDataFile) DistinctValueCounts() map[int]int64           { return nil }
func (m *testMockDataFile) EqualityFieldIDs() []int                      { return nil }

// Mock filesystem for testing
type mockTestFS struct{}

func (fs *mockTestFS) Open(name string) (iceio.File, error) {
	return nil, fmt.Errorf("mock filesystem: cannot open %s", name)
}

func (fs *mockTestFS) Create(name string) (iceio.FileWriter, error) {
	return nil, fmt.Errorf("mock filesystem: cannot create %s", name)
}

func (fs *mockTestFS) Remove(name string) error {
	return fmt.Errorf("mock filesystem: cannot remove %s", name)
}

func (fs *mockTestFS) WriteFile(name string, data []byte) error {
	return fmt.Errorf("mock filesystem: cannot write file %s", name)
}

// Mock snapshot
type mockSnapshot struct {
	id int64
}

func (s *mockSnapshot) SnapshotID() int64 { return s.id }
func (s *mockSnapshot) ParentSnapshotID() *int64 { return nil }
func (s *mockSnapshot) SequenceNumber() int64 { return 0 }
func (s *mockSnapshot) TimestampMs() int64 { return 0 }
func (s *mockSnapshot) ManifestListLocation() string { return "" }
func (s *mockSnapshot) Summary() Summary { return Summary{} }
func (s *mockSnapshot) SchemaID() *int { return nil }
func (s *mockSnapshot) Manifests(iceio.IO) ([]iceberg.ManifestFile, error) { return nil, nil }

// Mock metadata
type mockMetadata struct {
	currentSnap *mockSnapshot
}

func (m *mockMetadata) Version() int                                            { return 1 }
func (m *mockMetadata) TableUUID() uuid.UUID                                    { return uuid.New() }
func (m *mockMetadata) Location() string                                        { return "s3://test-bucket/test-table" }
func (m *mockMetadata) LastSequenceNumber() int64                               { return 0 }
func (m *mockMetadata) LastUpdatedMs() int64                                    { return 0 }
func (m *mockMetadata) LastColumnID() int                                       { return 0 }
func (m *mockMetadata) Schemas() []*iceberg.Schema                              { return nil }
func (m *mockMetadata) SchemaByID(int) *iceberg.Schema                          { return nil }
func (m *mockMetadata) Schema() *iceberg.Schema                                 { return nil }
func (m *mockMetadata) PartitionSpecs() []iceberg.PartitionSpec                 { return nil }
func (m *mockMetadata) PartitionSpecByID(int) (iceberg.PartitionSpec, bool)     { return iceberg.PartitionSpec{}, false }
func (m *mockMetadata) PartitionSpec() iceberg.PartitionSpec                    { return iceberg.PartitionSpec{} }
func (m *mockMetadata) DefaultSortOrderID() int                                 { return 0 }
func (m *mockMetadata) SortOrders() []SortOrder                                 { return nil }
func (m *mockMetadata) SortOrderByID(int) (SortOrder, bool)                     { return SortOrder{}, false }
func (m *mockMetadata) SortOrder() SortOrder                                    { return SortOrder{} }
func (m *mockMetadata) Properties() iceberg.Properties                          { return nil }
func (m *mockMetadata) SnapshotByID(int64) (iceberg.Snapshot, error)            { return m.currentSnap, nil }
func (m *mockMetadata) SnapshotByName(string) (iceberg.Snapshot, error)         { return nil, nil }
func (m *mockMetadata) Snapshots() []iceberg.Snapshot                           { return nil }
func (m *mockMetadata) SnapshotLog() []SnapshotLogEntry                         { return nil }
func (m *mockMetadata) MetadataLog() []MetadataLogEntry                         { return nil }
func (m *mockMetadata) Refs() map[string]SnapshotRef                            { return nil }
func (m *mockMetadata) Ref(string) (SnapshotRef, bool)                          { return SnapshotRef{}, false }
func (m *mockMetadata) currentSnapshot() iceberg.Snapshot                       { return m.currentSnap }
func (m *mockMetadata) currentSnapshotID() *int64                               { if m.currentSnap != nil { id := m.currentSnap.id; return &id }; return nil }

// Test cases

func TestNewRowDelta(t *testing.T) {
	table, fs := createTestTable(t)
	
	txn := table.NewTransaction()
	rowDelta, err := NewRowDelta(txn, fs, nil)
	
	require.NoError(t, err)
	assert.NotNil(t, rowDelta)
	assert.Equal(t, txn, rowDelta.txn)
	assert.NotNil(t, rowDelta.commitUUID)
	assert.False(t, rowDelta.committed)
}

func TestRowDelta_AddDataFiles(t *testing.T) {
	table, fs := createTestTable(t)
	
	txn := table.NewTransaction()
	rowDelta, err := NewRowDelta(txn, fs, nil)
	require.NoError(t, err)
	
	dataFile1 := createMockDataFile("file1.parquet", 100, iceberg.EntryContentData)
	dataFile2 := createMockDataFile("file2.parquet", 200, iceberg.EntryContentData)
	
	result := rowDelta.AddDataFiles(dataFile1, dataFile2)
	
	assert.Equal(t, rowDelta, result) // Should return self for chaining
	assert.Len(t, rowDelta.addedDataFiles, 2)
	assert.Equal(t, dataFile1, rowDelta.addedDataFiles[0])
	assert.Equal(t, dataFile2, rowDelta.addedDataFiles[1])
}

func TestRowDelta_AddDeleteFiles(t *testing.T) {
	table, fs := createTestTable(t)
	
	txn := table.NewTransaction()
	rowDelta, err := NewRowDelta(txn, fs, nil)
	require.NoError(t, err)
	
	eqDeleteFile := createMockDataFile("eq_delete.parquet", 50, iceberg.EntryContentEqDeletes)
	posDeleteFile := createMockDataFile("pos_delete.parquet", 25, iceberg.EntryContentPosDeletes)
	nonDeleteFile := createMockDataFile("data.parquet", 100, iceberg.EntryContentData)
	
	result := rowDelta.AddDeleteFiles(eqDeleteFile, posDeleteFile, nonDeleteFile)
	
	assert.Equal(t, rowDelta, result) // Should return self for chaining
	assert.Len(t, rowDelta.addedDeleteFiles, 2) // Should only include delete files
	assert.Equal(t, eqDeleteFile, rowDelta.addedDeleteFiles[0])
	assert.Equal(t, posDeleteFile, rowDelta.addedDeleteFiles[1])
}

func TestRowDelta_ValidationMethods(t *testing.T) {
	table, fs := createTestTable(t)
	
	txn := table.NewTransaction()
	rowDelta, err := NewRowDelta(txn, fs, nil)
	require.NoError(t, err)
	
	// Test chaining of validation methods
	result := rowDelta.
		ValidateFromSnapshot(123).
		ValidateDeletedFiles().
		ValidateNoConflictingDeletes()
	
	assert.Equal(t, rowDelta, result)
	assert.Equal(t, int64(123), *rowDelta.validateFromSnapshotID)
	assert.True(t, rowDelta.validateDeletedFilesEnabled)
	assert.True(t, rowDelta.validateNoConflicts)
}

func TestRowDelta_Summary(t *testing.T) {
	table, fs := createTestTable(t)
	
	txn := table.NewTransaction()
	rowDelta, err := NewRowDelta(txn, fs, nil)
	require.NoError(t, err)
	
	dataFile1 := createMockDataFile("file1.parquet", 100, iceberg.EntryContentData)
	dataFile2 := createMockDataFile("file2.parquet", 200, iceberg.EntryContentData)
	eqDeleteFile := createMockDataFile("eq_delete.parquet", 50, iceberg.EntryContentEqDeletes)
	
	rowDelta.AddDataFiles(dataFile1, dataFile2)
	rowDelta.AddDeleteFiles(eqDeleteFile)
	
	summary := rowDelta.Summary()
	
	assert.Equal(t, 2, summary.DataFileCount)
	assert.Equal(t, 1, summary.DeleteFileCount)
	assert.Equal(t, int64(300), summary.TotalRecords) // 100 + 200
	assert.Equal(t, int64(50), summary.TotalDeletes)
	assert.Equal(t, rowDelta.commitUUID, summary.CommitUUID)
	
	// Test string representation
	summaryStr := summary.String()
	assert.Contains(t, summaryStr, "dataFiles=2")
	assert.Contains(t, summaryStr, "deleteFiles=1")
	assert.Contains(t, summaryStr, "records=300")
	assert.Contains(t, summaryStr, "deletes=50")
}

func TestRowDelta_DoubleCommit(t *testing.T) {
	table, fs := createTestTable(t)
	
	txn := table.NewTransaction()
	rowDelta, err := NewRowDelta(txn, fs, nil)
	require.NoError(t, err)
	
	// First commit should fail due to mock filesystem
	_, err = rowDelta.Commit(context.Background())
	assert.Error(t, err) // Expected to fail due to mock
	
	// Second commit should fail with "already committed"
	_, err = rowDelta.Commit(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already been committed")
}

func TestTable_NewRowDelta(t *testing.T) {
	table, _ := createTestTable(t)
	
	rowDelta, err := table.NewRowDelta(nil)
	
	require.NoError(t, err)
	assert.NotNil(t, rowDelta)
	assert.NotNil(t, rowDelta.txn)
}

func TestTransaction_NewRowDelta(t *testing.T) {
	table, _ := createTestTable(t)
	
	txn := table.NewTransaction()
	rowDelta, err := txn.NewRowDelta(nil)
	
	require.NoError(t, err)
	assert.NotNil(t, rowDelta)
	assert.Equal(t, txn, rowDelta.txn)
} 