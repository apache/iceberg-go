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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplacePartitionsInterface(t *testing.T) {
	// Test that the ReplacePartitions interface methods work correctly
	
	// Create a mock transaction
	txn := &Transaction{}
	
	// Create the replace partitions operation
	rp := NewReplacePartitions(txn)
	require.NotNil(t, rp)
	
	// Test method chaining
	result := rp.Set("test-prop", "test-value")
	assert.Same(t, rp, result, "Set should return self for method chaining")
	
	result = rp.StageOnly()
	assert.Same(t, rp, result, "StageOnly should return self for method chaining")
	
	result = rp.ValidateAddedFilesMatchOverwriteFilter()
	assert.Same(t, rp, result, "ValidateAddedFilesMatchOverwriteFilter should return self for method chaining")
	
	result = rp.ValidateFromSnapshot(12345)
	assert.Same(t, rp, result, "ValidateFromSnapshot should return self for method chaining")
}

func TestReplacePartitionsAddFiles(t *testing.T) {
	txn := &Transaction{}
	rp := NewReplacePartitions(txn)
	
	// Test AddFile
	dataFile := &testDataFile{
		filePath: "/test/file1.parquet",
		specID:   0,
	}
	
	result := rp.AddFile(dataFile)
	assert.Same(t, rp, result, "AddFile should return self for method chaining")
	
	// Verify file was added
	baseRp, ok := rp.(*BaseReplacePartitions)
	require.True(t, ok)
	assert.Len(t, baseRp.addedFiles, 1)
	assert.Equal(t, dataFile, baseRp.addedFiles[0])
	
	// Test AddFiles (multiple)
	dataFile2 := &testDataFile{
		filePath: "/test/file2.parquet",
		specID:   0,
	}
	dataFile3 := &testDataFile{
		filePath: "/test/file3.parquet", 
		specID:   0,
	}
	
	result = rp.AddFiles(dataFile2, dataFile3)
	assert.Same(t, rp, result, "AddFiles should return self for method chaining")
	
	// Verify all files were added
	baseRp2, ok := rp.(*BaseReplacePartitions)
	require.True(t, ok)
	assert.Len(t, baseRp2.addedFiles, 3)
	assert.Equal(t, dataFile2, baseRp2.addedFiles[1])
	assert.Equal(t, dataFile3, baseRp2.addedFiles[2])
}

func TestReplacePartitionsSnapshotProperties(t *testing.T) {
	txn := &Transaction{}
	rp := NewReplacePartitions(txn)
	
	// Test setting properties
	rp.Set("prop1", "value1")
	rp.Set("prop2", "value2")
	
	baseRp, ok := rp.(*BaseReplacePartitions)
	require.True(t, ok)
	assert.Equal(t, "value1", baseRp.snapshotProps["prop1"])
	assert.Equal(t, "value2", baseRp.snapshotProps["prop2"])
}

func TestReplacePartitionsValidationOptions(t *testing.T) {
	txn := &Transaction{}
	rp := NewReplacePartitions(txn)
	
	baseRp, ok := rp.(*BaseReplacePartitions)
	require.True(t, ok)
	
	// Test validation flags
	assert.False(t, baseRp.validateAddedFiles)
	rp.ValidateAddedFilesMatchOverwriteFilter()
	assert.True(t, baseRp.validateAddedFiles)
	
	// Test from snapshot ID
	assert.Nil(t, baseRp.fromSnapshotID)
	rp.ValidateFromSnapshot(12345)
	require.NotNil(t, baseRp.fromSnapshotID)
	assert.Equal(t, int64(12345), *baseRp.fromSnapshotID)
	
	// Test stage only
	assert.False(t, baseRp.stageOnly)
	rp.StageOnly()
	assert.True(t, baseRp.stageOnly)
}

func TestPartitionKeyString(t *testing.T) {
	// Test partition key string generation
	partitionType := &iceberg.StructType{
		FieldList: []iceberg.NestedField{
			{ID: 1, Name: "year", Type: iceberg.Int32Type{}},
			{ID: 2, Name: "month", Type: iceberg.Int32Type{}},
		},
	}
	
	// Test with values
	partition := map[int]any{
		1: 2023,
		2: 12,
	}
	key := partitionKeyString(partition, partitionType)
	assert.Equal(t, "year=2023/month=12", key)
	
	// Test with null values
	partitionWithNull := map[int]any{
		1: 2023,
		2: nil,
	}
	keyWithNull := partitionKeyString(partitionWithNull, partitionType)
	assert.Equal(t, "year=2023/month=__HIVE_DEFAULT_PARTITION__", keyWithNull)
	
	// Test empty partition
	emptyPartition := map[int]any{}
	emptyKey := partitionKeyString(emptyPartition, partitionType)
	assert.Equal(t, "__HIVE_DEFAULT_PARTITION__", emptyKey)
}

// Simple mock data file for testing
type testDataFile struct {
	filePath  string
	partition map[int]any
	specID    int32
}

func (m *testDataFile) ContentType() iceberg.ManifestEntryContent { return iceberg.EntryContentData }
func (m *testDataFile) FilePath() string                          { return m.filePath }
func (m *testDataFile) FileFormat() iceberg.FileFormat            { return iceberg.ParquetFile }
func (m *testDataFile) Partition() map[int]any                    { return m.partition }
func (m *testDataFile) Count() int64                              { return 100 }
func (m *testDataFile) FileSizeBytes() int64                      { return 1024 }
func (m *testDataFile) ColumnSizes() map[int]int64                { return nil }
func (m *testDataFile) ValueCounts() map[int]int64                { return nil }
func (m *testDataFile) NullValueCounts() map[int]int64            { return nil }
func (m *testDataFile) NaNValueCounts() map[int]int64             { return nil }
func (m *testDataFile) DistinctValueCounts() map[int]int64        { return nil }
func (m *testDataFile) LowerBoundValues() map[int][]byte          { return nil }
func (m *testDataFile) UpperBoundValues() map[int][]byte          { return nil }
func (m *testDataFile) KeyMetadata() []byte                       { return nil }
func (m *testDataFile) SplitOffsets() []int64                     { return nil }
func (m *testDataFile) EqualityFieldIDs() []int                   { return nil }
func (m *testDataFile) SortOrderID() *int                         { return nil }
func (m *testDataFile) SpecID() int32                             { return m.specID } 