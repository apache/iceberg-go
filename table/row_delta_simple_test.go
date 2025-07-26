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
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRowDeltaSummary_StringRepresentation(t *testing.T) {
	table, _ := createTestTable(t)
	summary := RowDeltaSummary{
		DataFileCount:   3,
		DeleteFileCount: 2,
		TotalRecords:    1500,
		TotalDeletes:    75,
		CommitUUID:      uuid.New(),
	}

	str := summary.String()
	
	assert.Contains(t, str, "RowDelta{")
	assert.Contains(t, str, "dataFiles=3")
	assert.Contains(t, str, "deleteFiles=2")
	assert.Contains(t, str, "records=1500")
	assert.Contains(t, str, "deletes=75")
	assert.Contains(t, str, "commit=")
	assert.Contains(t, str, "}")
}

func TestRowDelta_ConflictDetectionFilter(t *testing.T) {
	table, fs := createTestTable(t)
	
	txn := table.NewTransaction()
	rowDelta, err := NewRowDelta(txn, fs, nil)
	require.NoError(t, err)
	
	// Create a mock filter
	filter := iceberg.AlwaysFalse{}
	
	result := rowDelta.ConflictDetectionFilter(filter)
	
	assert.Equal(t, rowDelta, result) // Should return self for chaining
	assert.Equal(t, filter, rowDelta.conflictDetectionFilter)
}

func TestRowDelta_ValidateDataFilesExist(t *testing.T) {
	table, fs := createTestTable(t)
	
	txn := table.NewTransaction()
	rowDelta, err := NewRowDelta(txn, fs, nil)
	require.NoError(t, err)
	
	dataFile := createMockDataFile("test.parquet", 100, iceberg.EntryContentData)
	
	result := rowDelta.ValidateDataFilesExist(dataFile)
	
	assert.Equal(t, rowDelta, result) // Should return self for chaining
	assert.Len(t, rowDelta.validateDataFilesExist, 1)
	assert.Equal(t, dataFile, rowDelta.validateDataFilesExist[0])
	
	// Test adding multiple files
	dataFile2 := createMockDataFile("test2.parquet", 200, iceberg.EntryContentData)
	rowDelta.ValidateDataFilesExist(dataFile2)
	
	assert.Len(t, rowDelta.validateDataFilesExist, 2)
}

func TestRowDelta_EmptyOperations(t *testing.T) {
	table, fs := createTestTable(t)
	
	txn := table.NewTransaction()
	rowDelta, err := NewRowDelta(txn, fs, nil)
	require.NoError(t, err)
	
	// Test with no files added
	summary := rowDelta.Summary()
	assert.Equal(t, 0, summary.DataFileCount)
	assert.Equal(t, 0, summary.DeleteFileCount)
	assert.Equal(t, int64(0), summary.TotalRecords)
	assert.Equal(t, int64(0), summary.TotalDeletes)
} 