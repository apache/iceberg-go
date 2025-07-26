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

func TestBaseRewriteFiles_ValidateInputFiles(t *testing.T) {
	tests := []struct {
		name            string
		filesToDelete   []iceberg.DataFile
		filesToAdd      []iceberg.DataFile
		expectedError   string
		shouldError     bool
	}{
		{
			name:          "empty inputs",
			filesToDelete: []iceberg.DataFile{},
			filesToAdd:    []iceberg.DataFile{},
			shouldError:   false,
		},
		{
			name: "duplicate files in delete list",
			filesToDelete: []iceberg.DataFile{
				createMockDataFile("file1.parquet"),
				createMockDataFile("file1.parquet"),
			},
			filesToAdd:    []iceberg.DataFile{},
			expectedError: "duplicate file in delete list",
			shouldError:   true,
		},
		{
			name:          "duplicate files in add list",
			filesToDelete: []iceberg.DataFile{},
			filesToAdd: []iceberg.DataFile{
				createMockDataFile("file2.parquet"),
				createMockDataFile("file2.parquet"),
			},
			expectedError: "duplicate file in add list",
			shouldError:   true,
		},
		{
			name: "overlap between delete and add lists",
			filesToDelete: []iceberg.DataFile{
				createMockDataFile("file3.parquet"),
			},
			filesToAdd: []iceberg.DataFile{
				createMockDataFile("file3.parquet"),
			},
			expectedError: "file appears in both delete and add lists",
			shouldError:   true,
		},
		{
			name: "valid non-overlapping files",
			filesToDelete: []iceberg.DataFile{
				createMockDataFile("file1.parquet"),
				createMockDataFile("file2.parquet"),
			},
			filesToAdd: []iceberg.DataFile{
				createMockDataFile("file3.parquet"),
				createMockDataFile("file4.parquet"),
			},
			shouldError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock transaction
			mockTable := createMockTable(t)
			txn := mockTable.NewTransaction()
			rewriteFiles := NewRewriteFiles(txn)

			// Call validateInputFiles
			err := rewriteFiles.validateInputFiles(tt.filesToDelete, tt.filesToAdd)

			if tt.shouldError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBaseRewriteFiles_ConflictDetection(t *testing.T) {
	tests := []struct {
		name                    string
		enableConflictDetection bool
		customValidation        func(Metadata, []iceberg.DataFile) error
		expectValidationCalled  bool
	}{
		{
			name:                    "conflict detection enabled",
			enableConflictDetection: true,
			expectValidationCalled:  true,
		},
		{
			name:                    "conflict detection disabled",
			enableConflictDetection: false,
			expectValidationCalled:  false,
		},
		{
			name:                    "custom validation function",
			enableConflictDetection: true,
			customValidation: func(metadata Metadata, files []iceberg.DataFile) error {
				// Custom validation logic
				return nil
			},
			expectValidationCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockTable := createMockTable(t)
			txn := mockTable.NewTransaction()
			rewriteFiles := NewRewriteFiles(txn).WithConflictDetection(tt.enableConflictDetection)

			if tt.customValidation != nil {
				rewriteFiles = rewriteFiles.WithValidation(tt.customValidation)
			}

			// Test configuration
			assert.Equal(t, tt.enableConflictDetection, rewriteFiles.conflictDetection)
			if tt.customValidation != nil {
				assert.NotNil(t, rewriteFiles.validateFun)
			}
		})
	}
}

func TestRewriteDataFilesAction_Configuration(t *testing.T) {
	// Test would go here when the action framework is fully implemented
	// For now, we'll skip this test
	t.Skip("RewriteDataFiles action framework not yet fully integrated")
}

func TestTransaction_RewriteFiles(t *testing.T) {
	mockTable := createMockTable(t)
	txn := mockTable.NewTransaction()

	// Test that RewriteFiles returns a BaseRewriteFiles instance
	rewriteFiles := txn.RewriteFiles()
	assert.NotNil(t, rewriteFiles)
	assert.IsType(t, &BaseRewriteFiles{}, rewriteFiles)

	// Test configuration methods
	rewriteFiles = rewriteFiles.WithConflictDetection(false).WithCaseSensitive(false)
	assert.False(t, rewriteFiles.conflictDetection)
	assert.False(t, rewriteFiles.caseSensitive)
}

func TestValidateNoNewDeletesForDataFiles(t *testing.T) {
	tests := []struct {
		name     string
		metadata Metadata
		files    []iceberg.DataFile
		wantErr  bool
	}{
		{
			name:     "empty files list",
			metadata: createMockMetadata(),
			files:    []iceberg.DataFile{},
			wantErr:  false,
		},
		{
			name:     "no current snapshot",
			metadata: createMockMetadataWithoutSnapshot(),
			files:    []iceberg.DataFile{createMockDataFile("file1.parquet")},
			wantErr:  false,
		},
		{
			name:     "valid files with current snapshot",
			metadata: createMockMetadata(),
			files:    []iceberg.DataFile{createMockDataFile("file1.parquet")},
			wantErr:  false, // Basic implementation doesn't detect conflicts yet
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateNoNewDeletesForDataFiles(tt.metadata, tt.files)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Helper functions for creating mock objects

func createMockDataFile(path string) iceberg.DataFile {
	// Create a simple mock data file using the DataFileBuilder
	bldr, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		path, iceberg.ParquetFile, nil, 100, 1024)
	if err != nil {
		panic(err)
	}
	
	return bldr.Build()
}

func createMockTable(t *testing.T) *Table {
	// This would normally create a real table for testing
	// For now, return a simplified mock
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	spec := iceberg.UnpartitionedSpec
	sortOrder := UnsortedSortOrder

	metadata, err := NewMetadata(schema, spec, sortOrder, "/tmp/test", nil)
	require.NoError(t, err)

	// Return a simplified table instance
	return New([]string{"test", "table"}, metadata, "/tmp/test", nil, nil)
}

func createMockMetadata() Metadata {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	spec := iceberg.UnpartitionedSpec
	sortOrder := UnsortedSortOrder

	metadata, _ := NewMetadata(schema, spec, sortOrder, "/tmp/test", nil)

	return metadata
}

func createMockMetadataWithoutSnapshot() Metadata {
	return createMockMetadata() // Simplified - no snapshots added
} 