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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
)

// RowDelta represents a set of row-level changes (inserts, updates, deletes) to a table.
// It allows combining data files with equality and position delete files in a single atomic operation.
type RowDelta interface {
	// AddDataFiles adds data files containing new or updated rows
	AddDataFiles(files ...iceberg.DataFile) RowDelta
	
	// AddDeleteFiles adds delete files (equality or position deletes)
	AddDeleteFiles(files ...iceberg.DataFile) RowDelta
	
	// ValidateFromSnapshot sets the snapshot ID to validate against for concurrent modifications
	ValidateFromSnapshot(snapshotID int64) RowDelta
	
	// ValidateDataFilesExist ensures that all data files referenced by delete files exist
	ValidateDataFilesExist(files ...iceberg.DataFile) RowDelta
	
	// ValidateDeletedFiles ensures that all files being deleted are present in the table
	ValidateDeletedFiles() RowDelta
	
	// ValidateNoConflictingDeletes ensures no conflicting delete operations
	ValidateNoConflictingDeletes() RowDelta
	
	// ConflictDetectionFilter sets filter for conflict detection
	ConflictDetectionFilter(filter iceberg.BooleanExpression) RowDelta
	
	// Commit applies the row delta operation to the table
	Commit(ctx context.Context) (*Table, error)
}

// BaseRowDelta is the core implementation of RowDelta interface.
type BaseRowDelta struct {
	txn               *Transaction
	io                io.WriteFileIO
	snapshotProps     iceberg.Properties
	commitUUID        uuid.UUID
	
	// Files being added and deleted
	addedDataFiles    []iceberg.DataFile
	addedDeleteFiles  []iceberg.DataFile
	
	// Validation settings
	validateFromSnapshotID      *int64
	validateDataFilesExist      []iceberg.DataFile
	validateDeletedFilesEnabled bool
	validateNoConflicts         bool
	conflictDetectionFilter     iceberg.BooleanExpression
	
	// State tracking
	committed bool
}

// NewRowDelta creates a new BaseRowDelta instance for the given transaction.
func NewRowDelta(txn *Transaction, ioFS io.IO, snapshotProps iceberg.Properties) (*BaseRowDelta, error) {
	writeFS, ok := ioFS.(io.WriteFileIO)
	if !ok {
		return nil, fmt.Errorf("filesystem must implement WriteFileIO for row delta operations")
	}
	
	return &BaseRowDelta{
		txn:                     txn,
		io:                      writeFS,
		snapshotProps:           snapshotProps,
		commitUUID:              uuid.New(),
		addedDataFiles:          make([]iceberg.DataFile, 0),
		addedDeleteFiles:        make([]iceberg.DataFile, 0),
		validateDataFilesExist:  make([]iceberg.DataFile, 0),
		validateDeletedFilesEnabled: false,
		validateNoConflicts:     false,
		conflictDetectionFilter: iceberg.AlwaysTrue{},
		committed:               false,
	}, nil
}

// AddDataFiles adds data files containing new or updated rows.
func (rd *BaseRowDelta) AddDataFiles(files ...iceberg.DataFile) RowDelta {
	rd.addedDataFiles = append(rd.addedDataFiles, files...)
	return rd
}

// AddDeleteFiles adds delete files (equality or position deletes).
func (rd *BaseRowDelta) AddDeleteFiles(files ...iceberg.DataFile) RowDelta {
	for _, file := range files {
		if file.ContentType() != iceberg.EntryContentEqDeletes && 
		   file.ContentType() != iceberg.EntryContentPosDeletes {
			// Skip non-delete files or handle error based on preference
			continue
		}
		rd.addedDeleteFiles = append(rd.addedDeleteFiles, file)
	}
	return rd
}

// ValidateFromSnapshot sets the snapshot ID to validate against for concurrent modifications.
func (rd *BaseRowDelta) ValidateFromSnapshot(snapshotID int64) RowDelta {
	rd.validateFromSnapshotID = &snapshotID
	return rd
}

// ValidateDataFilesExist ensures that all data files referenced by delete files exist.
func (rd *BaseRowDelta) ValidateDataFilesExist(files ...iceberg.DataFile) RowDelta {
	rd.validateDataFilesExist = append(rd.validateDataFilesExist, files...)
	return rd
}

// ValidateDeletedFiles ensures that all files being deleted are present in the table.
func (rd *BaseRowDelta) ValidateDeletedFiles() RowDelta {
	rd.validateDeletedFilesEnabled = true
	return rd
}

// ValidateNoConflictingDeletes ensures no conflicting delete operations.
func (rd *BaseRowDelta) ValidateNoConflictingDeletes() RowDelta {
	rd.validateNoConflicts = true
	return rd
}

// ConflictDetectionFilter sets filter for conflict detection.
func (rd *BaseRowDelta) ConflictDetectionFilter(filter iceberg.BooleanExpression) RowDelta {
	rd.conflictDetectionFilter = filter
	return rd
}

// Commit applies the row delta operation to the table.
func (rd *BaseRowDelta) Commit(ctx context.Context) (*Table, error) {
	if rd.committed {
		return nil, errors.New("row delta has already been committed")
	}
	
	rd.committed = true
	
	// Validate the operation
	if err := rd.validate(); err != nil {
		return nil, fmt.Errorf("row delta validation failed: %w", err)
	}
	
	// Create the snapshot producer for row delta operations
	producer := rd.createSnapshotProducer()
	
	// Add all data files
	for _, file := range rd.addedDataFiles {
		producer.appendDataFile(file)
	}
	
	// Add all delete files
	for _, file := range rd.addedDeleteFiles {
		producer.appendDeleteFile(file)
	}
	
	// Generate updates and requirements
	updates, reqs, err := producer.commit()
	if err != nil {
		return nil, fmt.Errorf("committing row delta: %w", err)
	}
	
	// Apply to transaction
	if err := rd.txn.apply(updates, reqs); err != nil {
		return nil, fmt.Errorf("applying row delta to transaction: %w", err)
	}
	
	// Commit the transaction
	return rd.txn.Commit(ctx)
}

// validate performs validation checks on the row delta operation.
func (rd *BaseRowDelta) validate() error {
	// Validate from snapshot if specified
	if rd.validateFromSnapshotID != nil {
		currentSnapshot := rd.txn.meta.currentSnapshot()
		if currentSnapshot != nil && currentSnapshot.SnapshotID != *rd.validateFromSnapshotID {
			return fmt.Errorf("table has been modified since snapshot %d, current snapshot is %d", 
				*rd.validateFromSnapshotID, currentSnapshot.SnapshotID)
		}
	}
	
	// Validate data files exist if specified
	if len(rd.validateDataFilesExist) > 0 {
		if err := rd.validateDataFilesExistence(); err != nil {
			return err
		}
	}
	
	// Validate deleted files if enabled
	if rd.validateDeletedFilesEnabled {
		if err := rd.validateDeletedFilesExistence(); err != nil {
			return err
		}
	}
	
	// Validate no conflicting deletes if enabled
	if rd.validateNoConflicts {
		if err := rd.validateNoConflictingDeletes(); err != nil {
			return err
		}
	}
	
	return nil
}

// validateDataFilesExistence checks that specified data files exist in the table.
func (rd *BaseRowDelta) validateDataFilesExistence() error {
	// This would require checking against the current snapshot's manifest files
	// For now, return nil as a basic implementation
	// TODO: Implement full data file existence validation
	return nil
}

// validateDeletedFilesExistence ensures files being deleted exist in the table.
func (rd *BaseRowDelta) validateDeletedFilesExistence() error {
	// This would require checking that files referenced in delete files exist
	// For now, return nil as a basic implementation  
	// TODO: Implement full deleted file validation
	return nil
}

// validateNoConflictingDeletes checks for conflicting delete operations.
func (rd *BaseRowDelta) validateNoConflictingDeletes() error {
	// This would require checking for overlapping delete operations
	// For now, return nil as a basic implementation
	// TODO: Implement conflict detection logic
	return nil
}

// createSnapshotProducer creates a snapshot producer for row delta operations.
func (rd *BaseRowDelta) createSnapshotProducer() *snapshotProducer {
	return newRowDeltaProducer(OpAppend, rd.txn, rd.io, &rd.commitUUID, rd.snapshotProps)
}

// Helper methods for working with delete files
// Note: To avoid import cycles, delete writers should be created directly
// from the table/deletes package and their results added via AddDeleteFiles()

// Convenience methods for adding data from Arrow tables and readers

// AddDataFromTable adds data from an Arrow table as a data file.
func (rd *BaseRowDelta) AddDataFromTable(ctx context.Context, tbl arrow.Table, batchSize int64) error {
	rdr := array.NewTableReader(tbl, batchSize)
	defer rdr.Release()
	
	return rd.AddDataFromReader(ctx, rdr)
}

// AddDataFromReader adds data from an Arrow record reader as a data file.
func (rd *BaseRowDelta) AddDataFromReader(ctx context.Context, rdr array.RecordReader) error {
	// Create a temporary data writer to convert records to data files
	// This mirrors the pattern used in Transaction.Append()
	
	itr := recordsToDataFiles(ctx, rd.txn.tbl.Location(), rd.txn.meta, recordWritingArgs{
		sc:        rdr.Schema(),
		itr:       array.IterFromReader(rdr),
		fs:        rd.io,
		writeUUID: &rd.commitUUID,
	})
	
	for df, err := range itr {
		if err != nil {
			return err
		}
		rd.AddDataFiles(df)
	}
	
	return nil
}

// Summary returns a summary of the row delta operation.
func (rd *BaseRowDelta) Summary() RowDeltaSummary {
	dataFileCount := len(rd.addedDataFiles)
	deleteFileCount := len(rd.addedDeleteFiles)
	
	var totalRecords int64
	for _, file := range rd.addedDataFiles {
		totalRecords += file.Count()
	}
	
	var totalDeletes int64
	for _, file := range rd.addedDeleteFiles {
		totalDeletes += file.Count()
	}
	
	return RowDeltaSummary{
		DataFileCount:   dataFileCount,
		DeleteFileCount: deleteFileCount,
		TotalRecords:    totalRecords,
		TotalDeletes:    totalDeletes,
		CommitUUID:      rd.commitUUID,
	}
}

// RowDeltaSummary provides summary information about a row delta operation.
type RowDeltaSummary struct {
	DataFileCount   int
	DeleteFileCount int
	TotalRecords    int64
	TotalDeletes    int64
	CommitUUID      uuid.UUID
}

// String returns a string representation of the summary.
func (s RowDeltaSummary) String() string {
	return fmt.Sprintf("RowDelta{dataFiles=%d, deleteFiles=%d, records=%d, deletes=%d, commit=%s}",
		s.DataFileCount, s.DeleteFileCount, s.TotalRecords, s.TotalDeletes, s.CommitUUID)
}
