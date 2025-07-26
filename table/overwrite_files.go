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

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
)

// OverwriteFiles defines the API for overwriting files in a table.
// This API accumulates file additions and produces a new Snapshot of the table
// by replacing all the deleted files with the set of additions. This operation
// is used to implement idempotent writes that always replace a section of a table
// with new data or update/delete operations that eagerly overwrite files.
type OverwriteFiles interface {
	// AddFile adds a DataFile to the table.
	AddFile(file iceberg.DataFile) OverwriteFiles

	// DeleteFile deletes a DataFile from the table.
	DeleteFile(file iceberg.DataFile) OverwriteFiles

	// OverwriteByRowFilter deletes files that match an Expression on data rows from the table.
	// A file is selected to be deleted by the expression if it could contain any rows that match
	// the expression (candidate files are selected using an inclusive projection). These candidate
	// files are deleted if all of the rows in the file must match the expression.
	OverwriteByRowFilter(expr iceberg.BooleanExpression) OverwriteFiles

	// ValidateAddedFilesMatchOverwriteFilter signals that each file added to the table must match
	// the overwrite expression. If this method is called, each added file is validated on commit
	// to ensure that it matches the overwrite row filter.
	ValidateAddedFilesMatchOverwriteFilter() OverwriteFiles

	// ValidateFromSnapshot sets the snapshot ID used in any reads for this operation.
	// Validations will check changes after this snapshot ID.
	ValidateFromSnapshot(snapshotId int64) OverwriteFiles

	// CaseSensitive enables or disables case sensitive expression binding for validations
	// that accept expressions.
	CaseSensitive(caseSensitive bool) OverwriteFiles

	// ConflictDetectionFilter sets a conflict detection filter used to validate concurrently
	// added data and delete files.
	ConflictDetectionFilter(conflictDetectionFilter iceberg.BooleanExpression) OverwriteFiles

	// ValidateNoConflictingData enables validation that data added concurrently does not
	// conflict with this commit's operation. This method should be called while committing
	// non-idempotent overwrite operations.
	ValidateNoConflictingData() OverwriteFiles

	// ValidateNoConflictingDeletes enables validation that deletes that happened concurrently
	// do not conflict with this commit's operation. Validating concurrent deletes is required
	// during non-idempotent overwrite operations.
	ValidateNoConflictingDeletes() OverwriteFiles

	// Commit applies the overwrite operation and returns the updated table.
	Commit(ctx context.Context) (*Table, error)
}

// BaseOverwriteFiles implements the OverwriteFiles interface with conflict detection.
type BaseOverwriteFiles struct {
	txn *Transaction
	fs  io.WriteFileIO

	// Files to add and delete
	addedFiles   []iceberg.DataFile
	deletedFiles map[string]iceberg.DataFile

	// Row filter for overwriting
	rowFilter iceberg.BooleanExpression

	// Validation settings
	validateAddedFilesMatchOverwriteFilter bool
	validateFromSnapshotId                 *int64
	caseSensitive                          bool
	conflictDetectionFilter                iceberg.BooleanExpression
	validateNoConflictingData              bool
	validateNoConflictingDeletes           bool

	// Snapshot properties
	snapshotProps iceberg.Properties
}

// NewOverwriteFiles creates a new OverwriteFiles instance for the given transaction.
func NewOverwriteFiles(txn *Transaction, fs io.WriteFileIO, snapshotProps iceberg.Properties) OverwriteFiles {
	return &BaseOverwriteFiles{
		txn:           txn,
		fs:            fs,
		addedFiles:    make([]iceberg.DataFile, 0),
		deletedFiles:  make(map[string]iceberg.DataFile),
		caseSensitive: true,
		snapshotProps: snapshotProps,
	}
}

func (o *BaseOverwriteFiles) AddFile(file iceberg.DataFile) OverwriteFiles {
	o.addedFiles = append(o.addedFiles, file)
	return o
}

func (o *BaseOverwriteFiles) DeleteFile(file iceberg.DataFile) OverwriteFiles {
	o.deletedFiles[file.FilePath()] = file
	return o
}

func (o *BaseOverwriteFiles) OverwriteByRowFilter(expr iceberg.BooleanExpression) OverwriteFiles {
	o.rowFilter = expr
	return o
}

func (o *BaseOverwriteFiles) ValidateAddedFilesMatchOverwriteFilter() OverwriteFiles {
	o.validateAddedFilesMatchOverwriteFilter = true
	return o
}

func (o *BaseOverwriteFiles) ValidateFromSnapshot(snapshotId int64) OverwriteFiles {
	o.validateFromSnapshotId = &snapshotId
	return o
}

func (o *BaseOverwriteFiles) CaseSensitive(caseSensitive bool) OverwriteFiles {
	o.caseSensitive = caseSensitive
	return o
}

func (o *BaseOverwriteFiles) ConflictDetectionFilter(conflictDetectionFilter iceberg.BooleanExpression) OverwriteFiles {
	o.conflictDetectionFilter = conflictDetectionFilter
	return o
}

func (o *BaseOverwriteFiles) ValidateNoConflictingData() OverwriteFiles {
	o.validateNoConflictingData = true
	return o
}

func (o *BaseOverwriteFiles) ValidateNoConflictingDeletes() OverwriteFiles {
	o.validateNoConflictingDeletes = true
	return o
}

func (o *BaseOverwriteFiles) Commit(ctx context.Context) (*Table, error) {
	// Validate the operation before committing
	if err := o.validateOperation(ctx); err != nil {
		return nil, err
	}

	// Generate requirements for conflict detection
	reqs := o.generateRequirements()

	// Use rewrite files implementation with requirements
	return o.txn.commitRewriteFilesWithRequirements(ctx, o.addedFiles, o.deletedFiles, o.snapshotProps, reqs)
}

// validateOperation performs validation checks before committing the overwrite operation.
func (o *BaseOverwriteFiles) validateOperation(ctx context.Context) error {
	// Validate that added files match overwrite filter if required
	if o.validateAddedFilesMatchOverwriteFilter && o.rowFilter != nil {
		if err := o.validateAddedFilesFilter(); err != nil {
			return err
		}
	}

	// Validate no conflicting data if required
	if o.validateNoConflictingData {
		if err := o.validateNoConflictingDataChanges(ctx); err != nil {
			return err
		}
	}

	// Validate no conflicting deletes if required
	if o.validateNoConflictingDeletes {
		if err := o.validateNoConflictingDeleteChanges(ctx); err != nil {
			return err
		}
	}

	return nil
}

// validateAddedFilesFilter ensures that added files match the overwrite filter.
func (o *BaseOverwriteFiles) validateAddedFilesFilter() error {
	// TODO: Implement file-level validation against row filter
	// This would require examining file statistics/metadata to ensure
	// the file could only contain rows that match the filter
	return nil
}

// validateNoConflictingDataChanges checks that no conflicting data was added concurrently.
func (o *BaseOverwriteFiles) validateNoConflictingDataChanges(ctx context.Context) error {
	fromSnapshotId := o.getValidationBaseSnapshot()
	currentSnapshot := o.txn.meta.currentSnapshot()
	
	if currentSnapshot == nil || currentSnapshot.SnapshotID == fromSnapshotId {
		// No snapshots to validate against
		return nil
	}

	// Find all snapshots after the validation base
	conflictingSnapshots := o.getSnapshotsAfter(fromSnapshotId)
	
	// Check each snapshot for conflicting data files
	for _, snapshot := range conflictingSnapshots {
		if err := o.checkSnapshotForConflictingData(ctx, snapshot); err != nil {
			return err
		}
	}

	return nil
}

// validateNoConflictingDeleteChanges checks that no conflicting deletes happened concurrently.
func (o *BaseOverwriteFiles) validateNoConflictingDeleteChanges(ctx context.Context) error {
	fromSnapshotId := o.getValidationBaseSnapshot()
	currentSnapshot := o.txn.meta.currentSnapshot()
	
	if currentSnapshot == nil || currentSnapshot.SnapshotID == fromSnapshotId {
		// No snapshots to validate against
		return nil
	}

	// Find all snapshots after the validation base
	conflictingSnapshots := o.getSnapshotsAfter(fromSnapshotId)
	
	// Check each snapshot for conflicting delete files
	for _, snapshot := range conflictingSnapshots {
		if err := o.checkSnapshotForConflictingDeletes(ctx, snapshot); err != nil {
			return err
		}
	}

	return nil
}

// getValidationBaseSnapshot returns the snapshot ID to use as baseline for validation.
func (o *BaseOverwriteFiles) getValidationBaseSnapshot() int64 {
	if o.validateFromSnapshotId != nil {
		return *o.validateFromSnapshotId
	}
	
	// Default to current snapshot
	if currentSnapshot := o.txn.meta.currentSnapshot(); currentSnapshot != nil {
		return currentSnapshot.SnapshotID
	}
	
	return -1
}

// getSnapshotsAfter returns all snapshots after the given snapshot ID.
func (o *BaseOverwriteFiles) getSnapshotsAfter(snapshotId int64) []*Snapshot {
	var result []*Snapshot
	
	snapshots := o.txn.meta.Snapshots()
	for _, snapshot := range snapshots {
		if snapshot.SnapshotID > snapshotId {
			result = append(result, &snapshot)
		}
	}
	
	return result
}

// checkSnapshotForConflictingData checks if a snapshot contains data that conflicts with this operation.
func (o *BaseOverwriteFiles) checkSnapshotForConflictingData(ctx context.Context, snapshot *Snapshot) error {
	// Get the file system for reading manifests
	fs, err := o.txn.tbl.fsF(ctx)
	if err != nil {
		return err
	}

	// Check data files in the snapshot
	for df, err := range snapshot.dataFiles(fs, nil) {
		if err != nil {
			return err
		}

		// If we have a conflict detection filter, check if this file could conflict
		if o.conflictDetectionFilter != nil {
			if o.couldFileConflict(df) {
				return fmt.Errorf("conflicting data file added concurrently: %s", df.FilePath())
			}
		} else {
			// Without a filter, any new data file is considered a conflict
			return fmt.Errorf("data file added concurrently without conflict detection filter: %s", df.FilePath())
		}
	}

	return nil
}

// checkSnapshotForConflictingDeletes checks if a snapshot contains deletes that conflict with this operation.
func (o *BaseOverwriteFiles) checkSnapshotForConflictingDeletes(ctx context.Context, snapshot *Snapshot) error {
	// Get the file system for reading manifests
	fs, err := o.txn.tbl.fsF(ctx)
	if err != nil {
		return err
	}

	// Check for delete files in manifests
	manifests, err := snapshot.Manifests(fs)
	if err != nil {
		return err
	}

	for _, manifest := range manifests {
		if manifest.ManifestContent() == iceberg.ManifestContentDeletes {
			entries, err := manifest.FetchEntries(fs, false)
			if err != nil {
				return err
			}

			for _, entry := range entries {
				if entry.Status() == iceberg.EntryStatusDELETED {
					// Check if this delete conflicts with our operation
					if o.deletesConflictWithOperation(entry) {
						return fmt.Errorf("conflicting delete operation found for file: %s", entry.DataFile().FilePath())
					}
				}
			}
		}
	}

	return nil
}

// couldFileConflict determines if a data file could conflict with the current operation.
func (o *BaseOverwriteFiles) couldFileConflict(df iceberg.DataFile) bool {
	// TODO: Implement proper conflict detection based on file statistics and partition data
	// For now, assume any file could conflict if we don't have more specific logic
	return true
}

// deletesConflictWithOperation determines if a delete entry conflicts with the current operation.
func (o *BaseOverwriteFiles) deletesConflictWithOperation(entry iceberg.ManifestEntry) bool {
	// Check if the deleted file is one that we're planning to overwrite
	deletedFilePath := entry.DataFile().FilePath()
	
	// If we're explicitly deleting this file, it's not a conflict
	if _, exists := o.deletedFiles[deletedFilePath]; exists {
		return false
	}
	
	// If we have a row filter, check if the deleted file would be affected by our filter
	if o.rowFilter != nil {
		// TODO: Implement proper partition/statistics-based conflict detection
		return true
	}
	
	return false
}

// generateRequirements creates the appropriate requirements for conflict detection.
func (o *BaseOverwriteFiles) generateRequirements() []Requirement {
	var reqs []Requirement

	// Add standard ref snapshot ID requirement
	reqs = append(reqs, AssertRefSnapshotID("main", o.txn.meta.currentSnapshotID))

	// Add conflict detection requirements if enabled
	if o.validateNoConflictingData {
		reqs = append(reqs, AssertNoConflictingData(o.validateFromSnapshotId, o.conflictDetectionFilter))
	}

	if o.validateNoConflictingDeletes {
		reqs = append(reqs, AssertNoConflictingDeletes(o.validateFromSnapshotId, o.conflictDetectionFilter))
	}

	return reqs
} 