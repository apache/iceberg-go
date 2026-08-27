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

import "github.com/apache/iceberg-go"

// AddedRowsScanTask is a changelog insert produced by adding a data file.
// Matching delete files committed in the same snapshot, or from squashed
// snapshots, are applied while reading so deleted rows are not emitted as
// inserts.
type AddedRowsScanTask struct {
	FileScanTask
	changeOrdinal    int
	commitSnapshotID int64
}

// NewAddedRowsScanTask constructs an insert task for dataFile. deletes are
// delete files that apply while reading the added file.
func NewAddedRowsScanTask(dataFile iceberg.DataFile, deletes []iceberg.DataFile, changeOrdinal int, commitSnapshotID int64) AddedRowsScanTask {
	return AddedRowsScanTask{
		FileScanTask: FileScanTask{
			File:        dataFile,
			DeleteFiles: deletes,
		},
		changeOrdinal:    changeOrdinal,
		commitSnapshotID: commitSnapshotID,
	}
}

func (t AddedRowsScanTask) ChangeOrdinal() int      { return t.changeOrdinal }
func (t AddedRowsScanTask) CommitSnapshotID() int64 { return t.commitSnapshotID }

// Deletes returns delete files to apply when reading the added data file.
func (t AddedRowsScanTask) Deletes() []iceberg.DataFile {
	return t.DeleteFiles
}

// DeletedDataFileScanTask is a changelog delete produced by removing a data
// file. ExistingDeletes are delete files that were already present and must
// be applied so only rows that were live when the file was removed appear as
// deletes.
type DeletedDataFileScanTask struct {
	FileScanTask
	changeOrdinal    int
	commitSnapshotID int64
}

// NewDeletedDataFileScanTask constructs a delete task for a removed data file.
func NewDeletedDataFileScanTask(dataFile iceberg.DataFile, existingDeletes []iceberg.DataFile, changeOrdinal int, commitSnapshotID int64) DeletedDataFileScanTask {
	return DeletedDataFileScanTask{
		FileScanTask: FileScanTask{
			File:        dataFile,
			DeleteFiles: existingDeletes,
		},
		changeOrdinal:    changeOrdinal,
		commitSnapshotID: commitSnapshotID,
	}
}

func (t DeletedDataFileScanTask) ChangeOrdinal() int      { return t.changeOrdinal }
func (t DeletedDataFileScanTask) CommitSnapshotID() int64 { return t.commitSnapshotID }

// ExistingDeletes returns delete files that applied before the data file was
// removed.
func (t DeletedDataFileScanTask) ExistingDeletes() []iceberg.DataFile {
	return t.DeleteFiles
}

// DeletedRowsScanTask is a changelog delete produced by adding delete files
// against a data file that remains in the table. AddedDeletes remove rows
// that should appear in the changelog. ExistingDeletes already applied and
// those rows must not be emitted again.
type DeletedRowsScanTask struct {
	FileScanTask
	addedDeletes     []iceberg.DataFile
	changeOrdinal    int
	commitSnapshotID int64
}

// NewDeletedRowsScanTask constructs a row-level delete task. existingDeletes
// are stored on the embedded FileScanTask as DeleteFiles so later readers can
// reuse the normal scan delete path for the live-row baseline.
func NewDeletedRowsScanTask(dataFile iceberg.DataFile, addedDeletes, existingDeletes []iceberg.DataFile, changeOrdinal int, commitSnapshotID int64) DeletedRowsScanTask {
	return DeletedRowsScanTask{
		FileScanTask: FileScanTask{
			File:        dataFile,
			DeleteFiles: existingDeletes,
		},
		addedDeletes:     addedDeletes,
		changeOrdinal:    changeOrdinal,
		commitSnapshotID: commitSnapshotID,
	}
}

func (t DeletedRowsScanTask) ChangeOrdinal() int      { return t.changeOrdinal }
func (t DeletedRowsScanTask) CommitSnapshotID() int64 { return t.commitSnapshotID }

// AddedDeletes returns delete files whose removals should appear in the
// changelog.
func (t DeletedRowsScanTask) AddedDeletes() []iceberg.DataFile {
	return t.addedDeletes
}

// ExistingDeletes returns delete files that already applied before this
// snapshot's added deletes.
func (t DeletedRowsScanTask) ExistingDeletes() []iceberg.DataFile {
	return t.DeleteFiles
}
