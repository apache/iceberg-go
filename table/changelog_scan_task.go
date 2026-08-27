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
// delete files that apply while reading the added file. Position deletes,
// equality deletes, and deletion vectors are stored on the matching
// FileScanTask fields.
func NewAddedRowsScanTask(dataFile iceberg.DataFile, deletes []iceberg.DataFile, changeOrdinal int, commitSnapshotID int64) AddedRowsScanTask {
	return AddedRowsScanTask{
		FileScanTask:     fileScanTaskWithDeletes(dataFile, deletes),
		changeOrdinal:    changeOrdinal,
		commitSnapshotID: commitSnapshotID,
	}
}

func (t AddedRowsScanTask) ChangeOrdinal() int      { return t.changeOrdinal }
func (t AddedRowsScanTask) CommitSnapshotID() int64 { return t.commitSnapshotID }

// Deletes returns every delete file applied while reading the added data
// file: position deletes, then equality deletes, then deletion vectors.
func (t AddedRowsScanTask) Deletes() []iceberg.DataFile {
	return allDeleteFiles(t.FileScanTask)
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
		FileScanTask:     fileScanTaskWithDeletes(dataFile, existingDeletes),
		changeOrdinal:    changeOrdinal,
		commitSnapshotID: commitSnapshotID,
	}
}

func (t DeletedDataFileScanTask) ChangeOrdinal() int      { return t.changeOrdinal }
func (t DeletedDataFileScanTask) CommitSnapshotID() int64 { return t.commitSnapshotID }

// ExistingDeletes returns delete files that applied before the data file was
// removed.
func (t DeletedDataFileScanTask) ExistingDeletes() []iceberg.DataFile {
	return allDeleteFiles(t.FileScanTask)
}

// DeletedRowsScanTask is a changelog delete produced by adding delete files
// against a data file that remains in the table. AddedDeletes remove rows
// that should appear in the changelog. ExistingDeletes already applied and
// those rows must not be emitted again.
type DeletedRowsScanTask struct {
	FileScanTask
	addedDeletes     FileScanTask
	changeOrdinal    int
	commitSnapshotID int64
}

// NewDeletedRowsScanTask constructs a row-level delete task. existingDeletes
// are stored on the embedded FileScanTask so later readers can reuse the
// normal scan delete path for the live-row baseline.
func NewDeletedRowsScanTask(dataFile iceberg.DataFile, addedDeletes, existingDeletes []iceberg.DataFile, changeOrdinal int, commitSnapshotID int64) DeletedRowsScanTask {
	return DeletedRowsScanTask{
		FileScanTask:     fileScanTaskWithDeletes(dataFile, existingDeletes),
		addedDeletes:     fileScanTaskWithDeletes(dataFile, addedDeletes),
		changeOrdinal:    changeOrdinal,
		commitSnapshotID: commitSnapshotID,
	}
}

func (t DeletedRowsScanTask) ChangeOrdinal() int      { return t.changeOrdinal }
func (t DeletedRowsScanTask) CommitSnapshotID() int64 { return t.commitSnapshotID }

// AddedDeletes returns delete files whose removals should appear in the
// changelog.
func (t DeletedRowsScanTask) AddedDeletes() []iceberg.DataFile {
	return allDeleteFiles(t.addedDeletes)
}

// ExistingDeletes returns delete files that already applied before this
// snapshot's added deletes.
func (t DeletedRowsScanTask) ExistingDeletes() []iceberg.DataFile {
	return allDeleteFiles(t.FileScanTask)
}

func fileScanTaskWithDeletes(dataFile iceberg.DataFile, deletes []iceberg.DataFile) FileScanTask {
	pos, eq, dv := classifyDeleteFiles(deletes)
	return FileScanTask{
		File:                dataFile,
		DeleteFiles:         pos,
		EqualityDeleteFiles: eq,
		DeletionVectorFiles: dv,
	}
}

func classifyDeleteFiles(files []iceberg.DataFile) (pos, eq, dv []iceberg.DataFile) {
	for _, f := range files {
		if f == nil {
			continue
		}
		switch {
		case IsDeletionVector(f):
			dv = append(dv, f)
		case f.ContentType() == iceberg.EntryContentEqDeletes:
			eq = append(eq, f)
		case f.ContentType() == iceberg.EntryContentPosDeletes:
			pos = append(pos, f)
		}
	}
	return pos, eq, dv
}

func allDeleteFiles(task FileScanTask) []iceberg.DataFile {
	out := make([]iceberg.DataFile, 0, len(task.DeleteFiles)+len(task.EqualityDeleteFiles)+len(task.DeletionVectorFiles))
	out = append(out, task.DeleteFiles...)
	out = append(out, task.EqualityDeleteFiles...)
	out = append(out, task.DeletionVectorFiles...)
	return out
}
