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

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// RowDelta encodes a set of row-level changes to a table: new data files
// (inserts) and delete files (equality or position deletes). All changes
// are committed atomically in a single snapshot.
//
// The operation type of the produced snapshot is determined automatically:
//   - Data files only → OpAppend
//   - Delete files only → OpDelete
//   - Both data and delete files → OpOverwrite
//
// This matches the semantics of Java's BaseRowDelta. It is the primary
// API for CDC/streaming workloads where INSERTs, UPDATEs, and DELETEs
// must be committed together.
//
// Note: conflict detection for concurrent writers is not yet implemented.
// Concurrent RowDelta commits against the same table may produce incorrect
// results if delete files miss newly appended data. For single-writer
// workloads this is safe.
//
// Usage:
//
//	rd := tx.NewRowDelta(snapshotProps)
//	rd.AddRows(dataFile1, dataFile2)
//	rd.AddDeletes(equalityDeleteFile1)
//	err := rd.Commit(ctx)
type RowDelta struct {
	txn       *Transaction
	dataFiles []iceberg.DataFile
	delFiles  []iceberg.DataFile
	props     iceberg.Properties
}

// NewRowDelta creates a new RowDelta for committing row-level changes
// within this transaction. The provided properties are included in the
// snapshot summary.
func (t *Transaction) NewRowDelta(snapshotProps iceberg.Properties) *RowDelta {
	return &RowDelta{
		txn:   t,
		props: snapshotProps,
	}
}

// AddRows adds data files containing new rows (inserts) to this RowDelta.
func (rd *RowDelta) AddRows(files ...iceberg.DataFile) *RowDelta {
	rd.dataFiles = append(rd.dataFiles, files...)

	return rd
}

// AddDeletes adds delete files (equality or position) to this RowDelta.
// Equality delete files must have ContentType == EntryContentEqDeletes
// and non-empty EqualityFieldIDs referencing valid schema columns.
// Position delete files must have ContentType == EntryContentPosDeletes.
func (rd *RowDelta) AddDeletes(files ...iceberg.DataFile) *RowDelta {
	rd.delFiles = append(rd.delFiles, files...)

	return rd
}

// Commit validates and commits all accumulated row-level changes as a
// single atomic snapshot. Returns an error if there are no files to
// commit, if any file has an unexpected content type, or if the table
// format version does not support delete files.
func (rd *RowDelta) Commit(ctx context.Context) error {
	if len(rd.dataFiles) == 0 && len(rd.delFiles) == 0 {
		return errors.New("row delta must have at least one data file or delete file")
	}

	// Delete files require format version >= 2.
	if len(rd.delFiles) > 0 && rd.txn.meta.formatVersion < 2 {
		return fmt.Errorf("delete files require table format version >= 2, got v%d",
			rd.txn.meta.formatVersion)
	}

	for _, f := range rd.dataFiles {
		if f.ContentType() != iceberg.EntryContentData {
			return fmt.Errorf("expected data file, got content type %s: %s",
				f.ContentType(), f.FilePath())
		}
	}

	schema := rd.txn.meta.CurrentSchema()
	for _, f := range rd.delFiles {
		ct := f.ContentType()
		if ct != iceberg.EntryContentPosDeletes && ct != iceberg.EntryContentEqDeletes {
			return fmt.Errorf("expected delete file, got content type %s: %s",
				ct, f.FilePath())
		}

		// Equality delete files must declare which columns form the delete key,
		// and those columns must exist in the current schema.
		if ct == iceberg.EntryContentEqDeletes {
			eqIDs := f.EqualityFieldIDs()
			if len(eqIDs) == 0 {
				return fmt.Errorf("equality delete file must have non-empty EqualityFieldIDs: %s",
					f.FilePath())
			}

			for _, id := range eqIDs {
				if _, ok := schema.FindFieldByID(id); !ok {
					return fmt.Errorf("equality field ID %d not found in table schema: %s",
						id, f.FilePath())
				}
			}
		}
	}

	fs, err := rd.txn.tbl.fsF(ctx)
	if err != nil {
		return err
	}

	wfs, ok := fs.(iceio.WriteFileIO)
	if !ok {
		return errors.New("filesystem does not support writing")
	}

	op := rd.Operation()
	producer := newFastAppendFilesProducer(op, rd.txn, wfs, nil, rd.props)

	for _, f := range rd.dataFiles {
		producer.appendDataFile(f)
	}

	for _, f := range rd.delFiles {
		producer.appendDeleteFile(f)
	}

	updates, reqs, err := producer.commit(ctx)
	if err != nil {
		return err
	}

	return rd.txn.apply(updates, reqs)
}

// Operation returns the snapshot operation type that will be used when
// this RowDelta is committed:
//   - data only → OpAppend
//   - deletes only → OpDelete
//   - both → OpOverwrite
func (rd *RowDelta) Operation() Operation {
	hasData := len(rd.dataFiles) > 0
	hasDeletes := len(rd.delFiles) > 0

	switch {
	case hasData && hasDeletes:
		return OpOverwrite
	case hasDeletes:
		return OpDelete
	default:
		return OpAppend
	}
}
