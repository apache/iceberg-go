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
// Client-side conflict validation runs before the commit is sent to
// the catalog:
//   - Position deletes: referenced data files must still be reachable
//     from the current branch head (validateDataFilesExist).
//   - Equality deletes under write.delete.isolation-level=serializable
//     (the default): concurrent data files in the same partition(s) as
//     the equality deletes are rejected. For partitioned tables an
//     OR-of-equalities filter is built from the eq-delete files'
//     partition tuples and routed through validateAddedDataFilesMatchingFilter
//     (spec-evolution safe, manifest-summary pruning, type-aware evaluation).
//     For unpartitioned tables the check is conservative (AlwaysTrue —
//     any concurrent append is a conflict). Opt out by setting
//     write.delete.isolation-level=snapshot.
//
// Refresh-and-replay between retries is deferred to a follow-up PR;
// today the pre-flight runs once on the first attempt.
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

	// Register RowDelta's pre-commit conflict validator. The underlying
	// fast-append producer's validator is a no-op; RowDelta semantics
	// (pos-delete references, eq-delete predicate) require a dedicated
	// check that snapshot_producers does not know about.
	rd.txn.addValidator(rd.validate)

	return rd.txn.apply(updates, reqs)
}

// validate is the client-side conflict check for a RowDelta commit. It
// runs against cc, which reflects the branch state at the first commit
// attempt. Two invariants are enforced:
//
//   - Every data file referenced by a position-delete in this RowDelta
//     must still be reachable from the branch head. A concurrent
//     compaction or overwrite that rewrote a referenced file would
//     orphan this pos-delete and produce incorrect results — reject.
//     Always runs, no isolation gating.
//
//   - When any equality-delete is included and isolation is
//     SERIALIZABLE, reject the commit if a concurrent snapshot added
//     conflicting data files. For unpartitioned tables the check is
//     conservative (AlwaysTrue — any concurrent append is a conflict).
//     For partitioned tables, an OR-of-equalities filter is built from
//     the eq-delete files' partition tuples and routed through
//     validateAddedDataFilesMatchingFilter, which performs per-spec
//     projection (spec-evolution safe), manifest-summary pruning, and
//     type-aware partition evaluation — so only concurrent data files
//     in the same partitions as the equality deletes are rejected.
//
// Fast appends alongside a RowDelta see no validators from RowDelta:
// data-only commits are as safe as a fastAppend.
func (rd *RowDelta) validate(cc *conflictContext) error {
	if cc == nil {
		return nil
	}

	// Collect every data-file path the pos-deletes in this delta
	// reference. A nil ReferencedDataFile means the pos-delete does
	// not record its target — we cannot check it here; the file is
	// still present in the per-row position_delete_file column and
	// would apply correctly regardless of concurrent removals,
	// matching Java's behavior when the referenced-file column is
	// unset.
	var referenced []string
	var eqDeleteFiles []iceberg.DataFile
	for _, f := range rd.delFiles {
		switch f.ContentType() {
		case iceberg.EntryContentPosDeletes:
			if ref := f.ReferencedDataFile(); ref != nil && *ref != "" {
				referenced = append(referenced, *ref)
			}
		case iceberg.EntryContentEqDeletes:
			eqDeleteFiles = append(eqDeleteFiles, f)
		}
	}

	if len(referenced) > 0 {
		if err := validateDataFilesExist(cc, referenced); err != nil {
			return err
		}
	}

	if len(eqDeleteFiles) > 0 {
		level := readIsolationLevel(rd.txn.meta.props,
			WriteDeleteIsolationLevelKey, WriteDeleteIsolationLevelDefault)
		// Route through the existing validateNoConflictingDataFiles path,
		// which calls validateAddedDataFilesMatchingFilter internally.
		// For unpartitioned tables, use AlwaysTrue conservatively — an
		// equality delete can affect any row. For partitioned tables,
		// build an OR-of-equalities filter from the eq-delete files'
		// partition tuples so that concurrent appends to different
		// partitions are not falsely rejected.
		currentSpec, specErr := rd.txn.meta.CurrentSpec()
		if specErr != nil {
			return fmt.Errorf("reading current partition spec: %w", specErr)
		}

		var err error
		if currentSpec == nil || currentSpec.NumFields() == 0 {
			err = validateNoConflictingDataFiles(cc, iceberg.AlwaysTrue{}, level)
		} else {
			err = validateNoConflictingDataFilesInPartitions(cc, eqDeleteFiles, level)
		}
		if err != nil {
			return err
		}
	}

	return nil
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
