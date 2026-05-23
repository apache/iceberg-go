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
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

// PositionDeltaWriter writes data files for the position-delta MoR pattern,
// distinguishing reinserted rows (survivors of a position-delta rewrite that
// preserve their original _row_id) from fresh inserts (rows that get a new
// _row_id synthesized at read time).
//
// Scope: this writer produces *data files only*. The position-delete entries
// that pair with reinserts (and that turn an UPDATE into delete-old + reinsert)
// are not emitted here — the engine driver is responsible for composing this
// writer with a position-delete writer and committing both via a RowDelta-style
// snapshot. This mirrors the data-file half of Java's
// SparkPositionDeltaWrite.reinsert(meta, row) vs insert(row) split.
//
// _last_updated_sequence_number is intentionally not written: the row-id-only
// file schema lets the reader synthesize it from the manifest entry's
// data_sequence_number, which after the rewrite is the new snapshot's sequence
// number — the value the spec requires.
//
// Usage:
//
//	w, err := table.NewPositionDeltaWriter(tbl)
//	w.Reinsert(survivorBatch)  // batch must include _row_id column with non-null values
//	w.Insert(freshBatch)       // batch without _row_id (writer appends nulls)
//	dataFiles, err := w.Close(ctx)
type PositionDeltaWriter struct {
	tbl       *Table
	writeUUID uuid.UUID

	// reinsertBatches hold records with explicit _row_id values (preserving lineage).
	reinsertBatches []arrow.RecordBatch
	// insertBatches hold fresh records without lineage.
	insertBatches []arrow.RecordBatch

	closed bool
}

// NewPositionDeltaWriter creates a writer for the position-delta MoR update
// pattern on the given table. The table must be format version 3 or higher.
func NewPositionDeltaWriter(tbl *Table) (*PositionDeltaWriter, error) {
	if tbl.metadata.Version() < 3 {
		return nil, fmt.Errorf("%w: PositionDeltaWriter requires format version >= 3, got %d",
			iceberg.ErrInvalidArgument, tbl.metadata.Version())
	}

	return &PositionDeltaWriter{
		tbl:       tbl,
		writeUUID: uuid.New(),
	}, nil
}

// Reinsert adds survivor rows that preserve their original _row_id. The batch
// MUST contain a _row_id column (field name "_row_id") with non-null int64
// values representing the preserved row identities.
//
// Per the Iceberg spec, only position-delete rewrites and CoW can preserve
// lineage. Equality deletes cannot preserve lineage because the engine writes
// without reading old identity.
func (w *PositionDeltaWriter) Reinsert(batch arrow.RecordBatch) error {
	if w.closed {
		return fmt.Errorf("%w: writer is already closed", ErrInvalidOperation)
	}

	indices := batch.Schema().FieldIndices(iceberg.RowIDColumnName)
	if len(indices) == 0 {
		return fmt.Errorf("%w: Reinsert batch must contain %s column",
			iceberg.ErrInvalidArgument, iceberg.RowIDColumnName)
	}

	col := batch.Column(indices[0])
	if col.NullN() > 0 {
		return fmt.Errorf("%w: Reinsert batch %s column must not contain null values",
			iceberg.ErrInvalidArgument, iceberg.RowIDColumnName)
	}

	batch.Retain()
	w.reinsertBatches = append(w.reinsertBatches, batch)

	return nil
}

// Insert adds fresh rows that get a new _row_id at read time. The batch should
// NOT contain a _row_id column; if it does, all values must be null. These rows
// represent genuinely new data (not survivors of a rewrite).
func (w *PositionDeltaWriter) Insert(batch arrow.RecordBatch) error {
	if w.closed {
		return fmt.Errorf("%w: writer is already closed", ErrInvalidOperation)
	}

	if indices := batch.Schema().FieldIndices(iceberg.RowIDColumnName); len(indices) > 0 {
		if batch.Column(indices[0]).NullN() != int(batch.NumRows()) {
			return fmt.Errorf("%w: Insert batch %s column must be all null (use Reinsert for preserved IDs)",
				iceberg.ErrInvalidArgument, iceberg.RowIDColumnName)
		}
	}

	batch.Retain()
	w.insertBatches = append(w.insertBatches, batch)

	return nil
}

// Close finalizes the writer and returns the data files produced. The returned
// files contain both reinserted and fresh rows, with the _row_id column written
// explicitly for reinserted rows (non-null) and left null for fresh inserts.
//
// The caller is responsible for adding these files to a snapshot (typically via
// a Transaction's snapshot producer) along with any position-delete entries
// that pair with the reinserts.
func (w *PositionDeltaWriter) Close(ctx context.Context) ([]iceberg.DataFile, error) {
	if w.closed {
		return nil, fmt.Errorf("%w: writer is already closed", ErrInvalidOperation)
	}
	w.closed = true

	defer func() {
		for _, b := range w.reinsertBatches {
			b.Release()
		}
		for _, b := range w.insertBatches {
			b.Release()
		}
	}()

	if len(w.reinsertBatches) == 0 && len(w.insertBatches) == 0 {
		return nil, nil
	}

	fileSchema := iceberg.SchemaWithRowID(w.tbl.Schema())
	arrowSc, err := SchemaToArrowSchema(fileSchema, nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("PositionDeltaWriter: build arrow schema: %w", err)
	}

	writeOpts := []WriteRecordOption{
		WithPreserveRowLineage(fileSchema),
		WithWriteUUID(w.writeUUID),
	}

	records := w.buildUnifiedIterator()

	var result []iceberg.DataFile
	for df, err := range WriteRecords(ctx, w.tbl, arrowSc, records, writeOpts...) {
		if err != nil {
			return nil, fmt.Errorf("PositionDeltaWriter: write records: %w", err)
		}
		result = append(result, df)
	}

	return result, nil
}

// buildUnifiedIterator merges reinsert and insert batches into a single
// iterator. Reinsert batches already have _row_id; insert batches get a null
// _row_id column appended.
func (w *PositionDeltaWriter) buildUnifiedIterator() iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		alloc := memory.NewGoAllocator()

		for _, batch := range w.reinsertBatches {
			batch.Retain()
			if !yield(batch, nil) {
				batch.Release()

				return
			}
		}

		for _, batch := range w.insertBatches {
			enriched, err := appendNullRowIDColumn(alloc, batch)
			if err != nil {
				yield(nil, err)

				return
			}
			if !yield(enriched, nil) {
				enriched.Release()

				return
			}
		}
	}
}

// appendNullRowIDColumn appends a null-filled _row_id column to a batch that
// doesn't have one, signaling that these are fresh inserts. If the batch
// already has _row_id (validated to be all-null by Insert), it is returned
// retained as-is.
func appendNullRowIDColumn(alloc memory.Allocator, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	if indices := batch.Schema().FieldIndices(iceberg.RowIDColumnName); len(indices) > 0 {
		batch.Retain()

		return batch, nil
	}

	nrows := batch.NumRows()
	bldr := array.NewInt64Builder(alloc)
	defer bldr.Release()
	bldr.AppendNulls(int(nrows))
	nullCol := bldr.NewArray()
	defer nullCol.Release()

	fields := append(batch.Schema().Fields(), arrow.Field{
		Name:     iceberg.RowIDColumnName,
		Type:     arrow.PrimitiveTypes.Int64,
		Nullable: true,
	})
	cols := append(batch.Columns(), nullCol)
	newSchema := arrow.NewSchema(fields, nil)

	return array.NewRecordBatch(newSchema, cols, nrows), nil
}
