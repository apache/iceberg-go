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
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"math"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
)

// ErrChangelogBufferLimit indicates that one snapshot's changes exceed a
// normalization buffer limit. No changes from that snapshot have been emitted.
var ErrChangelogBufferLimit = errors.New("changelog normalization buffer limit exceeded")

// ChangelogNormalizationOptions configures per-snapshot carryover removal and
// optional update images. Its zero value removes carryovers without inferring
// updates. Changes from different snapshots are never cancelled or paired.
type ChangelogNormalizationOptions struct {
	// ComputeUpdates labels a remaining DELETE and INSERT with the same
	// identifier as UPDATE_BEFORE and UPDATE_AFTER. Carryovers are removed
	// first. Each identifier must have at most one remaining row of each
	// change type in a snapshot; ambiguous identifiers return an error.
	ComputeUpdates bool
	// IdentifierFields contains primitive column names, including paths through
	// structs, resolved using the scan's case sensitivity. If empty,
	// ComputeUpdates uses the effective schema's identifier-field-ids.
	// Explicit identifiers may be nullable; NULL equals NULL for matching.
	IdentifierFields []string
	// MaxBufferedBytes limits retained Arrow buffers and encoded matching keys
	// per snapshot. Zero uses 64 MiB. Shared buffers are counted conservatively,
	// including their parents. This is not a process-memory limit: decoding,
	// output batches, temporary key encoding, and per-row indexes need additional
	// memory. Temporary keys are also individually capped at MaxBufferedBytes.
	MaxBufferedBytes int64
	// MaxBufferedRows bounds per-row bookkeeping, including rows later removed
	// as carryovers. Zero uses 1,000,000 rows per snapshot.
	MaxBufferedRows int64
}

// ToNormalizedArrowRecords removes copy-on-write carryovers and optionally
// produces update images. It compares complete logical rows, with multiplicity,
// within each snapshot. Row-lineage metadata is not part of row equality.
// Variant equality compares encoded metadata/value bytes; it does not
// canonicalize different encodings of the same Variant value.
// Filters, projection, and the scan's row limit are applied after normalization.
// A limit counts output rows and may split an update pair.
//
// Each snapshot is buffered and validated before any of its rows are emitted.
// Output follows snapshot and source-task order, except that UPDATE_AFTER is
// emitted immediately after its UPDATE_BEFORE. WithArrowBatchSize also caps
// output batch rows; the default output batch size is 4096. Before/after images
// may straddle a batch boundary. The iterator is single-use, and callers release
// every output batch. For the raw file-level changelog, use ToArrowRecords.
func (s *IncrementalChangelogScan) ToNormalizedArrowRecords(ctx context.Context, opts ChangelogNormalizationOptions) (*arrow.Schema, iter.Seq2[arrow.RecordBatch, error], error) {
	if s == nil || s.scan == nil {
		return nil, nil, fmt.Errorf("%w: incremental changelog scan is not initialized", ErrInvalidOperation)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if opts.MaxBufferedBytes < 0 || opts.MaxBufferedRows < 0 {
		return nil, nil, fmt.Errorf("%w: changelog buffer limits must be positive", iceberg.ErrInvalidArgument)
	}
	if !opts.ComputeUpdates && len(opts.IdentifierFields) != 0 {
		return nil, nil, fmt.Errorf("%w: identifier fields require ComputeUpdates", iceberg.ErrInvalidArgument)
	}
	if opts.MaxBufferedBytes == 0 {
		opts.MaxBufferedBytes = 64 << 20
	}
	if opts.MaxBufferedRows == 0 {
		opts.MaxBufferedRows = 1_000_000
	}
	outSchema, useLargeTypes, err := changelogArrowSchema(s.scan)
	if err != nil {
		return nil, nil, err
	}
	requested, err := s.scan.Projection()
	if err != nil {
		return nil, nil, err
	}

	// Reading the complete snapshot prevents a hidden changed column or a raw
	// limit/filter from turning an update into a carryover or an unmatched row.
	readScan := *s.scan
	readScan.selectedFields = []string{"*"}
	readScan.rowFilter = iceberg.AlwaysTrue{}
	readScan.limit = -1
	_, rowID := requested.FindFieldByID(iceberg.RowIDFieldID)
	_, sequence := requested.FindFieldByID(iceberg.LastUpdatedSequenceNumberFieldID)
	readScan.includeRowLineage = rowID || sequence
	full := *s
	full.scan = &readScan
	fullProjection, err := readScan.Projection()
	if err != nil {
		return nil, nil, err
	}
	_, _, err = changelogArrowSchema(&readScan)
	if err != nil {
		return nil, nil, err
	}
	identifiers, err := changelogIdentifiers(fullProjection, opts, s.scan.caseSensitive)
	if err != nil {
		return nil, nil, err
	}
	logicalColumns := make([]int, 0, fullProjection.NumFields())
	for i, field := range fullProjection.Fields() {
		if field.ID != iceberg.RowIDFieldID && field.ID != iceberg.LastUpdatedSequenceNumberFieldID {
			logicalColumns = append(logicalColumns, i)
		}
	}
	boundFilter, _, err := bindTaskFilter(fullProjection, s.scan.rowFilter, s.scan.caseSensitive)
	if err != nil {
		return nil, nil, err
	}
	// Synthetic Variant Extract columns are appended after all existing columns.
	// Include changelog metadata so the compiled positions match output batches.
	filterFields := append(fullProjection.Fields(),
		iceberg.NestedField{ID: math.MaxInt32 - 104, Name: ChangelogChangeTypeColumn, Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: math.MaxInt32 - 105, Name: ChangelogChangeOrdinalColumn, Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: math.MaxInt32 - 106, Name: ChangelogCommitSnapshotIDColumn, Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	filterScan := arrowScan{caseSensitive: s.scan.caseSensitive}
	filter, dropAll, err := filterScan.getRecordFilter(ctx, iceberg.NewSchema(fullProjection.ID, filterFields...), boundFilter)
	if err != nil {
		return nil, nil, err
	}
	tasks, err := full.PlanFiles(ctx)
	if err != nil {
		return nil, nil, err
	}
	if s.scan.limit == 0 || dropAll {
		return outSchema, func(func(arrow.RecordBatch, error) bool) {}, nil
	}
	batchSize := s.scan.arrowBatchSize
	if batchSize <= 0 {
		batchSize = 4096
	}
	limit := s.scan.limit
	props := s.scan.metadata.Properties()
	version := s.scan.metadata.FormatVersion()

	return outSchema, func(yield func(arrow.RecordBatch, error) bool) {
		var emitted int64
		for start := 0; start < len(tasks); {
			end := start + 1
			for end < len(tasks) && tasks[end].ChangeOrdinal() == tasks[start].ChangeOrdinal() {
				end++
			}
			_, records, err := full.Read(ctx, tasks[start:end])
			if err != nil {
				yield(nil, err)

				return
			}
			keepGoing, err := func() (bool, error) {
				buffer := changelogSnapshotBuffer{opts: opts}
				defer buffer.release()
				for rec, err := range records {
					if err != nil {
						return false, err
					}
					err = buffer.add(ctx, rec, logicalColumns)
					rec.Release()
					if err != nil {
						return false, err
					}
				}
				if err := buffer.computeUpdates(ctx, identifiers); err != nil {
					return false, err
				}
				refs := make([]rowRef, 0, min(batchSize, len(buffer.rows)))
				operations := make([]ChangelogOperation, 0, cap(refs))
				emit := func() (bool, error) {
					rec, err := gatherNormalizedChangelogRows(ctx, buffer.batches, refs, operations)
					if err != nil {
						return false, err
					}
					refs = refs[:0]
					operations = operations[:0]
					if filter != nil {
						rec, err = filter(rec) // record processors consume their input
						if err != nil {
							return false, err
						}
					}
					defer rec.Release()
					if rec.NumRows() == 0 {
						return true, nil
					}
					out, err := projectNormalizedChangelog(ctx, rec, outSchema, requested, fullProjection, SchemaOptions{
						UseLargeTypes: useLargeTypes, FormatVersion: version, TableProperties: props,
					})
					if err != nil {
						return false, err
					}
					if limit > 0 && out.NumRows() > limit-emitted {
						cut := out.NewSlice(0, limit-emitted)
						out.Release()
						out = cut
					}
					emitted += out.NumRows()
					keepGoing := yield(out, nil)

					return keepGoing && (limit < 0 || emitted < limit), nil
				}
				appendRow := func(row bufferedChangelogRow) (bool, error) {
					if err := ctx.Err(); err != nil {
						return false, err
					}
					refs = append(refs, newRowRef(row.batch, row.row))
					operations = append(operations, row.operation)
					if len(refs) == batchSize {
						return emit()
					}

					return true, nil
				}
				for _, row := range buffer.rows {
					if row.operation == "" || row.operation == ChangelogOpUpdateAfter {
						continue
					}
					if ok, err := appendRow(row); !ok || err != nil {
						return ok, err
					}
					if row.operation == ChangelogOpUpdateBefore {
						if ok, err := appendRow(buffer.rows[row.partner]); !ok || err != nil {
							return ok, err
						}
					}
				}
				if len(refs) > 0 {
					return emit()
				}

				return true, ctx.Err()
			}()
			if err != nil {
				yield(nil, err)

				return
			}
			if !keepGoing {
				return
			}
			start = end
		}
	}, nil
}

type changelogIdentifier struct {
	ref  arrowFieldRef
	id   int
	name string
}

func changelogIdentifiers(schema *iceberg.Schema, opts ChangelogNormalizationOptions, caseSensitive bool) ([]changelogIdentifier, error) {
	if !opts.ComputeUpdates {
		return nil, nil
	}
	ids := slices.Clone(schema.IdentifierFieldIDs)
	if len(opts.IdentifierFields) != 0 {
		ids = make([]int, 0, len(opts.IdentifierFields))
		for _, name := range opts.IdentifierFields {
			field, found := schema.FindFieldByName(name)
			if !caseSensitive {
				field, found = schema.FindFieldByNameCaseInsensitive(name)
			}
			if !found {
				return nil, fmt.Errorf("%w: changelog identifier field %q not found", iceberg.ErrInvalidArgument, name)
			}
			if slices.Contains(ids, field.ID) {
				return nil, fmt.Errorf("%w: duplicate changelog identifier field %q", iceberg.ErrInvalidArgument, name)
			}
			ids = append(ids, field.ID)
		}
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: update images require identifier fields", iceberg.ErrInvalidArgument)
	}
	refs := resolveArrowFieldsByID(schema, ids)
	result := make([]changelogIdentifier, 0, len(ids))
	for _, id := range ids {
		field, _ := schema.FindFieldByID(id)
		name, _ := schema.FindColumnName(id)
		if _, ok := field.Type.(iceberg.PrimitiveType); !ok {
			return nil, fmt.Errorf("%w: changelog identifier %q must be primitive", iceberg.ErrInvalidArgument, name)
		}
		ref, err := resolveArrowField(refs, id, name, "changelog")
		if err != nil {
			return nil, fmt.Errorf("%w: changelog identifier %q must be outside lists and maps: %v", iceberg.ErrInvalidArgument, name, err)
		}
		result = append(result, changelogIdentifier{ref: ref, id: id, name: name})
	}

	return result, nil
}

type bufferedChangelogRow struct {
	batch, row int
	operation  ChangelogOperation
	partner    int
}

type changelogSnapshotBuffer struct {
	opts      ChangelogNormalizationOptions
	batches   []arrow.RecordBatch
	rows      []bufferedChangelogRow
	pending   map[string]*changelogPendingRows
	usedBytes int64
	keyBytes  int64
}

type changelogPendingRows struct {
	indices []int
}

func (b *changelogSnapshotBuffer) release() {
	for _, rec := range b.batches {
		rec.Release()
	}
}

func (b *changelogSnapshotBuffer) reserve(n int64) error {
	if n > b.opts.MaxBufferedBytes-b.usedBytes {
		return fmt.Errorf("%w: snapshot exceeds MaxBufferedBytes (%d)", ErrChangelogBufferLimit, b.opts.MaxBufferedBytes)
	}
	b.usedBytes += n

	return nil
}

func (b *changelogSnapshotBuffer) add(ctx context.Context, rec arrow.RecordBatch, columns []int) error {
	if uint64(len(b.batches)) > math.MaxUint32 || rec.NumRows() > int64(math.MaxUint32)+1 {
		return fmt.Errorf("%w: snapshot contains too many batches or rows in a batch", ErrChangelogBufferLimit)
	}
	if rec.NumRows() > b.opts.MaxBufferedRows-int64(len(b.rows)) {
		return fmt.Errorf("%w: snapshot exceeds MaxBufferedRows (%d)", ErrChangelogBufferLimit, b.opts.MaxBufferedRows)
	}
	if err := b.reserve(changelogRetainedBytes(rec)); err != nil {
		return err
	}
	batch := len(b.batches)
	rec.Retain()
	b.batches = append(b.batches, rec)
	if b.pending == nil {
		b.pending = make(map[string]*changelogPendingRows)
	}
	var key bytes.Buffer
	for row := range int(rec.NumRows()) {
		if err := ctx.Err(); err != nil {
			return err
		}
		key.Reset()
		for _, column := range columns {
			if err := encodeChangelogValue(ctx, &key, rec.Column(column), row, b.opts.MaxBufferedBytes); err != nil {
				return err
			}
		}
		op := ChangelogOperation(rec.Column(int(rec.NumCols()) - 3).ValueStr(row))
		if op != ChangelogOpDelete && op != ChangelogOpInsert {
			return fmt.Errorf("%w: expected raw changelog INSERT or DELETE, got %q", iceberg.ErrInvalidArgument, op)
		}
		index := len(b.rows)
		b.rows = append(b.rows, bufferedChangelogRow{batch: batch, row: row, operation: op, partner: -1})
		pending, found := b.pending[key.String()]
		if !found {
			if err := b.reserve(int64(key.Len())); err != nil {
				return err
			}
			b.keyBytes += int64(key.Len())
			pending = &changelogPendingRows{}
			b.pending[key.String()] = pending
		}
		indices := pending.indices
		if len(indices) > 0 && b.rows[indices[len(indices)-1]].operation != op {
			// Cancel occurrences rather than distinct values: 2 DELETEs and
			// 1 identical INSERT must leave one DELETE.
			b.rows[indices[len(indices)-1]].operation = ""
			b.rows[index].operation = ""
			pending.indices = indices[:len(indices)-1]
		} else {
			pending.indices = append(indices, index)
		}
	}

	return nil
}

func (b *changelogSnapshotBuffer) computeUpdates(ctx context.Context, identifiers []changelogIdentifier) error {
	if len(identifiers) == 0 {
		return ctx.Err()
	}
	// Full-row matching is complete. Identifier matching is a separate pass so
	// identical carryovers do not make otherwise unique update keys ambiguous.
	b.pending = nil
	b.usedBytes -= b.keyBytes
	b.keyBytes = 0
	type pair struct{ before, after int }
	pairs := make(map[string]*pair)
	var key bytes.Buffer
	for i := range b.rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		row := &b.rows[i]
		if row.operation == "" {
			continue
		}
		rec := b.batches[row.batch]
		key.Reset()
		for _, id := range identifiers {
			column, parents, err := arrowArraysAtField(rec, id.ref, id.id, id.name, "changelog")
			if err != nil {
				return err
			}
			nullParent := false
			for _, parent := range parents {
				nullParent = nullParent || parent.IsNull(row.row)
			}
			if nullParent {
				if int64(key.Len()) >= b.opts.MaxBufferedBytes {
					return fmt.Errorf("%w: encoded identifier exceeds MaxBufferedBytes", ErrChangelogBufferLimit)
				}
				key.WriteByte(0)
			} else if err := encodeChangelogValue(ctx, &key, column, row.row, b.opts.MaxBufferedBytes); err != nil {
				return err
			}
		}
		p, found := pairs[key.String()]
		if !found {
			if err := b.reserve(int64(key.Len())); err != nil {
				return err
			}
			p = &pair{before: -1, after: -1}
			pairs[key.String()] = p
		}
		if row.operation == ChangelogOpDelete {
			if p.before >= 0 {
				return fmt.Errorf("%w: cannot compute update images: multiple DELETE rows have the same identifier", ErrInvalidOperation)
			}
			p.before = i
		} else {
			if p.after >= 0 {
				return fmt.Errorf("%w: cannot compute update images: multiple INSERT rows have the same identifier", ErrInvalidOperation)
			}
			p.after = i
		}
	}
	for _, p := range pairs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if p.before >= 0 && p.after >= 0 {
			b.rows[p.before].operation = ChangelogOpUpdateBefore
			b.rows[p.before].partner = p.after
			b.rows[p.after].operation = ChangelogOpUpdateAfter
		}
	}

	return ctx.Err()
}

func gatherNormalizedChangelogRows(ctx context.Context, batches []arrow.RecordBatch, refs []rowRef, operations []ChangelogOperation) (arrow.RecordBatch, error) {
	if len(refs) == 0 || len(refs) != len(operations) {
		return nil, fmt.Errorf("%w: invalid normalized changelog rows", iceberg.ErrInvalidArgument)
	}
	// Only pass referenced batches to gatherSortedRows. Its scratch space is
	// proportional to the batch slice, and small output batches may otherwise
	// repeatedly scan every input batch in the snapshot.
	batchIndices := make(map[int]int, min(len(batches), len(refs)))
	selected := make([]arrow.RecordBatch, 0, min(len(batches), len(refs)))
	localRefs := make([]rowRef, len(refs))
	for i, ref := range refs {
		if i%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		batch := ref.batch()
		if batch >= len(batches) || batches[batch] == nil || int64(ref.row()) >= batches[batch].NumRows() {
			return nil, fmt.Errorf("%w: invalid normalized changelog row reference", iceberg.ErrInvalidArgument)
		}
		local, ok := batchIndices[batch]
		if !ok {
			local = len(selected)
			batchIndices[batch] = local
			selected = append(selected, batches[batch])
		}
		localRefs[i] = newRowRef(local, ref.row())
	}
	rec, err := gatherSortedRows(allocatorFrom(ctx), partitionBatchByKey(ctx), selected, localRefs)
	if err != nil {
		return nil, err
	}
	defer rec.Release()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	changeIndex := int(rec.NumCols()) - 3
	builder := array.NewBuilder(allocatorFrom(ctx), rec.Schema().Field(changeIndex).Type)
	defer builder.Release()
	for i, op := range operations {
		if i%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		switch b := builder.(type) {
		case *array.StringBuilder:
			b.Append(string(op))
		case *array.LargeStringBuilder:
			b.Append(string(op))
		default:
			return nil, fmt.Errorf("%w: unexpected changelog operation type %s", iceberg.ErrInvalidArgument, rec.Schema().Field(changeIndex).Type)
		}
	}
	change := builder.NewArray()
	defer change.Release()
	cols := slices.Clone(rec.Columns())
	cols[changeIndex] = change
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return array.NewRecordBatch(rec.Schema(), cols, rec.NumRows()), nil
}

func projectNormalizedChangelog(ctx context.Context, rec arrow.RecordBatch, out *arrow.Schema, requested, full *iceberg.Schema, opts SchemaOptions) (arrow.RecordBatch, error) {
	n := int(rec.NumCols()) - 3
	data := array.NewRecordBatch(arrow.NewSchema(rec.Schema().Fields()[:n], nil), rec.Columns()[:n], rec.NumRows())
	defer data.Release()
	projected, err := ToRequestedSchema(ctx, requested, full, data, opts)
	if err != nil {
		return nil, err
	}
	defer projected.Release()
	columns := append(slices.Clone(projected.Columns()), rec.Columns()[n:]...)

	return array.NewRecordBatch(out, columns, rec.NumRows()), nil
}

func changelogRetainedBytes(rec arrow.RecordBatch) int64 {
	var size func(arrow.ArrayData) int64
	size = func(data arrow.ArrayData) (total int64) {
		for _, buf := range data.Buffers() {
			if buf != nil {
				for buf.Parent() != nil {
					buf = buf.Parent()
				}
				total += int64(buf.Cap())
			}
		}
		for _, child := range data.Children() {
			total += size(child)
		}
		if dict := data.Dictionary(); dict != nil {
			// Plain Arrow arrays return a typed nil *array.Data here.
			if concrete, ok := dict.(*array.Data); !ok || concrete != nil {
				total += size(dict)
			}
		}

		return total
	}
	var total int64
	for _, column := range rec.Columns() {
		total += size(column.Data())
	}

	return total
}
