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
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute/exprs"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/apache/iceberg-go/table/substrait"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

type snapshotUpdate struct {
	txn           *Transaction
	io            io.WriteFileIO
	snapshotProps iceberg.Properties
	operation     Operation
}

func (s snapshotUpdate) fastAppend() *snapshotProducer {
	return newFastAppendFilesProducer(OpAppend, s.txn, s.io, nil, s.snapshotProps)
}

func (s snapshotUpdate) mergeOverwrite(commitUUID *uuid.UUID) *snapshotProducer {
	op := s.operation
	if s.operation == OpOverwrite && s.txn.meta.currentSnapshot() == nil {
		op = OpAppend
	}

	return newOverwriteFilesProducer(op, s.txn, s.io, commitUUID, s.snapshotProps)
}

func (s snapshotUpdate) mergeAppend() *snapshotProducer {
	return newMergeAppendFilesProducer(OpAppend, s.txn, s.io, nil, s.snapshotProps)
}

type Transaction struct {
	tbl  *Table
	meta *MetadataBuilder

	reqs []Requirement

	mx        sync.Mutex
	committed bool
}

func (t *Transaction) apply(updates []Update, reqs []Requirement) error {
	t.mx.Lock()
	defer t.mx.Unlock()

	if t.committed {
		return errors.New("transaction has already been committed")
	}

	current, err := t.meta.Build()
	if err != nil {
		return err
	}

	for _, r := range reqs {
		if err := r.Validate(current); err != nil {
			return err
		}
	}

	existing := map[string]struct{}{}
	for _, r := range t.reqs {
		existing[r.GetType()] = struct{}{}
	}

	for _, r := range reqs {
		if _, ok := existing[r.GetType()]; !ok {
			t.reqs = append(t.reqs, r)
		}
	}

	prevUpdates, prevLastUpdated := len(t.meta.updates), t.meta.lastUpdatedMS
	for _, u := range updates {
		if err := u.Apply(t.meta); err != nil {
			return err
		}
	}

	// u.Apply will add updates to t.meta.updates if they are not no-ops
	// and actually perform changes. So let's check if we actually had any
	// changes added and thus need to update the lastupdated value.
	if prevUpdates < len(t.meta.updates) {
		if prevLastUpdated == t.meta.lastUpdatedMS {
			t.meta.lastUpdatedMS = time.Now().UnixMilli()
		}
	}

	return nil
}

func (t *Transaction) appendSnapshotProducer(afs io.IO, props iceberg.Properties) *snapshotProducer {
	manifestMerge := t.meta.props.GetBool(ManifestMergeEnabledKey, ManifestMergeEnabledDefault)
	updateSnapshot := t.updateSnapshot(afs, props, OpAppend)
	if manifestMerge {
		return updateSnapshot.mergeAppend()
	}

	return updateSnapshot.fastAppend()
}

func (t *Transaction) updateSnapshot(fs io.IO, props iceberg.Properties, operation Operation) snapshotUpdate {
	return snapshotUpdate{
		txn:           t,
		io:            fs.(io.WriteFileIO),
		snapshotProps: props,
		operation:     operation,
	}
}

func (t *Transaction) SetProperties(props iceberg.Properties) error {
	if len(props) > 0 {
		return t.apply([]Update{NewSetPropertiesUpdate(props)}, nil)
	}

	return nil
}

func (t *Transaction) UpdateSpec(caseSensitive bool) *UpdateSpec {
	return NewUpdateSpec(t, caseSensitive)
}

// UpdateSchema creates a new UpdateSchema instance for managing schema changes
// within this transaction.
//
// Parameters:
//   - caseSensitive: If true, field name lookups are case-sensitive; if false,
//     field names are matched case-insensitively.
//   - allowIncompatibleChanges: If true, allows schema changes that would normally
//     be rejected for being incompatible (e.g., adding required fields without
//     default values, changing field types in non-promotable ways, or changing
//     column nullability from optional to required).
//   - opts: Optional configuration functions to customize the UpdateSchema behavior.
//
// Returns an UpdateSchema instance that can be used to build and apply schema changes.
func (t *Transaction) UpdateSchema(caseSensitive bool, allowIncompatibleChanges bool, opts ...UpdateSchemaOption) *UpdateSchema {
	return NewUpdateSchema(t, caseSensitive, allowIncompatibleChanges, opts...)
}

type expireSnapshotsCfg struct {
	minSnapshotsToKeep *int
	maxSnapshotAgeMs   *int64
	postCommit         bool
}

type ExpireSnapshotsOpt func(*expireSnapshotsCfg)

func WithRetainLast(n int) ExpireSnapshotsOpt {
	return func(cfg *expireSnapshotsCfg) {
		cfg.minSnapshotsToKeep = &n
	}
}

func WithOlderThan(t time.Duration) ExpireSnapshotsOpt {
	return func(cfg *expireSnapshotsCfg) {
		n := t.Milliseconds()
		cfg.maxSnapshotAgeMs = &n
	}
}

// WithPostCommit controls whether orphaned files (manifests, manifest lists,
// data files) are deleted immediately after expiring snapshots. Defaults to true.
// Set to false to defer file deletion to a separate maintenance job, avoiding
// conflicts with in-flight queries that may still reference those files.
func WithPostCommit(postCommit bool) ExpireSnapshotsOpt {
	return func(cfg *expireSnapshotsCfg) {
		cfg.postCommit = postCommit
	}
}

func (t *Transaction) ExpireSnapshots(opts ...ExpireSnapshotsOpt) error {
	var (
		cfg         = expireSnapshotsCfg{postCommit: true}
		updates     []Update
		reqs        []Requirement
		snapsToKeep = make(map[int64]struct{})
		nowMs       = time.Now().UnixMilli()
	)

	for _, opt := range opts {
		opt(&cfg)
	}

	for refName, ref := range t.meta.refs {
		// Assert that this ref's snapshot ID hasn't changed concurrently.
		// This ensures we don't accidentally expire snapshots that are now
		// referenced by updated refs.
		snapshotID := ref.SnapshotID
		reqs = append(reqs, AssertRefSnapshotID(refName, &snapshotID))

		if refName == MainBranch {
			snapsToKeep[ref.SnapshotID] = struct{}{}
		}

		snap, err := t.meta.SnapshotByID(ref.SnapshotID)
		if err != nil {
			return err
		}

		maxRefAgeMs := cmp.Or(ref.MaxRefAgeMs, cfg.maxSnapshotAgeMs)
		if maxRefAgeMs == nil {
			return errors.New("cannot find a valid value for maxRefAgeMs")
		}

		refAge := nowMs - snap.TimestampMs
		if refAge > *maxRefAgeMs && refName != MainBranch {
			updates = append(updates, NewRemoveSnapshotRefUpdate(refName))

			continue
		}

		var (
			minSnapshotsToKeep = cmp.Or(ref.MinSnapshotsToKeep, cfg.minSnapshotsToKeep)
			maxSnapshotAgeMs   = cmp.Or(ref.MaxSnapshotAgeMs, cfg.maxSnapshotAgeMs)
		)

		if minSnapshotsToKeep == nil || maxSnapshotAgeMs == nil {
			return errors.New("cannot find a valid value for minSnapshotsToKeep and maxSnapshotAgeMs")
		}

		if ref.SnapshotRefType != BranchRef {
			snapsToKeep[ref.SnapshotID] = struct{}{}

			continue
		}

		var (
			numSnapshots int
			snapId       = ref.SnapshotID
		)

		for {
			snap, err := t.meta.SnapshotByID(snapId)
			if err != nil {
				// Parent snapshot may have been removed by a previous expiration.
				// Treat missing parent as end of chain - this is expected behavior.
				break
			}

			snapAge := time.Now().UnixMilli() - snap.TimestampMs
			if (snapAge > *maxSnapshotAgeMs) && (numSnapshots >= *minSnapshotsToKeep) {
				break
			}

			snapsToKeep[snap.SnapshotID] = struct{}{}

			if snap.ParentSnapshotID == nil {
				break
			}

			snapId = *snap.ParentSnapshotID
			numSnapshots++
		}
	}

	var snapsToDelete []int64

	for _, snap := range t.meta.snapshotList {
		if _, found := snapsToKeep[snap.SnapshotID]; !found {
			snapsToDelete = append(snapsToDelete, snap.SnapshotID)
		}
	}

	// Only add the update if there are actually snapshots to delete
	if len(snapsToDelete) > 0 {
		update := NewRemoveSnapshotsUpdate(snapsToDelete).SetSkipIfReferenced()
		update.postCommit = cfg.postCommit
		updates = append(updates, update)
	}

	return t.apply(updates, reqs)
}

func (t *Transaction) AppendTable(ctx context.Context, tbl arrow.Table, batchSize int64, snapshotProps iceberg.Properties) error {
	rdr := array.NewTableReader(tbl, batchSize)
	defer rdr.Release()

	return t.Append(ctx, rdr, snapshotProps)
}

func (t *Transaction) Append(ctx context.Context, rdr array.RecordReader, snapshotProps iceberg.Properties) error {
	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return err
	}
	appendFiles := t.appendSnapshotProducer(fs, snapshotProps)
	itr := recordsToDataFiles(ctx, t.tbl.Location(), t.meta, recordWritingArgs{
		sc:        rdr.Schema(),
		itr:       array.IterFromReader(rdr),
		fs:        fs.(io.WriteFileIO),
		writeUUID: &appendFiles.commitUuid,
	})

	for df, err := range itr {
		if err != nil {
			return err
		}
		appendFiles.appendDataFile(df)
	}

	updates, reqs, err := appendFiles.commit()
	if err != nil {
		return err
	}

	return t.apply(updates, reqs)
}

// ReplaceFiles is actually just an overwrite operation with multiple
// files deleted and added.
//
// TODO: technically, this could be a REPLACE operation but we aren't performing
// any validation here that there are no changes to the underlying data. A REPLACE
// operation is only valid if the data is exactly the same as the previous snapshot.
//
// For now, we'll keep using an overwrite operation.
func (t *Transaction) ReplaceDataFiles(ctx context.Context, filesToDelete, filesToAdd []string, snapshotProps iceberg.Properties) error {
	if len(filesToDelete) == 0 {
		if len(filesToAdd) > 0 {
			return t.AddFiles(ctx, filesToAdd, snapshotProps, false)
		}
	}

	var (
		setToDelete = make(map[string]struct{})
		setToAdd    = make(map[string]struct{})
	)

	for _, f := range filesToDelete {
		setToDelete[f] = struct{}{}
	}

	for _, f := range filesToAdd {
		setToAdd[f] = struct{}{}
	}

	if len(setToDelete) != len(filesToDelete) {
		return errors.New("delete file paths must be unique for ReplaceDataFiles")
	}

	if len(setToAdd) != len(filesToAdd) {
		return errors.New("add file paths must be unique for ReplaceDataFiles")
	}

	s := t.meta.currentSnapshot()
	if s == nil {
		return fmt.Errorf("%w: cannot replace files in a table without an existing snapshot", ErrInvalidOperation)
	}

	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return err
	}
	markedForDeletion := make([]iceberg.DataFile, 0, len(setToDelete))
	for df, err := range s.dataFiles(fs, nil) {
		if err != nil {
			return err
		}

		if _, ok := setToDelete[df.FilePath()]; ok {
			markedForDeletion = append(markedForDeletion, df)
		}

		if _, ok := setToAdd[df.FilePath()]; ok {
			return fmt.Errorf("cannot add files that are already referenced by table, files: %s", df.FilePath())
		}
	}

	if len(markedForDeletion) != len(setToDelete) {
		return errors.New("cannot delete files that do not belong to the table")
	}

	if t.meta.NameMapping() == nil {
		nameMapping := t.meta.CurrentSchema().NameMapping()
		mappingJson, err := json.Marshal(nameMapping)
		if err != nil {
			return err
		}
		err = t.SetProperties(iceberg.Properties{DefaultNameMappingKey: string(mappingJson)})
		if err != nil {
			return err
		}
	}

	commitUUID := uuid.New()
	updater := t.updateSnapshot(fs, snapshotProps, OpOverwrite).mergeOverwrite(&commitUUID)

	for _, df := range markedForDeletion {
		updater.deleteDataFile(df)
	}

	dataFiles := filesToDataFiles(ctx, fs, t.meta, slices.Values(filesToAdd))
	for df, err := range dataFiles {
		if err != nil {
			return err
		}
		updater.appendDataFile(df)
	}

	updates, reqs, err := updater.commit()
	if err != nil {
		return err
	}

	return t.apply(updates, reqs)
}

// validateDataFilePartitionData verifies that DataFile partition values match
// the current partition spec fields by ID without reading file contents.
func validateDataFilePartitionData(df iceberg.DataFile, spec *iceberg.PartitionSpec) error {
	partitionData := df.Partition()

	expectedFieldIDs := make(map[int]string)
	for field := range spec.Fields() {
		expectedFieldIDs[field.FieldID] = field.Name
		if _, ok := partitionData[field.FieldID]; !ok {
			return fmt.Errorf("missing partition value for field id %d (%s)", field.FieldID, field.Name)
		}
	}

	for fieldID := range partitionData {
		if _, ok := expectedFieldIDs[fieldID]; !ok {
			return fmt.Errorf("unknown partition field id %d for spec id %d", fieldID, spec.ID())
		}
	}

	return nil
}

// validateDataFilesToAdd performs metadata-only validation for caller-provided
// DataFiles and returns a set of paths that passed validation.
func (t *Transaction) validateDataFilesToAdd(dataFiles []iceberg.DataFile, operation string) (map[string]struct{}, error) {
	currentSpec, err := t.meta.CurrentSpec()
	if err != nil {
		return nil, fmt.Errorf("could not get current partition spec: %w", err)
	}
	if currentSpec == nil {
		return nil, errors.New("could not get current partition spec: no current partition spec found")
	}

	expectedSpecID := int32(currentSpec.ID())
	setToAdd := make(map[string]struct{}, len(dataFiles))

	for i, df := range dataFiles {
		if df == nil {
			return nil, fmt.Errorf("nil data file at index %d for %s", i, operation)
		}

		path := df.FilePath()
		if path == "" {
			return nil, fmt.Errorf("data file path cannot be empty for %s", operation)
		}

		if _, ok := setToAdd[path]; ok {
			return nil, fmt.Errorf("add data file paths must be unique for %s", operation)
		}
		setToAdd[path] = struct{}{}

		if df.ContentType() != iceberg.EntryContentData {
			return nil, fmt.Errorf("adding files other than data files is not yet implemented: file %s has content type %s for %s", path, df.ContentType(), operation)
		}

		switch df.FileFormat() {
		case iceberg.ParquetFile, iceberg.OrcFile, iceberg.AvroFile:
		default:
			return nil, fmt.Errorf("data file %s has invalid file format %s for %s", path, df.FileFormat(), operation)
		}

		if df.SpecID() != expectedSpecID {
			return nil, fmt.Errorf("data file %s has invalid partition spec id %d for %s: expected %d",
				path, df.SpecID(), operation, expectedSpecID)
		}

		if err := validateDataFilePartitionData(df, currentSpec); err != nil {
			return nil, fmt.Errorf("data file %s has invalid partition data for %s: %w", path, operation, err)
		}
	}

	return setToAdd, nil
}

// AddDataFiles adds pre-built DataFiles to the table without scanning them from storage.
// This is useful for clients who have already constructed DataFile objects with metadata,
// avoiding the need to read files to extract schema and statistics.
//
// Unlike AddFiles, this method does not read files from storage. It validates only metadata
// that can be checked without opening files (for example spec-id and partition field IDs).
//
// Callers are responsible for ensuring each DataFile is valid and consistent with the table.
// Supplying incorrect DataFile metadata can produce an invalid snapshot and break reads.
func (t *Transaction) AddDataFiles(ctx context.Context, dataFiles []iceberg.DataFile, snapshotProps iceberg.Properties) error {
	if len(dataFiles) == 0 {
		return nil
	}

	setToAdd, err := t.validateDataFilesToAdd(dataFiles, "AddDataFiles")
	if err != nil {
		return err
	}

	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return err
	}

	if s := t.meta.currentSnapshot(); s != nil {
		referenced := make([]string, 0)
		for df, err := range s.dataFiles(fs, nil) {
			if err != nil {
				return err
			}

			if _, ok := setToAdd[df.FilePath()]; ok {
				referenced = append(referenced, df.FilePath())
			}
		}

		if len(referenced) > 0 {
			return fmt.Errorf("cannot add files that are already referenced by table, files: %v", referenced)
		}
	}

	if t.meta.NameMapping() == nil {
		nameMapping := t.meta.CurrentSchema().NameMapping()
		mappingJson, err := json.Marshal(nameMapping)
		if err != nil {
			return err
		}
		err = t.SetProperties(iceberg.Properties{DefaultNameMappingKey: string(mappingJson)})
		if err != nil {
			return err
		}
	}

	appendFiles := t.appendSnapshotProducer(fs, snapshotProps)
	for _, df := range dataFiles {
		appendFiles.appendDataFile(df)
	}

	updates, reqs, err := appendFiles.commit()
	if err != nil {
		return err
	}

	return t.apply(updates, reqs)
}

// ReplaceDataFilesWithDataFiles replaces files using pre-built DataFile objects.
// This avoids scanning files to extract schema and statistics - the caller provides
// DataFile objects directly with all required metadata.
//
// For the files to add, use iceberg.NewDataFileBuilder to construct DataFile objects
// with the appropriate metadata (path, record count, file size, partition values).
//
// This method does not open files. It validates only metadata that can be checked
// without reading file contents.
//
// Callers are responsible for ensuring each DataFile is valid and consistent with the table.
// Supplying incorrect DataFile metadata can produce an invalid snapshot and break reads.
//
// This is useful when:
//   - Files are written via a separate I/O path and metadata is already known
//   - Avoiding file scanning improves performance or reliability
//   - Working with storage systems where immediate file reads may be unreliable
func (t *Transaction) ReplaceDataFilesWithDataFiles(ctx context.Context, filesToDelete, filesToAdd []iceberg.DataFile, snapshotProps iceberg.Properties) error {
	if len(filesToDelete) == 0 {
		if len(filesToAdd) > 0 {
			return t.AddDataFiles(ctx, filesToAdd, snapshotProps)
		}

		return nil
	}

	setToAdd, err := t.validateDataFilesToAdd(filesToAdd, "ReplaceDataFilesWithDataFiles")
	if err != nil {
		return err
	}

	setToDelete := make(map[string]struct{}, len(filesToDelete))
	for i, df := range filesToDelete {
		if df == nil {
			return fmt.Errorf("nil data file at index %d for ReplaceDataFilesWithDataFiles", i)
		}

		path := df.FilePath()
		if path == "" {
			return errors.New("delete data file paths must be non-empty for ReplaceDataFilesWithDataFiles")
		}

		if _, ok := setToDelete[path]; ok {
			return errors.New("delete data file paths must be unique for ReplaceDataFilesWithDataFiles")
		}
		setToDelete[path] = struct{}{}
	}

	s := t.meta.currentSnapshot()
	if s == nil {
		return fmt.Errorf("%w: cannot replace files in a table without an existing snapshot", ErrInvalidOperation)
	}

	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return err
	}

	markedForDeletion := make([]iceberg.DataFile, 0, len(setToDelete))
	for df, err := range s.dataFiles(fs, nil) {
		if err != nil {
			return err
		}

		if _, ok := setToDelete[df.FilePath()]; ok {
			markedForDeletion = append(markedForDeletion, df)
		}

		if _, ok := setToAdd[df.FilePath()]; ok {
			return fmt.Errorf("cannot add files that are already referenced by table, files: %s", df.FilePath())
		}
	}

	if len(markedForDeletion) != len(setToDelete) {
		return errors.New("cannot delete files that do not belong to the table")
	}

	if t.meta.NameMapping() == nil {
		nameMapping := t.meta.CurrentSchema().NameMapping()
		mappingJson, err := json.Marshal(nameMapping)
		if err != nil {
			return err
		}
		err = t.SetProperties(iceberg.Properties{DefaultNameMappingKey: string(mappingJson)})
		if err != nil {
			return err
		}
	}

	commitUUID := uuid.New()
	updater := t.updateSnapshot(fs, snapshotProps, OpOverwrite).mergeOverwrite(&commitUUID)

	for _, df := range markedForDeletion {
		updater.deleteDataFile(df)
	}

	for _, df := range filesToAdd {
		updater.appendDataFile(df)
	}

	updates, reqs, err := updater.commit()
	if err != nil {
		return err
	}

	return t.apply(updates, reqs)
}

func (t *Transaction) AddFiles(ctx context.Context, files []string, snapshotProps iceberg.Properties, ignoreDuplicates bool) error {
	set := make(map[string]string)
	for _, f := range files {
		set[f] = f
	}

	if len(set) != len(files) {
		return errors.New("file paths must be unique for AddFiles")
	}

	if !ignoreDuplicates {
		if s := t.meta.currentSnapshot(); s != nil {
			referenced := make([]string, 0)
			fs, err := t.tbl.fsF(ctx)
			if err != nil {
				return err
			}
			for df, err := range s.dataFiles(fs, nil) {
				if err != nil {
					return err
				}

				if _, ok := set[df.FilePath()]; ok {
					referenced = append(referenced, df.FilePath())
				}
			}
			if len(referenced) > 0 {
				return fmt.Errorf("cannot add files that are already referenced by table, files: %v", referenced)
			}
		}
	}

	if t.meta.NameMapping() == nil {
		nameMapping := t.meta.CurrentSchema().NameMapping()
		mappingJson, err := json.Marshal(nameMapping)
		if err != nil {
			return err
		}
		err = t.SetProperties(iceberg.Properties{DefaultNameMappingKey: string(mappingJson)})
		if err != nil {
			return err
		}
	}

	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return err
	}

	updater := t.updateSnapshot(fs, snapshotProps, OpAppend).fastAppend()

	dataFiles := filesToDataFiles(ctx, fs, t.meta, slices.Values(files))
	for df, err := range dataFiles {
		if err != nil {
			return err
		}
		updater.appendDataFile(df)
	}

	updates, reqs, err := updater.commit()
	if err != nil {
		return err
	}

	return t.apply(updates, reqs)
}

// OverwriteTable overwrites the table data using an Arrow Table.
//
// An optional filter (see WithOverwriteFilter) determines which existing data to delete or rewrite:
//   - If filter is nil or AlwaysTrue, all existing data files are deleted and replaced with new data.
//   - If a filter is provided, it acts as a row-level predicate on existing data:
//   - Files where all rows match the filter (strict match) are completely deleted
//   - Files where some rows match and others don't (partial match) are rewritten to keep only non-matching rows
//   - Files where no rows match the filter are kept unchanged
//
// The filter uses both inclusive and strict metrics evaluators on file statistics to classify files:
//   - Inclusive evaluator identifies candidate files that may contain matching rows
//   - Strict evaluator determines if all rows in a file must match the filter
//   - Files that pass inclusive but not strict evaluation are rewritten with filtered data
//
// New data from the provided table is written to the table regardless of the filter.
//
// The batchSize parameter refers to the batch size for reading the input data, not the batch size for writes.
// The concurrency parameter controls the level of parallelism for manifest processing and file rewriting and
// can be overridden using the WithOverwriteConcurrency option.
// If concurrency <= 0, defaults to runtime.GOMAXPROCS(0).
func (t *Transaction) OverwriteTable(ctx context.Context, tbl arrow.Table, batchSize int64, snapshotProps iceberg.Properties, opts ...OverwriteOption) error {
	rdr := array.NewTableReader(tbl, batchSize)
	defer rdr.Release()

	return t.Overwrite(ctx, rdr, snapshotProps, opts...)
}

type overwriteOperation struct {
	concurrency   int
	filter        iceberg.BooleanExpression
	caseSensitive bool
}

// OverwriteOption applies options to overwrite operations
type OverwriteOption func(op *overwriteOperation)

// WithOverwriteConcurrency overwrites the default concurrency for overwrite operations.
// Default: runtime.GOMAXPROCS(0)
func WithOverwriteConcurrency(concurrency int) OverwriteOption {
	return func(op *overwriteOperation) {
		if concurrency <= 0 {
			op.concurrency = runtime.GOMAXPROCS(0)

			return
		}
		op.concurrency = concurrency
	}
}

// WithOverwriteFilter overwrites the default deletion filter on overwrite operations.
// Default: iceberg.AlwaysTrue
func WithOverwriteFilter(filter iceberg.BooleanExpression) OverwriteOption {
	return func(op *overwriteOperation) {
		op.filter = filter
	}
}

// WithOverwriteCaseInsensitive overwrites the default case sensitivity that applies on the binding of the filter.
// Default: case sensitive
// Note that the sensitivity only applies to the field name and not the evaluation of the literals on string fields.
func WithOverwriteCaseInsensitive() OverwriteOption {
	return func(op *overwriteOperation) {
		op.caseSensitive = false
	}
}

// Overwrite overwrites the table data using a RecordReader.
//
// An optional filter (see WithOverwriteFilter) determines which existing data to delete or rewrite:
//   - If filter is nil or AlwaysTrue, all existing data files are deleted and replaced with new data.
//   - If a filter is provided, it acts as a row-level predicate on existing data:
//   - Files where all rows match the filter (strict match) are completely deleted
//   - Files where some rows match and others don't (partial match) are rewritten to keep only non-matching rows
//   - Files where no rows match the filter are kept unchanged
//
// The filter uses both inclusive and strict metrics evaluators on file statistics to classify files:
//   - Inclusive evaluator identifies candidate files that may contain matching rows
//   - Strict evaluator determines if all rows in a file must match the filter
//   - Files that pass inclusive but not strict evaluation are rewritten with filtered data
//
// New data from the provided RecordReader is written to the table regardless of the filter.
//
// The concurrency parameter controls the level of parallelism for manifest processing and file rewriting and
// can be overridden using the WithOverwriteConcurrency option.
// If concurrency <= 0, defaults to runtime.GOMAXPROCS(0).
func (t *Transaction) Overwrite(ctx context.Context, rdr array.RecordReader, snapshotProps iceberg.Properties, opts ...OverwriteOption) error {
	overwrite := overwriteOperation{
		concurrency:   runtime.GOMAXPROCS(0),
		filter:        iceberg.AlwaysTrue{},
		caseSensitive: true,
	}
	for _, apply := range opts {
		apply(&overwrite)
	}

	updater, err := t.performCopyOnWriteDeletion(ctx, OpOverwrite, snapshotProps, overwrite.filter, overwrite.caseSensitive, overwrite.concurrency)
	if err != nil {
		return err
	}

	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return err
	}
	itr := recordsToDataFiles(ctx, t.tbl.Location(), t.meta, recordWritingArgs{
		sc:        rdr.Schema(),
		itr:       array.IterFromReader(rdr),
		fs:        fs.(io.WriteFileIO),
		writeUUID: &updater.commitUuid,
	})

	for df, err := range itr {
		if err != nil {
			return err
		}
		updater.appendDataFile(df)
	}

	updates, reqs, err := updater.commit()
	if err != nil {
		return err
	}

	return t.apply(updates, reqs)
}

func (t *Transaction) performCopyOnWriteDeletion(ctx context.Context, operation Operation, snapshotProps iceberg.Properties, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int) (*snapshotProducer, error) {
	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return nil, err
	}

	if t.meta.NameMapping() == nil {
		nameMapping := t.meta.CurrentSchema().NameMapping()
		mappingJson, err := json.Marshal(nameMapping)
		if err != nil {
			return nil, err
		}
		err = t.SetProperties(iceberg.Properties{DefaultNameMappingKey: string(mappingJson)})
		if err != nil {
			return nil, err
		}
	}

	commitUUID := uuid.New()
	updater := t.updateSnapshot(fs, snapshotProps, operation).mergeOverwrite(&commitUUID)

	filesToDelete, filesToRewrite, err := t.classifyFilesForDeletions(ctx, fs, filter, caseSensitive, concurrency)
	if err != nil {
		return nil, err
	}

	for _, df := range filesToDelete {
		updater.deleteDataFile(df)
	}

	if len(filesToRewrite) > 0 {
		if err := t.rewriteFilesWithFilter(ctx, fs, updater, filesToRewrite, filter, caseSensitive, concurrency); err != nil {
			return nil, err
		}
	}

	return updater, nil
}

func (t *Transaction) performMergeOnReadDeletion(ctx context.Context, snapshotProps iceberg.Properties, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int) (*snapshotProducer, error) {
	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return nil, err
	}

	if t.meta.NameMapping() == nil {
		nameMapping := t.meta.CurrentSchema().NameMapping()
		mappingJson, err := json.Marshal(nameMapping)
		if err != nil {
			return nil, err
		}
		err = t.SetProperties(iceberg.Properties{DefaultNameMappingKey: string(mappingJson)})
		if err != nil {
			return nil, err
		}
	}

	commitUUID := uuid.New()
	updater := t.updateSnapshot(fs, snapshotProps, OpDelete).mergeOverwrite(&commitUUID)

	filesToDelete, withPartialDeletions, err := t.classifyFilesForDeletions(ctx, fs, filter, caseSensitive, concurrency)
	if err != nil {
		return nil, err
	}

	for _, df := range filesToDelete {
		updater.deleteDataFile(df)
	}

	if len(withPartialDeletions) > 0 {
		if err := t.writePositionDeletesForFiles(ctx, fs, updater, withPartialDeletions, filter, caseSensitive, concurrency, commitUUID); err != nil {
			return nil, err
		}
	}

	return updater, nil
}

type DeleteOption func(deleteOp *deleteOperation)

type deleteOperation struct {
	caseSensitive bool
	concurrency   int
}

// WithDeleteConcurrency overwrites the default concurrency for delete operations.
// Default: runtime.GOMAXPROCS(0)
func WithDeleteConcurrency(concurrency int) DeleteOption {
	return func(op *deleteOperation) {
		if concurrency <= 0 {
			op.concurrency = runtime.GOMAXPROCS(0)

			return
		}
		op.concurrency = concurrency
	}
}

// WithDeleteCaseInsensitive changes the binding of the filter to be case insensitive instead of the
// Default: case sensitive
// Note that the sensitivity only applies to the field name and not the evaluation of the literals on string fields.
func WithDeleteCaseInsensitive() DeleteOption {
	return func(deleteOp *deleteOperation) {
		deleteOp.caseSensitive = false
	}
}

// Delete deletes records matching the provided filter.
//
// The provided filter acts as a row-level predicate on existing data:
//   - Files where all rows match the filter (strict match) are completely deleted
//   - Files where some rows match and others don't (partial match) are rewritten to keep only non-matching rows
//   - Files where no rows match the filter are kept unchanged
//
// The filter uses both inclusive and strict metrics evaluators on file statistics to classify files:
//   - Inclusive evaluator identifies candidate files that may contain matching rows
//   - Strict evaluator determines if all rows in a file must match the filter
//   - Files that pass inclusive but not strict evaluation are rewritten with filtered data
//
// The concurrency parameter controls the level of parallelism for manifest processing and file rewriting and
// can be overridden using the WithOverwriteConcurrency option. Defaults to runtime.GOMAXPROCS(0).
func (t *Transaction) Delete(ctx context.Context, filter iceberg.BooleanExpression, snapshotProps iceberg.Properties, opts ...DeleteOption) (err error) {
	deleteOp := deleteOperation{
		concurrency:   runtime.GOMAXPROCS(0),
		caseSensitive: true,
	}
	for _, apply := range opts {
		apply(&deleteOp)
	}

	var updater *snapshotProducer
	writeDeleteMode := WriteDeleteModeDefault
	// Only copy on write is supported on v1 so we ignore any override to the write delete mode unless the version is
	// 2 and up
	if t.meta.formatVersion > 1 {
		writeDeleteMode = t.meta.props.Get(WriteDeleteModeKey, WriteDeleteModeDefault)
	}
	switch writeDeleteMode {
	case WriteModeCopyOnWrite:
		updater, err = t.performCopyOnWriteDeletion(ctx, OpDelete, snapshotProps, filter, deleteOp.caseSensitive, deleteOp.concurrency)
		if err != nil {
			return err
		}
	case WriteModeMergeOnRead:
		updater, err = t.performMergeOnReadDeletion(ctx, snapshotProps, filter, deleteOp.caseSensitive, deleteOp.concurrency)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported write mode: '%s'", writeDeleteMode)
	}

	updates, reqs, err := updater.commit()
	if err != nil {
		return err
	}

	return t.apply(updates, reqs)
}

// classifyFilesForDeletions classifies existing data files based on the provided filter.
// Returns files to delete completely, files to rewrite partially, and any error.
func (t *Transaction) classifyFilesForDeletions(ctx context.Context, fs io.IO, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int) (filesToDelete, filesWithPartialDeletions []iceberg.DataFile, err error) {
	s := t.meta.currentSnapshot()
	if s == nil {
		return nil, nil, nil
	}

	if filter == nil || filter.Equals(iceberg.AlwaysTrue{}) {
		for df, err := range s.dataFiles(fs, nil) {
			if err != nil {
				return nil, nil, err
			}
			if df.ContentType() == iceberg.EntryContentData {
				filesToDelete = append(filesToDelete, df)
			}
		}

		return filesToDelete, filesWithPartialDeletions, nil
	}

	return t.classifyFilesForFilteredDeletions(ctx, fs, filter, caseSensitive, concurrency)
}

type fileClassificationTask struct {
	meta             Metadata
	partitionFilters *keyDefaultMap[int, iceberg.BooleanExpression]
	caseSensitive    bool
	rowFilter        iceberg.BooleanExpression
}

func newFileClassificationTask(meta Metadata, rowFilter iceberg.BooleanExpression, caseSensitive bool) *fileClassificationTask {
	classificationTask := &fileClassificationTask{
		meta:          meta,
		caseSensitive: caseSensitive,
		rowFilter:     rowFilter,
	}
	classificationTask.partitionFilters = newKeyDefaultMapWrapErr(classificationTask.buildPartitionProjection)

	return classificationTask
}

func (t *fileClassificationTask) buildManifestEvaluator(specID int) (func(iceberg.ManifestFile) (bool, error), error) {
	return buildManifestEvaluator(specID, t.meta, t.partitionFilters, t.caseSensitive)
}

func (t *fileClassificationTask) buildPartitionProjection(specID int) (iceberg.BooleanExpression, error) {
	return buildPartitionProjection(specID, t.meta, t.rowFilter, t.caseSensitive)
}

// classifyFilesForFilteredDeletions classifies files for filtered overwrite operations.
// Returns files to delete completely, files to rewrite partially, and any error.
func (t *Transaction) classifyFilesForFilteredDeletions(ctx context.Context, fs io.IO, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int) (filesToDelete, filesWithPartialDeletes []iceberg.DataFile, err error) {
	schema := t.meta.CurrentSchema()
	meta, err := t.meta.Build()
	if err != nil {
		return nil, nil, err
	}

	inclusiveEvaluator, err := newInclusiveMetricsEvaluator(schema, filter, caseSensitive, false)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create inclusive metrics evaluator: %w", err)
	}

	strictEvaluator, err := newStrictMetricsEvaluator(schema, filter, caseSensitive, false)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create strict metrics evaluator: %w", err)
	}

	classificationTask := newFileClassificationTask(meta, filter, caseSensitive)
	manifestEvaluators := newKeyDefaultMapWrapErr(classificationTask.buildManifestEvaluator)

	s := t.meta.currentSnapshot()
	var manifests []iceberg.ManifestFile
	if s != nil {
		manifests, err = s.Manifests(fs)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get manifests: %w", err)
		}
	}

	var mu sync.Mutex

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(min(concurrency, len(manifests)))

	for _, manifest := range manifests {
		manifest := manifest // capture loop variable
		g.Go(func() error {
			manifestEval := manifestEvaluators.Get(int(manifest.PartitionSpecID()))
			if manifestEval != nil {
				match, err := manifestEval(manifest)
				if err != nil {
					return fmt.Errorf("failed to evaluate manifest %s: %w", manifest.FilePath(), err)
				}
				if !match {
					return nil
				}
			}

			entries, err := manifest.FetchEntries(fs, false)
			if err != nil {
				return fmt.Errorf("failed to fetch manifest entries: %w", err)
			}

			localDelete := make([]iceberg.DataFile, 0)
			localRewrite := make([]iceberg.DataFile, 0)

			for _, entry := range entries {
				if entry.Status() == iceberg.EntryStatusDELETED {
					continue
				}

				df := entry.DataFile()
				if df.ContentType() != iceberg.EntryContentData {
					continue
				}

				inclusive, err := inclusiveEvaluator(df)
				if err != nil {
					return fmt.Errorf("failed to evaluate data file %s with inclusive evaluator: %w", df.FilePath(), err)
				}

				if !inclusive {
					continue
				}

				strict, err := strictEvaluator(df)
				if err != nil {
					return fmt.Errorf("failed to evaluate data file %s with strict evaluator: %w", df.FilePath(), err)
				}

				if strict {
					localDelete = append(localDelete, df)
				} else {
					localRewrite = append(localRewrite, df)
				}
			}

			if len(localDelete) > 0 || len(localRewrite) > 0 {
				mu.Lock()
				filesToDelete = append(filesToDelete, localDelete...)
				filesWithPartialDeletes = append(filesWithPartialDeletes, localRewrite...)
				mu.Unlock()
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	return filesToDelete, filesWithPartialDeletes, nil
}

// rewriteFilesWithFilter rewrites data files by preserving only rows that do NOT match the filter
func (t *Transaction) rewriteFilesWithFilter(ctx context.Context, fs io.IO, updater *snapshotProducer, files []iceberg.DataFile, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int) error {
	complementFilter := iceberg.NewNot(filter)

	for _, originalFile := range files {
		// Use a separate UUID for rewrite operations to avoid filename collisions with new data files
		rewriteUUID := uuid.New()
		rewrittenFiles, err := t.rewriteSingleFile(ctx, fs, originalFile, complementFilter, caseSensitive, rewriteUUID, concurrency)
		if err != nil {
			return fmt.Errorf("failed to rewrite file %s: %w", originalFile.FilePath(), err)
		}

		updater.deleteDataFile(originalFile)
		for _, rewrittenFile := range rewrittenFiles {
			updater.appendDataFile(rewrittenFile)
		}
	}

	return nil
}

// rewriteSingleFile reads a single data file, applies the filter, and writes new files with filtered data
func (t *Transaction) rewriteSingleFile(ctx context.Context, fs io.IO, originalFile iceberg.DataFile, filter iceberg.BooleanExpression, caseSensitive bool, commitUUID uuid.UUID, concurrency int) ([]iceberg.DataFile, error) {
	scanTask := &FileScanTask{
		File:   originalFile,
		Start:  0,
		Length: originalFile.FileSizeBytes(),
	}

	boundFilter, err := iceberg.BindExpr(t.meta.CurrentSchema(), filter, caseSensitive)
	if err != nil {
		return nil, fmt.Errorf("failed to bind filter: %w", err)
	}

	meta, err := t.meta.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build metadata: %w", err)
	}

	scanner := &arrowScan{
		metadata:        meta,
		fs:              fs,
		projectedSchema: t.meta.CurrentSchema(),
		boundRowFilter:  boundFilter,
		caseSensitive:   caseSensitive,
		rowLimit:        -1, // No limit
		concurrency:     concurrency,
	}

	arrowSchema, recordIter, err := scanner.GetRecords(ctx, []FileScanTask{*scanTask})
	if err != nil {
		return nil, fmt.Errorf("failed to get records from original file: %w", err)
	}

	// Wrap the iterator to release records after consumption
	releaseIter := func(yield func(arrow.RecordBatch, error) bool) {
		for rec, err := range recordIter {
			if err != nil {
				yield(nil, err)

				return
			}
			if !yield(rec, nil) {
				rec.Release()

				return
			}
			rec.Release()
		}
	}

	var result []iceberg.DataFile
	itr := recordsToDataFiles(ctx, t.tbl.Location(), t.meta, recordWritingArgs{
		sc:        arrowSchema,
		itr:       releaseIter,
		fs:        fs.(io.WriteFileIO),
		writeUUID: &commitUUID,
	})

	for df, err := range itr {
		if err != nil {
			return nil, err
		}
		result = append(result, df)
	}

	return result, nil
}

// writePositionDeletesForFiles rewrites data files by preserving only rows that do NOT match the filter
func (t *Transaction) writePositionDeletesForFiles(ctx context.Context, fs io.IO, updater *snapshotProducer, files []iceberg.DataFile, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int, commitUUID uuid.UUID) error {
	posDeleteRecIter, err := t.makePositionDeleteRecordsForFilter(ctx, fs, files, filter, caseSensitive, concurrency)
	if err != nil {
		return err
	}

	partitionContextByFilePath := make(map[string]partitionContext, len(files))
	for _, df := range files {
		partitionContextByFilePath[df.FilePath()] = partitionContext{partitionData: df.Partition(), specID: df.SpecID()}
	}

	posDeleteFiles := positionDeleteRecordsToDataFiles(ctx, t.tbl.Location(), t.meta, partitionContextByFilePath, recordWritingArgs{
		sc:        PositionalDeleteArrowSchema,
		itr:       posDeleteRecIter,
		writeUUID: &commitUUID,
		fs:        fs.(io.WriteFileIO),
	})

	for f, err := range posDeleteFiles {
		if err != nil {
			return err
		}
		updater.appendPositionDeleteFile(f)
	}

	return nil
}

func (t *Transaction) makePositionDeleteRecordsForFilter(ctx context.Context, fs io.IO, files []iceberg.DataFile, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int) (seq2 iter.Seq2[arrow.RecordBatch, error], err error) {
	tasks := make([]FileScanTask, 0, len(files))
	for _, f := range files {
		tasks = append(tasks, FileScanTask{
			File:   f,
			Start:  0,
			Length: f.FileSizeBytes(),
		})
	}

	boundFilter, err := iceberg.BindExpr(t.meta.CurrentSchema(), filter, caseSensitive)
	if err != nil {
		return nil, fmt.Errorf("failed to bind filter: %w", err)
	}

	meta, err := t.meta.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build metadata: %w", err)
	}

	scanner := &arrowScan{
		metadata:        meta,
		fs:              fs,
		projectedSchema: t.meta.CurrentSchema(),
		boundRowFilter:  boundFilter,
		caseSensitive:   caseSensitive,
		rowLimit:        -1, // No limit
		concurrency:     concurrency,
	}

	deletesPerFile, err := readAllDeleteFiles(ctx, fs, tasks, concurrency)
	if err != nil {
		return nil, err
	}

	extSet := substrait.NewExtensionSet()

	ctx, cancel := context.WithCancelCause(exprs.WithExtensionIDSet(ctx, extSet))
	taskChan := make(chan internal.Enumerated[FileScanTask], len(tasks))

	numWorkers := min(concurrency, len(tasks))
	records := make(chan enumeratedRecord, numWorkers)

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case task, ok := <-taskChan:
					if !ok {
						return
					}

					if err := scanner.producePosDeletesFromTask(ctx, task, deletesPerFile[task.Value.File.FilePath()], records); err != nil {
						cancel(err)

						return
					}
				}
			}
		}()
	}

	go func() {
		for i, t := range tasks {
			taskChan <- internal.Enumerated[FileScanTask]{
				Value: t, Index: i, Last: i == len(tasks)-1,
			}
		}
		close(taskChan)

		wg.Wait()
		close(records)
	}()

	return createIterator(ctx, uint(numWorkers), records, deletesPerFile, cancel, scanner.rowLimit), nil
}

func (t *Transaction) Scan(opts ...ScanOption) (*Scan, error) {
	updatedMeta, err := t.meta.Build()
	if err != nil {
		return nil, err
	}

	s := &Scan{
		metadata:       updatedMeta,
		ioF:            t.tbl.fsF,
		rowFilter:      iceberg.AlwaysTrue{},
		selectedFields: []string{"*"},
		caseSensitive:  true,
		limit:          ScanNoLimit,
		concurrency:    runtime.GOMAXPROCS(0),
	}

	for _, opt := range opts {
		opt(s)
	}

	s.partitionFilters = newKeyDefaultMapWrapErr(s.buildPartitionProjection)

	return s, nil
}

func (t *Transaction) StagedTable() (*StagedTable, error) {
	updatedMeta, err := t.meta.Build()
	if err != nil {
		return nil, err
	}

	return &StagedTable{
		Table: New(
			t.tbl.identifier,
			updatedMeta,
			updatedMeta.Location(),
			t.tbl.fsF,
			t.tbl.cat,
		),
	}, nil
}

func (t *Transaction) Commit(ctx context.Context) (*Table, error) {
	t.mx.Lock()
	defer t.mx.Unlock()

	if t.committed {
		return nil, errors.New("transaction has already been committed")
	}

	t.committed = true

	if len(t.meta.updates) > 0 {
		t.reqs = append(t.reqs, AssertTableUUID(t.meta.uuid))
		tbl, err := t.tbl.doCommit(ctx, t.meta.updates, t.reqs)
		if err != nil {
			return tbl, err
		}

		for _, u := range t.meta.updates {
			if perr := u.PostCommit(ctx, t.tbl, tbl); perr != nil {
				err = errors.Join(err, perr)
			}
		}

		return tbl, err
	}

	return t.tbl, nil
}

type StagedTable struct {
	*Table
}

func (s *StagedTable) Refresh(ctx context.Context) (*Table, error) {
	return nil, fmt.Errorf("%w: cannot refresh a staged table", ErrInvalidOperation)
}

func (s *StagedTable) Scan(opts ...ScanOption) *Scan {
	panic(fmt.Errorf("%w: cannot scan a staged table", ErrInvalidOperation))
}
