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
	"encoding/json"
	"fmt"
	"iter"
	"runtime"
	"slices"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute/exprs"
	"github.com/apache/iceberg-go"
	iceberginternal "github.com/apache/iceberg-go/internal"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/dv"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/apache/iceberg-go/table/substrait"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

func (t *Transaction) performCopyOnWriteDeletion(ctx context.Context, operation Operation, snapshotProps iceberg.Properties, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int) (*snapshotProducer, io.WriteFileIO, error) {
	meta, err := t.txnMeta()
	if err != nil {
		return nil, nil, err
	}
	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return nil, nil, err
	}
	wfs, err := requireWriteFileIO(fs)
	if err != nil {
		return nil, nil, err
	}

	if meta.NameMapping() == nil {
		nameMapping := meta.CurrentSchema().NameMapping()
		mappingJson, err := json.Marshal(nameMapping)
		if err != nil {
			return nil, nil, err
		}
		err = t.SetProperties(iceberg.Properties{DefaultNameMappingKey: string(mappingJson)})
		if err != nil {
			return nil, nil, err
		}
	}

	commitUUID := uuid.New()
	updater := t.updateSnapshot(wfs, snapshotProps, operation).mergeOverwrite(&commitUUID, filter)
	updater.setManifestConcurrency(concurrency)

	filesToDelete, entriesToRewrite, err := t.classifyFilesForDeletions(ctx, fs, filter, caseSensitive, concurrency)
	if err != nil {
		return nil, nil, err
	}

	// SetProperties above may have staged a name mapping, which replaces t.meta, so re-read it.
	meta, err = t.txnMeta()
	if err != nil {
		return nil, nil, err
	}

	// Rewrites need the live deletes to drop rows already deleted, and every removed file must take its deletion vector with it.
	// DVs only exist from v3, so a v2 delete that only drops whole files skips the read.
	var (
		deletes   planningDeletes
		builtMeta Metadata
	)
	if len(entriesToRewrite) > 0 || (len(filesToDelete) > 0 && meta.formatVersion >= 3) {
		builtMeta, err = meta.Build()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to build metadata: %w", err)
		}
		deletes, err = t.readPlanningDeletes(fs, builtMeta)
		if err != nil {
			return nil, nil, err
		}
	}

	for _, df := range filesToDelete {
		deletes.dvs.removeDataFile(updater, df)
	}

	if len(entriesToRewrite) > 0 {
		if err := t.rewriteFilesWithFilter(ctx, fs, updater, builtMeta, deletes, entriesToRewrite, filter, caseSensitive, concurrency); err != nil {
			return nil, nil, err
		}
	}

	// Reject the commit if a concurrent snapshot added deletes against any
	// data file this operation removes. The overwriteFiles producer only
	// checks added data files (and only under serializable isolation);
	// without this check a refresh-and-replay would swap the original file
	// for a rewrite built from the stale snapshot, dropping the concurrent
	// deletes and resurrecting their rows. Mirrors Java's copy-on-write
	// validateNoConflictingDeletes: no isolation gating. The check is
	// shared with RewriteDataFiles via rewriteValidator, so changes there
	// apply here too. A concurrent removal of one of these files is caught
	// earlier, by the retry rebuild's checkRemovedFiles (ErrCommitDiverged).
	removed := slices.Clip(filesToDelete)
	for _, entry := range entriesToRewrite {
		removed = append(removed, entry.DataFile())
	}
	if len(removed) > 0 {
		t.addValidator(rewriteValidator(removed))
	}

	return updater, wfs, nil
}

func (t *Transaction) performMergeOnReadDeletion(ctx context.Context, snapshotProps iceberg.Properties, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int) (*snapshotProducer, error) {
	meta, err := t.txnMeta()
	if err != nil {
		return nil, err
	}
	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return nil, err
	}
	wfs, err := requireWriteFileIO(fs)
	if err != nil {
		return nil, err
	}

	if meta.NameMapping() == nil {
		nameMapping := meta.CurrentSchema().NameMapping()
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
	updater := t.updateSnapshot(wfs, snapshotProps, OpDelete).mergeOverwrite(&commitUUID, filter)
	updater.setManifestConcurrency(concurrency)

	filesToDelete, withPartialDeletions, err := t.classifyFilesForDeletions(ctx, fs, filter, caseSensitive, concurrency)
	if err != nil {
		return nil, err
	}

	// On v3 every removed file takes its deletion vector with it, and a partially deleted file's vector is
	// merged into its replacement. Both need only the deletion vectors, read once.
	// DVs only exist from v3, so v2 skips the read.
	var dvs liveDeletionVectors
	if meta.formatVersion >= 3 && (len(filesToDelete) > 0 || len(withPartialDeletions) > 0) {
		dvs, err = t.readLiveDeletionVectors(fs)
		if err != nil {
			return nil, err
		}
	}

	for _, df := range filesToDelete {
		dvs.removeDataFile(updater, df)
	}

	if len(withPartialDeletions) > 0 {
		partialFiles := make([]iceberg.DataFile, 0, len(withPartialDeletions))
		for _, entry := range withPartialDeletions {
			partialFiles = append(partialFiles, entry.DataFile())
		}
		referenced, err := t.writePositionDeletesForFiles(ctx, fs, updater, dvs, partialFiles, filter, caseSensitive, concurrency, commitUUID)
		if err != nil {
			return nil, err
		}

		// Verify the data files these position deletes target still exist on
		// the branch head at commit time. The overwriteFiles producer only
		// validates partition-filter overlap; without this check a concurrent
		// commit that removed a referenced data file would silently orphan the
		// deletes (they would apply to rewritten data or dangle). Mirrors
		// RowDelta: collect referenced data-file paths, leave out the ones
		// staged earlier in the transaction, and run validateDataFilesExist
		// unconditionally (no isolation gating).
		referenced, err = t.headDataFiles(fs, referenced, nil)
		if err != nil {
			return nil, err
		}
		if len(referenced) > 0 {
			t.addValidator(func(cc *conflictContext) error {
				return validateDataFilesExist(cc, referenced)
			})
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
// A row matches only if the filter is true for it. Rows where the filter evaluates to NULL, for example
// `age = 30` on a NULL age, do not match and are kept.
//
// The filter uses both inclusive and strict metrics evaluators on file statistics to classify files:
//   - Inclusive evaluator identifies candidate files that may contain matching rows
//   - Strict evaluator determines if all rows in a file must match the filter
//   - Files that pass inclusive but not strict evaluation are rewritten with filtered data
//
// On a v3 table, every data file the delete removes or rewrites takes its deletion vector with it.
// A merge-on-read delete replaces the existing deletion vector of a file it deletes rows from.
//
// A commit that removes a deletion vector is not replayed: if another writer commits first,
// it fails with ErrCommitFailed instead of refreshing and retrying, so reload the table and run the delete again.
//
// The concurrency parameter controls the level of parallelism for manifest processing and file rewriting and
// can be overridden using the WithOverwriteConcurrency option. Defaults to runtime.GOMAXPROCS(0).
func (t *Transaction) Delete(ctx context.Context, filter iceberg.BooleanExpression, snapshotProps iceberg.Properties, opts ...DeleteOption) (err error) {
	meta, err := t.txnMeta()
	if err != nil {
		return err
	}
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
	if meta.formatVersion > 1 {
		writeDeleteMode = meta.props.Get(WriteDeleteModeKey, WriteDeleteModeDefault)
	}
	switch writeDeleteMode {
	case WriteModeCopyOnWrite:
		updater, _, err = t.performCopyOnWriteDeletion(ctx, OpDelete, snapshotProps, filter, deleteOp.caseSensitive, deleteOp.concurrency)
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

	updates, reqs, err := updater.commit(ctx)
	if err != nil {
		return err
	}

	return t.apply(updates, reqs)
}

// classifyFilesForDeletions classifies existing data files based on the provided filter.
// Returns files to delete completely, the manifest entries of files that partially match, and any error.
// Partial matches stay entries because their sequence numbers decide which deletes apply and seed row-lineage synthesis.
func (t *Transaction) classifyFilesForDeletions(ctx context.Context, fs io.IO, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int) (filesToDelete []iceberg.DataFile, filesWithPartialDeletions []iceberg.ManifestEntry, err error) {
	meta, err := t.txnMeta()
	if err != nil {
		return nil, nil, err
	}

	s := t.planningSnapshot(meta)
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

		return filesToDelete, nil, nil
	}

	return t.classifyFilesForFilteredDeletions(ctx, fs, filter, caseSensitive, concurrency)
}

type fileClassificationTask struct {
	meta             Metadata
	partitionFilters *keyDefaultMapErr[int, iceberg.BooleanExpression]
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
	return buildManifestEvaluator(specID, t.meta, t.meta.CurrentSchema(), t.partitionFilters, t.caseSensitive)
}

func (t *fileClassificationTask) buildPartitionProjection(specID int) (iceberg.BooleanExpression, error) {
	return buildPartitionProjection(specID, t.meta, t.meta.CurrentSchema(), t.rowFilter, t.caseSensitive)
}

// classifyFilesForFilteredDeletions classifies files for filtered overwrite operations.
// Returns files to delete completely, the manifest entries of files that partially match, and any error.
func (t *Transaction) classifyFilesForFilteredDeletions(ctx context.Context, fs io.IO, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int) (filesToDelete []iceberg.DataFile, filesWithPartialDeletes []iceberg.ManifestEntry, err error) {
	meta, err := t.txnMeta()
	if err != nil {
		return nil, nil, err
	}

	schema := meta.CurrentSchema()
	builtMeta, err := meta.Build()
	if err != nil {
		return nil, nil, err
	}

	inclusiveEvaluator, err := newInclusiveMetricsEvaluator(schema, filter, caseSensitive, false)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create inclusive metrics evaluator: %w", err)
	}

	// A file is deleted as a whole only if the filter is true for all of its
	// rows. The strict evaluator treats predicates as two-valued, so NotEqual
	// and NotIn would also count rows where the filter is NULL. Evaluate the
	// negation of the rows the rewrite keeps instead, so that both paths agree.
	notTrue, err := isNotTrueExpr(filter)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build negated filter: %w", err)
	}

	strictEvaluator, err := newStrictMetricsEvaluator(schema, iceberg.NewNot(notTrue), caseSensitive, false)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create strict metrics evaluator: %w", err)
	}

	classificationTask := newFileClassificationTask(builtMeta, filter, caseSensitive)
	manifestEvaluators := newKeyDefaultMapWrapErr(classificationTask.buildManifestEvaluator)

	s := t.planningSnapshot(meta)
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
		g.Go(func() error {
			manifestEval, err := manifestEvaluators.Get(int(manifest.PartitionSpecID()))
			if err != nil {
				return fmt.Errorf("failed to build manifest evaluator for spec %d: %w", manifest.PartitionSpecID(), err)
			}
			match, err := manifestEval(manifest)
			if err != nil {
				return fmt.Errorf("failed to evaluate manifest %s: %w", manifest.FilePath(), err)
			}
			if !match {
				return nil
			}

			localDelete := make([]iceberg.DataFile, 0)
			localRewrite := make([]iceberg.ManifestEntry, 0)

			for entry, err := range manifest.Entries(fs, false) {
				if err != nil {
					return fmt.Errorf("failed to fetch manifest entries: %w", err)
				}
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
					localRewrite = append(localRewrite, entry)
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

// maps a data file path to the live deletion vector that references it.
type liveDeletionVectors map[string]iceberg.ManifestEntry

// removeDataFile removes df and, as the spec requires, the deletion vector that references it.
//
// Removing a deletion vector makes the commit non-replayable (see snapshotProducer.commitManifests).
// Position and equality delete files are left alone, since they can still apply to other files.
func (dvs liveDeletionVectors) removeDataFile(updater *snapshotProducer, df iceberg.DataFile) {
	updater.deleteDataFile(df)
	if dv, ok := dvs[df.FilePath()]; ok {
		updater.removeDeletionVector(dv.DataFile())
	}
}

// planningDeletes indexes the live delete files of the planning snapshot the same way scan planning indexes them.
type planningDeletes struct {
	positional *positionalDeleteIndex
	dvs        liveDeletionVectors
	equality   *equalityDeleteIndex
}

// classifyLiveDeletes reads every live delete file of the planning snapshot and sorts it by kind.
//
// Known limitation (#2160): every live delete manifest of the snapshot is read on each call,
// with no partition pruning, so a narrow delete or rewrite on a delete-heavy table pays for all of them.
func (t *Transaction) classifyLiveDeletes(fs io.IO) (classifiedManifestEntries, error) {
	meta, err := t.txnMeta()
	if err != nil {
		return classifiedManifestEntries{}, err
	}

	var liveDeletes []iceberg.ManifestEntry
	if s := t.planningSnapshot(meta); s != nil {
		// Skip DELETED entries: a replaced DV stays behind as one, and merging it
		// into a new DV would bring back deleted rows. See #1372.
		for entry, err := range s.entries(fs, iceberg.ManifestContentDeletes, true) {
			if err != nil {
				return classifiedManifestEntries{}, fmt.Errorf("failed to read delete manifests: %w", err)
			}
			liveDeletes = append(liveDeletes, entry)
		}
	}

	return classifyManifestEntries(liveDeletes)
}

// readPlanningDeletes reads and indexes every live delete file of the planning snapshot. Copy-on-write
// uses it because a rewrite must apply every kind of delete to the rows it keeps.
//
// Delete manifests are not pruned by the row filter: with filter id=2 the rewrite keeps id=1,
// so a delete on id=1 must still be applied.
func (t *Transaction) readPlanningDeletes(fs io.IO, builtMeta Metadata) (planningDeletes, error) {
	meta, err := t.txnMeta()
	if err != nil {
		return planningDeletes{}, err
	}

	classified, err := t.classifyLiveDeletes(fs)
	if err != nil {
		return planningDeletes{}, err
	}
	positional, err := buildPositionalDeleteIndex(classified.positionalDeleteEntries)
	if err != nil {
		return planningDeletes{}, err
	}
	dvs, err := buildDVIndex(classified.dvEntries)
	if err != nil {
		return planningDeletes{}, err
	}
	equality, err := buildEqualityDeleteIndex(classified.equalityDeleteEntries, builtMeta, meta.CurrentSchema())
	if err != nil {
		return planningDeletes{}, err
	}

	return planningDeletes{positional: positional, dvs: dvs, equality: equality}, nil
}

// readLiveDeletionVectors returns the live deletion vectors of the planning snapshot. Merge-on-read uses it
// because it only drops or merges deletion vectors, so it skips building the position and equality delete
// indexes and neither pays for them nor fails on them.
func (t *Transaction) readLiveDeletionVectors(fs io.IO) (liveDeletionVectors, error) {
	classified, err := t.classifyLiveDeletes(fs)
	if err != nil {
		return nil, err
	}

	return buildDVIndex(classified.dvEntries)
}

// rewriteFilesWithFilter rewrites data files by preserving only rows that do NOT match the filter.
// Rows already removed by a delete are dropped too, since those deletes stop applying once the file is replaced.
// The filter binding and substrait conversion are computed once here and reused across every file.
func (t *Transaction) rewriteFilesWithFilter(ctx context.Context, fs io.IO, updater *snapshotProducer, builtMeta Metadata, deletes planningDeletes, entries []iceberg.ManifestEntry, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int) error {
	meta, err := t.txnMeta()
	if err != nil {
		return err
	}

	complementFilter, err := isNotTrueExpr(filter)
	if err != nil {
		return fmt.Errorf("failed to build complement filter: %w", err)
	}

	// Bind + convert the complement filter once for the whole rewrite. The
	// per-batch filter function is reused across every record batch from
	// every file in this rewrite — substrait conversion in particular is
	// non-trivial so doing it inside the iterator loop wastes work.
	postFilter, err := prepareBatchFilter(complementFilter, meta.CurrentSchema(), caseSensitive)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		task, err := fileScanTaskForDataEntry(entry, deletes.positional, deletes.dvs, deletes.equality)
		if err != nil {
			return err
		}

		// Use a separate UUID for rewrite operations to avoid filename collisions with new data files
		rewriteUUID := uuid.New()
		args := rewriteSingleFileArgs{
			fs:            fs,
			builtMeta:     builtMeta,
			task:          task,
			filter:        complementFilter,
			postFilter:    postFilter,
			caseSensitive: caseSensitive,
			commitUUID:    rewriteUUID,
			concurrency:   concurrency,
		}
		rewrittenFiles, err := t.rewriteSingleFile(ctx, args)
		if err != nil {
			return fmt.Errorf("failed to rewrite file %s: %w", task.File.FilePath(), err)
		}

		deletes.dvs.removeDataFile(updater, task.File)
		for _, rewrittenFile := range rewrittenFiles {
			updater.appendDataFile(rewrittenFile)
		}
	}

	return nil
}

// isNotTrueExpr returns an expression that holds exactly for the rows where
// filter is not true, i.e. where it is false or NULL. Plain NOT(filter) is not
// enough: under three-valued logic `v = 5` is NULL when v is NULL, and so is
// NOT(v = 5), so a copy-on-write rewrite would drop rows that never matched.
//
// The result contains no NOT. Row-group pruning pushes a NOT down with
// RewriteNotExpr, which turns NOT(x < 5) into x >= 5, and that is false for
// NaN. Negating the predicates here keeps NaN rows in both places.
func isNotTrueExpr(filter iceberg.BooleanExpression) (iceberg.BooleanExpression, error) {
	res, err := iceberg.VisitExpr(filter, isNotTrueVisitor{})
	if err != nil {
		return nil, err
	}

	return res.notTrue, nil
}

// truthExprs holds two never-NULL expressions for a sub-expression e:
// notTrue holds iff e is false or NULL, notFalse holds iff e is true or NULL.
type truthExprs struct {
	notTrue, notFalse iceberg.BooleanExpression
}

type isNotTrueVisitor struct{}

func (isNotTrueVisitor) VisitTrue() truthExprs {
	return truthExprs{notTrue: iceberg.AlwaysFalse{}, notFalse: iceberg.AlwaysTrue{}}
}

func (isNotTrueVisitor) VisitFalse() truthExprs {
	return truthExprs{notTrue: iceberg.AlwaysTrue{}, notFalse: iceberg.AlwaysFalse{}}
}

func (isNotTrueVisitor) VisitNot(child truthExprs) truthExprs {
	return truthExprs{notTrue: child.notFalse, notFalse: child.notTrue}
}

func (isNotTrueVisitor) VisitAnd(left, right truthExprs) truthExprs {
	return truthExprs{
		notTrue:  iceberg.NewOr(left.notTrue, right.notTrue),
		notFalse: iceberg.NewAnd(left.notFalse, right.notFalse),
	}
}

func (isNotTrueVisitor) VisitOr(left, right truthExprs) truthExprs {
	return truthExprs{
		notTrue:  iceberg.NewAnd(left.notTrue, right.notTrue),
		notFalse: iceberg.NewOr(left.notFalse, right.notFalse),
	}
}

func (isNotTrueVisitor) VisitUnbound(pred iceberg.UnboundPredicate) truthExprs {
	negated := pred.Negate()
	switch pred.Op() {
	case iceberg.OpIsNull, iceberg.OpNotNull, iceberg.OpNotNan:
		// never evaluates to NULL
		return truthExprs{notTrue: negated, notFalse: pred}
	case iceberg.OpIsNan:
		// is_nan(NULL) is false, so NULL rows survive. The guard also covers
		// files written before the column existed, where NotNaN on the missing
		// column translates to AlwaysFalse.
		return truthExprs{notTrue: iceberg.NewOr(negated, iceberg.IsNull(pred.Term())), notFalse: pred}
	case iceberg.OpLT, iceberg.OpLTEQ, iceberg.OpGT, iceberg.OpGTEQ:
		// NaN fails both x < 5 and x >= 5. IsNaN binds to AlwaysFalse for
		// non-floating-point terms.
		negated = iceberg.NewOr(negated, iceberg.IsNaN(pred.Term()))
	}

	// Any other predicate is NULL when its term is NULL. Guard In and NotIn
	// too: binding turns a set that ends up with one value into Equal or
	// NotEqual, which are NULL for a NULL term. A scan with a multi-valued
	// NotIn still matches NULL rows, see
	// https://github.com/apache/iceberg-go/issues/2132.
	isNull := iceberg.IsNull(pred.Term())

	return truthExprs{
		notTrue:  iceberg.NewOr(negated, isNull),
		notFalse: iceberg.NewOr(pred, isNull),
	}
}

func (isNotTrueVisitor) VisitBound(pred iceberg.BoundPredicate) truthExprs {
	panic(fmt.Errorf("%w: found already bound predicate: %s", iceberg.ErrInvalidArgument, pred))
}

// rewriteSingleFileArgs bundles the parameters for [Transaction.rewriteSingleFile]
// — there are several same-typed fields (two filter values, plus a UUID and an
// int) so positional ordering offers no compile-time order protection.
type rewriteSingleFileArgs struct {
	fs io.IO
	// builtMeta is the transaction's metadata, built once for the whole rewrite.
	builtMeta Metadata
	// task is the source file as planned by [fileScanTaskForDataEntry].
	// Its deletes must be applied, and its sequence number seeds _last_updated_sequence_number
	// for rows whose value is null in the file.
	task FileScanTask
	// filter is the per-row complement (rows that survive the rewrite).
	filter iceberg.BooleanExpression
	// postFilter is the pre-bound, pre-substrait-converted version of
	// filter. When preserveRowLineage applies it is the only place the
	// filter is evaluated; the scanner's row filter is bypassed so
	// _row_id positions stay correct.
	postFilter    func(context.Context, arrow.RecordBatch) (arrow.RecordBatch, error)
	caseSensitive bool
	commitUUID    uuid.UUID
	concurrency   int
}

// rewriteSingleFile reads a single data file, applies the filter, and writes
// new files with filtered data. See [rewriteSingleFileArgs] for parameter
// semantics.
func (t *Transaction) rewriteSingleFile(ctx context.Context, args rewriteSingleFileArgs) ([]iceberg.DataFile, error) {
	meta, err := t.txnMeta()
	if err != nil {
		return nil, err
	}

	// Preserve row lineage for v3 tables: include _row_id and
	// _last_updated_sequence_number in the scan projection so they are read
	// from the source file (or synthesized from file metadata when null) and
	// written explicitly into the output Parquet. The Iceberg v3 spec says
	// _last_updated_sequence_number on a data file row is the sequence
	// number of the snapshot that last *logically* updated the row — for a
	// pure CoW rewrite the surviving rows are unchanged, so they must keep
	// their original value rather than inheriting the new commit's seq.
	//
	// When preserving lineage, the row filter must NOT be applied inside the
	// scanner because _row_id synthesis depends on original file position. The
	// filter is applied post-synthesis in the record iterator instead.
	//
	// Keep the real filter available to Parquet row-group pruning while the
	// scanner's row filter stays AlwaysTrue. Pruning happens before rows are
	// materialized, so _row_id synthesis still sees the original positions of
	// every surviving row group and the post-synthesis filter remains correct.
	_, originalFirstRowID, _, _, _ := iceberginternal.BorrowedDataFilePointers(args.task.File)
	preserveRowLineage := meta.formatVersion >= 3 && originalFirstRowID != nil
	projectedSchema := meta.CurrentSchema()
	var factoryOpts []writerFactoryOption
	var rowGroupFilter iceberg.BooleanExpression
	if preserveRowLineage {
		rowGroupFilter, err = iceberg.BindExpr(meta.CurrentSchema(), args.filter, args.caseSensitive)
		if err != nil {
			return nil, fmt.Errorf("failed to bind row-group filter: %w", err)
		}

		projectedSchema = iceberg.SchemaWithRowLineage(projectedSchema)
		factoryOpts = append(factoryOpts, withFactoryFileSchema(projectedSchema))
	}

	// When preserving row lineage, the bound scan-time filter is replaced
	// with AlwaysTrue and the rebound version inside prepareBatchFilter is
	// the one actually applied. Skip the wasted BindExpr in that case.
	scanFilter := iceberg.BooleanExpression(iceberg.AlwaysTrue{})
	if !preserveRowLineage {
		boundFilter, err := iceberg.BindExpr(meta.CurrentSchema(), args.filter, args.caseSensitive)
		if err != nil {
			return nil, fmt.Errorf("failed to bind filter: %w", err)
		}
		scanFilter = boundFilter
	}

	scanner := &arrowScan{
		metadata:        args.builtMeta,
		fs:              args.fs,
		scanSchema:      meta.CurrentSchema(),
		projectedSchema: projectedSchema,
		boundRowFilter:  scanFilter,
		rowGroupFilter:  rowGroupFilter,
		caseSensitive:   args.caseSensitive,
		rowLimit:        -1, // No limit
		concurrency:     args.concurrency,
	}

	arrowSchema, recordIter, err := scanner.GetRecords(ctx, []FileScanTask{args.task})
	if err != nil {
		return nil, fmt.Errorf("failed to get records from original file: %w", err)
	}

	// When preserving row lineage, use an Arrow schema with field IDs so that
	// recordsToDataFiles can resolve _row_id via field ID rather than the name
	// mapping (which doesn't include metadata columns).
	if preserveRowLineage {
		arrowSchema, err = SchemaToArrowSchemaWithOptions(projectedSchema, ArrowSchemaOptions{
			IncludeFieldIDs: true,
			TableProperties: args.builtMeta.Properties(),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to build arrow schema with field IDs: %w", err)
		}
	}

	// Wrap the iterator to release records after consumption.
	// When preserving row lineage, also apply the (pre-bound) filter here
	// post-synthesis so _row_id positions stay correct.
	releaseIter := func(yield func(arrow.RecordBatch, error) bool) {
		for rec, err := range recordIter {
			if err != nil {
				yield(nil, err)

				return
			}

			if preserveRowLineage {
				rec, err = args.postFilter(ctx, rec)
				if err != nil {
					yield(nil, err)

					return
				}
				if rec.NumRows() == 0 {
					rec.Release()

					continue
				}
			}

			if !yield(rec, nil) {
				rec.Release()

				return
			}
			rec.Release()
		}
	}

	wfs, err := requireWriteFileIO(args.fs)
	if err != nil {
		return nil, err
	}

	var result []iceberg.DataFile
	itr := recordsToDataFiles(ctx, t.tbl.Location(), meta, recordWritingArgs{
		sc:          arrowSchema,
		itr:         releaseIter,
		fs:          wfs,
		writeUUID:   &args.commitUUID,
		factoryOpts: factoryOpts,
	})

	for df, err := range itr {
		if err != nil {
			return nil, err
		}
		result = append(result, df)
	}

	return result, nil
}

// writePositionDeletesForFiles writes position-delete files for the rows in
// files that match filter, appends them to updater, and returns the distinct
// data-file paths those deletes reference. A position-delete that does not
// record its referenced data file is omitted from the returned paths (it
// cannot be existence-checked), matching RowDelta.referencedDataFiles.
func (t *Transaction) writePositionDeletesForFiles(ctx context.Context, fs io.IO, updater *snapshotProducer, dvs liveDeletionVectors, files []iceberg.DataFile, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int, commitUUID uuid.UUID) ([]string, error) {
	meta, err := t.txnMeta()
	if err != nil {
		return nil, err
	}

	posDeleteRecIter, err := t.makePositionDeleteRecordsForFilter(ctx, fs, files, filter, caseSensitive, concurrency)
	if err != nil {
		return nil, err
	}
	wfs, err := requireWriteFileIO(fs)
	if err != nil {
		return nil, err
	}

	partitionContextByFilePath := make(map[string]partitionContext, len(files))
	for _, df := range files {
		partitionContextByFilePath[df.FilePath()] = partitionContext{partitionData: df.Partition(), specID: df.SpecID()}
	}

	// V3 tables store deletes as deletion vectors, and the spec allows at most
	// one DV per data file. A data file being partially deleted may already
	// carry a DV from an earlier delete; those existing positions are folded
	// into the new DV (the incremental deletes) and the superseded DVs are
	// removed. Without this a second delete on the same file would write a
	// second live DV, corrupting the table (buildDVIndex rejects it).
	// dvs is empty on v2.
	existingDVs := make(map[string]iceberg.DataFile)
	existingDVBitmaps := make(map[string]*dv.RoaringPositionBitmap)
	for _, df := range files {
		entry, ok := dvs[df.FilePath()]
		if !ok {
			continue
		}
		bitmap, err := dv.ReadDV(fs, entry.DataFile())
		if err != nil {
			return nil, fmt.Errorf("failed to read existing deletion vector for %s: %w", df.FilePath(), err)
		}
		existingDVs[df.FilePath()] = entry.DataFile()
		existingDVBitmaps[df.FilePath()] = bitmap
	}

	posDeleteFiles := positionDeleteRecordsToDataFiles(ctx, t.tbl.Location(), meta, partitionContextByFilePath, recordWritingArgs{
		sc:          PositionalDeleteArrowSchema,
		itr:         posDeleteRecIter,
		writeUUID:   &commitUUID,
		fs:          wfs,
		existingDVs: existingDVBitmaps,
	})

	var referenced []string
	seen := make(map[string]struct{})
	for f, err := range posDeleteFiles {
		if err != nil {
			return nil, err
		}
		updater.appendDeleteFile(f)
		if ref := iceberginternal.BorrowedDataFileReferencedDataFile(f); ref != nil && *ref != "" {
			if _, ok := seen[*ref]; !ok {
				seen[*ref] = struct{}{}
				referenced = append(referenced, *ref)
			}
		}
	}

	// Supersede the old DVs only after the merged replacements are staged, so a
	// write failure above leaves the existing DVs untouched.
	for _, dvFile := range existingDVs {
		updater.removeDeletionVector(dvFile)
	}

	return referenced, nil
}

func (t *Transaction) makePositionDeleteRecordsForFilter(ctx context.Context, fs io.IO, files []iceberg.DataFile, filter iceberg.BooleanExpression, caseSensitive bool, concurrency int) (seq2 iter.Seq2[arrow.RecordBatch, error], err error) {
	meta, err := t.txnMeta()
	if err != nil {
		return nil, err
	}

	tasks := make([]FileScanTask, 0, len(files))
	for _, f := range files {
		tasks = append(tasks, FileScanTask{
			File:   f,
			Start:  0,
			Length: f.FileSizeBytes(),
		})
	}

	boundFilter, err := iceberg.BindExpr(meta.CurrentSchema(), filter, caseSensitive)
	if err != nil {
		return nil, fmt.Errorf("failed to bind filter: %w", err)
	}

	builtMeta, err := meta.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build metadata: %w", err)
	}

	scanner := &arrowScan{
		metadata:        builtMeta,
		fs:              fs,
		scanSchema:      meta.CurrentSchema(),
		projectedSchema: meta.CurrentSchema(),
		boundRowFilter:  boundFilter,
		caseSensitive:   caseSensitive,
		rowLimit:        -1, // No limit
		concurrency:     concurrency,
	}
	invariants, err := scanner.scanInvariants(builtMeta.Properties())
	if err != nil {
		return nil, err
	}

	deletesPerFile, err := readAllDeleteFiles(ctx, fs, tasks, concurrency)
	if err != nil {
		// readAllDeleteFiles can return a partially-populated map alongside
		// the error if some goroutines completed before the failure.
		releasePerFilePosDeletes(deletesPerFile)

		return nil, err
	}

	extSet := substrait.NewExtensionSet()

	ctx, cancel := context.WithCancelCause(exprs.WithExtensionIDSet(ctx, extSet))
	taskChan := make(chan tblutils.Enumerated[FileScanTask], len(tasks))

	numWorkers := min(concurrency, len(tasks))
	records := make(chan enumeratedRecord, numWorkers)

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for range numWorkers {
		go func() {
			defer wg.Done()
			sink := newRecordSink(records)
			for {
				if err := sink.reserve(ctx); err != nil {
					return
				}
				select {
				case <-ctx.Done():
					return
				case task, ok := <-taskChan:
					if !ok {
						return
					}

					if err := scanner.producePosDeletesFromTask(ctx, task, deletesPerFile[task.Value.File.FilePath()], sink, invariants); err != nil {
						cancel(err)

						return
					}
				}
			}
		}()
	}

	go func() {
		for i, t := range tasks {
			taskChan <- tblutils.Enumerated[FileScanTask]{
				Value: t, Index: i, Last: i == len(tasks)-1,
			}
		}
		close(taskChan)

		wg.Wait()
		close(records)
	}()

	return createIterator(ctx, uint(numWorkers), records, deletesPerFile, cancel, scanner.rowLimit), nil
}
