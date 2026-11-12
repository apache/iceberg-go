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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"context"
	"fmt"
	"maps"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	tableinternal "github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
)

// dynamicPartitionKeys normalizes all partition values before comparing them.
// A manifest decoder may represent the same logical day as an int32 while the
// writer uses iceberg.Date. The shared canonical encoder handles both forms,
// decimals, bytes, UUIDs, and nulls without lossy string formatting.
func dynamicPartitionKeys(files []iceberg.DataFile) (map[string]struct{}, error) {
	keys := make(map[string]struct{}, len(files))
	for _, file := range files {
		key, err := canonicalPartitionKey(file.SpecID(), dataFilePartition(file))
		if err != nil {
			return nil, fmt.Errorf("dynamic partition overwrite file %q: %w", file.FilePath(), err)
		}
		keys[key] = struct{}{}
	}

	return keys, nil
}

func (t *Transaction) dynamicOverwriteSpec() (*iceberg.PartitionSpec, error) {
	meta, err := t.txnMeta()
	if err != nil {
		return nil, err
	}

	if meta.formatVersion > 3 {
		return nil, fmt.Errorf("%w: dynamic partition overwrite does not support format v%d", ErrInvalidOperation, meta.formatVersion)
	}

	spec, err := meta.CurrentSpec()
	if err != nil {
		return nil, err
	}
	if spec == nil {
		return nil, fmt.Errorf("%w: dynamic partition overwrite requires a current partition spec", ErrInvalidOperation)
	}
	if err := checkNoUnknownTransform(spec); err != nil {
		return nil, err
	}

	return spec, nil
}

// dynamicOverwriteManifestCandidates uses partition summaries to select the
// current-spec manifests that might contain one of the input partitions. The
// inclusive projection and manifest evaluator are shared with table scans so
// missing or inconclusive summaries are kept conservatively.
func dynamicOverwriteManifestCandidates(
	ctx context.Context,
	manifests []iceberg.ManifestFile,
	spec iceberg.PartitionSpec,
	schema *iceberg.Schema,
	partitions []map[int]any,
) ([]bool, error) {
	candidates := make([]bool, len(manifests))
	// An unpartitioned overwrite replaces every live file, including files
	// written with older specs. Never prune those manifests.
	if spec.IsUnpartitioned() {
		for idx := range candidates {
			candidates[idx] = true
		}

		return candidates, nil
	}

	// This projected filter only prunes manifests; entry deletion still uses
	// exact stored partition tuples below.
	rowFilter, err := tableinternal.BuildPartitionMatchPredicate(spec, schema, partitions)
	if err != nil {
		return nil, fmt.Errorf("dynamic partition overwrite: build partition filter: %w", err)
	}
	partitionFilter, err := newInclusiveProjection(schema, spec, true)(rowFilter)
	if err != nil {
		return nil, fmt.Errorf("dynamic partition overwrite: project partition filter: %w", err)
	}
	eval, err := newManifestEvaluator(spec, schema, partitionFilter, true)
	if err != nil {
		return nil, fmt.Errorf("dynamic partition overwrite: build manifest evaluator: %w", err)
	}

	for idx, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Files under older specs have a different partition tuple space and
		// cannot belong to an input partition in the current spec.
		// Legacy v1 snapshots may list only manifest locations without
		// reliable spec IDs or partition summaries. Keep them conservatively.
		if manifest.Version() == 1 && int(manifest.PartitionSpecID()) != spec.ID() {
			candidates[idx] = true

			continue
		}
		if int(manifest.PartitionSpecID()) != spec.ID() {
			continue
		}

		// A field_summary is optional, and imported manifests can have
		// incomplete bounds. Keep such manifests instead of letting the
		// evaluator mistake unknown values for an empty partition range.
		if summaries := manifest.Partitions(); len(summaries) != 0 {
			if len(summaries) != spec.NumFields() {
				candidates[idx] = true

				continue
			}
			inconclusive := false
			for _, summary := range summaries {
				if (summary.LowerBound == nil) != (summary.UpperBound == nil) ||
					(summary.LowerBound == nil && !summary.ContainsNull &&
						(summary.ContainsNaN == nil || !*summary.ContainsNaN)) {
					inconclusive = true

					break
				}
			}
			if inconclusive {
				candidates[idx] = true

				continue
			}
		}

		use, err := eval(manifest)
		if err != nil {
			return nil, fmt.Errorf("dynamic partition overwrite: evaluate manifest %s: %w", manifest.FilePath(), err)
		}
		candidates[idx] = use
	}

	return candidates, nil
}

// mayHaveOldSpecData treats any older-spec data manifest as potentially live.
// Some imported manifest lists have zero live-file counts despite live entries.
// Over-retaining a position delete is safer than dropping one that still
// applies to an older-spec data file.
func mayHaveOldSpecData(manifests []iceberg.ManifestFile, currentSpecID int) bool {
	for _, manifest := range manifests {
		if manifest.ManifestContent() == iceberg.ManifestContentData &&
			int(manifest.PartitionSpecID()) != currentSpecID {
			return true
		}
	}

	return false
}

// validateDynamicOverwriteConflicts uses exact stored partition tuples so
// transformed and null partitions cannot become false-negative row predicates.
// It rejects concurrent additions and removals in touched partitions, plus
// global equality deletes that could apply to data files being replaced, while
// allowing unrelated partition appends during refresh-and-replay.
func validateDynamicOverwriteConflicts(
	cc *conflictContext,
	specID int,
	unpartitioned bool,
	touched map[string]struct{},
	replacedDataFilePaths map[string]struct{},
) error {
	if cc == nil || len(cc.concurrent) == 0 {
		return nil
	}
	for _, snapshot := range cc.concurrent {
		manifests, err := cc.manifestsFor(snapshot)
		if err != nil {
			return fmt.Errorf("dynamic partition overwrite: concurrent manifests for snapshot %d: %w", snapshot.SnapshotID, err)
		}
		for _, manifest := range manifests {
			for entry, err := range manifest.Entries(cc.ctx, cc.manifestReadIO(), false) {
				if err != nil {
					return fmt.Errorf("dynamic partition overwrite: read concurrent manifest %s: %w", manifest.FilePath(), err)
				}
				if entry.SnapshotID() != snapshot.SnapshotID || entry.Status() == iceberg.EntryStatusEXISTING {
					continue
				}
				file := entry.DataFile()
				conflict := ErrConflictingDataFiles
				if file.ContentType() != iceberg.EntryContentData {
					conflict = ErrConflictingDeleteFiles
				}
				// No partition scope exists for an unpartitioned overwrite.
				// Every concurrent change matters, including older spec IDs.
				if unpartitioned {
					return fmt.Errorf("%w: concurrent snapshot %d changed file %q during an unpartitioned overwrite",
						conflict, snapshot.SnapshotID, file.FilePath())
				}
				if file.ContentType() == iceberg.EntryContentEqDeletes && len(replacedDataFilePaths) > 0 {
					deleteSpec := cc.current.PartitionSpecByID(int(file.SpecID()))
					if deleteSpec == nil {
						return fmt.Errorf("dynamic partition overwrite: %w: concurrent delete file %q references spec %d",
							ErrPartitionSpecNotFound, file.FilePath(), file.SpecID())
					}
					if deleteSpec.IsUnpartitioned() {
						return fmt.Errorf("%w: concurrent snapshot %d added global equality delete %q while replacing data files",
							ErrConflictingDeleteFiles, snapshot.SnapshotID, file.FilePath())
					}
				}
				if int(file.SpecID()) != specID {
					continue
				}
				key, err := canonicalPartitionKey(file.SpecID(), dataFilePartition(file))
				if err != nil {
					return fmt.Errorf("dynamic partition overwrite: concurrent file %q: %w", file.FilePath(), err)
				}
				if _, ok := touched[key]; ok {
					return fmt.Errorf("%w: concurrent snapshot %d changed file %q in an overwritten partition",
						conflict, snapshot.SnapshotID, file.FilePath())
				}
			}
		}
	}

	return nil
}

// DynamicPartitionOverwriteDataFiles atomically replaces all live data in
// the partitions represented by filesToAdd. Empty input is a no-op; an
// unpartitioned table has one partition. New partitions still use OpOverwrite.
//
// Added files must use the table's current partition spec; existing live files
// under older specs are preserved unless the current spec is unpartitioned,
// in which case all existing files are replaced. Supports v1-v3, including v3 row lineage
// and associated deletion vectors. On v3, a new data file may omit first_row_id
// and inherit it from its manifest.
// Concurrent writes to touched partitions fail while independent appends to
// other partitions can be retried unless delete-file removal prevents replay.
// A missing target branch is pinned together with main to guard its origin.
// Default partition and sort order changes also fail.
//
// All input files must already exist. Uncommitted data files remain the
// caller's responsibility after a staging or commit failure. By default this
// sets the table's name mapping; WithoutAutoNameMapping disables that step.
// WithoutDuplicateCheck is ignored to avoid adding an already live file twice.
func (t *Transaction) DynamicPartitionOverwriteDataFiles(
	ctx context.Context, filesToAdd []iceberg.DataFile, snapshotProps iceberg.Properties, opts ...WriteOption,
) error {
	return t.dynamicPartitionOverwriteDataFiles(ctx, filesToAdd, snapshotProps, true, opts...)
}

// dynamicPartitionOverwriteDataFiles scans all manifests for duplicate paths
// when callers supply prewritten files. Arrow writers create UUID paths and
// may prune unrelated manifests while planning the overwrite.
func (t *Transaction) dynamicPartitionOverwriteDataFiles(
	ctx context.Context,
	filesToAdd []iceberg.DataFile,
	snapshotProps iceberg.Properties,
	checkExistingPaths bool,
	opts ...WriteOption,
) error {
	spec, err := t.dynamicOverwriteSpec()
	if err != nil {
		return err
	}
	if len(filesToAdd) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	var cfg dataFileCfg
	for _, option := range opts {
		option(&cfg)
	}
	if cfg.rewriteSemantics {
		return fmt.Errorf("%w: dynamic partition overwrite cannot use data-preserving rewrite semantics", ErrInvalidOperation)
	}

	// Dynamic overwrite adds new rows, so v3 first_row_id can be inherited
	// from the manifest. Java ReplacePartitions uses the same inheritance.
	addedPaths, err := t.validateDataFilesToAdd(filesToAdd, "DynamicPartitionOverwriteDataFiles", false)
	if err != nil {
		return err
	}
	for _, df := range filesToAdd {
		if int(df.SpecID()) != spec.ID() {
			return fmt.Errorf("%w: dynamic partition overwrite file %q uses spec %d, current spec is %d",
				ErrInvalidOperation, df.FilePath(), df.SpecID(), spec.ID())
		}
	}

	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return err
	}
	wfs, err := requireWriteFileIO(fs)
	if err != nil {
		return err
	}

	keys, err := dynamicPartitionKeys(filesToAdd)
	if err != nil {
		return err
	}
	var toDelete, deleteFilesToRemove []iceberg.DataFile
	replacedDataFilePaths := make(map[string]struct{})
	unpartitioned := spec.IsUnpartitioned()
	survivingDataPaths := make(map[string]struct{})
	var hasSurvivingOldSpecData bool
	validationPlan := newPartitionValidationPlan(spec)
	if snapshot := t.planningSnapshot(t.meta); snapshot != nil {
		dataFiles := snapshot.dataFiles(ctx, fs, nil)
		deleteEntries := snapshot.entries(ctx, fs, iceberg.ManifestContentDeletes, true)
		if !checkExistingPaths {
			manifests, err := snapshot.Manifests(ctx, fs)
			if err != nil {
				return fmt.Errorf("dynamic partition overwrite: read manifest list: %w", err)
			}
			meta, err := t.txnMeta()
			if err != nil {
				return err
			}
			partitions := make([]map[int]any, len(filesToAdd))
			for idx, file := range filesToAdd {
				partitions[idx] = dataFilePartition(file)
			}
			candidates, err := dynamicOverwriteManifestCandidates(ctx, manifests, *spec, meta.CurrentSchema(), partitions)
			if err != nil {
				return err
			}

			// Older-spec files may still be targeted by current-spec position
			// deletes, even when manifest list live-file counts are unreliable.
			if !unpartitioned {
				hasSurvivingOldSpecData = mayHaveOldSpecData(manifests, spec.ID())
			}

			dataFiles = func(yield func(iceberg.DataFile, error) bool) {
				for idx, manifest := range manifests {
					if !candidates[idx] || manifest.ManifestContent() != iceberg.ManifestContentData {
						continue
					}
					for entry, err := range manifest.Entries(ctx, fs, true) {
						if err != nil {
							yield(nil, fmt.Errorf("read manifest %s: %w", manifest.FilePath(), err))

							return
						}
						if !yield(entry.DataFile(), nil) {
							return
						}
					}
				}
			}
			deleteEntries = func(yield func(iceberg.ManifestEntry, error) bool) {
				for idx, manifest := range manifests {
					if !candidates[idx] || manifest.ManifestContent() != iceberg.ManifestContentDeletes {
						continue
					}
					for entry, err := range manifest.Entries(ctx, fs, true) {
						if err != nil {
							yield(nil, fmt.Errorf("read manifest %s: %w", manifest.FilePath(), err))

							return
						}
						if !yield(entry, nil) {
							return
						}
					}
				}
			}
		}
		for df, err := range dataFiles {
			if err != nil {
				return fmt.Errorf("dynamic partition overwrite: read data files: %w", err)
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if checkExistingPaths {
				if _, exists := addedPaths[df.FilePath()]; exists {
					return fmt.Errorf("%w: added file %q already exists in the target snapshot", ErrInvalidOperation, df.FilePath())
				}
			}
			if df.ContentType() != iceberg.EntryContentData {
				continue
			}
			if int(df.SpecID()) != spec.ID() {
				if !unpartitioned {
					survivingDataPaths[df.FilePath()] = struct{}{}
					hasSurvivingOldSpecData = true

					continue
				}
			} else if err := validationPlan.validate(df); err != nil {
				return fmt.Errorf("%w: invalid live partition in file %q: %w",
					ErrInvalidOperation, df.FilePath(), err)
			}

			// A current unpartitioned spec represents the whole table, even
			// when existing files were written under older partition specs.
			if unpartitioned {
				toDelete = append(toDelete, df)
				replacedDataFilePaths[df.FilePath()] = struct{}{}

				continue
			}

			key, err := canonicalPartitionKey(df.SpecID(), dataFilePartition(df))
			if err != nil {
				return fmt.Errorf("dynamic partition overwrite live file %q: %w", df.FilePath(), err)
			}
			if _, touched := keys[key]; touched {
				toDelete = append(toDelete, df)
				replacedDataFilePaths[df.FilePath()] = struct{}{}
			} else {
				survivingDataPaths[df.FilePath()] = struct{}{}
			}
		}

		removedDataPaths := make(map[string]struct{}, len(toDelete))
		if !checkExistingPaths {
			for _, file := range toDelete {
				removedDataPaths[file.FilePath()] = struct{}{}
			}
		}

		// A replaced partition has no surviving current-spec data that
		// its old deletes should affect. Preserve deletes that can still
		// target surviving files under an older spec. Deletion vectors are
		// removed separately with their referenced data files.
		for entry, err := range deleteEntries {
			if err != nil {
				return fmt.Errorf("dynamic partition overwrite: read delete files: %w", err)
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			df := entry.DataFile()
			if checkExistingPaths {
				if _, exists := addedPaths[df.FilePath()]; exists {
					return fmt.Errorf("%w: added file %q already exists as a delete file", ErrInvalidOperation, df.FilePath())
				}
			}
			if IsDeletionVector(df) {
				continue
			}
			if unpartitioned {
				deleteFilesToRemove = append(deleteFilesToRemove, df)

				continue
			}
			if ref := df.ReferencedDataFile(); ref != nil && *ref != "" {
				if checkExistingPaths {
					if _, survives := survivingDataPaths[*ref]; survives {
						continue
					}
				} else {
					// Any data file we did not select for removal may still
					// be live in a pruned manifest.
					if _, removed := removedDataPaths[*ref]; !removed {
						continue
					}
				}
			} else if df.ContentType() == iceberg.EntryContentPosDeletes && hasSurvivingOldSpecData {
				// V2 manifests do not carry referenced_data_file. A position
				// delete may still target an old-spec file, so do not discard
				// it while any such data file survives.
				continue
			}
			if int(df.SpecID()) != spec.ID() {
				continue
			}
			key, err := canonicalPartitionKey(df.SpecID(), dataFilePartition(df))
			if err != nil {
				return fmt.Errorf("dynamic partition overwrite live delete file %q: %w", df.FilePath(), err)
			}
			if _, touched := keys[key]; touched {
				deleteFilesToRemove = append(deleteFilesToRemove, df)
			}
		}
	}

	var dvs liveDeletionVectors
	if t.meta.formatVersion >= 3 && len(toDelete) != 0 {
		if dvs, err = t.readLiveDeletionVectors(ctx, fs); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := t.AssertDefaultShape(); err != nil {
		return err
	}
	branch := t.branch
	if branch == "" {
		branch = MainBranch
	}
	if t.baseRefSnapshotID(branch) == nil {
		if err := t.AssertRefSnapshotID(branch); err != nil {
			return err
		}
		if branch != MainBranch {
			if err := t.AssertRefSnapshotID(MainBranch); err != nil {
				return err
			}
		}
	}

	if !cfg.skipAutoNameMapping {
		if err := t.ensureNameMapping(); err != nil {
			return err
		}
	}

	props := make(iceberg.Properties, len(snapshotProps)+1)
	maps.Copy(props, snapshotProps)
	props["replace-partitions"] = "true"

	// Use one producer for additions and removals. In contrast,
	// ReplaceDataFiles would scan manifests again and produce an append
	// operation when there are no existing files in the touched partitions.
	producer := newOverwriteFilesProducer(OpOverwrite, t, wfs, nil, props)
	producer.setSkipDefaultValidator(true)
	for _, file := range toDelete {
		dvs.removeDataFile(producer, file)
	}
	for _, file := range deleteFilesToRemove {
		producer.removeDeleteFile(file)
	}
	for _, file := range filesToAdd {
		producer.appendDataFile(file)
	}
	updates, reqs, err := producer.commit(ctx)
	if err != nil {
		return err
	}

	// Publish the staged snapshot and validator together so concurrent
	// Commit cannot see one without the other.
	t.mx.Lock()
	defer t.mx.Unlock()
	if err := t.applyLocked(updates, reqs); err != nil {
		return err
	}
	t.validators = append(t.validators, func(cc *conflictContext) error {
		return validateDynamicOverwriteConflicts(cc, spec.ID(), unpartitioned, keys, replacedDataFilePaths)
	})

	return nil
}

// DynamicPartitionOverwrite writes Arrow records to data files and replaces
// only the partitions represented by the written records. Empty input is
// a no-op. Supports v1-v3 and preserves live files under older partition
// specs. Concurrent touched-partition changes fail; removal of delete files or
// DVs prevents replay. Written files remain the caller's responsibility after
// failure. Pass WithoutAutoNameMapping to avoid changing that property.
func (t *Transaction) DynamicPartitionOverwrite(
	ctx context.Context, rdr array.RecordReader, snapshotProps iceberg.Properties, opts ...WriteOption,
) error {
	if rdr == nil {
		return fmt.Errorf("%w: nil record reader for dynamic partition overwrite", ErrInvalidOperation)
	}
	if _, err := t.dynamicOverwriteSpec(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	meta, err := t.txnMeta()
	if err != nil {
		return err
	}
	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return err
	}
	wfs, err := requireWriteFileIO(fs)
	if err != nil {
		return err
	}

	writeUUID := uuid.New()
	itr := recordsToDataFiles(ctx, meta.loc, meta, recordWritingArgs{
		sc:        rdr.Schema(),
		itr:       array.IterFromReader(rdr),
		fs:        wfs,
		writeUUID: &writeUUID,
	})

	var written []iceberg.DataFile
	for df, err := range itr {
		if err != nil {
			return err
		}
		written = append(written, df)
	}

	return t.dynamicPartitionOverwriteDataFiles(ctx, written, snapshotProps, false, opts...)
}

// DynamicPartitionOverwriteTable is the Arrow-table variant of
// DynamicPartitionOverwrite. It shares the v3, older-spec file handling,
// concurrency and uncommitted-file behavior of the record-reader method.
func (t *Transaction) DynamicPartitionOverwriteTable(
	ctx context.Context, tbl arrow.Table, batchSize int64, snapshotProps iceberg.Properties, opts ...WriteOption,
) error {
	if tbl == nil {
		return fmt.Errorf("%w: nil Arrow table for dynamic partition overwrite", ErrInvalidOperation)
	}
	rdr := array.NewTableReader(tbl, batchSize)
	defer rdr.Release()

	return t.DynamicPartitionOverwrite(ctx, rdr, snapshotProps, opts...)
}

// DynamicPartitionOverwrite commits a dynamic overwrite in one transaction.
// It shares the older-spec, v3, concurrency and uncommitted-file rules above.
func (t Table) DynamicPartitionOverwrite(
	ctx context.Context, rdr array.RecordReader, snapshotProps iceberg.Properties, opts ...WriteOption,
) (*Table, error) {
	txn := t.NewTransaction()
	if err := txn.DynamicPartitionOverwrite(ctx, rdr, snapshotProps, opts...); err != nil {
		return nil, err
	}

	return txn.Commit(ctx)
}

// DynamicPartitionOverwriteTable is the Arrow-table convenience wrapper.
// It shares the older-spec, v3, concurrency and uncommitted-file rules above.
func (t Table) DynamicPartitionOverwriteTable(
	ctx context.Context, tbl arrow.Table, batchSize int64, snapshotProps iceberg.Properties, opts ...WriteOption,
) (*Table, error) {
	txn := t.NewTransaction()
	if err := txn.DynamicPartitionOverwriteTable(ctx, tbl, batchSize, snapshotProps, opts...); err != nil {
		return nil, err
	}

	return txn.Commit(ctx)
}
