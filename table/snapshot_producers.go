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
	"io"
	"iter"
	"maps"
	"slices"
	"sync/atomic"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/config"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

type producerImpl interface {
	// to perform any post-processing on the manifests before writing them
	// to the new snapshot. This will be called as the last step
	// before writing a manifest list file, using the result of this function
	// as the final list of manifests to write.
	processManifests(manifests []iceberg.ManifestFile) ([]iceberg.ManifestFile, error)
	// perform any processing necessary and return the list of existing
	// manifests that should be included in the snapshot
	existingManifests() ([]iceberg.ManifestFile, error)
	// return the deleted entries for writing delete file manifests
	deletedEntries(ctx context.Context) ([]iceberg.ManifestEntry, error)
	// validate runs producer-specific conflict checks against the
	// current catalog state. Implementations should return a wrapped
	// ErrCommit* sentinel on conflict, ErrCommitDiverged on terminal
	// divergence, or nil on success. A no-op default is fine for
	// producers that are safe against concurrent appends (fast-append
	// and merge-append).
	validate(cc *conflictContext) error
	// needsValidation reports whether this producer's validate method
	// performs real conflict checks. Return false only if validate is
	// unconditionally a no-op; commit() skips validator registration
	// entirely when this returns false, so validate will never run.
	needsValidation() bool
}

func newManifestFileName(num int, commit uuid.UUID) string {
	return fmt.Sprintf("%s-m%d.avro", commit, num)
}

func newManifestListFileName(snapshotID int64, attempt int, commit uuid.UUID) string {
	// mimics behavior of java
	// https://github.com/apache/iceberg/blob/c862b9177af8e2d83122220764a056f3b96fd00c/core/src/main/java/org/apache/iceberg/SnapshotProducer.java#L491
	return fmt.Sprintf("snap-%d-%d-%s.avro", snapshotID, attempt, commit)
}

func newFastAppendFilesProducer(op Operation, txn *Transaction, fs iceio.WriteFileIO, commitUUID *uuid.UUID, snapshotProps iceberg.Properties) *snapshotProducer {
	prod := createSnapshotProducer(op, txn, fs, commitUUID, snapshotProps)
	prod.producerImpl = &fastAppendFiles{base: prod}

	return prod
}

type fastAppendFiles struct {
	base *snapshotProducer
}

func (fa *fastAppendFiles) processManifests(manifests []iceberg.ManifestFile) ([]iceberg.ManifestFile, error) {
	return manifests, nil
}

func (fa *fastAppendFiles) existingManifests() ([]iceberg.ManifestFile, error) {
	if fa.base.parentSnapshotID <= 0 {
		return nil, nil
	}

	previous, err := fa.base.txn.meta.SnapshotByID(fa.base.parentSnapshotID)
	if err != nil {
		return nil, fmt.Errorf("could not find parent snapshot %d: %w", fa.base.parentSnapshotID, err)
	}

	return previous.Manifests(fa.base.io)
}

func (fa *fastAppendFiles) deletedEntries(_ context.Context) ([]iceberg.ManifestEntry, error) {
	// for fast appends, there are no deleted entries
	return nil, nil
}

// validate is a no-op for fastAppendFiles: appends are commutative
// per the Iceberg spec, and Java's BaseFastAppend / SnapshotProducer
// likewise skip pre-commit validation for the append path. Two
// concurrent fast-appends of distinct files merge cleanly into the
// resulting manifest list. Two concurrent fast-appends adding the
// same file path is a writer-side error (paths are expected to be
// unique, normally via UUID-stamped filenames); detecting it here
// is out of Java parity scope.
func (fa *fastAppendFiles) validate(_ *conflictContext) error {
	return nil
}

func (fa *fastAppendFiles) needsValidation() bool { return false }

type overwriteFiles struct {
	base *snapshotProducer

	// filter is the row-level predicate the caller declared for this
	// overwrite (e.g. WithOverwriteFilter). Nil or AlwaysTrue means
	// "overwrite everything" — any concurrent added data file is a
	// conflict under serializable isolation. validate() reads this to
	// decide whether to run filter-bounded or full-table checks.
	filter iceberg.BooleanExpression

	// skipDefaultValidator disables the overwrite's pre-commit
	// conflict check when the producer is driven by a higher-level
	// operation that registers its own validator (e.g. compaction via
	// RewriteDataFiles). Without this flag a compaction would falsely
	// reject a concurrent append into the same partition, which is
	// semantically compatible with a rewrite.
	skipDefaultValidator bool
}

func newOverwriteFilesProducer(op Operation, txn *Transaction, fs iceio.WriteFileIO, commitUUID *uuid.UUID, snapshotProps iceberg.Properties) *snapshotProducer {
	prod := createSnapshotProducer(op, txn, fs, commitUUID, snapshotProps)
	prod.producerImpl = &overwriteFiles{base: prod}

	return prod
}

func (of *overwriteFiles) processManifests(manifests []iceberg.ManifestFile) ([]iceberg.ManifestFile, error) {
	// no post processing
	return manifests, nil
}

func (of *overwriteFiles) existingManifests() ([]iceberg.ManifestFile, error) {
	// determine if there are any existing manifest files
	existingFiles := make([]iceberg.ManifestFile, 0)

	snap := of.base.txn.meta.currentSnapshot()
	if snap == nil {
		return existingFiles, nil
	}

	manifestList, err := snap.Manifests(of.base.io)
	if err != nil {
		return existingFiles, err
	}

	for _, m := range manifestList {
		// Counts may be -1 (unset) on V1 manifests, so clamp before allocating.
		capacity := int(m.AddedDataFiles()) + int(m.ExistingDataFiles())
		notDeleted := make([]iceberg.ManifestEntry, 0, max(0, capacity))
		foundDeletedCount := 0
		for entry, err := range of.base.iterManifestEntries(m, true) {
			if err != nil {
				return existingFiles, err
			}
			path := entry.DataFile().FilePath()
			content := entry.DataFile().ContentType()
			_, isDeletedData := of.base.deletedFiles[path]
			_, isDeletedDelete := of.base.deletedDeleteFiles[path]

			isData := content == iceberg.EntryContentData
			matched := (isDeletedData && isData) || (isDeletedDelete && !isData)
			if matched {
				foundDeletedCount++
			} else {
				notDeleted = append(notDeleted, entry)
			}
		}

		if foundDeletedCount == 0 {
			existingFiles = append(existingFiles, m)

			continue
		}

		if len(notDeleted) == 0 {
			continue
		}

		// wrap in a function to ensure that the writer is closed even if a panic occurs
		rewriteManifest := func(m iceberg.ManifestFile, notDeleted []iceberg.ManifestEntry) (_ iceberg.ManifestFile, retErr error) {
			spec, err := of.base.txn.meta.GetSpecByID(int(m.PartitionSpecID()))
			if err != nil {
				return nil, err
			}

			wr, path, counter, fileCloser, err := of.base.newManifestWriter(*spec, iceberg.WithManifestWriterContent(m.ManifestContent()))
			if err != nil {
				return nil, err
			}
			defer internal.CheckedClose(fileCloser, &retErr)
			defer internal.CheckedClose(wr, &retErr)

			for _, entry := range notDeleted {
				if err := wr.Existing(entry); err != nil {
					return nil, err
				}
			}

			// close the writer to force a flush and ensure counter.Count is accurate
			if err := wr.Close(); err != nil {
				return nil, err
			}

			return wr.ToManifestFile(path, counter.Count, iceberg.WithManifestFileContent(m.ManifestContent()))
		}

		mf, err := rewriteManifest(m, notDeleted)
		if err != nil {
			return existingFiles, err
		}

		existingFiles = append(existingFiles, mf)
	}

	return existingFiles, nil
}

// validate rejects the overwrite if a concurrent commit added data
// files that fall inside the committer's filter region.
//
// Isolation level is read per-operation: write.delete.isolation-level
// for OpDelete (mirroring Java's BaseDeleteFiles), write.update.iso-
// lation-level otherwise. SNAPSHOT returns nil — concurrent appends
// are allowed. SERIALIZABLE runs validateAddedDataFilesMatchingFilter
// against the committer's filter (AlwaysTrue when no filter is set).
func (of *overwriteFiles) validate(cc *conflictContext) error {
	if cc == nil || of.skipDefaultValidator {
		return nil
	}

	// Delete operations (copy-on-write / merge-on-read deletes that
	// run through overwriteFiles) must read write.delete.isolation-
	// level, not write.update.isolation-level. Java's BaseDeleteFiles
	// makes the same split. Any other op (Overwrite, Replace) reads
	// the update key.
	key, defVal := WriteUpdateIsolationLevelKey, WriteUpdateIsolationLevelDefault
	if of.base.op == OpDelete {
		key, defVal = WriteDeleteIsolationLevelKey, WriteDeleteIsolationLevelDefault
	}
	if readIsolationLevel(of.base.txn.meta.props, key, defVal) != IsolationSerializable {
		// SNAPSHOT isolation allows concurrent appends into the
		// filter region. No further checks on this path.
		return nil
	}

	filter := of.filter
	if filter == nil {
		filter = iceberg.AlwaysTrue{}
	}

	return validateAddedDataFilesMatchingFilter(cc, filter)
}

func (of *overwriteFiles) deletedEntries(ctx context.Context) ([]iceberg.ManifestEntry, error) {
	// determine if we need to record any deleted entries
	//
	// with a full overwrite all the entries are considered deleted
	// with partial overwrites we have to use the predicate to evaluate
	// which entries are affected
	if of.base.parentSnapshotID <= 0 {
		return nil, nil
	}

	parent, err := of.base.txn.meta.SnapshotByID(of.base.parentSnapshotID)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot overwrite empty table", err)
	}

	previousManifests, err := parent.Manifests(of.base.io)
	if err != nil {
		return nil, err
	}

	getEntries := func(m iceberg.ManifestFile) ([]iceberg.ManifestEntry, error) {
		// Counts may be -1 (unset) on V1 manifests, so clamp before allocating.
		capacity := int(m.AddedDataFiles()) + int(m.ExistingDataFiles())
		result := make([]iceberg.ManifestEntry, 0, max(0, capacity))
		for entry, err := range of.base.iterManifestEntries(m, true) {
			if err != nil {
				return nil, err
			}
			path := entry.DataFile().FilePath()
			content := entry.DataFile().ContentType()

			_, isDeletedData := of.base.deletedFiles[path]
			_, isDeletedDelete := of.base.deletedDeleteFiles[path]

			if (isDeletedData && content == iceberg.EntryContentData) ||
				(isDeletedDelete && content != iceberg.EntryContentData) {
				seqNum := entry.SequenceNum()
				result = append(result,
					iceberg.NewManifestEntry(iceberg.EntryStatusDELETED,
						&of.base.snapshotID, &seqNum, entry.FileSequenceNum(),
						entry.DataFile()))
			}
		}

		return result, nil
	}

	nWorkers := config.EnvConfig.MaxWorkers
	finalResult := make([]iceberg.ManifestEntry, 0, len(previousManifests))
	for entries, err := range tblutils.MapExec(ctx, nWorkers, slices.Values(previousManifests), getEntries) {
		if err != nil {
			return nil, err
		}
		finalResult = append(finalResult, entries...)
	}

	return finalResult, nil
}

func (of *overwriteFiles) needsValidation() bool { return true }

type manifestMergeManager struct {
	targetSizeBytes int
	minCountToMerge int
	mergeEnabled    bool
	snap            *snapshotProducer
}

func (m *manifestMergeManager) groupBySpec(manifests []iceberg.ManifestFile) map[int][]iceberg.ManifestFile {
	groups := make(map[int][]iceberg.ManifestFile)
	for _, m := range manifests {
		specid := int(m.PartitionSpecID())
		group := groups[specid]
		groups[specid] = append(group, m)
	}

	return groups
}

func (m *manifestMergeManager) createManifest(specID int, bin []iceberg.ManifestFile) (mf iceberg.ManifestFile, err error) {
	wr, path, counter, fileCloser, err := m.snap.newManifestWriter(m.snap.spec(specID))
	if err != nil {
		return nil, err
	}
	defer internal.CheckedClose(fileCloser, &err)
	defer internal.CheckedClose(wr, &err)

	for _, manifest := range bin {
		for entry, err := range m.snap.iterManifestEntries(manifest, false) {
			if err != nil {
				return nil, err
			}

			switch {
			case entry.Status() == iceberg.EntryStatusDELETED && entry.SnapshotID() == m.snap.snapshotID:
				// only files deleted by this snapshot should be added to the new manifest
				err = wr.Delete(entry)
			case entry.Status() == iceberg.EntryStatusADDED && entry.SnapshotID() == m.snap.snapshotID:
				// added entries from this snapshot are still added, otherwise they should be existing
				err = wr.Add(entry)
			case entry.Status() != iceberg.EntryStatusDELETED:
				// add all non-deleted files from the old manifest as existing files
				err = wr.Existing(entry)
			}

			if err != nil {
				return nil, err
			}
		}
	}

	// close the writer to force a flush and ensure counter.Count is accurate
	if err := wr.Close(); err != nil {
		return nil, err
	}

	return wr.ToManifestFile(path, counter.Count)
}

func (m *manifestMergeManager) mergeGroup(firstManifest iceberg.ManifestFile, specID int, manifests []iceberg.ManifestFile) ([]iceberg.ManifestFile, error) {
	packer := internal.SlicePacker[iceberg.ManifestFile]{
		TargetWeight:    int64(m.targetSizeBytes),
		Lookback:        1,
		LargestBinFirst: false,
	}
	bins := packer.PackEnd(manifests, func(m iceberg.ManifestFile) int64 {
		return m.Length()
	})

	mergeBin := func(bin []iceberg.ManifestFile) ([]iceberg.ManifestFile, error) {
		output := make([]iceberg.ManifestFile, 0, 1)
		if len(bin) == 1 {
			output = append(output, bin[0])
		} else if len(bin) < m.minCountToMerge && slices.ContainsFunc(bin, func(m iceberg.ManifestFile) bool { return m == firstManifest }) {
			// if the bin has the first manifest (the new data files or an appended
			// manifest file) then only merge it if the number of manifests is above
			// the minimum count. this is applied only to bins with an in-memory manifest
			// so that large manifests don't prevent merging older groups
			output = append(output, bin...)
		} else {
			created, err := m.createManifest(specID, bin)
			if err != nil {
				return nil, err
			}
			output = append(output, created)
		}

		return output, nil
	}

	binResults := make([][]iceberg.ManifestFile, len(bins))
	g := errgroup.Group{}
	for i, bin := range bins {
		i, bin := i, bin
		g.Go(func() error {
			var err error
			binResults[i], err = mergeBin(bin)

			return err
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return slices.Concat(binResults...), nil
}

func (m *manifestMergeManager) mergeManifests(manifests []iceberg.ManifestFile) ([]iceberg.ManifestFile, error) {
	if !m.mergeEnabled || len(manifests) == 0 {
		return manifests, nil
	}

	first := manifests[0]
	groups := m.groupBySpec(manifests)

	merged := make([]iceberg.ManifestFile, 0, len(groups))
	for _, specID := range slices.Backward(slices.Sorted(maps.Keys(groups))) {
		manifests, err := m.mergeGroup(first, specID, groups[specID])
		if err != nil {
			return nil, err
		}

		merged = append(merged, manifests...)
	}

	return merged, nil
}

type mergeAppendFiles struct {
	fastAppendFiles

	targetSizeBytes int
	minCountToMerge int
	mergeEnabled    bool
}

func newMergeAppendFilesProducer(op Operation, txn *Transaction, fs iceio.WriteFileIO, commitUUID *uuid.UUID, snapshotProps iceberg.Properties) *snapshotProducer {
	prod := createSnapshotProducer(op, txn, fs, commitUUID, snapshotProps)
	prod.producerImpl = &mergeAppendFiles{
		fastAppendFiles: fastAppendFiles{base: prod},
		targetSizeBytes: txn.meta.props.GetInt(ManifestTargetSizeBytesKey, ManifestTargetSizeBytesDefault),
		minCountToMerge: txn.meta.props.GetInt(ManifestMinMergeCountKey, ManifestMinMergeCountDefault),
		mergeEnabled:    txn.meta.props.GetBool(ManifestMergeEnabledKey, ManifestMergeEnabledDefault),
	}

	return prod
}

func (m *mergeAppendFiles) processManifests(manifests []iceberg.ManifestFile) ([]iceberg.ManifestFile, error) {
	unmergedDataManifests, unmergedDeleteManifests := []iceberg.ManifestFile{}, []iceberg.ManifestFile{}
	for _, m := range manifests {
		if m.ManifestContent() == iceberg.ManifestContentData {
			unmergedDataManifests = append(unmergedDataManifests, m)
		} else if m.ManifestContent() == iceberg.ManifestContentDeletes {
			unmergedDeleteManifests = append(unmergedDeleteManifests, m)
		}
	}

	dataManifestMergeMgr := manifestMergeManager{
		targetSizeBytes: m.targetSizeBytes,
		minCountToMerge: m.minCountToMerge,
		mergeEnabled:    m.mergeEnabled,
		snap:            m.base,
	}

	result, err := dataManifestMergeMgr.mergeManifests(unmergedDataManifests)
	if err != nil {
		return nil, err
	}

	return append(result, unmergedDeleteManifests...), nil
}

func (m *mergeAppendFiles) needsValidation() bool { return false }

type snapshotProducer struct {
	producerImpl

	commitUuid         uuid.UUID
	io                 iceio.WriteFileIO
	txn                *Transaction
	op                 Operation
	snapshotID         int64
	parentSnapshotID   int64
	addedFiles         []iceberg.DataFile
	addedDeleteFiles   []iceberg.DataFile
	manifestCount      atomic.Int32
	deletedFiles       map[string]iceberg.DataFile
	deletedDeleteFiles map[string]iceberg.DataFile
	snapshotProps      iceberg.Properties
}

func createSnapshotProducer(op Operation, txn *Transaction, fs iceio.WriteFileIO, commitUUID *uuid.UUID, snapshotProps iceberg.Properties) *snapshotProducer {
	var (
		commit         uuid.UUID
		parentSnapshot int64 = -1
	)

	if commitUUID == nil {
		commit = uuid.New()
	} else {
		commit = *commitUUID
	}

	if snap := txn.meta.currentSnapshot(); snap != nil {
		parentSnapshot = snap.SnapshotID
	}

	return &snapshotProducer{
		commitUuid:         commit,
		io:                 fs,
		txn:                txn,
		op:                 op,
		snapshotID:         txn.meta.newSnapshotID(),
		parentSnapshotID:   parentSnapshot,
		addedFiles:         []iceberg.DataFile{},
		deletedFiles:       make(map[string]iceberg.DataFile),
		deletedDeleteFiles: make(map[string]iceberg.DataFile),
		snapshotProps:      snapshotProps,
	}
}

func (sp *snapshotProducer) spec(id int) iceberg.PartitionSpec {
	if spec, _ := sp.txn.meta.GetSpecByID(id); spec != nil {
		return *spec
	}

	return iceberg.NewPartitionSpec()
}

func (sp *snapshotProducer) appendDataFile(df iceberg.DataFile) *snapshotProducer {
	sp.addedFiles = append(sp.addedFiles, df)

	return sp
}

func (sp *snapshotProducer) appendDeleteFile(df iceberg.DataFile) *snapshotProducer {
	sp.addedDeleteFiles = append(sp.addedDeleteFiles, df)

	return sp
}

func (sp *snapshotProducer) deleteDataFile(df iceberg.DataFile) *snapshotProducer {
	sp.deletedFiles[df.FilePath()] = df

	return sp
}

func (sp *snapshotProducer) removeDeleteFile(df iceberg.DataFile) *snapshotProducer {
	sp.deletedDeleteFiles[df.FilePath()] = df

	return sp
}

func (sp *snapshotProducer) newManifestWriter(spec iceberg.PartitionSpec, opts ...iceberg.ManifestWriterOption) (_ *iceberg.ManifestWriter, _ string, _ *internal.CountingWriter, _ io.Closer, err error) {
	out, path, err := sp.newManifestOutput()
	if err != nil {
		return nil, "", nil, nil, err
	}

	counter := &internal.CountingWriter{W: out}
	wr, err := iceberg.NewManifestWriter(sp.txn.meta.formatVersion, counter, spec,
		sp.txn.meta.CurrentSchema(), sp.snapshotID, opts...)
	if err != nil {
		return nil, "", nil, nil, errors.Join(err, out.Close())
	}

	return wr, path, counter, out, nil
}

func (sp *snapshotProducer) newManifestOutput() (io.WriteCloser, string, error) {
	provider, err := sp.txn.tbl.LocationProvider()
	if err != nil {
		return nil, "", err
	}
	fname := newManifestFileName(int(sp.manifestCount.Add(1)), sp.commitUuid)
	filepath := provider.NewMetadataLocation(fname)
	f, err := sp.io.Create(filepath)
	if err != nil {
		return nil, "", fmt.Errorf("could not create manifest file: %w", err)
	}

	return f, filepath, nil
}

func (sp *snapshotProducer) iterManifestEntries(m iceberg.ManifestFile, discardDeleted bool) iter.Seq2[iceberg.ManifestEntry, error] {
	return m.Entries(sp.io, discardDeleted)
}

func (sp *snapshotProducer) manifests(ctx context.Context) (_ []iceberg.ManifestFile, err error) {
	deleted, err := sp.deletedEntries(ctx)
	if err != nil {
		return nil, err
	}

	var g errgroup.Group

	addedManifests := make([]iceberg.ManifestFile, 0)
	positionDeleteManifests := make([]iceberg.ManifestFile, 0)
	var deletedFilesManifests []iceberg.ManifestFile
	var existingManifests []iceberg.ManifestFile

	if len(sp.addedFiles) > 0 {
		g.Go(sp.manifestProducer(iceberg.ManifestContentData, sp.addedFiles, &addedManifests))
	}

	if len(sp.addedDeleteFiles) > 0 {
		g.Go(sp.manifestProducer(iceberg.ManifestContentDeletes, sp.addedDeleteFiles, &positionDeleteManifests))
	}

	if len(deleted) > 0 {
		g.Go(func() error {
			// Group deleted entries by (specID, contentType) to ensure data and
			// delete file entries are written to separate manifests with the
			// correct ManifestContent.
			type groupKey struct {
				specID  int
				content iceberg.ManifestContent
			}
			groups := map[groupKey][]iceberg.ManifestEntry{}
			for _, entry := range deleted {
				content := iceberg.ManifestContentData
				if entry.DataFile().ContentType() != iceberg.EntryContentData {
					content = iceberg.ManifestContentDeletes
				}
				key := groupKey{specID: int(entry.DataFile().SpecID()), content: content}
				groups[key] = append(groups[key], entry)
			}

			writeGroup := func(key groupKey, entries []iceberg.ManifestEntry) (_ iceberg.ManifestFile, retErr error) {
				out, path, err := sp.newManifestOutput()
				if err != nil {
					return nil, err
				}
				defer internal.CheckedClose(out, &retErr)

				counter := &internal.CountingWriter{W: out}
				wr, err := iceberg.NewManifestWriter(sp.txn.meta.formatVersion, counter,
					sp.spec(key.specID), sp.txn.meta.CurrentSchema(),
					sp.snapshotID, iceberg.WithManifestWriterContent(key.content))
				if err != nil {
					return nil, err
				}
				defer internal.CheckedClose(wr, &retErr)

				for _, entry := range entries {
					if err := wr.Delete(entry); err != nil {
						return nil, err
					}
				}

				if err := wr.Close(); err != nil {
					return nil, err
				}

				return wr.ToManifestFile(path, counter.Count, iceberg.WithManifestFileContent(key.content))
			}

			for key, entries := range groups {
				mf, err := writeGroup(key, entries)
				if err != nil {
					return err
				}
				deletedFilesManifests = append(deletedFilesManifests, mf)
			}

			return nil
		})
	}

	g.Go(func() error {
		m, err := sp.existingManifests()
		if err != nil {
			return err
		}
		existingManifests = m

		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	manifests := slices.Concat(addedManifests, positionDeleteManifests, deletedFilesManifests, existingManifests)

	return sp.processManifests(manifests)
}

func (sp *snapshotProducer) manifestProducer(content iceberg.ManifestContent, files []iceberg.DataFile, output *[]iceberg.ManifestFile) func() (err error) {
	return func() (err error) {
		out, path, err := sp.newManifestOutput()
		if err != nil {
			return err
		}
		defer internal.CheckedClose(out, &err)

		counter := &internal.CountingWriter{W: out}
		currentSpec, err := sp.txn.meta.CurrentSpec()
		if err != nil || currentSpec == nil {
			return fmt.Errorf("could not get current partition spec: %w", err)
		}
		wr, err := iceberg.NewManifestWriter(sp.txn.meta.formatVersion, counter,
			*currentSpec, sp.txn.meta.CurrentSchema(),
			sp.snapshotID, iceberg.WithManifestWriterContent(content))
		if err != nil {
			return err
		}
		defer internal.CheckedClose(wr, &err)

		for _, df := range files {
			err := wr.Add(iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &sp.snapshotID,
				nil, nil, df))
			if err != nil {
				return err
			}
		}

		// close the writer to force a flush and ensure counter.Count is accurate
		if err := wr.Close(); err != nil {
			return err
		}

		mf, err := wr.ToManifestFile(path, counter.Count, iceberg.WithManifestFileContent(content))
		if err != nil {
			return err
		}
		*output = []iceberg.ManifestFile{mf}

		return nil
	}
}

func (sp *snapshotProducer) summary(props iceberg.Properties) (Summary, error) {
	var ssc SnapshotSummaryCollector
	partitionSummaryLimit := sp.txn.meta.props.
		GetInt(WritePartitionSummaryLimitKey, WritePartitionSummaryLimitDefault)
	ssc.setPartitionSummaryLimit(partitionSummaryLimit)

	currentSchema := sp.txn.meta.CurrentSchema()
	partitionSpec, err := sp.txn.meta.CurrentSpec()
	if err != nil || partitionSpec == nil {
		return Summary{}, fmt.Errorf("could not get current partition spec: %w", err)
	}
	for _, df := range sp.addedFiles {
		if err = ssc.addFile(df, currentSchema, *partitionSpec); err != nil {
			return Summary{}, err
		}
	}
	for _, df := range sp.addedDeleteFiles {
		if err = ssc.addFile(df, currentSchema, *partitionSpec); err != nil {
			return Summary{}, err
		}
	}

	if len(sp.deletedFiles) > 0 {
		specs := sp.txn.meta.specs
		for _, df := range sp.deletedFiles {
			if err = ssc.removeFile(df, currentSchema, specs[df.SpecID()]); err != nil {
				return Summary{}, err
			}
		}
	}

	if len(sp.deletedDeleteFiles) > 0 {
		specs := sp.txn.meta.specs
		for _, df := range sp.deletedDeleteFiles {
			if err = ssc.removeFile(df, currentSchema, specs[df.SpecID()]); err != nil {
				return Summary{}, err
			}
		}
	}

	var previousSnapshot *Snapshot
	if sp.parentSnapshotID > 0 {
		previousSnapshot, _ = sp.txn.meta.SnapshotByID(sp.parentSnapshotID)
	}

	var previousSummary iceberg.Properties
	if previousSnapshot != nil {
		previousSummary = previousSnapshot.Summary.Properties
	}

	summaryProps := ssc.build()
	maps.Copy(summaryProps, props)

	return updateSnapshotSummaries(Summary{
		Operation:  sp.op,
		Properties: summaryProps,
	}, previousSummary)
}

// computeOwnManifests returns the subset of allManifests that were written
// by this producer (i.e. not inherited from the parent snapshot). These are
// preserved across OCC retry attempts when the manifest list is rebuilt
// against a fresh parent.
func (sp *snapshotProducer) computeOwnManifests(allManifests []iceberg.ManifestFile) ([]iceberg.ManifestFile, error) {
	if sp.parentSnapshotID <= 0 {
		// No parent means all manifests are new — nothing to exclude.
		return allManifests, nil
	}

	parent, err := sp.txn.meta.SnapshotByID(sp.parentSnapshotID)
	if err != nil {
		return nil, fmt.Errorf("computeOwnManifests: lookup parent snapshot %d: %w", sp.parentSnapshotID, err)
	}
	if parent == nil {
		return nil, fmt.Errorf("%w: computeOwnManifests parent id %d", ErrSnapshotNotFound, sp.parentSnapshotID)
	}

	parentManifests, err := parent.Manifests(sp.io)
	if err != nil {
		return nil, fmt.Errorf("computeOwnManifests: read parent manifests: %w", err)
	}

	inherited := make(map[string]bool, len(parentManifests))
	for _, m := range parentManifests {
		inherited[m.FilePath()] = true
	}

	own := make([]iceberg.ManifestFile, 0, len(allManifests))
	for _, m := range allManifests {
		if !inherited[m.FilePath()] {
			own = append(own, m)
		}
	}

	return own, nil
}

func (sp *snapshotProducer) commit(ctx context.Context) (_ []Update, _ []Requirement, err error) {
	newManifests, err := sp.manifests(ctx)
	if err != nil {
		return nil, nil, err
	}

	// Separate "own" manifests (those written by this producer) from
	// manifests inherited from the stale parent. The own manifests are
	// preserved when the manifest list is rebuilt during OCC retries.
	ownManifests, err := sp.computeOwnManifests(newManifests)
	if err != nil {
		return nil, nil, err
	}

	nextSequence := sp.txn.meta.nextSequenceNumber()
	summary, err := sp.summary(sp.snapshotProps)
	if err != nil {
		return nil, nil, err
	}

	fname := newManifestListFileName(sp.snapshotID, 0, sp.commitUuid)
	locProvider, err := sp.txn.tbl.LocationProvider()
	if err != nil {
		return nil, nil, err
	}

	manifestListFilePath := locProvider.NewMetadataLocation(fname)

	var parentSnapshot *int64
	if sp.parentSnapshotID > 0 {
		parentSnapshot = &sp.parentSnapshotID
	}

	firstRowID := int64(0)
	var addedRows int64

	out, err := sp.io.Create(manifestListFilePath)
	if err != nil {
		return nil, nil, err
	}
	defer internal.CheckedClose(out, &err)

	if sp.txn.meta.formatVersion == 3 {
		firstRowID = sp.txn.meta.NextRowID()
		writer, err := iceberg.NewManifestListWriterV3(out, sp.snapshotID, nextSequence, firstRowID, parentSnapshot)
		if err != nil {
			return nil, nil, err
		}
		defer internal.CheckedClose(writer, &err)
		if err = writer.AddManifests(newManifests); err != nil {
			return nil, nil, err
		}
		if writer.NextRowID() != nil {
			addedRows = *writer.NextRowID() - firstRowID
		}
	} else {
		err = iceberg.WriteManifestList(sp.txn.meta.formatVersion, out,
			sp.snapshotID, parentSnapshot, &nextSequence, firstRowID, newManifests)
		if err != nil {
			return nil, nil, err
		}
	}

	snapshot := Snapshot{
		SnapshotID:       sp.snapshotID,
		ParentSnapshotID: parentSnapshot,
		SequenceNumber:   nextSequence,
		ManifestList:     manifestListFilePath,
		Summary:          &summary,
		SchemaID:         &sp.txn.meta.currentSchemaID,
		TimestampMs:      time.Now().UnixMilli(),
	}
	if sp.txn.meta.formatVersion == 3 {
		snapshot.FirstRowID = &firstRowID
		snapshot.AddedRows = &addedRows
	}

	branch := sp.txn.branch
	if branch == "" {
		branch = MainBranch
	}

	// Register this producer's client-side conflict validator on the
	// transaction. doCommit runs it against the current catalog state
	// before cat.CommitTable so conflicts the catalog can't see
	// (partition-filter overlap, referenced-file removal) are caught
	// pre-flight. Producers that are commutative against concurrent
	// appends (fast-append, merge-append) opt out via needsValidation()
	// == false and skip registration entirely. The nil guard remains for
	// unit tests that drive commit() on a bare snapshotProducer.
	if impl := sp.producerImpl; impl != nil && impl.needsValidation() {
		sp.txn.validators = append(sp.txn.validators, func(cc *conflictContext) error {
			return impl.validate(cc)
		})
	}

	// Build the manifest-list rebuild closure. It is called by doCommit
	// on each OCC retry to regenerate the manifest list so it correctly
	// inherits all data files committed by concurrent writers since the
	// original snapshot was built.
	formatVersion := sp.txn.meta.formatVersion
	snapshotID := sp.snapshotID
	commitUUID := sp.commitUuid
	capturedSnapshot := snapshot // copy the value so the closure is self-contained
	processManifestsFn := func(m []iceberg.ManifestFile) ([]iceberg.ManifestFile, error) {
		return sp.processManifests(m)
	}

	rebuildFn := func(_ context.Context, freshMeta Metadata, freshParent *Snapshot, fio iceio.WriteFileIO, attempt int) (_ *Snapshot, retErr error) {
		// Load inherited manifests from the fresh parent.
		var inherited []iceberg.ManifestFile
		if freshParent != nil {
			inherited, retErr = freshParent.Manifests(fio)
			if retErr != nil {
				return nil, fmt.Errorf("rebuild manifest list: load parent manifests: %w", retErr)
			}
		}

		// Combine own manifests with inherited ones, applying any
		// producer-specific processing (no-op for fast/merge-append).
		combined, procErr := processManifestsFn(slices.Concat(ownManifests, inherited))
		if procErr != nil {
			return nil, fmt.Errorf("rebuild manifest list: process manifests: %w", procErr)
		}

		// Derive the sequence number from the fresh table-wide last-sequence-number.
		// Using freshParent.SequenceNumber + 1 would violate the spec when a
		// concurrent writer on a different branch bumps last-sequence-number
		// without advancing this branch's parent — MetadataBuilder.AddSnapshot
		// rejects SequenceNumber <= lastSequenceNumber.
		var newSeq int64
		if formatVersion >= 2 {
			newSeq = freshMeta.LastSequenceNumber() + 1
		}

		// Write the rebuilt manifest list to a path unique to this retry
		// attempt. Each retry uses a different attempt counter in the filename
		// (snap-{id}-{attempt}-{uuid}.avro) so that S3 conditional-write
		// semantics (if-none-match) do not reject the overwrite. Orphaned files
		// from superseded retry attempts are removed by doCommit after the
		// commit succeeds.
		fname := newManifestListFileName(snapshotID, attempt, commitUUID)
		manifestListPath := locProvider.NewMetadataLocation(fname)

		out, createErr := fio.Create(manifestListPath)
		if createErr != nil {
			return nil, fmt.Errorf("rebuild manifest list: create file: %w", createErr)
		}
		defer internal.CheckedClose(out, &retErr)

		var parentID *int64
		if freshParent != nil {
			id := freshParent.SnapshotID
			parentID = &id
		}

		firstRowID := int64(0)
		var addedRows int64
		if formatVersion == 3 {
			// Derive firstRowID from the fresh metadata so the manifest-list
			// first-row-id field is consistent with the catalog's nextRowID
			// after concurrent writers have advanced it since attempt 0.
			firstRowID = freshMeta.NextRowID()
			writer, wrErr := iceberg.NewManifestListWriterV3(out, snapshotID, newSeq, firstRowID, parentID)
			if wrErr != nil {
				return nil, fmt.Errorf("rebuild manifest list: create v3 writer: %w", wrErr)
			}
			defer internal.CheckedClose(writer, &retErr)
			if addErr := writer.AddManifests(combined); addErr != nil {
				return nil, fmt.Errorf("rebuild manifest list: add manifests: %w", addErr)
			}
			if writer.NextRowID() != nil {
				addedRows = *writer.NextRowID() - firstRowID
			}
		} else {
			if wErr := iceberg.WriteManifestList(formatVersion, out, snapshotID, parentID, &newSeq, firstRowID, combined); wErr != nil {
				return nil, fmt.Errorf("rebuild manifest list: write: %w", wErr)
			}
		}

		rebuilt := capturedSnapshot
		rebuilt.ManifestList = manifestListPath
		rebuilt.ParentSnapshotID = parentID
		rebuilt.SequenceNumber = newSeq
		if formatVersion == 3 {
			rebuilt.FirstRowID = &firstRowID
			rebuilt.AddedRows = &addedRows
		}

		// Recompute snapshot summary against the fresh parent so that totals
		// (total-records, total-data-files, total-files-size) are not regressed
		// to the stale values captured at attempt 0. The per-operation delta
		// (added-data-files, added-records, etc.) is preserved in
		// capturedSnapshot.Summary and is replayed on top of the fresh base.
		if freshParent != nil && freshParent.Summary != nil && capturedSnapshot.Summary != nil {
			deltaSummary := Summary{
				Operation:  capturedSnapshot.Summary.Operation,
				Properties: maps.Clone(capturedSnapshot.Summary.Properties),
			}
			if s, sumErr := updateSnapshotSummaries(deltaSummary, freshParent.Summary.Properties); sumErr == nil {
				rebuilt.Summary = &s
			}
		}

		return &rebuilt, nil
	}

	addSnap := NewAddSnapshotUpdate(&snapshot)
	addSnap.ownManifests = ownManifests
	addSnap.rebuildManifestList = rebuildFn

	return []Update{
			addSnap,
			// Use 0 (not -1) for the optional fields so they are omitted by
			// `omitempty` in JSON marshalling. -1 is a sentinel meaning
			// "no limit" internally, but strict catalogs such as AWS S3 Tables
			// reject a payload that explicitly contains negative values.
			NewSetSnapshotRefUpdate(branch, sp.snapshotID, BranchRef, 0, 0, 0),
		}, []Requirement{
			AssertRefSnapshotID(branch, sp.txn.meta.currentSnapshotID),
		}, nil
}
