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
	"fmt"
	"io"
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
	deletedEntries() ([]iceberg.ManifestEntry, error)
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
	existing := make([]iceberg.ManifestFile, 0)
	if fa.base.parentSnapshotID > 0 {
		previous, err := fa.base.txn.meta.SnapshotByID(fa.base.parentSnapshotID)
		if err != nil {
			return nil, fmt.Errorf("could not find parent snapshot %d", fa.base.parentSnapshotID)
		}

		manifests, err := previous.Manifests(fa.base.io)
		if err != nil {
			return nil, err
		}

		for _, m := range manifests {
			if m.HasAddedFiles() || m.HasExistingFiles() || m.SnapshotID() == fa.base.snapshotID {
				existing = append(existing, m)
			}
		}
	}

	return existing, nil
}

func (fa *fastAppendFiles) deletedEntries() ([]iceberg.ManifestEntry, error) {
	// for fast appends, there are no deleted entries
	return nil, nil
}

type overwriteFiles struct {
	base *snapshotProducer
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
		entries, err := of.base.fetchManifestEntry(m, true)
		if err != nil {
			return existingFiles, err
		}

		foundDeleted := make([]iceberg.ManifestEntry, 0)
		notDeleted := make([]iceberg.ManifestEntry, 0, len(entries))
		for _, entry := range entries {
			if _, ok := of.base.deletedFiles[entry.DataFile().FilePath()]; ok {
				foundDeleted = append(foundDeleted, entry)
			} else {
				notDeleted = append(notDeleted, entry)
			}
		}

		if len(foundDeleted) == 0 {
			existingFiles = append(existingFiles, m)

			continue
		}

		if len(notDeleted) == 0 {
			continue
		}

		spec, err := of.base.txn.meta.GetSpecByID(int(m.PartitionSpecID()))
		if err != nil {
			return existingFiles, err
		}

		wr, path, counter, err := of.base.newManifestWriter(*spec)
		if err != nil {
			return existingFiles, err
		}

		for _, entry := range notDeleted {
			if err := wr.Existing(entry); err != nil {
				return existingFiles, err
			}
		}

		// close the writer to force a flush and ensure counter.Count is accurate
		if err := wr.Close(); err != nil {
			return existingFiles, err
		}

		mf, err := wr.ToManifestFile(path, counter.Count)
		if err != nil {
			return existingFiles, err
		}

		existingFiles = append(existingFiles, mf)
	}

	return existingFiles, nil
}

func (of *overwriteFiles) deletedEntries() ([]iceberg.ManifestEntry, error) {
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
		entries, err := of.base.fetchManifestEntry(m, true)
		if err != nil {
			return nil, err
		}

		result := make([]iceberg.ManifestEntry, 0, len(entries))
		for _, entry := range entries {
			_, ok := of.base.deletedFiles[entry.DataFile().FilePath()]
			if ok && entry.DataFile().ContentType() == iceberg.EntryContentData {
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
	for entries, err := range tblutils.MapExec(nWorkers, slices.Values(previousManifests), getEntries) {
		if err != nil {
			return nil, err
		}
		finalResult = append(finalResult, entries...)
	}

	return finalResult, nil
}

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

func (m *manifestMergeManager) createManifest(specID int, bin []iceberg.ManifestFile) (iceberg.ManifestFile, error) {
	wr, path, counter, err := m.snap.newManifestWriter(m.snap.spec(specID))
	if err != nil {
		return nil, err
	}

	for _, manifest := range bin {
		entries, err := m.snap.fetchManifestEntry(manifest, false)
		if err != nil {
			return nil, err
		}

		for _, entry := range entries {
			switch {
			case entry.Status() == iceberg.EntryStatusDELETED && entry.SnapshotID() == m.snap.snapshotID:
				// only files deleted by this snapshot should be added to the new manifest
				wr.Delete(entry)
			case entry.Status() == iceberg.EntryStatusADDED && entry.SnapshotID() == m.snap.snapshotID:
				// added entries from this snapshot are still added, otherwise they should be existing
				wr.Add(entry)
			case entry.Status() != iceberg.EntryStatusDELETED:
				// add all non-deleted files from the old manifest as existing files
				wr.Existing(entry)
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

type snapshotProducer struct {
	producerImpl

	commitUuid       uuid.UUID
	io               iceio.WriteFileIO
	txn              *Transaction
	op               Operation
	snapshotID       int64
	parentSnapshotID int64
	addedFiles       []iceberg.DataFile
	manifestCount    atomic.Int32
	deletedFiles     map[string]iceberg.DataFile
	snapshotProps    iceberg.Properties
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
		commitUuid:       commit,
		io:               fs,
		txn:              txn,
		op:               op,
		snapshotID:       txn.meta.newSnapshotID(),
		parentSnapshotID: parentSnapshot,
		addedFiles:       []iceberg.DataFile{},
		deletedFiles:     make(map[string]iceberg.DataFile),
		snapshotProps:    snapshotProps,
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

func (sp *snapshotProducer) deleteDataFile(df iceberg.DataFile) *snapshotProducer {
	sp.deletedFiles[df.FilePath()] = df

	return sp
}

func (sp *snapshotProducer) newManifestWriter(spec iceberg.PartitionSpec) (*iceberg.ManifestWriter, string, *internal.CountingWriter, error) {
	out, path, err := sp.newManifestOutput()
	if err != nil {
		return nil, "", nil, err
	}

	counter := &internal.CountingWriter{W: out}
	wr, err := iceberg.NewManifestWriter(sp.txn.meta.formatVersion, counter, spec,
		sp.txn.meta.CurrentSchema(), sp.snapshotID)
	if err != nil {
		defer out.Close()

		return nil, "", nil, err
	}

	return wr, path, counter, nil
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

func (sp *snapshotProducer) fetchManifestEntry(m iceberg.ManifestFile, discardDeleted bool) ([]iceberg.ManifestEntry, error) {
	return m.FetchEntries(sp.io, discardDeleted)
}

func (sp *snapshotProducer) manifests() ([]iceberg.ManifestFile, error) {
	var g errgroup.Group

	results := [...][]iceberg.ManifestFile{nil, nil, nil}

	if len(sp.addedFiles) > 0 {
		g.Go(func() error {
			out, path, err := sp.newManifestOutput()
			if err != nil {
				return err
			}
			defer out.Close()

			counter := &internal.CountingWriter{W: out}
			currentSpec, err := sp.txn.meta.CurrentSpec()
			if err != nil || currentSpec == nil {
				return fmt.Errorf("could not get current partition spec: %w", err)
			}
			wr, err := iceberg.NewManifestWriter(sp.txn.meta.formatVersion, counter,
				*currentSpec, sp.txn.meta.CurrentSchema(),
				sp.snapshotID)
			if err != nil {
				return err
			}

			for _, df := range sp.addedFiles {
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

			mf, err := wr.ToManifestFile(path, counter.Count)
			if err == nil {
				results[0] = append(results[0], mf)
			}

			return err
		})
	}

	deleted, err := sp.deletedEntries()
	if err != nil {
		return nil, err
	}

	if len(deleted) > 0 {
		g.Go(func() error {
			partitionGroups := map[int][]iceberg.ManifestEntry{}
			for _, entry := range deleted {
				specid := int(entry.DataFile().SpecID())

				group := partitionGroups[specid]
				partitionGroups[specid] = append(group, entry)
			}

			for specid, entries := range partitionGroups {
				out, path, err := sp.newManifestOutput()
				if err != nil {
					return err
				}
				defer out.Close()

				mf, err := iceberg.WriteManifest(path, out, sp.txn.meta.formatVersion,
					sp.spec(specid), sp.txn.meta.CurrentSchema(), sp.snapshotID, entries)
				if err != nil {
					return err
				}
				results[1] = append(results[1], mf)
			}

			return nil
		})
	}

	g.Go(func() error {
		m, err := sp.existingManifests()
		if err != nil {
			return err
		}
		results[2] = m

		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	manifests := slices.Concat(results[0], results[1], results[2])

	return sp.processManifests(manifests)
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
		ssc.addFile(df, currentSchema, *partitionSpec)
	}

	if len(sp.deletedFiles) > 0 {
		specs := sp.txn.meta.specs
		for _, df := range sp.deletedFiles {
			ssc.removeFile(df, currentSchema, specs[df.SpecID()])
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

func (sp *snapshotProducer) commit() ([]Update, []Requirement, error) {
	newManifests, err := sp.manifests()
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

	out, err := sp.io.Create(manifestListFilePath)
	if err != nil {
		return nil, nil, err
	}
	defer out.Close()

	err = iceberg.WriteManifestList(sp.txn.meta.formatVersion, out,
		sp.snapshotID, parentSnapshot, &nextSequence, newManifests)
	if err != nil {
		return nil, nil, err
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

	return []Update{
			NewAddSnapshotUpdate(&snapshot),
			NewSetSnapshotRefUpdate("main", sp.snapshotID, BranchRef, -1, -1, -1),
		}, []Requirement{
			AssertRefSnapshotID("main", sp.txn.meta.currentSnapshotID),
		}, nil
}
