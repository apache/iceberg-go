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
	"fmt"
	"io"
	"maps"
	"slices"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

type producerImpl interface {
	processManifests(manifests []iceberg.ManifestFile) ([]iceberg.ManifestFile, error)
	existingManifests() ([]iceberg.ManifestFile, error)
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
	return nil, nil
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

			wr, err := iceberg.NewManifestWriter(sp.txn.meta.formatVersion, counter,
				sp.txn.meta.CurrentSpec(), sp.txn.meta.CurrentSchema(),
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
	partitionSpec := sp.txn.meta.CurrentSpec()
	for _, df := range sp.addedFiles {
		ssc.addFile(df, currentSchema, partitionSpec)
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
	}, previousSummary, sp.op == OpOverwrite)
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

func truncateUpperBoundText(s string, trunc int) string {
	if trunc == utf8.RuneCountInString(s) {
		return s
	}

	result := []rune(s)[:trunc]
	for i := len(result) - 1; i >= 0; i-- {
		next := result[i] + 1
		if utf8.ValidRune(next) {
			result[i] = next

			return string(result)
		}
	}

	return ""
}

func truncateUpperBoundBinary(val []byte, trunc int) []byte {
	result := val[:trunc]
	if bytes.Equal(result, val) {
		return result
	}

	for i := len(result) - 1; i >= 0; i-- {
		if result[i] < 255 {
			result[i]++

			return result
		}
	}

	return nil
}
