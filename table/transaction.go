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
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
)

type snapshotUpdate struct {
	txn           *Transaction
	io            io.WriteFileIO
	snapshotProps iceberg.Properties
}

func (s snapshotUpdate) fastAppend() *snapshotProducer {
	return newFastAppendFilesProducer(OpAppend, s.txn, s.io, nil, s.snapshotProps)
}

func (s snapshotUpdate) mergeOverwrite(commitUUID *uuid.UUID) *snapshotProducer {
	op := OpOverwrite
	if s.txn.meta.currentSnapshot() == nil {
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

func (t *Transaction) appendSnapshotProducer(props iceberg.Properties) *snapshotProducer {
	manifestMerge := t.meta.props.GetBool(ManifestMergeEnabledKey, ManifestMergeEnabledDefault)
	updateSnapshot := t.updateSnapshot(props)
	if manifestMerge {
		return updateSnapshot.mergeAppend()
	}

	return updateSnapshot.fastAppend()
}

func (t *Transaction) updateSnapshot(props iceberg.Properties) snapshotUpdate {
	return snapshotUpdate{
		txn:           t,
		io:            t.tbl.fs.(io.WriteFileIO),
		snapshotProps: props,
	}
}

func (t *Transaction) SetProperties(props iceberg.Properties) error {
	if len(props) > 0 {
		return t.apply([]Update{NewSetPropertiesUpdate(props)}, nil)
	}

	return nil
}

func (t *Transaction) AppendTable(ctx context.Context, tbl arrow.Table, batchSize int64, snapshotProps iceberg.Properties) error {
	rdr := array.NewTableReader(tbl, batchSize)
	defer rdr.Release()

	return t.Append(ctx, rdr, snapshotProps)
}

func (t *Transaction) Append(ctx context.Context, rdr array.RecordReader, snapshotProps iceberg.Properties) error {
	appendFiles := t.appendSnapshotProducer(snapshotProps)

	itr := recordsToDataFiles(ctx, t.tbl.Location(), t.meta, recordWritingArgs{
		sc:        rdr.Schema(),
		itr:       array.IterFromReader(rdr),
		fs:        t.tbl.fs.(io.WriteFileIO),
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

	markedForDeletion := make([]iceberg.DataFile, 0, len(setToDelete))
	for df, err := range s.dataFiles(t.tbl.fs, nil) {
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
	updater := t.updateSnapshot(snapshotProps).mergeOverwrite(&commitUUID)

	for _, df := range markedForDeletion {
		updater.deleteDataFile(df)
	}

	dataFiles := filesToDataFiles(ctx, t.tbl.fs, t.meta, slices.Values(filesToAdd))
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
			for df, err := range s.dataFiles(t.tbl.fs, nil) {
				if err != nil {
					return err
				}

				if _, ok := set[df.FilePath()]; ok {
					referenced = append(referenced, df.FilePath())
				}
			}
			if len(referenced) > 0 {
				return fmt.Errorf("cannot add files that are already referenced by table, files: %s", referenced)
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

	updater := t.updateSnapshot(snapshotProps).fastAppend()

	dataFiles := filesToDataFiles(ctx, t.tbl.fs, t.meta, slices.Values(files))
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

func (t *Transaction) Scan(opts ...ScanOption) (*Scan, error) {
	updatedMeta, err := t.meta.Build()
	if err != nil {
		return nil, err
	}

	s := &Scan{
		metadata:       updatedMeta,
		io:             t.tbl.fs,
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

	return &StagedTable{Table: New(t.tbl.identifier, updatedMeta,
		updatedMeta.Location(), t.tbl.fs, t.tbl.cat)}, nil
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

		return t.tbl.doCommit(ctx, t.meta.updates, t.reqs)
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
