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
	"slices"
	"sync"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table/dv"
	"github.com/google/uuid"
)

// EqualityDeleteToDVResult summarizes an equality-delete conversion.
type EqualityDeleteToDVResult struct {
	// Table is the table returned by the table-level action. It is nil when the
	// action is staged on a Transaction and set after that transaction commits.
	Table *Table

	// ConvertedEqualityDeleteFiles is the number of live equality delete files
	// removed by the rewrite.
	ConvertedEqualityDeleteFiles int
	// AddedDeletionVectors is the number of file-specific deletion vectors added.
	AddedDeletionVectors int
	// RemovedPositionDeleteFiles is the number of legacy position delete files
	// scoped to one data file, either by referenced_data_file or equal
	// file_path bounds, removed with the conversion.
	RemovedPositionDeleteFiles int
	// ReplacedDeletionVectors is the number of existing vectors merged into new
	// vectors and removed by the rewrite.
	ReplacedDeletionVectors int
}

// ConvertEqualityDeletesToDeletionVectors converts live equality deletes on
// the main branch of a v3 table to file-specific deletion vectors. The rewrite
// commits as one replace snapshot so changelog readers do not see a second
// logical deletion event.
func (t Table) ConvertEqualityDeletesToDeletionVectors(ctx context.Context) (*EqualityDeleteToDVResult, error) {
	txn, err := t.NewTransactionOnBranch(MainBranch)
	if err != nil {
		return nil, err
	}

	result, err := txn.ConvertEqualityDeletesToDeletionVectors(ctx)
	if err != nil {
		return result, err
	}
	if result.ConvertedEqualityDeleteFiles == 0 {
		result.Table = &t

		return result, nil
	}

	committed, err := txn.Commit(ctx)
	if err == nil {
		result.Table = committed
	}

	return result, err
}

// ConvertEqualityDeletesToDeletionVectors stages an equality-delete to
// deletion-vector conversion on the transaction's branch. Commit the
// transaction to publish the replace snapshot.
func (t *Transaction) ConvertEqualityDeletesToDeletionVectors(ctx context.Context) (*EqualityDeleteToDVResult, error) {
	meta, err := t.txnMeta()
	if err != nil {
		return nil, err
	}
	if meta.formatVersion != 3 {
		return nil, fmt.Errorf("%w: equality-delete to deletion-vector conversion requires a v3 table, got v%d",
			iceberg.ErrNotImplemented, meta.formatVersion)
	}

	snapshot := t.planningSnapshot(meta)
	if snapshot == nil {
		return &EqualityDeleteToDVResult{}, nil
	}

	fs, err := t.tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	manifests, err := snapshot.Manifests(fs)
	if err != nil {
		return nil, err
	}
	equalityDeleteFiles := make(map[string]iceberg.DataFile)
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if manifest.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		for entry, err := range manifest.Entries(fs, true) {
			if err != nil {
				return nil, err
			}
			file := entry.DataFile()
			if file.ContentType() != iceberg.EntryContentEqDeletes {
				continue
			}
			if _, exists := equalityDeleteFiles[file.FilePath()]; exists {
				return nil, fmt.Errorf("%w: duplicate live equality delete file %s",
					ErrInvalidMetadata, file.FilePath())
			}
			equalityDeleteFiles[file.FilePath()] = file
		}
	}
	if len(equalityDeleteFiles) == 0 {
		return &EqualityDeleteToDVResult{}, nil
	}
	// Only projected equality keys are needed to resolve their matching row
	// positions. Missing (dropped) keys are handled by the delete loader.
	keyNames := make(map[string]struct{})
	for _, file := range equalityDeleteFiles {
		for _, id := range file.EqualityFieldIDs() {
			if name, ok := meta.CurrentSchema().FindColumnName(id); ok {
				keyNames[name] = struct{}{}
			}
		}
	}
	selectedFields := make([]string, 0, len(keyNames))
	for name := range keyNames {
		selectedFields = append(selectedFields, name)
	}
	slices.Sort(selectedFields)
	scanOpts := []ScanOption{WithScanPlanningMode(ScanPlanningLocal)}
	// WithSelectedFields with no names selects every column. Keep that
	// fallback when all equality keys were dropped from the current schema.
	if len(selectedFields) > 0 {
		scanOpts = append(scanOpts, WithSelectedFields(selectedFields...))
	}

	scan, err := t.Scan(scanOpts...)
	if err != nil {
		return nil, err
	}
	if t.branch != "" && t.branch != MainBranch && scan.metadata.SnapshotByName(t.branch) != nil {
		branchScan, err := scan.UseRef(t.branch)
		_ = scan.Close()
		if err != nil {
			return nil, err
		}
		scan = branchScan
	}
	defer scan.Close()

	plannedTasks, err := scan.PlanFiles(ctx)
	if err != nil {
		return nil, err
	}
	baseTasks := make([]FileScanTask, 0, len(plannedTasks))
	addedDeleteTasks := make([]FileScanTask, 0, len(plannedTasks))
	positionsByPath := make(map[string]*dv.RoaringPositionBitmap)
	positionSinks := make(map[string]func([]int64))
	addedTaskByPath := make(map[string]int)
	var positionsMu sync.Mutex
	for _, task := range plannedTasks {
		if len(task.EqualityDeleteFiles) == 0 {
			continue
		}
		if task.File == nil {
			return nil, fmt.Errorf("%w: scan task with equality deletes has no data file", ErrInvalidMetadata)
		}
		path := task.File.FilePath()
		if _, exists := positionsByPath[path]; !exists {
			filePath := path
			positionsByPath[path] = nil
			positionSinks[path] = func(positions []int64) {
				positionsMu.Lock()
				bitmap := positionsByPath[filePath]
				if bitmap == nil {
					bitmap = dv.NewRoaringPositionBitmap()
					positionsByPath[filePath] = bitmap
				}
				for _, position := range positions {
					bitmap.Set(uint64(position))
				}
				positionsMu.Unlock()
			}
		}

		base := task
		base.EqualityDeleteFiles = nil
		baseTasks = append(baseTasks, base)

		if _, exists := addedTaskByPath[path]; !exists {
			added := task
			added.DeleteFiles = nil
			added.DeletionVectorFiles = nil
			addedTaskByPath[path] = len(addedDeleteTasks)
			addedDeleteTasks = append(addedDeleteTasks, added)
		}
	}

	if len(baseTasks) > 0 {
		_, records, err := scan.readFileTasksWithAddedDeletes(ctx, baseTasks, addedDeleteTasks, positionSinks)
		if err != nil {
			return nil, err
		}
		for record, err := range records {
			if err != nil {
				return nil, err
			}
			record.Release()
		}
	}

	writeFS, err := requireWriteFileIO(fs)
	if err != nil {
		return nil, err
	}
	metadata, err := meta.Build()
	if err != nil {
		return nil, err
	}
	positionDeleteLoader := newLazyPositionDeleteLoader(fs, baseTasks)
	defer positionDeleteLoader.release()
	deletionVectorLoader, err := newLazyDeletionVectorLoader(fs, baseTasks)
	if err != nil {
		return nil, err
	}

	result := &EqualityDeleteToDVResult{ConvertedEqualityDeleteFiles: len(equalityDeleteFiles)}
	deleteFilesToRemove := make([]iceberg.DataFile, 0, len(equalityDeleteFiles))
	addRemoval := func(file iceberg.DataFile) {
		deleteFilesToRemove = append(deleteFilesToRemove, file)
		if IsDeletionVector(file) {
			result.ReplacedDeletionVectors++
		}
	}

	equalityPaths := make([]string, 0, len(equalityDeleteFiles))
	for path := range equalityDeleteFiles {
		equalityPaths = append(equalityPaths, path)
	}
	slices.Sort(equalityPaths)
	for _, path := range equalityPaths {
		addRemoval(equalityDeleteFiles[path])
	}

	var writer *dv.DVWriter
	seenDataPaths := make(map[string]struct{}, len(positionsByPath))
	for i, task := range baseTasks {
		path := task.File.FilePath()
		if _, seen := seenDataPaths[path]; seen {
			continue
		}
		seenDataPaths[path] = struct{}{}
		positions := positionsByPath[path]
		if positions == nil || positions.IsEmpty() {
			continue
		}
		partition := dataFilePartition(task.File)
		if writer == nil {
			writer = dv.NewDVWriter(writeFS, func(id int32) *iceberg.PartitionSpec {
				return metadata.PartitionSpecByID(int(id))
			})
		}
		writer.Load(path, positions, task.File.SpecID(), partition)

		var positionDeletes *dv.RoaringPositionBitmap
		if positionDeleteLoader != nil {
			positionDeletes, err = positionDeleteLoader.load(ctx, i, path)
			if err != nil {
				return nil, fmt.Errorf("read existing position deletes for %s: %w", path, err)
			}
		}
		if positionDeletes != nil {
			writer.Load(path, positionDeletes, task.File.SpecID(), partition)
		}
		existingDV, err := deletionVectorLoader.load(ctx, path)
		if err != nil {
			return nil, fmt.Errorf("read existing deletion vector for %s: %w", path, err)
		}
		if existingDV != nil {
			writer.Load(path, existingDV, task.File.SpecID(), partition)
		}
		for _, file := range task.DeletionVectorFiles {
			addRemoval(file)
		}
		for _, file := range task.DeleteFiles {
			if referencedDataFilePath(file) == path {
				addRemoval(file)
				result.RemovedPositionDeleteFiles++
			}
		}
	}

	var additions []rewriteDeleteFileAddition
	var outputPath string
	staged := false
	defer func() {
		if !staged && outputPath != "" {
			_ = writeFS.Remove(context.WithoutCancel(ctx), outputPath)
		}
	}()
	if writer != nil {
		// Respect staged table location and write.data.path properties, including
		// object store location providers. The base table may still point elsewhere.
		locationProvider, err := LoadLocationProvider(meta.loc, meta.props)
		if err != nil {
			return nil, fmt.Errorf("resolve deletion vector location: %w", err)
		}
		outputPath = locationProvider.NewDataLocation(
			"00000-0-"+uuid.New().String()+"-equality-delete-conversion.puffin", "")
		deletionVectors, err := writer.Flush(ctx, outputPath)
		if err != nil {
			return nil, err
		}
		sequenceNumber := meta.nextSequenceNumber()
		for _, file := range deletionVectors {
			sequence := sequenceNumber
			additions = append(additions, rewriteDeleteFileAddition{
				file:               file,
				dataSequenceNumber: &sequence,
			})
		}
		result.AddedDeletionVectors = len(deletionVectors)
	}

	if err := t.replaceFiles(ctx, nil, nil, deleteFilesToRemove, nil, additions, nil,
		withRewriteSemantics(), withEqualityDeleteToDVConversion(), WithoutAutoNameMapping()); err != nil {
		return nil, err
	}
	staged = true

	return result, nil
}
