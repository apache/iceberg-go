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

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// CompactionSourceRowLineage describes a source file as read by the worker.
type CompactionSourceRowLineage struct {
	FirstRowID         *int64 // nil for a source from a pre-v3 snapshot
	DataSequenceNumber int64
}

func (l CompactionSourceRowLineage) validate(entry iceberg.ManifestEntry) error {
	return l.compare(entry.DataFile().FilePath(), manifestSourceRowLineage(entry))
}

func manifestSourceRowLineage(entry iceberg.ManifestEntry) CompactionSourceRowLineage {
	return CompactionSourceRowLineage{
		FirstRowID:         entry.DataFile().FirstRowID(),
		DataSequenceNumber: entry.DataSequenceNumber(),
	}
}

func (l CompactionSourceRowLineage) compare(path string, actual CompactionSourceRowLineage) error {
	id := actual.FirstRowID
	if id != l.FirstRowID && (id == nil || l.FirstRowID == nil || *id != *l.FirstRowID) {
		return fmt.Errorf("%w: first row ID of compaction source %q changed; replan from the target branch",
			ErrInvalidOperation, path)
	}
	if actual.DataSequenceNumber != l.DataSequenceNumber {
		return fmt.Errorf("%w: data sequence number of compaction source %q changed; replan from the target branch",
			ErrInvalidOperation, path)
	}

	return nil
}

func validateCompactionRowLineage(meta Metadata, branch string, tasks []FileScanTask) error {
	if len(tasks) == 0 || meta.FormatVersion() < 3 {
		return nil
	}

	// A branch can still point to a pre-v3 snapshot after another branch has
	// assigned row IDs. Standalone workers do not know the target branch.
	requireRowID := meta.NextRowID() > 0
	if branch != "" {
		parent := latestSnapshotForBranch(meta, branch)
		requireRowID = parent != nil && parent.FirstRowID != nil
	}
	requireRowID = requireRowID || slices.ContainsFunc(tasks, func(task FileScanTask) bool { return task.FirstRowID != nil })

	for _, task := range tasks {
		switch {
		case task.File == nil:
			return fmt.Errorf("%w: compaction task has no data file", ErrInvalidOperation)
		case requireRowID && (task.FirstRowID == nil || *task.FirstRowID < 0):
			return fmt.Errorf("%w: cannot preserve v3 row lineage for %q without a valid first row ID; replan from the target branch", ErrInvalidOperation, task.File.FilePath())
		case task.DataSequenceNumber == nil || *task.DataSequenceNumber < 0:
			return fmt.Errorf("%w: cannot preserve v3 row lineage for %q without a valid data sequence number", ErrInvalidOperation, task.File.FilePath())
		}
	}

	return nil
}

func validateCompactionPlanLineage(ctx context.Context, tbl *Table, branch string, groups []CompactionTaskGroup) error {
	if tbl.metadata.FormatVersion() < 3 {
		return nil
	}

	paths := make(map[string]struct{})
	for _, group := range groups {
		if err := validateCompactionRowLineage(tbl.metadata, branch, group.Tasks); err != nil {
			return err
		}
		for _, task := range group.Tasks {
			paths[task.File.FilePath()] = struct{}{}
		}
	}
	if len(paths) == 0 {
		return nil
	}

	fs, err := tbl.fsF(ctx)
	if err != nil {
		return err
	}
	sourceLineage, err := readCompactionSourceLineage(ctx, fs, latestSnapshotForBranch(tbl.metadata, branch), paths)
	if err != nil {
		return err
	}
	for _, group := range groups {
		for _, task := range group.Tasks {
			path := task.File.FilePath()
			actual, ok := sourceLineage[path]
			if !ok {
				return fmt.Errorf("%w: compaction source %q is missing from the target branch; replan", ErrInvalidOperation, path)
			}
			expected := CompactionSourceRowLineage{FirstRowID: task.FirstRowID, DataSequenceNumber: *task.DataSequenceNumber}
			if err := expected.compare(path, actual); err != nil {
				return err
			}
		}
	}

	return nil
}

func readCompactionSourceLineage(ctx context.Context, fs iceio.IO, parent *Snapshot, paths map[string]struct{}) (map[string]CompactionSourceRowLineage, error) {
	lineage := make(map[string]CompactionSourceRowLineage, len(paths))
	if parent == nil || len(paths) == 0 {
		return lineage, nil
	}

	for entry, err := range parent.entries(ctx, fs, iceberg.ManifestContentData, true) {
		if err != nil {
			return nil, err
		}
		path := entry.DataFile().FilePath()
		if _, ok := paths[path]; ok {
			lineage[path] = manifestSourceRowLineage(entry)
			if len(lineage) == len(paths) {
				break
			}
		}
	}

	return lineage, nil
}

// The first v3 commit gives surviving legacy files their row IDs. Fill in
// only those newly assigned values, keeping the original tasks and deletes.
func refreshCompactionRowIDs(group CompactionTaskGroup, lineage map[string]CompactionSourceRowLineage) (CompactionTaskGroup, error) {
	group.Tasks = slices.Clone(group.Tasks)
	for i := range group.Tasks {
		task := &group.Tasks[i]
		if task.FirstRowID != nil || task.File == nil || task.DataSequenceNumber == nil {
			continue
		}
		path := task.File.FilePath()
		actual, ok := lineage[path]
		if !ok || actual.FirstRowID == nil {
			return CompactionTaskGroup{}, fmt.Errorf("%w: compaction source %q has no newly assigned first row ID; replan", ErrInvalidOperation, path)
		}
		expected := CompactionSourceRowLineage{FirstRowID: actual.FirstRowID, DataSequenceNumber: *task.DataSequenceNumber}
		if err := expected.compare(path, actual); err != nil {
			return CompactionTaskGroup{}, err
		}
		task.FirstRowID = actual.FirstRowID
	}

	return group, nil
}

func (r *RewriteFiles) validateWorkerLineage() error {
	if !r.workerLineageUnverified && r.workerSourceWithoutLineage == "" {
		return nil
	}

	meta, err := r.txn.txnMeta()
	if err != nil {
		return err
	}
	if meta.formatVersion < 3 {
		return nil
	}

	if r.workerLineageUnverified {
		return fmt.Errorf("%w: worker result did not confirm v3 row lineage preservation; replan and rerun compaction with a v3 worker",
			ErrInvalidOperation)
	}

	return fmt.Errorf("%w: worker result for %q is missing source row lineage; replan from the target branch",
		ErrInvalidOperation, r.workerSourceWithoutLineage)
}
