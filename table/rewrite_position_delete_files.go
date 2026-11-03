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
	"slices"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/iceberg-go"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
)

// PositionDeleteRewriteOptions controls position delete compaction.
// Zero values use a minimum of two files and a maximum of 128 MiB of input
// file sizes per group. Decoded rows are held in memory for sorting.
type PositionDeleteRewriteOptions struct {
	MinInputFiles int
	MaxInputBytes int64
}

type positionDeleteRewriteKey struct {
	partition string
	sequence  int64
}

type positionDeleteRewriteGroup struct {
	files     []iceberg.DataFile
	partition map[int]any
	bytes     int64
}

// A job owns a file subset that fits the input budget and minimum count.
type positionDeleteRewriteJob struct {
	key   positionDeleteRewriteKey
	group *positionDeleteRewriteGroup
}

type positionDeleteLiveFile struct {
	partition string
	sequence  int64
}

type positionDeleteRow struct {
	path     string
	position int64
}

// RewritePositionDeleteFiles compacts compatible classic Parquet position
// delete files without rewriting data files. It also removes records targeting
// data files that are no longer live in the planning snapshot.
//
// Only v2 files in the current partition spec and without the optional "row"
// payload are selected. Groups never mix partition tuples or data sequence
// numbers, so the replacement does not change delete applicability. Other
// files are left untouched. Physical source files remain available to older
// snapshots after the commit.
func (t Table) RewritePositionDeleteFiles(
	ctx context.Context, opts PositionDeleteRewriteOptions,
) (*Table, int, error) {
	if t.metadata.FormatVersion() != 2 {
		return nil, 0, fmt.Errorf("%w: position delete rewrites require v2 table format", iceberg.ErrNotImplemented)
	}
	if opts.MinInputFiles <= 0 {
		opts.MinInputFiles = 2
	}
	if opts.MaxInputBytes <= 0 {
		opts.MaxInputBytes = 128 << 20
	}
	if snap := t.CurrentSnapshot(); snap == nil {
		return &t, 0, nil
	}
	spec := t.Spec()

	fs, err := t.FS(ctx)
	if err != nil {
		return nil, 0, err
	}
	manifests, err := t.CurrentSnapshot().Manifests(fs)
	if err != nil {
		return nil, 0, err
	}

	live := make(map[string]positionDeleteLiveFile)
	// Build a snapshot-scoped live-file index. This is independent of
	// removeDanglingDeleteFiles; the two features can merge in either order.
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		if manifest.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		for entry, err := range manifest.Entries(fs, true) {
			if err != nil {
				return nil, 0, err
			}
			df := entry.DataFile()
			if df.ContentType() != iceberg.EntryContentData {
				continue
			}
			partition, err := canonicalPartitionKey(df.SpecID(), dataFilePartition(df))
			if err != nil {
				return nil, 0, fmt.Errorf("data file %s: %w", df.FilePath(), err)
			}
			if entry.DataSequenceNumber() < 0 {
				return nil, 0, fmt.Errorf("%w: data file %s has unknown sequence number", ErrInvalidMetadata, df.FilePath())
			}
			live[df.FilePath()] = positionDeleteLiveFile{partition: partition, sequence: entry.DataSequenceNumber()}
		}
	}

	groups := make(map[positionDeleteRewriteKey]*positionDeleteRewriteGroup)
	seen := make(map[string]struct{})
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		if manifest.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		for entry, err := range manifest.Entries(fs, true) {
			if err != nil {
				return nil, 0, err
			}
			df := entry.DataFile()
			if df.ContentType() != iceberg.EntryContentPosDeletes ||
				df.FileFormat() != iceberg.ParquetFile ||
				IsDeletionVector(df) || int(df.SpecID()) != spec.ID() ||
				entry.DataSequenceNumber() < 0 {
				continue
			}
			if _, ok := seen[df.FilePath()]; ok {
				continue
			}
			seen[df.FilePath()] = struct{}{}
			partition, err := canonicalPartitionKey(df.SpecID(), dataFilePartition(df))
			if err != nil {
				return nil, 0, fmt.Errorf("position delete %s: %w", df.FilePath(), err)
			}
			key := positionDeleteRewriteKey{partition: partition, sequence: entry.DataSequenceNumber()}
			group := groups[key]
			if group == nil {
				group = &positionDeleteRewriteGroup{partition: df.Partition()}
				groups[key] = group
			}
			group.files = append(group.files, df)
			group.bytes += df.FileSizeBytes()
		}
	}

	// Avoid opening a writer or requiring write-capable I/O when no group
	// qualifies. Split before writing so the selected work is deterministic.
	var jobs []positionDeleteRewriteJob
	for key, group := range groups {
		for _, chunk := range splitPositionDeleteRewriteGroup(group, opts.MaxInputBytes) {
			if len(chunk.files) < opts.MinInputFiles || chunk.bytes > opts.MaxInputBytes {
				continue
			}
			jobs = append(jobs, positionDeleteRewriteJob{key: key, group: chunk})
		}
	}
	if len(jobs) == 0 {
		return &t, 0, nil
	}
	slices.SortFunc(jobs, func(a, b positionDeleteRewriteJob) int {
		if order := strings.Compare(a.key.partition, b.key.partition); order != 0 {
			return order
		}
		if a.key.sequence < b.key.sequence {
			return -1
		}
		if a.key.sequence > b.key.sequence {
			return 1
		}

		return strings.Compare(a.group.files[0].FilePath(), b.group.files[0].FilePath())
	})

	wfs, err := requireWriteFileIO(fs)
	if err != nil {
		return nil, 0, err
	}

	txn := t.NewTransaction()
	if err := txn.AssertRefSnapshotID(""); err != nil {
		return nil, 0, err
	}
	rewrite := txn.NewRewrite(nil)
	props := maps.Clone(t.Properties())
	if props == nil {
		props = make(iceberg.Properties)
	}
	props[WriteFormatDefaultKey] = string(iceberg.ParquetFile)
	writer, err := newPositionDeleteWriter(t.Location(), wfs, txn.meta, props)
	if err != nil {
		return nil, 0, err
	}

	var produced []string
	commitStarted := false
	defer func() {
		if !commitStarted {
			for _, path := range produced {
				_ = wfs.Remove(context.WithoutCancel(ctx), path)
			}
		}
	}()

	var removed int
	for _, job := range jobs {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		key, group := job.key, job.group
		supported := true
		for _, df := range group.files {
			src, err := tblutils.GetFile(ctx, fs, df, true)
			if err != nil {
				return nil, 0, err
			}
			rdr, err := src.GetReader(ctx)
			if err != nil {
				return nil, 0, err
			}
			schema, schemaErr := rdr.Schema()
			closeErr := rdr.Close()
			if schemaErr != nil {
				return nil, 0, schemaErr
			}
			if closeErr != nil {
				return nil, 0, closeErr
			}
			// The two-column writer cannot preserve optional deleted rows.
			if schema.NumFields() != 2 ||
				len(schema.FieldIndices("file_path")) != 1 ||
				len(schema.FieldIndices("pos")) != 1 {
				supported = false

				break
			}
		}
		if !supported {
			continue
		}

		var rows []positionDeleteRow
		for _, df := range group.files {
			byPath, err := readDeletesForPaths(ctx, fs, df, nil)
			if err != nil {
				releasePosDeletes(byPath)

				return nil, 0, fmt.Errorf("read position deletes from %s: %w", df.FilePath(), err)
			}
			for path, chunk := range byPath {
				if !rewritePositionDeletePathInScope(df, path) {
					continue
				}
				data, ok := live[path]
				if !ok || data.partition != key.partition || data.sequence > key.sequence {
					continue
				}
				for _, part := range chunk.Chunks() {
					positions, ok := part.(*array.Int64)
					if !ok {
						releasePosDeletes(byPath)

						return nil, 0, fmt.Errorf("%w: non-int64 position column in %s", iceberg.ErrInvalidSchema, df.FilePath())
					}
					if err := validateRewritePositionDeletePositions(df.FilePath(), positions); err != nil {
						releasePosDeletes(byPath)

						return nil, 0, err
					}
					for _, pos := range positions.Int64Values() {
						rows = append(rows, positionDeleteRow{path: path, position: pos})
					}
				}
			}
			releasePosDeletes(byPath)
		}

		rows = sortUniquePositionDeleteRows(rows)
		if len(rows) > 0 {
			b := array.NewRecordBuilder(compute.GetAllocator(ctx), PositionalDeleteArrowSchema)
			pathBuilder := b.Field(0).(*array.StringBuilder)
			posBuilder := b.Field(1).(*array.Int64Builder)
			for _, row := range rows {
				pathBuilder.Append(row.path)
				posBuilder.Append(row.position)
			}
			record := b.NewRecordBatch()
			b.Release()
			task := WriteTask{
				Uuid: uuid.New(), PartitionID: spec.ID(),
				FileCount: 1, Schema: iceberg.PositionalDeleteSchema,
				Batches: []arrow.RecordBatch{record},
			}
			// Track the intended output before writing. A failed writer
			// may have already created a partial file.
			produced = append(produced, writer.loc.NewDataLocation(
				task.GenerateDataFileName(writer.invariants.extension)))
			newFile, err := writer.writeFile(ctx, group.partition, task)
			if err != nil {
				return nil, 0, err
			}
			rewrite.AddDeleteFileWithDataSequenceNumber(newFile, key.sequence)
		}
		for _, df := range group.files {
			rewrite.DeleteFile(df)
			removed++
		}
	}
	if removed == 0 {
		return &t, 0, nil
	}
	if err := rewrite.Commit(ctx); err != nil {
		return nil, 0, err
	}
	// A failed catalog response may represent an unknown commit state.
	// Do not delete newly written objects after an attempted commit.
	commitStarted = true
	updated, err := txn.Commit(ctx)
	if err != nil {
		return nil, 0, err
	}

	return updated, removed, nil
}

func splitPositionDeleteRewriteGroup(group *positionDeleteRewriteGroup, limit int64) []*positionDeleteRewriteGroup {
	slices.SortFunc(group.files, func(a, b iceberg.DataFile) int { return strings.Compare(a.FilePath(), b.FilePath()) })
	var groups []*positionDeleteRewriteGroup
	var current *positionDeleteRewriteGroup
	for _, file := range group.files {
		size := file.FileSizeBytes()
		if size < 0 || size > limit {
			continue
		}
		if current == nil || size > limit-current.bytes {
			current = &positionDeleteRewriteGroup{partition: group.partition}
			groups = append(groups, current)
		}
		current.files = append(current.files, file)
		current.bytes += size
	}

	return groups
}

func sortUniquePositionDeleteRows(rows []positionDeleteRow) []positionDeleteRow {
	slices.SortFunc(rows, func(a, b positionDeleteRow) int {
		if a.path < b.path {
			return -1
		}
		if a.path > b.path {
			return 1
		}
		if a.position < b.position {
			return -1
		}
		if a.position > b.position {
			return 1
		}

		return 0
	})

	return slices.Compact(rows)
}

// A delete file's statistics can help planning, but do not establish an
// explicit one-file scope for dropping rows during maintenance.
func rewritePositionDeletePathInScope(df iceberg.DataFile, path string) bool {
	target := explicitReferencedDataFile(df)

	return target == "" || target == path
}

// Reject invalid positions before a rewrite could convert a null to zero or
// carry a negative index into a replacement delete file.
func validateRewritePositionDeletePositions(path string, positions *array.Int64) error {
	if positions.NullN() != 0 {
		return fmt.Errorf("%w: null position in delete file %s", iceberg.ErrInvalidSchema, path)
	}
	for _, pos := range positions.Int64Values() {
		if pos < 0 {
			return fmt.Errorf("%w: negative position %d in delete file %s", iceberg.ErrInvalidSchema, pos, path)
		}
	}

	return nil
}
