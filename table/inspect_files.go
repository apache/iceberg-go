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
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// DataFiles returns the live data files in the current snapshot. Deleted
// manifest entries are omitted, matching the data_files metadata table.
func (i InspectTable) DataFiles(ctx context.Context) (array.RecordReader, error) {
	return i.inspectFiles(ctx, "data files", DataFilesSchema, false,
		func(manifest iceberg.ManifestFile) bool {
			return manifest.ManifestContent() == iceberg.ManifestContentData
		})
}

// Files returns all live data and delete files in the current snapshot.
// Deleted manifest entries are omitted.
func (i InspectTable) Files(ctx context.Context) (array.RecordReader, error) {
	return i.inspectFiles(ctx, "files", FilesSchema, false, nil)
}

// AllFiles returns the live data and delete files reachable from every
// snapshot currently tracked by the table. Shared manifests are scanned once,
// while duplicate file rows from different manifests are preserved.
func (i InspectTable) AllFiles(ctx context.Context) (array.RecordReader, error) {
	return i.inspectFiles(ctx, "all files", AllFilesSchema, true, nil)
}

// AllDataFiles returns the live data files reachable from every snapshot
// currently tracked by the table.
func (i InspectTable) AllDataFiles(ctx context.Context) (array.RecordReader, error) {
	return i.inspectFiles(ctx, "all data files", AllDataFilesSchema, true,
		func(manifest iceberg.ManifestFile) bool {
			return manifest.ManifestContent() == iceberg.ManifestContentData
		})
}

// AllDeleteFiles returns the live delete files reachable from every snapshot
// currently tracked by the table.
func (i InspectTable) AllDeleteFiles(ctx context.Context) (array.RecordReader, error) {
	return i.inspectFiles(ctx, "all delete files", AllDeleteFilesSchema, true,
		func(manifest iceberg.ManifestFile) bool {
			return manifest.ManifestContent() == iceberg.ManifestContentDeletes
		})
}

type inspectFilesSchema func(*iceberg.StructType) *iceberg.Schema

func (i InspectTable) inspectFiles(
	ctx context.Context,
	name string,
	schemaFn inspectFilesSchema,
	allSnapshots bool,
	includeManifest func(iceberg.ManifestFile) bool,
) (array.RecordReader, error) {
	partitionType, err := unifiedPartitionType(i.tbl.Metadata())
	if err != nil {
		return nil, fmt.Errorf("inspect %s: %w", name, err)
	}
	arrowSchema, err := SchemaToArrowSchema(schemaFn(partitionType), nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("inspect %s: build arrow schema: %w", name, err)
	}

	appendFile := newInspectContentFileAppender(partitionType)
	appendEntry := func(bldr *array.RecordBuilder, entry iceberg.ManifestEntry) error {
		return appendFile(bldr, entry.DataFile())
	}
	var rr array.RecordReader
	if allSnapshots {
		rr, err = i.allManifestEntryReader(ctx, arrowSchema, true, includeManifest, appendEntry)
	} else {
		rr, err = i.manifestEntryReader(ctx, arrowSchema, true, includeManifest, appendEntry)
	}
	if err != nil {
		return nil, fmt.Errorf("inspect %s: %w", name, err)
	}

	return rr, nil
}

// manifestEntryReader streams manifest entries into bounded Arrow record
// batches. It keeps only the current batch and manifest decoder state in
// memory instead of materializing every entry before returning a reader.
func (i InspectTable) manifestEntryReader(
	ctx context.Context,
	arrowSchema *arrow.Schema,
	discardDeleted bool,
	includeManifest func(iceberg.ManifestFile) bool,
	appendEntry func(*array.RecordBuilder, iceberg.ManifestEntry) error,
) (array.RecordReader, error) {
	snapshot := i.tbl.Metadata().CurrentSnapshot()
	if snapshot == nil {
		return array.ReaderFromIter(arrowSchema, emptyInspectRecordBatch(i.alloc, arrowSchema)), nil
	}

	provider := i.tbl.ManifestProvider()
	manifests, err := provider.Manifests(ctx, *snapshot)
	if err != nil {
		return nil, err
	}
	fs, err := provider.FS(ctx)
	if err != nil {
		return nil, err
	}

	return i.manifestEntryReaderFromManifestSource(
		ctx, arrowSchema, fs, discardDeleted, includeManifest, appendEntry,
		func(yield func(iceberg.ManifestFile, error) bool) {
			for manifest := range manifests {
				if !yield(manifest, nil) {
					return
				}
			}
		}), nil
}

// allManifestEntryReader scans each manifest reachable from the table's
// tracked snapshots exactly once. Manifest paths are immutable and unique, so
// the path is the stable identity used by Java and PyIceberg's all_* tables.
func (i InspectTable) allManifestEntryReader(
	ctx context.Context,
	arrowSchema *arrow.Schema,
	discardDeleted bool,
	includeManifest func(iceberg.ManifestFile) bool,
	appendEntry func(*array.RecordBuilder, iceberg.ManifestEntry) error,
) (array.RecordReader, error) {
	snapshots := i.tbl.Metadata().Snapshots()
	if len(snapshots) == 0 {
		return array.ReaderFromIter(arrowSchema, emptyInspectRecordBatch(i.alloc, arrowSchema)), nil
	}

	provider := i.tbl.ManifestProvider()
	fs, err := provider.FS(ctx)
	if err != nil {
		return nil, err
	}

	return i.manifestEntryReaderFromManifestSource(
		ctx, arrowSchema, fs, discardDeleted, includeManifest, appendEntry,
		provider.uniqueManifests(ctx, snapshots)), nil
}

func (i InspectTable) manifestEntryReaderFromManifestSource(
	ctx context.Context,
	arrowSchema *arrow.Schema,
	fs iceio.IO,
	discardDeleted bool,
	includeManifest func(iceberg.ManifestFile) bool,
	appendEntry func(*array.RecordBuilder, iceberg.ManifestEntry) error,
	source iter.Seq2[iceberg.ManifestFile, error],
) array.RecordReader {
	return array.ReaderFromIter(arrowSchema, func(yield func(arrow.RecordBatch, error) bool) {
		bldr := array.NewRecordBuilder(i.alloc, arrowSchema)
		defer bldr.Release()

		rows := 0
		emitted := false
		emit := func() bool {
			if rows == 0 {
				return true
			}

			batch := bldr.NewRecordBatch()
			rows = 0
			emitted = true

			return yield(batch, nil)
		}
		emitEmpty := func() {
			batch := bldr.NewRecordBatch()
			emitted = true
			_ = yield(batch, nil)
		}
		yieldError := func(err error) {
			_ = yield(nil, err)
		}

		for manifest, sourceErr := range source {
			if sourceErr != nil {
				yieldError(sourceErr)

				return
			}
			if err := ctx.Err(); err != nil {
				yieldError(err)

				return
			}
			if includeManifest != nil && !includeManifest(manifest) {
				continue
			}

			for entry, err := range manifest.Entries(ctx, fs, discardDeleted) {
				if err != nil {
					yieldError(fmt.Errorf("read manifest %s: %w", manifest.FilePath(), err))

					return
				}
				if err := ctx.Err(); err != nil {
					yieldError(err)

					return
				}
				if err := appendEntry(bldr, entry); err != nil {
					yieldError(fmt.Errorf("append manifest entry from %s: %w", manifest.FilePath(), err))

					return
				}
				rows++
				if rows == recordBatchSize && !emit() {
					return
				}
			}
		}

		if rows > 0 {
			_ = emit()
		} else if !emitted {
			emitEmpty()
		}
	})
}

func emptyInspectRecordBatch(alloc memory.Allocator, schema *arrow.Schema) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		bldr := array.NewRecordBuilder(alloc, schema)
		defer bldr.Release()

		batch := bldr.NewRecordBatch()
		_ = yield(batch, nil)
	}
}
