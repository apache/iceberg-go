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
	"errors"
	"fmt"

	"github.com/apache/iceberg-go"
	tblutils "github.com/apache/iceberg-go/table/internal"
)

// DataFileArgs collects everything needed to compute a fully populated
// [iceberg.DataFile] (data, equality-delete, or positional-delete) from
// a file's in-memory, format-specific metadata object without performing
// any filesystem reads.
type DataFileArgs struct {
	// Schema is the Iceberg schema the file's contents conform to. The
	// file's columns MUST carry per-leaf field-ID metadata whose values
	// match this schema's field IDs (for Parquet this is the
	// "PARQUET:field_id" key on each leaf).
	Schema *iceberg.Schema

	// Spec is the partition spec the resulting DataFile is bound to —
	// typically tbl.Spec(). For an unpartitioned table pass
	// *iceberg.UnpartitionedSpec.
	Spec iceberg.PartitionSpec

	// Format identifies the on-disk file format of the file being
	// described and selects the internal FileFormat implementation used
	// to extract per-column statistics from Metadata.
	Format iceberg.FileFormat

	// Metadata is the format-specific, in-memory file metadata object
	// produced by the external writer (for example
	// *parquet/metadata.FileMetaData when Format is
	// [iceberg.ParquetFile]).
	Metadata any

	// FilePath is the fully qualified location the file bytes were
	// written to (the same path that will be recorded in the manifest entry).
	FilePath string

	// FileSize is the byte length of the object at FilePath. Must be
	// greater than zero.
	FileSize int64

	// Content selects which manifest-entry kind to produce — one of
	// [iceberg.EntryContentData], [iceberg.EntryContentEqDeletes] or
	// [iceberg.EntryContentPosDeletes].
	Content iceberg.ManifestEntryContent

	// PartitionValues maps spec field ID → partition value. Required when Spec
	// is partitioned; leave nil/empty for unpartitioned tables.
	PartitionValues map[int]any

	// SortOrderID is the sort-order ID recorded on the manifest entry,
	// typically tbl.SortOrder().OrderID(). Zero means "unsorted".
	SortOrderID int

	// Properties are consulted only for write-side metrics-mode keys
	// (write.metadata.metrics.default and per-column
	// write.metadata.metrics.column.<name> overrides) that decide
	// which columns get full vs truncated vs no statistics. Pass
	// tbl.Properties() for table-default behavior; nil is safe.
	Properties iceberg.Properties

	// EqualityFieldIDs lists the schema field IDs that an equality
	// delete file matches on. Required when
	// Content == [iceberg.EntryContentEqDeletes] and MUST be empty
	// for every other Content value.
	EqualityFieldIDs []int
}

// DataFileFromMetadata builds a fully populated [iceberg.DataFile] from a
// file's in-memory, format-specific metadata object, extracting per-column
// statistics (column_sizes, value_counts, null_value_counts, lower_bounds,
// upper_bounds), split offsets, and the manifest-entry shape required for
// the requested content type. The format-specific extraction is dispatched
// through the internal FileFormat interface; no filesystem reads are
// performed.
//
// The returned DataFile is intended to be handed to
// [Transaction.AddDataFiles] (data files) or [Transaction.NewRowDelta]
// (delete files); it is NOT committed automatically. The file bytes
// described by args.Metadata must already have been uploaded to
// args.FilePath.
func DataFileFromMetadata(args DataFileArgs) (iceberg.DataFile, error) {
	if args.Schema == nil {
		return nil, errors.New("schema is required")
	}
	if args.Metadata == nil {
		return nil, errors.New("file metadata is required")
	}
	if args.FilePath == "" {
		return nil, errors.New("file path is required")
	}
	if args.FileSize <= 0 {
		return nil, fmt.Errorf("file size must be greater than 0, got %d", args.FileSize)
	}

	switch args.Content {
	case iceberg.EntryContentData,
		iceberg.EntryContentEqDeletes,
		iceberg.EntryContentPosDeletes:
	default:
		return nil, fmt.Errorf("invalid content: %v", args.Content)
	}

	if args.Content == iceberg.EntryContentEqDeletes && len(args.EqualityFieldIDs) == 0 {
		return nil, errors.New("equalityFieldIDs is required for equality-delete files")
	}
	if args.Content != iceberg.EntryContentEqDeletes && len(args.EqualityFieldIDs) > 0 {
		return nil, fmt.Errorf("equalityFieldIDs must be empty for content %v", args.Content)
	}

	format := tblutils.GetFileFormat(args.Format)
	if format == nil {
		return nil, fmt.Errorf("unsupported file format: %s", args.Format)
	}

	statsPlan, err := computeStatsPlan(args.Schema, args.Properties)
	if err != nil {
		return nil, fmt.Errorf("failed to build stats plan: %w", err)
	}

	pathMapping, err := format.PathToIDMapping(args.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to build path mapping: %w", err)
	}

	stats := format.DataFileStatsFromMeta(
		args.Metadata,
		statsPlan,
		pathMapping,
		tblutils.VariantFieldIDsFromSchema(args.Schema),
	)
	if stats == nil {
		return nil, errors.New("failed to build data file stats from metadata")
	}

	if len(args.EqualityFieldIDs) > 0 {
		stats.EqualityFieldIDs = append(stats.EqualityFieldIDs, args.EqualityFieldIDs...)
	}

	partitionValues := args.PartitionValues
	if partitionValues == nil {
		partitionValues = make(map[int]any)
	}

	df := stats.ToDataFile(tblutils.DataFileOpts{
		Schema:          args.Schema,
		Spec:            args.Spec,
		Path:            args.FilePath,
		Format:          args.Format,
		Content:         args.Content,
		FileSize:        args.FileSize,
		PartitionValues: partitionValues,
		SortOrderID:     args.SortOrderID,
	})

	return df, nil
}
