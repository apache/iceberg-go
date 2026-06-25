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

	"github.com/apache/arrow-go/v18/parquet/metadata"
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
	// typically tbl.Spec(). The zero value [iceberg.PartitionSpec]{} is
	// equivalent to *iceberg.UnpartitionedSpec and is accepted for
	// unpartitioned tables.
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
	// for every other Content value. Every listed ID must be present in Schema.
	EqualityFieldIDs []int

	// FirstRowID is the _row_id assigned to the first row of the data file.
	// Required when the target table uses format version 3 or later and
	// Content == [iceberg.EntryContentData]; ignored for v1/v2 tables
	// or delete files.
	FirstRowID *int64
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
func DataFileFromMetadata(args DataFileArgs) (df iceberg.DataFile, err error) {
	defer func() {
		if r := recover(); r != nil {
			df = nil
			switch e := r.(type) {
			case error:
				err = fmt.Errorf("error while building an iceberg datafile: %w", e)
			default:
				err = fmt.Errorf("error while building an iceberg datafile: %v", e)
			}
		}
	}()

	if args.Schema == nil {
		return nil, errors.New("schema is required")
	}
	if args.Metadata == nil {
		return nil, errors.New("file metadata is required")
	}
	if _, ok := args.Metadata.(*metadata.FileMetaData); !ok {
		return nil, fmt.Errorf(
			"unsupported metadata type: expected *metadata.FileMetaData, got %T",
			args.Metadata)
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
		return nil, errors.New("EqualityFieldIDs is required for equality-delete files")
	}
	if args.Content != iceberg.EntryContentEqDeletes && len(args.EqualityFieldIDs) > 0 {
		return nil, fmt.Errorf("EqualityFieldIDs must be empty for content %v", args.Content)
	}

	if args.Content == iceberg.EntryContentPosDeletes && args.SortOrderID != UnsortedSortOrderID {
		return nil, fmt.Errorf("position delete file claims sort order id %d; the spec requires unsorted order id (%d)",
			args.SortOrderID, UnsortedSortOrderID)
	}

	if err := validateContentSchema(args); err != nil {
		return nil, err
	}

	if err := validatePartitionValues(args.Spec, args.PartitionValues); err != nil {
		return nil, err
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
		stats.EqualityFieldIDs = args.EqualityFieldIDs
	}

	partitionValues := args.PartitionValues
	if partitionValues == nil {
		partitionValues = make(map[int]any)
	}

	df = stats.ToDataFile(tblutils.DataFileOpts{
		Schema:          args.Schema,
		Spec:            args.Spec,
		Path:            args.FilePath,
		Format:          args.Format,
		Content:         args.Content,
		FileSize:        args.FileSize,
		PartitionValues: partitionValues,
		SortOrderID:     args.SortOrderID,
		FirstRowID:      args.FirstRowID,
	})

	return df, nil
}

func validateContentSchema(args DataFileArgs) error {
	switch args.Content {
	case iceberg.EntryContentEqDeletes:
		for _, id := range args.EqualityFieldIDs {
			if _, ok := args.Schema.FindFieldByID(id); !ok {
				return fmt.Errorf(
					"EqualityFieldIDs contains field id %d which is not present in the supplied schema", id)
			}
		}
	case iceberg.EntryContentPosDeletes:
		for _, want := range iceberg.PositionalDeleteSchema.Fields() {
			got, ok := args.Schema.FindFieldByID(want.ID)
			if !ok {
				return fmt.Errorf(
					"positional delete schema requires reserved field id %d (%s); schema is missing it", want.ID, want.Name)
			}
			if !got.Type.Equals(want.Type) {
				return fmt.Errorf(
					"positional delete schema field id %d (%s) must have type %s, got %s", want.ID, want.Name, want.Type, got.Type)
			}
		}
	}

	return nil
}

func validatePartitionValues(spec iceberg.PartitionSpec, values map[int]any) error {
	if spec.Equals(*iceberg.UnpartitionedSpec) {
		return nil
	}

	expected := make(map[int]string, spec.NumFields())
	for _, field := range spec.Fields() {
		expected[field.FieldID] = field.Name
		if _, ok := values[field.FieldID]; !ok {
			return fmt.Errorf("missing partition value for field id %d (%s) in spec id %d",
				field.FieldID, field.Name, spec.ID())
		}
	}

	for id := range values {
		if _, ok := expected[id]; !ok {
			return fmt.Errorf("partition value supplied for field id %d which is not part of the spec id %d",
				id, spec.ID())
		}
	}

	return nil
}
