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
	// Schema is the Iceberg schema the file's contents conform to.
	// Per-column statistics are matched by leaf-column path: each file
	// column's dotted path MUST equal the path derived from this schema's
	// field names. Embedded field-id metadata in the file (e.g. the Parquet
	// "PARQUET:field_id" key) is not consulted, so a column renamed relative
	// to the file will not resolve.
	Schema *iceberg.Schema

	// Spec is the partition spec the resulting DataFile is bound to —
	// typically tbl.Spec(). The zero value [iceberg.PartitionSpec]{} is
	// equivalent to *iceberg.UnpartitionedSpec and is accepted for
	// unpartitioned tables.
	Spec iceberg.PartitionSpec

	// Format identifies the on-disk file format and selects the internal
	// FileFormat implementation used to extract per-column statistics from
	// Metadata. It must match the concrete type of Metadata: the only supported
	// combination today is [iceberg.ParquetFile] with a *metadata.FileMetaData.
	Format iceberg.FileFormat

	// Metadata is the format-specific, in-memory file metadata object produced
	// by the external writer.
	//
	// Today only Format == [iceberg.ParquetFile] with *metadata.FileMetaData is supported;
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

	// PartitionValues maps spec field ID → partition value. Every spec field
	// must be present; leave nil/empty for unpartitioned tables, where any
	// supplied value is rejected.
	//
	// Values must already be the transform results in the expected Go types.
	// A nil value is treated as "not supplied" and is inferred from the file's
	// column statistics; void-transform fields therefore should be nil (the
	// void transform always yields null), though this is not enforced.
	//
	// This helper validates only that the supplied field IDs match the spec
	// (every field present, no stray IDs); it does not coerce or type-check
	// the values themselves. Callers are responsible for supplying values of
	// the correct type.
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
	// Set it for v3 data files; it is not required on v1/v2 tables, and
	// first_row_id does not apply to delete files.
	FirstRowID *int64

	// ReferencedDataFile, when non-nil, records the single data file that a
	// file-scoped position-delete file applies to (spec field referenced_data_file).
	//
	// It is only valid for position delete files. Leave it nil for data files, equality-delete files,
	// and partition-scoped position deletes.
	//
	// For partition-scoped position deletes whose file_path column references more
	// than one data file (cardinality > 1) — such a file has no single referenced data file.
	ReferencedDataFile *string
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
//
// Scope and limitations:
//   - Only Parquet is supported today (args.Format must be
//     [iceberg.ParquetFile] and args.Metadata a *metadata.FileMetaData).
//   - All three manifest-entry contents are supported end to end: data,
//     equality deletes, and positional deletes.
//   - Row lineage is supported: a data file's first_row_id can be set via
//     DataFileArgs.FirstRowID for format-version-3+ tables.
//   - Iceberg v3 deletion vectors (Puffin DVs) are not supported in this
//     helper function; it emits only Parquet position-delete files. A
//     file-scoped position delete may still record its target via
//     DataFileArgs.ReferencedDataFile; partition-scoped deletes leave it nil,
//     in which case engines may infer scoping from the file_path column bounds.
func DataFileFromMetadata(args DataFileArgs) (iceberg.DataFile, error) {
	if args.Schema == nil {
		return nil, errors.New("schema is required")
	}
	// Both nil checks are intentional:
	//   - args.Metadata == nil catches an unset interface, while
	//   - pqMeta == nil below catches a typed-nil (*metadata.FileMetaData)(nil)
	if args.Metadata == nil {
		return nil, errors.New("file metadata is required")
	}
	pqMeta, ok := args.Metadata.(*metadata.FileMetaData)
	if !ok {
		return nil, fmt.Errorf(
			"unsupported metadata type: expected *metadata.FileMetaData, got %T",
			args.Metadata)
	}
	if pqMeta == nil {
		return nil, errors.New("metadata pointer is nil")
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

	if args.Content != iceberg.EntryContentData && args.FirstRowID != nil {
		return nil, fmt.Errorf("FirstRowID must be nil for delete files, got a non-nil value for content %v", args.Content)
	}

	if args.ReferencedDataFile != nil {
		if args.Content != iceberg.EntryContentPosDeletes {
			return nil, fmt.Errorf("ReferencedDataFile may only be set for position-delete files, got content %v", args.Content)
		}
		if *args.ReferencedDataFile == "" {
			return nil, errors.New("ReferencedDataFile must be non-empty when set")
		}
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

	partitionValues := args.PartitionValues
	if partitionValues == nil {
		partitionValues = make(map[int]any)
	}

	var df iceberg.DataFile
	// DataFileStatsFromMeta and ToDataFile are the only calls wrapped in recover because they may panic on invalid input
	// (missing field IDs or builder validation failures),
	if err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				switch e := r.(type) {
				case error:
					err = fmt.Errorf("error encountered during extracting stats and building the data file: %w", e)
				default:
					err = fmt.Errorf("error encountered during extracting stats and building the data file: %v", e)
				}
			}
		}()

		stats := format.DataFileStatsFromMeta(
			args.Metadata,
			statsPlan,
			pathMapping,
			tblutils.VariantFieldIDsFromSchema(args.Schema),
		)
		if stats == nil {
			return errors.New("failed to build data file stats from metadata")
		}

		if len(args.EqualityFieldIDs) > 0 {
			stats.EqualityFieldIDs = args.EqualityFieldIDs
		}

		df = stats.ToDataFile(tblutils.DataFileOpts{
			Schema:             args.Schema,
			Spec:               args.Spec,
			Path:               args.FilePath,
			Format:             args.Format,
			Content:            args.Content,
			FileSize:           args.FileSize,
			PartitionValues:    partitionValues,
			SortOrderID:        args.SortOrderID,
			FirstRowID:         args.FirstRowID,
			ReferencedDataFile: args.ReferencedDataFile,
		})

		return nil
	}(); err != nil {
		return nil, err
	}

	return df, nil
}

func validateContentSchema(args DataFileArgs) error {
	switch args.Content {
	case iceberg.EntryContentData:
		// Parquet field ids are validated implicitly by DataFileStatsFromMeta
		// via format.PathToIDMapping(args.Schema), which rejects columns absent
		// from the schema's field-id mapping and reports
		// them as an error through the scoped recover.
	case iceberg.EntryContentEqDeletes:
		for _, id := range args.EqualityFieldIDs {
			f, ok := args.Schema.FindFieldByID(id)
			if !ok {
				return fmt.Errorf(
					"EqualityFieldIDs contains field id %d which is not present in the supplied schema", id)
			}
			if _, ok := f.Type.(iceberg.PrimitiveType); !ok {
				return fmt.Errorf(
					"EqualityFieldIDs field id %d (%s) must resolve to a primitive leaf, got %s", id, f.Name, f.Type)
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
		if len(values) > 0 {
			return fmt.Errorf("partition values supplied for an unpartitioned spec (id %d); expected none", spec.ID())
		}

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
