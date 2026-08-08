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
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
)

// InspectTable exposes a table's metadata (snapshots, history, manifests, and
// so on) as queryable Arrow tables. It mirrors the metadata tables offered by
// the Java, PyIceberg, and Rust clients.
//
// Obtain one via Table.Inspect. Each method returns an array.RecordReader that
// the caller is responsible for releasing.
type InspectTable struct {
	tbl   Table
	alloc memory.Allocator
}

// Inspect returns an InspectTable for reading this table's metadata tables,
// allocating Arrow buffers from opts, if provided, otherwise the default
// allocator.
func (t Table) Inspect(opts ...InspectOption) InspectTable {
	i := InspectTable{tbl: t, alloc: memory.DefaultAllocator}
	for _, opt := range opts {
		opt(&i)
	}

	return i
}

// InspectOption configures an InspectTable.
type InspectOption func(*InspectTable)

// WithInspectAllocator sets the Arrow memory allocator used to build
// metadata-table records. Tests can pass a memory.CheckedAllocator to detect
// leaks; callers with memory accounting can inject their own pool.
func WithInspectAllocator(alloc memory.Allocator) InspectOption {
	return func(i *InspectTable) {
		if alloc != nil {
			i.alloc = alloc
		}
	}
}

// History returns the chronological log of every snapshot that was ever the
// table's current snapshot, one row per snapshot-log entry. Rolled-back
// snapshots remain visible but are flagged via is_current_ancestor.
//
// Columns:
//   - made_current_at (timestamptz, required): when the snapshot became current
//   - snapshot_id (long, required): the snapshot that became current
//   - parent_id (long, optional): the snapshot's parent, null when the snapshot
//     has no parent or has since been expired
//   - is_current_ancestor (boolean, required): whether the snapshot is an
//     ancestor of the current snapshot; false for rolled-back snapshots
//
// is_current_ancestor is derived by walking the current snapshot's parent
// chain. If an intermediate ancestor has been expired (removed from the
// snapshot list), that walk is truncated and snapshots below the gap are
// reported as non-ancestors. Well-formed tables never hit this: ExpireSnapshots
// keeps the current snapshot and its full ancestry intact. This matches the
// silent-truncation behavior of the Java, PyIceberg, and Rust clients.
//
// The returned reader holds a single record batch. The caller must Release it.
func (i InspectTable) History(ctx context.Context) (array.RecordReader, error) {
	arrowSchema, err := SchemaToArrowSchema(HistorySchema(), nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("inspect history: build arrow schema: %w", err)
	}

	// Walk the current snapshot's parent chain once. Membership in this set
	// is what tells the live lineage apart from rolled-back history entries.
	// AncestorsOf already guards against cycles in malformed metadata, so a
	// corrupt parent chain cannot hang the scan.
	ancestors := make(map[int64]struct{})
	if current := i.tbl.metadata.CurrentSnapshot(); current != nil {
		for _, snap := range AncestorsOf(current.SnapshotID, i.tbl.metadata.SnapshotByID) {
			ancestors[snap.SnapshotID] = struct{}{}
		}
	}

	bldr := array.NewRecordBuilder(i.alloc, arrowSchema)
	defer bldr.Release()

	// Field positions and concrete builder types follow HistorySchema; the
	// assertions are safe as long as the two stay in sync.
	madeCurrentAt := bldr.Field(0).(*array.TimestampBuilder)
	snapshotID := bldr.Field(1).(*array.Int64Builder)
	parentID := bldr.Field(2).(*array.Int64Builder)
	isCurrentAncestor := bldr.Field(3).(*array.BooleanBuilder)

	for entry := range i.tbl.metadata.SnapshotLogs() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Iceberg stores timestamps as epoch milliseconds; Arrow uses
		// microseconds for the timestamptz type.
		madeCurrentAt.Append(arrow.Timestamp(entry.TimestampMs * 1000))
		snapshotID.Append(entry.SnapshotID)

		// parent_id resolves through the live snapshot table, so an entry
		// referencing an expired snapshot renders a null parent.
		if snap := i.tbl.metadata.SnapshotByID(entry.SnapshotID); snap != nil && snap.ParentSnapshotID != nil {
			parentID.Append(*snap.ParentSnapshotID)
		} else {
			parentID.AppendNull()
		}

		_, ok := ancestors[entry.SnapshotID]
		isCurrentAncestor.Append(ok)
	}

	rr, err := singleBatchReader(arrowSchema, bldr)
	if err != nil {
		return nil, fmt.Errorf("inspect history: %w", err)
	}

	return rr, nil
}

// singleBatchReader finishes bldr into one record batch and wraps it in a
// RecordReader. NewRecordReader retains the batch, so we release our own
// reference unconditionally: on success the reader owns it, and on error
// NewRecordReader has already released its own retain. This avoids a deferred
// double-release.
func singleBatchReader(arrowSchema *arrow.Schema, bldr *array.RecordBuilder) (array.RecordReader, error) {
	rec := bldr.NewRecordBatch()
	rr, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{rec})
	rec.Release()
	if err != nil {
		return nil, fmt.Errorf("new record reader: %w", err)
	}

	return rr, nil
}

// Snapshots returns one row per snapshot known to the table, in the order they
// are stored in metadata.
//
// Columns:
//   - committed_at (timestamptz, required): when the snapshot was committed
//   - snapshot_id (long, required): the snapshot id
//   - parent_id (long, optional): the parent snapshot id, null for a root
//   - operation (string, optional): the snapshot summary operation, null when
//     the snapshot carries no summary
//   - manifest_list (string, optional): path to the snapshot's manifest list
//   - summary (map<string,string>, optional): the stored snapshot summary,
//     including the "operation" key alongside its additional properties; null
//     when the snapshot carries no summary
//
// The returned reader holds a single record batch. The caller must Release it.
func (i InspectTable) Snapshots(ctx context.Context) (array.RecordReader, error) {
	arrowSchema, err := SchemaToArrowSchema(SnapshotsSchema(), nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("inspect snapshots: build arrow schema: %w", err)
	}

	bldr := array.NewRecordBuilder(i.alloc, arrowSchema)
	defer bldr.Release()

	// Field positions and concrete builder types follow SnapshotsSchema; the
	// assertions are safe as long as the two stay in sync.
	committedAt := bldr.Field(0).(*array.TimestampBuilder)
	snapshotID := bldr.Field(1).(*array.Int64Builder)
	parentID := bldr.Field(2).(*array.Int64Builder)
	operation := bldr.Field(3).(*array.StringBuilder)
	manifestList := bldr.Field(4).(*array.StringBuilder)
	summary := bldr.Field(5).(*array.MapBuilder)
	summaryKeys := summary.KeyBuilder().(*array.StringBuilder)
	summaryValues := summary.ItemBuilder().(*array.StringBuilder)

	for _, snap := range i.tbl.metadata.Snapshots() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Iceberg stores timestamps as epoch milliseconds; Arrow uses
		// microseconds for the timestamptz type.
		committedAt.Append(arrow.Timestamp(snap.TimestampMs * 1000))
		snapshotID.Append(snap.SnapshotID)

		if snap.ParentSnapshotID != nil {
			parentID.Append(*snap.ParentSnapshotID)
		} else {
			parentID.AppendNull()
		}

		// operation and summary come from the snapshot summary, which is
		// optional: a snapshot without one renders both as null.
		if snap.Summary != nil {
			operation.Append(string(snap.Summary.Operation))
			summary.Append(true)
			// The summary map mirrors the stored/serialized summary, which
			// folds the operation back in under the "operation" key (see
			// Summary.MarshalJSON). Emit it alongside the extra properties so
			// the table faithfully reflects the on-disk summary.
			if snap.Summary.Operation != "" {
				summaryKeys.Append(operationKey)
				summaryValues.Append(string(snap.Summary.Operation))
			}
			for k, v := range snap.Summary.Properties {
				summaryKeys.Append(k)
				summaryValues.Append(v)
			}
		} else {
			operation.AppendNull()
			summary.AppendNull()
		}

		// manifest_list is optional: a snapshot without a manifest-list path
		// (e.g. some V1 snapshots) renders null rather than an empty string.
		if snap.ManifestList != "" {
			manifestList.Append(snap.ManifestList)
		} else {
			manifestList.AppendNull()
		}
	}

	rr, err := singleBatchReader(arrowSchema, bldr)
	if err != nil {
		return nil, fmt.Errorf("inspect snapshots: %w", err)
	}

	return rr, nil
}

// Manifests returns one row for each manifest in the current snapshot.
// Partition summaries are exposed as the human-readable values used by the
// other Iceberg clients.
func (i InspectTable) Manifests(ctx context.Context) (array.RecordReader, error) {
	schema := ManifestsSchema()
	arrowSchema, err := SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("inspect manifests: build arrow schema: %w", err)
	}

	manifests, err := i.currentSnapshotManifests(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect manifests: %w", err)
	}

	bldr := array.NewRecordBuilder(i.alloc, arrowSchema)
	defer bldr.Release()

	content := bldr.Field(0).(*array.Int32Builder)
	path := bldr.Field(1).(*array.StringBuilder)
	length := bldr.Field(2).(*array.Int64Builder)
	partitionSpecID := bldr.Field(3).(*array.Int32Builder)
	addedSnapshotID := bldr.Field(4).(*array.Int64Builder)
	addedDataFiles := bldr.Field(5).(*array.Int32Builder)
	existingDataFiles := bldr.Field(6).(*array.Int32Builder)
	deletedDataFiles := bldr.Field(7).(*array.Int32Builder)
	addedDeleteFiles := bldr.Field(8).(*array.Int32Builder)
	existingDeleteFiles := bldr.Field(9).(*array.Int32Builder)
	deletedDeleteFiles := bldr.Field(10).(*array.Int32Builder)
	partitionSummaries := bldr.Field(11).(*array.ListBuilder)
	summaryStruct := partitionSummaries.ValueBuilder().(*array.StructBuilder)
	summaryContainsNull := summaryStruct.FieldBuilder(0).(*array.BooleanBuilder)
	summaryContainsNaN := summaryStruct.FieldBuilder(1).(*array.BooleanBuilder)
	summaryLower := summaryStruct.FieldBuilder(2).(*array.StringBuilder)
	summaryUpper := summaryStruct.FieldBuilder(3).(*array.StringBuilder)

	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		manifestContent := manifest.ManifestContent()
		switch manifestContent {
		case iceberg.ManifestContentData, iceberg.ManifestContentDeletes:
		default:
			return nil, fmt.Errorf("manifest %s has unknown content %d", manifest.FilePath(), manifestContent)
		}

		content.Append(int32(manifestContent))
		path.Append(manifest.FilePath())
		length.Append(manifest.Length())
		partitionSpecID.Append(manifest.PartitionSpecID())
		addedSnapshotID.Append(manifest.SnapshotID())

		switch manifestContent {
		case iceberg.ManifestContentData:
			appendManifestCount(addedDataFiles, manifest.AddedDataFiles())
			appendManifestCount(existingDataFiles, manifest.ExistingDataFiles())
			appendManifestCount(deletedDataFiles, manifest.DeletedDataFiles())
			addedDeleteFiles.Append(0)
			existingDeleteFiles.Append(0)
			deletedDeleteFiles.Append(0)
		case iceberg.ManifestContentDeletes:
			addedDataFiles.Append(0)
			existingDataFiles.Append(0)
			deletedDataFiles.Append(0)
			appendManifestCount(addedDeleteFiles, manifest.AddedDataFiles())
			appendManifestCount(existingDeleteFiles, manifest.ExistingDataFiles())
			appendManifestCount(deletedDeleteFiles, manifest.DeletedDataFiles())
		}

		spec := i.tbl.metadata.PartitionSpecByID(int(manifest.PartitionSpecID()))
		if spec == nil {
			return nil, fmt.Errorf("manifest %s references missing partition spec %d",
				manifest.FilePath(), manifest.PartitionSpecID())
		}
		partType := spec.PartitionType(i.tbl.metadata.CurrentSchema())
		partitions := manifest.Partitions()
		if partitions == nil {
			partitionSummaries.AppendNull()

			continue
		}
		if len(partitions) > spec.NumFields() {
			return nil, fmt.Errorf("manifest %s has %d partition summaries for partition spec %d with %d fields",
				manifest.FilePath(), len(partitions), manifest.PartitionSpecID(), spec.NumFields())
		}

		partitionSummaries.Append(true)
		for idx, summary := range partitions {
			summaryStruct.Append(true)
			summaryContainsNull.Append(summary.ContainsNull)
			if summary.ContainsNaN == nil {
				summaryContainsNaN.AppendNull()
			} else {
				summaryContainsNaN.Append(*summary.ContainsNaN)
			}

			fieldType := partType.FieldList[idx].Type
			transform := spec.Field(idx).Transform
			if err := appendManifestBound(summaryLower, fieldType, transform, summary.LowerBound); err != nil {
				return nil, fmt.Errorf("manifest %s partition field %d lower bound: %w", manifest.FilePath(), idx, err)
			}
			if err := appendManifestBound(summaryUpper, fieldType, transform, summary.UpperBound); err != nil {
				return nil, fmt.Errorf("manifest %s partition field %d upper bound: %w", manifest.FilePath(), idx, err)
			}
		}
	}

	rr, err := singleBatchReader(arrowSchema, bldr)
	if err != nil {
		return nil, fmt.Errorf("inspect manifests: %w", err)
	}

	return rr, nil
}

func appendManifestCount(builder *array.Int32Builder, count int32) {
	if count < 0 {
		builder.AppendNull()

		return
	}

	builder.Append(count)
}

func (i InspectTable) currentSnapshotManifests(ctx context.Context) ([]iceberg.ManifestFile, error) {
	snapshot := i.tbl.metadata.CurrentSnapshot()
	if snapshot == nil {
		return nil, nil
	}
	if i.tbl.fsF == nil {
		return nil, errors.New("table file IO is not configured")
	}

	fs, err := i.tbl.fsF(ctx)
	if err != nil {
		return nil, err
	}

	return snapshot.Manifests(fs)
}

func appendManifestBound(builder *array.StringBuilder, typ iceberg.Type, transform iceberg.Transform, bound *[]byte) error {
	if bound == nil {
		builder.AppendNull()

		return nil
	}

	literal, err := iceberg.LiteralFromBytes(typ, *bound)
	if err != nil {
		return err
	}
	builder.Append(transform.ToHumanStrType(typ, literal.Any()))

	return nil
}

// HistorySchema returns the Iceberg schema of the history metadata table. The
// field IDs are fixed by the Iceberg metadata-tables spec and match the Java,
// PyIceberg, and Rust clients for cross-client parity. A fresh schema value is
// returned on each call; callers must not mutate it or rely on pointer identity.
func HistorySchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "made_current_at", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
		iceberg.NestedField{ID: 2, Name: "snapshot_id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 3, Name: "parent_id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 4, Name: "is_current_ancestor", Type: iceberg.PrimitiveTypes.Bool, Required: true},
	)
}

// SnapshotsSchema returns the Iceberg schema of the snapshots metadata table.
// The field IDs are fixed by the Iceberg metadata-tables spec and match the
// Java, PyIceberg, and Rust clients for cross-client parity. A fresh schema
// value is returned on each call; callers must not mutate it or rely on pointer
// identity.
//
// The summary map's values are optional, matching PyIceberg and Rust. Java
// declares them required; the choice is not observable in practice because
// snapshot-summary values are always non-null strings.
func SnapshotsSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "committed_at", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
		iceberg.NestedField{ID: 2, Name: "snapshot_id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 3, Name: "parent_id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 4, Name: "operation", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 5, Name: "manifest_list", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 6, Name: "summary", Required: false, Type: &iceberg.MapType{
			KeyID:         7,
			KeyType:       iceberg.PrimitiveTypes.String,
			ValueID:       8,
			ValueType:     iceberg.PrimitiveTypes.String,
			ValueRequired: false,
		}},
	)
}

// ManifestsSchema returns the Iceberg schema of the manifests metadata table.
func ManifestsSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 14, Name: "content", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 1, Name: "path", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 2, Name: "length", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 3, Name: "partition_spec_id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 4, Name: "added_snapshot_id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 5, Name: "added_data_files_count", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 6, Name: "existing_data_files_count", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 7, Name: "deleted_data_files_count", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 15, Name: "added_delete_files_count", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 16, Name: "existing_delete_files_count", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 17, Name: "deleted_delete_files_count", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 8, Name: "partition_summaries", Required: true, Type: &iceberg.ListType{
			ElementID:       9,
			ElementRequired: true,
			Element: &iceberg.StructType{FieldList: []iceberg.NestedField{
				{ID: 10, Name: "contains_null", Type: iceberg.PrimitiveTypes.Bool, Required: true},
				{ID: 11, Name: "contains_nan", Type: iceberg.PrimitiveTypes.Bool, Required: false},
				{ID: 12, Name: "lower_bound", Type: iceberg.PrimitiveTypes.String, Required: false},
				{ID: 13, Name: "upper_bound", Type: iceberg.PrimitiveTypes.String, Required: false},
			}},
		}},
	)
}
