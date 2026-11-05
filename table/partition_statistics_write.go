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
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"slices"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
	"github.com/twmb/avro/ocf"
)

// PartitionStatisticsSchemaV1 is the v1 partition statistics file schema.
// It uses the unified partition type across all partition specs, not the
// current spec's partition type. Optional delete metrics are omitted in v1.
//
// https://iceberg.apache.org/spec/#partition-statistics-file
func PartitionStatisticsSchemaV1(partitionType *iceberg.StructType) *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "partition", Type: partitionType, Required: true},
		iceberg.NestedField{ID: 2, Name: "spec_id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 3, Name: "data_record_count", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 4, Name: "data_file_count", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 5, Name: "total_data_file_size_in_bytes", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 10, Name: "total_record_count", Type: iceberg.PrimitiveTypes.Int64, Required: false},
	)
}

// SetPartitionStatistics registers a previously written partition statistics
// file with a snapshot. Files must follow the Iceberg partition statistics
// file spec. This stages a metadata update; Commit publishes the reference.
// Re-registering a snapshot replaces its existing statistics file reference.
func (t *Transaction) SetPartitionStatistics(stats PartitionStatisticsFile) error {
	meta, err := t.txnMeta()
	if err != nil {
		return err
	}
	if stats.StatisticsPath == "" || stats.FileSizeInBytes <= 0 {
		return fmt.Errorf("%w: partition statistics path must be nonempty and file size must be positive",
			iceberg.ErrInvalidArgument)
	}
	if _, err := meta.SnapshotByID(stats.SnapshotID); err != nil {
		return fmt.Errorf("set partition statistics: %w", err)
	}

	return t.apply([]Update{NewSetPartitionStatisticsUpdate(stats)}, nil)
}

// RemovePartitionStatistics removes the partition statistics file reference
// associated with snapshotID. It does not delete the physical statistics file.
func (t *Transaction) RemovePartitionStatistics(snapshotID int64) error {
	if _, err := t.txnMeta(); err != nil {
		return err
	}

	return t.apply([]Update{NewRemovePartitionStatisticsUpdate(snapshotID)}, nil)
}

// ComputePartitionStatistics computes and writes a Parquet partition statistics
// file for the transaction's current v1 branch head, and stages its metadata
// reference. The caller must Commit to publish the reference. It does not
// rewrite any data files or create a new table snapshot.
//
// The statistics are derived from the live entries in the head's manifests,
// grouped by both partition spec ID and partition tuple. The file is sorted by
// the unified partition tuple (ascending, null first), then by spec ID for
// deterministic ties. Only tables with at least one partition field are
// currently supported.
// Partition fields whose source was dropped or whose result type changed are
// not yet supported. Embedded manifests may omit the v1 partition spec ID when the header
// fields identify exactly one historical spec.
//
// Use a fresh transaction: a staged schema, spec, or snapshot change would
// make the original head's partition type ambiguous. The branch head is pinned
// to prevent an OCC retry from publishing statistics for a different snapshot.
// The returned file may become orphaned if Commit fails; orphan-file cleanup
// can remove it once it is no longer needed.
func (t *Transaction) ComputePartitionStatistics(ctx context.Context) (*PartitionStatisticsFile, error) {
	meta, err := t.txnMeta()
	if err != nil {
		return nil, err
	}
	if t.tbl == nil || t.tbl.Metadata() == nil {
		return nil, fmt.Errorf("%w: partition statistics require a table", iceberg.ErrInvalidArgument)
	}
	if meta.formatVersion != 1 {
		return nil, fmt.Errorf("%w: partition statistics computation currently supports format v1 only",
			iceberg.ErrNotImplemented)
	}
	if meta.HasChanges() {
		return nil, fmt.Errorf("%w: compute partition statistics using a fresh transaction",
			iceberg.ErrInvalidArgument)
	}

	snapshot := t.planningSnapshot(meta)
	if snapshot == nil {
		return nil, fmt.Errorf("%w: cannot compute partition statistics without a snapshot",
			iceberg.ErrInvalidArgument)
	}
	if err := validatePartitionStatisticsSourceTypes(t.tbl.Metadata()); err != nil {
		return nil, err
	}
	partitionType, err := inspectPartitionType(t.tbl.Metadata())
	if err != nil {
		return nil, fmt.Errorf("partition statistics: %w", err)
	}
	if partitionType == nil || len(partitionType.FieldList) == 0 {
		return nil, fmt.Errorf("%w: partition statistics for unpartitioned tables",
			iceberg.ErrNotImplemented)
	}
	// This fences the snapshot whose manifests are about to be read. A
	// concurrent append/overwrite on this branch must not silently rebase the
	// stats registration to that newer head.
	if err := t.AssertRefSnapshotID(""); err != nil {
		return nil, err
	}
	if err := t.apply(nil, []Requirement{
		AssertCurrentSchemaID(t.tbl.Metadata().CurrentSchema().ID),
		AssertDefaultSpecID(t.tbl.Metadata().DefaultPartitionSpec()),
		AssertLastAssignedPartitionID(lastAssignedPartitionID(t.tbl.Metadata())),
	}); err != nil {
		return nil, err
	}

	fs, err := t.tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	wfs, err := requireWriteFileIO(fs)
	if err != nil {
		return nil, err
	}
	rows, err := collectPartitionStatistics(ctx, fs, *snapshot, partitionType, t.tbl.Metadata().PartitionSpecs())
	if err != nil {
		return nil, fmt.Errorf("partition statistics: %w", err)
	}
	if err := sortPartitionStatistics(rows, partitionType); err != nil {
		return nil, fmt.Errorf("partition statistics: %w", err)
	}

	locationProvider, err := t.stagedLocationProvider()
	if err != nil {
		return nil, err
	}
	path := locationProvider.NewMetadataLocation(uuid.NewString() + "-partition-stats.parquet")
	size, err := writePartitionStatisticsParquet(ctx, wfs, path, partitionType, rows)
	if err != nil {
		return nil, err
	}

	stats := PartitionStatisticsFile{
		SnapshotID:      snapshot.SnapshotID,
		StatisticsPath:  path,
		FileSizeInBytes: size,
	}
	if err := t.SetPartitionStatistics(stats); err != nil {
		_ = wfs.Remove(context.WithoutCancel(ctx), path)

		return nil, err
	}

	return &stats, nil
}

// validatePartitionStatisticsSourceTypes rejects historical partition fields
// whose source is missing or whose result type changed. The unified partition
// type would otherwise omit historical values or misrepresent their types.
func validatePartitionStatisticsSourceTypes(meta Metadata) error {
	current := meta.CurrentSchema()
	for _, spec := range meta.PartitionSpecs() {
		for _, field := range spec.Fields() {
			for _, id := range field.SourceIDs {
				typ, ok := current.FindTypeByID(id)
				if !ok {
					return fmt.Errorf("%w: partition source field %d was dropped", iceberg.ErrNotImplemented, id)
				}
				for _, schema := range meta.Schemas() {
					if old, exists := schema.FindTypeByID(id); exists &&
						!field.Transform.ResultType(typ).Equals(field.Transform.ResultType(old)) {
						return fmt.Errorf("%w: partition source field %d changed result type", iceberg.ErrNotImplemented, id)
					}
				}
			}
		}
	}

	return nil
}

// Embedded v1 paths lack manifest-list descriptors. Read and validate the
// header spec before constructing a descriptor with the real spec ID.
func partitionStatisticsEmbeddedManifest(ctx context.Context, fs iceio.IO, manifest iceberg.ManifestFile, specs map[int32]iceberg.PartitionSpec) (iceberg.ManifestFile, error) {
	f, err := fs.Open(ctx, manifest.FilePath())
	if err != nil {
		return nil, err
	}
	defer f.Close()
	ocfReader, err := ocf.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("read embedded manifest %q: %w", manifest.FilePath(), err)
	}
	header, parseErr := parseEmbeddedManifestSpecHeader(ocfReader.Metadata())
	if err := ocfReader.Close(); err != nil {
		return nil, err
	}
	if parseErr != nil {
		return nil, fmt.Errorf("%w: embedded manifest %q: %v",
			iceberg.ErrInvalidArgument, manifest.FilePath(), parseErr)
	}

	id, found, ambiguous := header.resolve(specs)
	if !found {
		if ambiguous {
			return nil, fmt.Errorf("%w: embedded manifest %q has an ambiguous partition spec without an ID",
				iceberg.ErrNotImplemented, manifest.FilePath())
		}

		return nil, fmt.Errorf("%w: embedded manifest %q has no matching partition spec",
			iceberg.ErrInvalidArgument, manifest.FilePath())
	}
	expected, ok := specs[id]
	if !ok {
		return nil, fmt.Errorf("%w: embedded manifest %q references unknown partition spec %d",
			iceberg.ErrInvalidArgument, manifest.FilePath(), id)
	}
	actual := iceberg.NewPartitionSpecID(int(id), header.fields...)
	if !actual.Equals(expected) {
		return nil, fmt.Errorf("%w: embedded manifest %q partition spec differs from table metadata",
			iceberg.ErrInvalidArgument, manifest.FilePath())
	}
	// Entries will open and validate the descriptor through ManifestFile.Entries.
	// Constructing and closing another reader here only repeats that work.
	return iceberg.NewManifestBuilder(1, manifest.FilePath(), manifest.Length(), id, manifest.SnapshotID()).
		AddedFiles(-1).ExistingFiles(-1).DeletedFiles(-1).AddedRows(-1).ExistingRows(-1).DeletedRows(-1).Build(), nil
}

// collectPartitionStatistics reads only manifest metadata, not Parquet data.
// Each partition spec gets a separate lookup tree: partitions that have the
// same tuple under two spec IDs are distinct statistics rows.
func collectPartitionStatistics(
	ctx context.Context,
	fs iceio.IO,
	snapshot Snapshot,
	partitionType *iceberg.StructType,
	specs []iceberg.PartitionSpec,
) ([]*inspectPartitionAggregate, error) {
	specByID := make(map[int32]iceberg.PartitionSpec, len(specs))
	for _, spec := range specs {
		specByID[int32(spec.ID())] = spec
	}
	manifests, err := snapshot.Manifests(fs)
	if err != nil {
		return nil, err
	}

	trees := make(map[int32]*inspectPartitionAggregateTree)
	seenPaths := make(map[string]string)
	rows := make([]*inspectPartitionAggregate, 0)
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if snapshot.ManifestList == "" && snapshot.ManifestLocations != nil {
			manifest, err = partitionStatisticsEmbeddedManifest(ctx, fs, manifest, specByID)
			if err != nil {
				return nil, err
			}
		}
		specID := manifest.PartitionSpecID()
		spec, ok := specByID[specID]
		if !ok {
			return nil, fmt.Errorf("%w: manifest %q references unknown partition spec %d", iceberg.ErrInvalidArgument, manifest.FilePath(), specID)
		}
		tree := trees[specID]
		if tree == nil {
			tree = newInspectPartitionAggregateTree()
			trees[specID] = tree
		}

		for entry, err := range manifest.Entries(fs, true) {
			if err != nil {
				return nil, fmt.Errorf("read manifest %s: %w", manifest.FilePath(), err)
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}

			if entry == nil || entry.DataFile() == nil {
				return nil, fmt.Errorf("%w: manifest %q contains a nil data file",
					iceberg.ErrInvalidArgument, manifest.FilePath())
			}
			file := entry.DataFile()
			path := file.FilePath()
			if path == "" {
				return nil, fmt.Errorf("%w: manifest %q contains an empty data file path",
					iceberg.ErrInvalidArgument, manifest.FilePath())
			}
			if previous, duplicate := seenPaths[path]; duplicate {
				return nil, fmt.Errorf("%w: duplicate live data file %q in manifests %q and %q",
					iceberg.ErrInvalidArgument, path, previous, manifest.FilePath())
			}
			seenPaths[path] = manifest.FilePath()
			if file.ContentType() != iceberg.EntryContentData {
				return nil, fmt.Errorf("%w: v1 manifest %s contains a delete file",
					iceberg.ErrInvalidArgument, manifest.FilePath())
			}
			if file.Count() < 0 || file.FileSizeBytes() < 0 {
				return nil, fmt.Errorf("%w: file %s has a negative count or size",
					iceberg.ErrInvalidArgument, file.FilePath())
			}
			values := dataFilePartition(file)
			if err := validatePartitionStatisticsTuple(spec, file.FilePath(), values); err != nil {
				return nil, err
			}
			partition := inspectCoercePartition(values, partitionType)
			for _, field := range partitionType.FieldList {
				value := partition[field.ID]
				// Manifest decoding uses DecimalLiteral, while callers can
				// supply Decimal. Normalize before building aggregate keys
				// so both representations count toward the same partition.
				if literal, ok := value.(iceberg.DecimalLiteral); ok {
					value = iceberg.Decimal(literal)
					partition[field.ID] = value
				}
				if decimal, ok := value.(iceberg.Decimal); ok {
					if typ, ok := field.Type.(iceberg.DecimalType); ok && decimal.Scale != typ.Scale() {
						return nil, fmt.Errorf("%w: partition field %q has decimal scale %d, expected %d",
							iceberg.ErrInvalidArgument, field.Name, decimal.Scale, typ.Scale())
					}
				}
				if _, err := comparePartitionStatisticValues(value, value); err != nil {
					return nil, fmt.Errorf("partition field %q: %w", field.Name, err)
				}
			}
			row := tree.lookupPartition(partition, partitionType)
			if row == nil {
				row = &inspectPartitionAggregate{
					specID:    specID,
					partition: cloneInspectPartition(partition),
				}
				rows = append(rows, row)
				tree.insert(newPartitionRecord(partition, partitionType), row)
			}
			if row.dataFileCount == math.MaxInt32 ||
				row.dataRecordCount > math.MaxInt64-file.Count() ||
				row.dataFileSize > math.MaxInt64-file.FileSizeBytes() {
				return nil, fmt.Errorf("%w: partition statistics count overflow",
					iceberg.ErrInvalidArgument)
			}
			row.dataFileCount++
			row.dataRecordCount += file.Count()
			row.dataFileSize += file.FileSizeBytes()
		}
	}

	return rows, nil
}

func validatePartitionStatisticsTuple(spec iceberg.PartitionSpec, path string, values map[int]any) error {
	for _, field := range spec.Fields() {
		if _, exists := values[field.FieldID]; !exists {
			return fmt.Errorf("%w: file %q is missing partition field %d", iceberg.ErrInvalidArgument, path, field.FieldID)
		}
	}

	return nil
}

// sortPartitionStatistics obeys the spec's NULL FIRST tuple ordering, not
// inspectPartitionKey's text ordering (where "10" sorts before "2").
// A spec ID only breaks equal tuple ties; it is not the primary sort key.
func sortPartitionStatistics(rows []*inspectPartitionAggregate, partitionType *iceberg.StructType) error {
	// Collection already validates each partition value before using it as
	// an aggregate key. Do not repeat that validation for every output row.
	var sortErr error
	slices.SortStableFunc(rows, func(a, b *inspectPartitionAggregate) int {
		for _, field := range partitionType.FieldList {
			comp, err := comparePartitionStatisticValues(a.partition[field.ID], b.partition[field.ID])
			if err != nil {
				sortErr = fmt.Errorf("partition field %q: %w", field.Name, err)

				return 0
			}
			if comp != 0 {
				return comp
			}
		}
		switch {
		case a.specID < b.specID:
			return -1
		case a.specID > b.specID:
			return 1
		default:
			return 0
		}
	})

	return sortErr
}

// comparePartitionStatisticValues compares primitive partition values by
// their underlying types, including named date/time integer types and binary
// UUIDs. Unsupported types are rejected instead of emitting a file whose
// partition ordering violates the Iceberg spec.
func comparePartitionStatisticValues(a, b any) (int, error) {
	if a == nil || b == nil {
		if a == nil && b == nil {
			return 0, nil
		}
		if a == nil {
			return -1, nil
		}

		return 1, nil
	}
	// Decimal partitions sort by their numeric value, not by the underlying
	// Decimal struct or the lexicographic encoding of the unscaled integer.
	if left, ok := a.(iceberg.Decimal); ok {
		right, valid := b.(iceberg.Decimal)
		if !valid || left.Scale != right.Scale {
			return 0, fmt.Errorf("%w: incompatible decimal partition values %v and %v",
				iceberg.ErrInvalidArgument, a, b)
		}

		return left.Val.Cmp(right.Val), nil
	}
	// Java's UUID.compareTo orders each 64-bit half as a signed integer.
	// Binary and fixed partitions retain unsigned lexicographic ordering.
	if left, ok := a.(uuid.UUID); ok {
		right, valid := b.(uuid.UUID)
		if !valid {
			return 0, fmt.Errorf("%w: incompatible partition values %T and %T",
				iceberg.ErrInvalidArgument, a, b)
		}
		if cmp := compareOrderedStats(int64(binary.BigEndian.Uint64(left[:8])),
			int64(binary.BigEndian.Uint64(right[:8]))); cmp != 0 {
			return cmp, nil
		}

		return compareOrderedStats(int64(binary.BigEndian.Uint64(left[8:])),
			int64(binary.BigEndian.Uint64(right[8:]))), nil
	}
	if _, ok := b.(uuid.UUID); ok {
		return 0, fmt.Errorf("%w: incompatible partition values %T and %T",
			iceberg.ErrInvalidArgument, a, b)
	}
	left, right := reflect.ValueOf(a), reflect.ValueOf(b)
	if left.Kind() != right.Kind() {
		return 0, fmt.Errorf("%w: incompatible partition values %T and %T",
			iceberg.ErrInvalidArgument, a, b)
	}

	switch left.Kind() {
	case reflect.Bool:
		if left.Bool() == right.Bool() {
			return 0, nil
		}
		if !left.Bool() {
			return -1, nil
		}

		return 1, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return compareOrderedStats(left.Int(), right.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return compareOrderedStats(left.Uint(), right.Uint()), nil
	case reflect.Float32, reflect.Float64:
		// Match Java Float.compare/Double.compare: all NaNs compare equal
		// and sort after +Inf; -0 sorts before +0.
		aFloat, bFloat := left.Float(), right.Float()
		switch {
		case math.IsNaN(aFloat):
			if math.IsNaN(bFloat) {
				return 0, nil
			}

			return 1, nil
		case math.IsNaN(bFloat):
			return -1, nil
		case aFloat == 0 && bFloat == 0:
			switch {
			case math.Signbit(aFloat) == math.Signbit(bFloat):
				return 0, nil
			case math.Signbit(aFloat):
				return -1, nil
			default:
				return 1, nil
			}
		default:
			return compareOrderedStats(aFloat, bFloat), nil
		}
	case reflect.String:
		return bytes.Compare([]byte(left.String()), []byte(right.String())), nil
	case reflect.Slice, reflect.Array:
		if left.Type().Elem().Kind() != reflect.Uint8 || right.Type().Elem().Kind() != reflect.Uint8 {
			break
		}
		limit := min(left.Len(), right.Len())
		for index := range limit {
			aByte, bByte := left.Index(index).Uint(), right.Index(index).Uint()
			if aByte != bByte {
				return compareOrderedStats(aByte, bByte), nil
			}
		}
		switch {
		case left.Len() < right.Len():
			return -1, nil
		case left.Len() > right.Len():
			return 1, nil
		default:
			return 0, nil
		}
	}

	return 0, fmt.Errorf("%w: cannot order partition values of type %T",
		iceberg.ErrNotImplemented, a)
}

func compareOrderedStats[T interface{ ~int64 | ~uint64 | ~float64 }](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

type partitionStatisticsByteCounter struct {
	writer iceio.FileWriter
	size   int64
}

func (w *partitionStatisticsByteCounter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	w.size += int64(n)

	return n, err
}

// writePartitionStatisticsParquet streams bounded Arrow batches to the table
// metadata directory and removes incomplete files on failure.
func writePartitionStatisticsParquet(
	ctx context.Context,
	fs iceio.WriteFileIO,
	path string,
	partitionType *iceberg.StructType,
	rows []*inspectPartitionAggregate,
) (_ int64, err error) {
	sc, err := SchemaToArrowSchema(PartitionStatisticsSchemaV1(partitionType), nil, true, false)
	if err != nil {
		return 0, err
	}
	fw, err := fs.Create(ctx, path)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			_ = fs.Remove(context.WithoutCancel(ctx), path)
		}
	}()

	counter := &partitionStatisticsByteCounter{writer: fw}
	writer, err := pqarrow.NewFileWriter(sc, counter, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		_ = fw.Close()

		return 0, err
	}
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer bldr.Release()

	part, err := newInspectPartitionBuilder(bldr.Field(0).(*array.StructBuilder), partitionType)
	if err != nil {
		_ = writer.Close()
		_ = fw.Close()

		return 0, err
	}
	for start := 0; start < len(rows); start += inspectRecordBatchSize {
		if err := ctx.Err(); err != nil {
			_ = writer.Close()
			_ = fw.Close()

			return 0, err
		}
		end := min(start+inspectRecordBatchSize, len(rows))
		for _, row := range rows[start:end] {
			if err := part.append(row.partition); err != nil {
				_ = writer.Close()
				_ = fw.Close()

				return 0, err
			}
			bldr.Field(1).(*array.Int32Builder).Append(row.specID)
			bldr.Field(2).(*array.Int64Builder).Append(row.dataRecordCount)
			bldr.Field(3).(*array.Int32Builder).Append(row.dataFileCount)
			bldr.Field(4).(*array.Int64Builder).Append(row.dataFileSize)
			// v1 has no delete files, so the exact live record count equals
			// the sum of data-file record counts.
			bldr.Field(5).(*array.Int64Builder).Append(row.dataRecordCount)
		}

		rec := bldr.NewRecordBatch()
		writeErr := writer.Write(rec)
		rec.Release()
		if writeErr != nil {
			_ = writer.Close()
			_ = fw.Close()

			return 0, writeErr
		}
	}
	if err := writer.Close(); err != nil {
		_ = fw.Close()

		return 0, err
	}
	if err := fw.Close(); err != nil {
		return 0, err
	}

	return counter.size, nil
}
