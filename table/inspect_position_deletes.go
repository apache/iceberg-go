// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/dv"
	tblutils "github.com/apache/iceberg-go/table/internal"
)

const (
	positionDeleteFilePathID       = math.MaxInt32 - 101
	positionDeletePosID            = math.MaxInt32 - 102
	positionDeleteRowID            = math.MaxInt32 - 103
	positionDeletePartitionID      = math.MaxInt32 - 5
	positionDeleteSpecID           = math.MaxInt32 - 4
	positionDeletePhysicalPathID   = math.MaxInt32 - 1
	positionDeleteContentOffsetID  = math.MaxInt32 - 6
	positionDeleteContentSizeID    = math.MaxInt32 - 7
	positionDeletePhysicalPathName = "delete_file_path"
)

// PositionDeletes returns the individual position-delete records referenced
// by the current snapshot. Parquet position-delete files and V3 deletion
// vectors are exposed through the same schema.
func (i InspectTable) PositionDeletes(ctx context.Context) (array.RecordReader, error) {
	partitionType, partitionIDs, err := positionDeletesPartitionType(i.tbl.metadata)
	if err != nil {
		return nil, fmt.Errorf("inspect position deletes: %w", err)
	}
	schema := PositionDeletesSchema(i.tbl.metadata.CurrentSchema(), partitionType, i.tbl.metadata.Version())
	arrowSchema, err := SchemaToArrowSchema(schema, nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("inspect position deletes: build arrow schema: %w", err)
	}

	fs, files, err := i.currentPositionDeleteFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect position deletes: %w", err)
	}
	ctx = compute.WithAllocator(ctx, i.alloc)

	return i.positionDeleteRecordReader(
		ctx, arrowSchema, fs, files, partitionType, partitionIDs, i.tbl.metadata.Version()), nil
}

func (i InspectTable) currentPositionDeleteFiles(
	ctx context.Context,
) (iceio.IO, []iceberg.DataFile, error) {
	snapshot := i.tbl.metadata.CurrentSnapshot()
	if snapshot == nil {
		return nil, nil, nil
	}
	if i.tbl.fsF == nil {
		return nil, nil, errors.New("table file IO is not configured")
	}
	fs, err := i.tbl.fsF(ctx)
	if err != nil {
		return nil, nil, err
	}
	manifests, err := snapshot.Manifests(fs)
	if err != nil {
		return nil, nil, err
	}

	files := make([]iceberg.DataFile, 0)
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if manifest.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		for entry, err := range manifest.Entries(fs, true) {
			if err != nil {
				return nil, nil, fmt.Errorf("read manifest %s: %w", manifest.FilePath(), err)
			}
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			if entry.DataFile().ContentType() == iceberg.EntryContentPosDeletes {
				files = append(files, entry.DataFile())
			}
		}
	}

	return fs, files, nil
}

type positionDeleteRecordAppender struct {
	filePath         *array.StringBuilder
	pos              *array.Int64Builder
	row              *array.StructBuilder
	partition        *inspectPartitionBuilder
	specID           *array.Int32Builder
	deleteFilePath   *array.StringBuilder
	contentOffset    *array.Int64Builder
	contentSize      *array.Int64Builder
	partitionType    *iceberg.StructType
	partitionIDByOld map[int]int
	formatVersion    int
}

func newPositionDeleteRecordAppender(
	bldr *array.RecordBuilder,
	partitionType *iceberg.StructType,
	partitionIDByOld map[int]int,
	formatVersion int,
) (positionDeleteRecordAppender, error) {
	nextField := 3
	out := positionDeleteRecordAppender{
		filePath:         bldr.Field(0).(*array.StringBuilder),
		pos:              bldr.Field(1).(*array.Int64Builder),
		row:              bldr.Field(2).(*array.StructBuilder),
		partitionType:    partitionType,
		partitionIDByOld: partitionIDByOld,
		formatVersion:    formatVersion,
	}
	if len(partitionType.FieldList) > 0 {
		partitionBuilder, err := newInspectPartitionBuilder(
			bldr.Field(nextField).(*array.StructBuilder), partitionType)
		if err != nil {
			return out, err
		}
		out.partition = partitionBuilder
		nextField++
	}
	out.specID = bldr.Field(nextField).(*array.Int32Builder)
	out.deleteFilePath = bldr.Field(nextField + 1).(*array.StringBuilder)
	if formatVersion >= 3 {
		out.contentOffset = bldr.Field(nextField + 2).(*array.Int64Builder)
		out.contentSize = bldr.Field(nextField + 3).(*array.Int64Builder)
	}

	return out, nil
}

func (a positionDeleteRecordAppender) append(
	file iceberg.DataFile,
	dataFilePath string,
	pos int64,
	deletedRow scalar.Scalar,
) error {
	a.filePath.Append(dataFilePath)
	a.pos.Append(pos)
	if err := appendProjectedPositionDeleteRow(a.row, deletedRow); err != nil {
		return fmt.Errorf("append deleted row: %w", err)
	}

	if a.partition != nil {
		partition := make(map[int]any, len(file.Partition()))
		for oldID, value := range file.Partition() {
			if newID, ok := a.partitionIDByOld[oldID]; ok {
				partition[newID] = value
			}
		}
		if err := a.partition.append(partition); err != nil {
			return err
		}
	}
	a.specID.Append(file.SpecID())
	a.deleteFilePath.Append(file.FilePath())
	if a.formatVersion >= 3 {
		appendInspectOptionalInt64(a.contentOffset, file.ContentOffset())
		appendInspectOptionalInt64(a.contentSize, file.ContentSizeInBytes())
	}

	return nil
}

// appendPositionDeleteRow projects a row from a position-delete file onto the
// current table schema. Position-delete row structs may contain only a subset
// of the table fields, so matching by Arrow field position or exact type is
// not sufficient. Field IDs are authoritative when the source schema carries
// them; names are used only for readers that do not preserve Parquet metadata.
func appendProjectedPositionDeleteRow(builder *array.StructBuilder, deletedRow scalar.Scalar) error {
	if deletedRow == nil || !deletedRow.IsValid() {
		builder.AppendNull()

		return nil
	}

	row, ok := deletedRow.(*scalar.Struct)
	if !ok {
		return fmt.Errorf("%w: row has type %s, want struct", iceberg.ErrInvalidSchema, deletedRow.DataType())
	}

	return appendPositionDeleteStruct(builder, row)
}

func appendPositionDeleteStruct(builder *array.StructBuilder, source *scalar.Struct) error {
	sourceType, ok := source.DataType().(*arrow.StructType)
	if !ok {
		return fmt.Errorf("%w: row has type %s, want struct", iceberg.ErrInvalidSchema, source.DataType())
	}
	if len(source.Value) != sourceType.NumFields() {
		return fmt.Errorf("%w: row has %d values for %d fields",
			iceberg.ErrInvalidSchema, len(source.Value), sourceType.NumFields())
	}

	sourceFields, err := newPositionDeleteFieldLookup(sourceType)
	if err != nil {
		return err
	}
	destinationType, ok := builder.Type().(*arrow.StructType)
	if !ok {
		return fmt.Errorf("%w: destination row has type %s, want struct", iceberg.ErrInvalidSchema, builder.Type())
	}

	if !source.IsValid() {
		builder.AppendNull()

		return nil
	}
	builder.Append(true)

	for index, destinationField := range destinationType.Fields() {
		sourceIndex, found := sourceFields.index(destinationField)
		if !found {
			builder.FieldBuilder(index).AppendNull()

			continue
		}

		value := source.Value[sourceIndex]
		if value == nil || !value.IsValid() {
			builder.FieldBuilder(index).AppendNull()

			continue
		}

		fieldBuilder := builder.FieldBuilder(index)
		if err := appendPositionDeleteValue(fieldBuilder, value); err != nil {
			return fmt.Errorf("field %q: %w", destinationField.Name, err)
		}
	}

	return nil
}

func appendPositionDeleteValue(builder array.Builder, value scalar.Scalar) error {
	if value == nil || !value.IsValid() {
		builder.AppendNull()

		return nil
	}

	switch builder := builder.(type) {
	case *array.StructBuilder:
		source, ok := value.(*scalar.Struct)
		if !ok {
			return fmt.Errorf("%w: value has type %s, want struct",
				iceberg.ErrInvalidSchema, value.DataType())
		}

		return appendPositionDeleteStruct(builder, source)
	case *array.MapBuilder:
		source, ok := value.(*scalar.Map)
		if !ok {
			return fmt.Errorf("%w: value has type %s, want map",
				iceberg.ErrInvalidSchema, value.DataType())
		}

		return appendPositionDeleteMap(builder, source)
	case array.ListLikeBuilder:
		source, ok := value.(scalar.ListScalar)
		if !ok {
			return fmt.Errorf("%w: value has type %s, want list",
				iceberg.ErrInvalidSchema, value.DataType())
		}

		return appendPositionDeleteList(builder, source)
	}

	if arrow.TypeEqual(builder.Type(), value.DataType()) {
		return scalar.Append(builder, value)
	}
	if !canPromotePositionDeleteValue(value.DataType(), builder.Type()) {
		return scalar.Append(builder, value)
	}

	appendBuilder := builder
	castType := builder.Type()
	if storageBuilder, ok := builder.(positionDeleteStorageBuilder); ok {
		appendBuilder = storageBuilder.StorageBuilder()
		castType = appendBuilder.Type()
	}
	casted, err := castPositionDeleteValue(value, castType)
	if err != nil {
		return fmt.Errorf("promote %s to %s: %w", value.DataType(), builder.Type(), err)
	}
	if releasable, ok := casted.(scalar.Releasable); ok {
		defer releasable.Release()
	}

	return scalar.Append(appendBuilder, casted)
}

type positionDeleteStorageBuilder interface {
	array.Builder
	StorageBuilder() array.Builder
}

func appendPositionDeleteList(
	builder array.ListLikeBuilder,
	source scalar.ListScalar,
) error {
	if !isPositionDeleteListType(source.DataType()) {
		return fmt.Errorf("%w: value has type %s, want list",
			iceberg.ErrInvalidSchema, source.DataType())
	}
	values := source.GetList()
	if values == nil {
		return fmt.Errorf("%w: valid list value has no child array",
			iceberg.ErrInvalidSchema)
	}

	if destination, ok := builder.Type().(*arrow.FixedSizeListType); ok &&
		values.Len() != int(destination.Len()) {
		return fmt.Errorf("%w: list value has %d elements, want %d",
			iceberg.ErrInvalidSchema, values.Len(), destination.Len())
	}

	builder.Append(true)
	valueBuilder := builder.ValueBuilder()
	for index := range values.Len() {
		child, err := scalar.GetScalar(values, index)
		if err != nil {
			return err
		}
		appendErr := appendPositionDeleteValue(valueBuilder, child)
		if releasable, ok := child.(scalar.Releasable); ok {
			releasable.Release()
		}
		if appendErr != nil {
			return fmt.Errorf("list element %d: %w", index, appendErr)
		}
	}

	return nil
}

func appendPositionDeleteMap(builder *array.MapBuilder, source *scalar.Map) error {
	if source.DataType().ID() != arrow.MAP {
		return fmt.Errorf("%w: value has type %s, want map",
			iceberg.ErrInvalidSchema, source.DataType())
	}
	entries := source.GetList()
	if entries == nil {
		return fmt.Errorf("%w: valid map value has no entry array",
			iceberg.ErrInvalidSchema)
	}
	entryType, ok := entries.DataType().(*arrow.StructType)
	if !ok || entryType.NumFields() != 2 {
		return fmt.Errorf("%w: map entries have type %s, want a two-field struct",
			iceberg.ErrInvalidSchema, entries.DataType())
	}
	entryBuilder, ok := builder.ValueBuilder().(*array.StructBuilder)
	if !ok {
		return fmt.Errorf("%w: map builder has entry builder of type %T, want struct",
			iceberg.ErrInvalidSchema, builder.ValueBuilder())
	}

	builder.Append(true)
	for index := range entries.Len() {
		entry, err := scalar.GetScalar(entries, index)
		if err != nil {
			return err
		}
		if !entry.IsValid() {
			if releasable, ok := entry.(scalar.Releasable); ok {
				releasable.Release()
			}

			return fmt.Errorf("%w: map entry %d is null", iceberg.ErrInvalidSchema, index)
		}
		appendErr := appendPositionDeleteValue(entryBuilder, entry)
		if releasable, ok := entry.(scalar.Releasable); ok {
			releasable.Release()
		}
		if appendErr != nil {
			return fmt.Errorf("map entry %d: %w", index, appendErr)
		}
	}

	return nil
}

func isPositionDeleteListType(typ arrow.DataType) bool {
	switch typ.ID() {
	case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST:
		return true
	default:
		return false
	}
}

func castPositionDeleteValue(value scalar.Scalar, destination arrow.DataType) (scalar.Scalar, error) {
	if destination.ID() == arrow.LARGE_BINARY {
		if binary, ok := value.(scalar.BinaryScalar); ok {
			return scalar.NewLargeBinaryScalar(binary.Buffer()), nil
		}
	}
	if destination.ID() == arrow.LARGE_STRING {
		if binary, ok := value.(scalar.BinaryScalar); ok {
			return scalar.NewLargeStringScalarFromBuffer(binary.Buffer()), nil
		}
	}

	return value.CastTo(destination)
}

func canPromotePositionDeleteValue(source, destination arrow.DataType) bool {
	switch source.ID() {
	case arrow.INT32:
		return destination.ID() == arrow.INT64
	case arrow.FLOAT32:
		return destination.ID() == arrow.FLOAT64
	case arrow.DECIMAL128:
		sourceDecimal, sourceOK := source.(*arrow.Decimal128Type)
		destinationDecimal, destinationOK := destination.(*arrow.Decimal128Type)

		return sourceOK && destinationOK &&
			sourceDecimal.Scale == destinationDecimal.Scale &&
			sourceDecimal.Precision <= destinationDecimal.Precision
	case arrow.STRING, arrow.LARGE_STRING:
		return destination.ID() == arrow.BINARY || destination.ID() == arrow.LARGE_BINARY
	case arrow.BINARY, arrow.LARGE_BINARY:
		return destination.ID() == arrow.STRING || destination.ID() == arrow.LARGE_STRING
	case arrow.FIXED_SIZE_BINARY:
		sourceFixed, sourceOK := source.(*arrow.FixedSizeBinaryType)
		destinationExtension, destinationOK := destination.(arrow.ExtensionType)
		if !sourceOK || !destinationOK || sourceFixed.ByteWidth != 16 ||
			destinationExtension.ExtensionName() != "arrow.uuid" {
			return false
		}
		destinationFixed, destinationOK := destinationExtension.StorageType().(*arrow.FixedSizeBinaryType)

		return destinationOK && destinationFixed.ByteWidth == 16
	default:
		return false
	}
}

type positionDeleteFieldLookup struct {
	byID   map[int]int
	byName map[string]int
	byIDs  bool
}

func newPositionDeleteFieldLookup(sourceType *arrow.StructType) (positionDeleteFieldLookup, error) {
	fields := positionDeleteFieldLookup{
		byID:   make(map[int]int, sourceType.NumFields()),
		byName: make(map[string]int, sourceType.NumFields()),
	}
	missingIDs := 0
	for index, field := range sourceType.Fields() {
		if _, exists := fields.byName[field.Name]; exists {
			return fields, fmt.Errorf("%w: row contains duplicate field %q",
				iceberg.ErrInvalidSchema, field.Name)
		}
		fields.byName[field.Name] = index

		id := getFieldID(field)
		if id == nil {
			missingIDs++

			continue
		}
		if _, exists := fields.byID[*id]; exists {
			return fields, fmt.Errorf("%w: row contains duplicate field ID %d",
				iceberg.ErrInvalidSchema, *id)
		}
		fields.byID[*id] = index
	}
	if len(fields.byID) > 0 {
		if missingIDs > 0 {
			return fields, fmt.Errorf("%w: row schema has fields with and without field IDs",
				iceberg.ErrInvalidSchema)
		}
		fields.byIDs = true
	}

	return fields, nil
}

func (f positionDeleteFieldLookup) index(destination arrow.Field) (int, bool) {
	if f.byIDs {
		id := getFieldID(destination)
		if id == nil {
			return 0, false
		}

		index, ok := f.byID[*id]

		return index, ok
	}

	index, ok := f.byName[destination.Name]

	return index, ok
}

func (i InspectTable) positionDeleteRecordReader(
	ctx context.Context,
	arrowSchema *arrow.Schema,
	fs iceio.IO,
	files []iceberg.DataFile,
	partitionType *iceberg.StructType,
	partitionIDByOld map[int]int,
	formatVersion int,
) array.RecordReader {
	return array.ReaderFromIter(arrowSchema, func(yield func(arrow.RecordBatch, error) bool) {
		if err := ctx.Err(); err != nil {
			_ = yield(nil, err)

			return
		}

		bldr := array.NewRecordBuilder(i.alloc, arrowSchema)
		defer bldr.Release()
		appender, err := newPositionDeleteRecordAppender(bldr, partitionType, partitionIDByOld, formatVersion)
		if err != nil {
			_ = yield(nil, err)

			return
		}

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
		yieldError := func(err error) {
			_ = yield(nil, err)
		}
		appendRow := func(file iceberg.DataFile, path string, pos int64, deletedRow scalar.Scalar) (bool, error) {
			if err := appender.append(file, path, pos, deletedRow); err != nil {
				return false, err
			}
			rows++
			if rows == inspectRecordBatchSize {
				return emit(), nil
			}

			return true, nil
		}

		for _, file := range files {
			if err := ctx.Err(); err != nil {
				yieldError(err)

				return
			}
			var keepGoing bool
			var err error
			switch file.FileFormat() {
			case iceberg.PuffinFile:
				keepGoing, err = appendDeletionVectorRows(ctx, fs, file, appendRow)
			default:
				keepGoing, err = appendParquetPositionDeleteRows(ctx, fs, file, appendRow)
			}
			if err != nil {
				yieldError(fmt.Errorf("read position delete file %s: %w", file.FilePath(), err))

				return
			}
			if !keepGoing {
				return
			}
		}

		if rows > 0 {
			_ = emit()
		} else if !emitted {
			batch := bldr.NewRecordBatch()
			_ = yield(batch, nil)
		}
	})
}

type appendPositionDeleteRow func(iceberg.DataFile, string, int64, scalar.Scalar) (bool, error)

func appendDeletionVectorRows(
	ctx context.Context,
	fs iceio.IO,
	file iceberg.DataFile,
	appendRow appendPositionDeleteRow,
) (bool, error) {
	referencedDataFile := file.ReferencedDataFile()
	if referencedDataFile == nil {
		return false, fmt.Errorf("%w: deletion vector is missing referenced_data_file",
			iceberg.ErrInvalidSchema)
	}
	if file.ContentOffset() == nil || file.ContentSizeInBytes() == nil {
		return false, fmt.Errorf("%w: deletion vector is missing content_offset/content_size_in_bytes",
			iceberg.ErrInvalidSchema)
	}
	bitmap, err := dv.ReadDV(fs, file)
	if err != nil {
		return false, err
	}
	for position := range bitmap.Positions() {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if position > math.MaxInt64 {
			return false, fmt.Errorf("%w: deletion position %d exceeds int64", iceberg.ErrInvalidSchema, position)
		}
		keepGoing, err := appendRow(file, *referencedDataFile, int64(position), nil)
		if err != nil || !keepGoing {
			return keepGoing, err
		}
	}

	return true, nil
}

func appendParquetPositionDeleteRows(
	ctx context.Context,
	fs iceio.IO,
	file iceberg.DataFile,
	appendRow appendPositionDeleteRow,
) (keepGoing bool, err error) {
	source, err := tblutils.GetFile(ctx, fs, file, true)
	if err != nil {
		return false, err
	}
	reader, err := source.GetReader(ctx)
	if err != nil {
		return false, err
	}
	defer func() {
		if closeErr := reader.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	records, err := reader.GetRecords(ctx, nil, nil)
	if err != nil {
		return false, err
	}
	defer records.Release()

	for records.Next() {
		record := records.RecordBatch()
		filePathIndex, posIndex, err := positionDeleteColumnIndices(record.Schema())
		if err != nil {
			return false, err
		}
		filePaths, err := filePathValues(record.Column(filePathIndex))
		if err != nil {
			return false, err
		}
		if err := validatePositionDeleteFilePathValues(filePaths, record.Column(filePathIndex)); err != nil {
			return false, err
		}
		positions, ok := record.Column(posIndex).(*array.Int64)
		if !ok {
			return false, fmt.Errorf("%w: pos column has type %s, want int64",
				iceberg.ErrInvalidSchema, record.Column(posIndex).DataType())
		}
		if positions.NullN() > 0 {
			return false, fmt.Errorf("%w: null pos in position delete file", iceberg.ErrInvalidSchema)
		}

		rowIndex := -1
		if indices := record.Schema().FieldIndices("row"); len(indices) > 1 {
			return false, fmt.Errorf("%w: position delete file contains multiple row columns",
				iceberg.ErrInvalidSchema)
		} else if len(indices) == 1 {
			rowIndex = indices[0]
		}
		for row := range int(record.NumRows()) {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			var deletedRow scalar.Scalar
			if rowIndex >= 0 && !record.Column(rowIndex).IsNull(row) {
				deletedRow, err = scalar.GetScalar(record.Column(rowIndex), row)
				if err != nil {
					return false, err
				}
			}
			continueReading, appendErr := appendRow(
				file, filePaths.Value(row), positions.Value(row), deletedRow)
			if releasable, ok := deletedRow.(scalar.Releasable); ok {
				releasable.Release()
			}
			if appendErr != nil || !continueReading {
				return continueReading, appendErr
			}
		}
	}
	if err := records.Err(); err != nil {
		return false, err
	}

	return true, nil
}

func validatePositionDeleteFilePathValues(values arrow.TypedArray[string], arr arrow.Array) error {
	var dictionary arrow.Array
	var indices *array.Dictionary
	if dict, ok := arr.(*array.Dictionary); ok {
		dictionary = dict.Dictionary()
		indices = dict
	}

	for index := range values.Len() {
		if values.IsNull(index) {
			return fmt.Errorf("%w: null file_path in position delete file", iceberg.ErrInvalidSchema)
		}
		if dictionary != nil && dictionary.IsNull(indices.GetValueIndex(index)) {
			return fmt.Errorf("%w: null file_path dictionary value in position delete file", iceberg.ErrInvalidSchema)
		}
	}

	return nil
}

func positionDeletesPartitionType(
	metadata Metadata,
) (*iceberg.StructType, map[int]int, error) {
	base, err := inspectPartitionType(metadata)
	if err != nil {
		return nil, nil, err
	}

	used := map[int]struct{}{
		positionDeleteFilePathID:      {},
		positionDeletePosID:           {},
		positionDeleteRowID:           {},
		positionDeletePartitionID:     {},
		positionDeleteSpecID:          {},
		positionDeletePhysicalPathID:  {},
		positionDeleteContentOffsetID: {},
		positionDeleteContentSizeID:   {},
	}
	for _, schema := range metadata.Schemas() {
		fields, err := iceberg.IndexByID(schema)
		if err != nil {
			return nil, nil, err
		}
		for id := range fields {
			used[id] = struct{}{}
		}
	}
	for _, field := range base.FieldList {
		used[field.ID] = struct{}{}
	}

	fields := make([]iceberg.NestedField, len(base.FieldList))
	idByOld := make(map[int]int, len(base.FieldList))
	nextID := 1
	for index, field := range base.FieldList {
		for {
			if _, exists := used[nextID]; !exists {
				break
			}
			nextID++
		}
		idByOld[field.ID] = nextID
		field.ID = nextID
		fields[index] = field
		used[nextID] = struct{}{}
		nextID++
	}

	return &iceberg.StructType{FieldList: fields}, idByOld, nil
}

// PositionDeletesSchema returns the schema of the position_deletes metadata table.
func PositionDeletesSchema(
	tableSchema *iceberg.Schema,
	partitionType *iceberg.StructType,
	formatVersion int,
) *iceberg.Schema {
	rowType := iceberg.StructType{}
	if tableSchema != nil {
		rowType = tableSchema.AsStruct()
	}
	fields := []iceberg.NestedField{
		{ID: positionDeleteFilePathID, Name: "file_path", Type: iceberg.PrimitiveTypes.String, Required: true},
		{ID: positionDeletePosID, Name: "pos", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		{ID: positionDeleteRowID, Name: "row", Type: &rowType, Required: false},
	}
	if len(partitionType.FieldList) > 0 {
		fields = append(fields, iceberg.NestedField{
			ID:       positionDeletePartitionID,
			Name:     "partition",
			Type:     partitionType,
			Required: true,
		})
	}
	fields = append(fields,
		iceberg.NestedField{
			ID:       positionDeleteSpecID,
			Name:     "spec_id",
			Type:     iceberg.PrimitiveTypes.Int32,
			Required: true,
		},
		iceberg.NestedField{
			ID:       positionDeletePhysicalPathID,
			Name:     positionDeletePhysicalPathName,
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
	)
	if formatVersion >= 3 {
		fields = append(fields,
			iceberg.NestedField{
				ID:       positionDeleteContentOffsetID,
				Name:     "content_offset",
				Type:     iceberg.PrimitiveTypes.Int64,
				Required: false,
			},
			iceberg.NestedField{
				ID:       positionDeleteContentSizeID,
				Name:     "content_size_in_bytes",
				Type:     iceberg.PrimitiveTypes.Int64,
				Required: false,
			},
		)
	}

	return iceberg.NewSchema(0, fields...)
}
