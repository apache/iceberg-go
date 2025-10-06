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

package internal

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
)

const (
	ParquetRowGroupSizeBytesKey              = "write.parquet.row-group-size-bytes"
	ParquetRowGroupSizeBytesDefault          = 128 * 1024 * 1024 // 128 MB
	ParquetRowGroupLimitKey                  = "write.parquet.row-group-limit"
	ParquetRowGroupLimitDefault              = 1048576
	ParquetPageSizeBytesKey                  = "write.parquet.page-size-bytes"
	ParquetPageSizeBytesDefault              = 1024 * 1024 // 1 MB
	ParquetPageRowLimitKey                   = "write.parquet.page-row-limit"
	ParquetPageRowLimitDefault               = 20000
	ParquetDictSizeBytesKey                  = "write.parquet.dict-size-bytes"
	ParquetDictSizeBytesDefault              = 2 * 1024 * 1024 // 2 MB
	ParquetCompressionKey                    = "write.parquet.compression-codec"
	ParquetCompressionDefault                = "zstd"
	ParquetCompressionLevelKey               = "write.parquet.compression-level"
	ParquetCompressionLevelDefault           = -1
	ParquetBloomFilterMaxBytesKey            = "write.parquet.bloom-filter-max-bytes"
	ParquetBloomFilterMaxBytesDefault        = 1024 * 1024
	ParquetBloomFilterColumnEnabledKeyPrefix = "write.parquet.bloom-filter-enabled.column"
)

type parquetFormat struct{}

func (parquetFormat) Open(ctx context.Context, fs iceio.IO, path string) (FileReader, error) {
	inputfile, err := fs.Open(path)
	if err != nil {
		return nil, err
	}

	rdr, err := file.NewParquetReader(inputfile)
	if err != nil {
		return nil, err
	}

	alloc := compute.GetAllocator(ctx)
	arrRdr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, alloc)
	if err != nil {
		return nil, err
	}

	return wrapPqArrowReader{arrRdr}, nil
}

func (parquetFormat) PathToIDMapping(sc *iceberg.Schema) (map[string]int, error) {
	result := make(map[string]int)

	paths, err := iceberg.PreOrderVisit(sc, &id2ParquetPathVisitor{})
	if err != nil {
		return nil, err
	}

	for _, entry := range paths {
		result[entry.path] = entry.fieldID
	}

	return result, nil
}

func (p parquetFormat) createStatsAgg(typ iceberg.PrimitiveType, physicalTypeStr string, truncLen int) (StatsAgg, error) {
	expectedPhysical := p.PrimitiveTypeToPhysicalType(typ)
	if physicalTypeStr != expectedPhysical {
		switch {
		case physicalTypeStr == "INT32" && expectedPhysical == "INT64":
		case physicalTypeStr == "FLOAT" && expectedPhysical == "DOUBLE":
		default:
			return nil, fmt.Errorf("unexpected physical type %s for %s, expected %s",
				physicalTypeStr, typ, expectedPhysical)
		}
	}

	switch physicalTypeStr {
	case "BOOLEAN":
		return newStatAgg[bool](typ, truncLen), nil
	case "INT32":
		switch typ.(type) {
		case iceberg.DecimalType:
			return &decAsIntAgg[int32]{
				newStatAgg[int32](typ, truncLen).(*statsAggregator[int32]),
			}, nil
		}

		return newStatAgg[int32](typ, truncLen), nil
	case "INT64":
		switch typ.(type) {
		case iceberg.DecimalType:
			return &decAsIntAgg[int64]{
				newStatAgg[int64](typ, truncLen).(*statsAggregator[int64]),
			}, nil
		}

		return newStatAgg[int64](typ, truncLen), nil
	case "FLOAT":
		return newStatAgg[float32](typ, truncLen), nil
	case "DOUBLE":
		return newStatAgg[float64](typ, truncLen), nil
	case "FIXED_LEN_BYTE_ARRAY":
		switch typ.(type) {
		case iceberg.UUIDType:
			return newStatAgg[uuid.UUID](typ, truncLen), nil
		case iceberg.DecimalType:
			return newStatAgg[iceberg.Decimal](typ, truncLen), nil
		default:
			return newStatAgg[[]byte](typ, truncLen), nil
		}
	case "BYTE_ARRAY":
		if typ.Equals(iceberg.PrimitiveTypes.String) {
			return newStatAgg[string](typ, truncLen), nil
		}

		return newStatAgg[[]byte](typ, truncLen), nil
	default:
		return nil, fmt.Errorf("unsupported physical type: %s", physicalTypeStr)
	}
}

func (parquetFormat) PrimitiveTypeToPhysicalType(typ iceberg.PrimitiveType) string {
	switch typ.(type) {
	case iceberg.BooleanType:
		return "BOOLEAN"
	case iceberg.Int32Type:
		return "INT32"
	case iceberg.Int64Type:
		return "INT64"
	case iceberg.Float32Type:
		return "FLOAT"
	case iceberg.Float64Type:
		return "DOUBLE"
	case iceberg.DateType:
		return "INT32"
	case iceberg.TimeType:
		return "INT64"
	case iceberg.TimestampType:
		return "INT64"
	case iceberg.TimestampTzType:
		return "INT64"
	case iceberg.StringType:
		return "BYTE_ARRAY"
	case iceberg.UUIDType:
		return "FIXED_LEN_BYTE_ARRAY"
	case iceberg.FixedType:
		return "FIXED_LEN_BYTE_ARRAY"
	case iceberg.BinaryType:
		return "BYTE_ARRAY"
	case iceberg.DecimalType:
		return "FIXED_LEN_BYTE_ARRAY"
	default:
		panic(fmt.Errorf("expected primitive type, got: %s", typ))
	}
}

func (parquetFormat) GetWriteProperties(props iceberg.Properties) any {
	writerProps := []parquet.WriterProperty{
		parquet.WithDictionaryDefault(false),
		parquet.WithMaxRowGroupLength(int64(props.GetInt(ParquetRowGroupLimitKey,
			ParquetRowGroupLimitDefault))),
		parquet.WithDataPageSize(int64(props.GetInt(ParquetPageSizeBytesKey,
			ParquetPageSizeBytesDefault))),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithBatchSize(int64(props.GetInt(ParquetPageRowLimitKey,
			ParquetPageRowLimitDefault))),
		parquet.WithDictionaryPageSizeLimit(int64(props.GetInt(ParquetDictSizeBytesKey,
			ParquetDictSizeBytesDefault))),
	}

	compression := props.Get(ParquetCompressionKey, ParquetCompressionDefault)
	compressionLevel := props.GetInt(ParquetCompressionLevelKey,
		ParquetCompressionLevelDefault)

	var codec compress.Compression
	switch compression {
	case "snappy":
		codec = compress.Codecs.Snappy
	case "zstd":
		codec = compress.Codecs.Zstd
	case "uncompressed":
		codec = compress.Codecs.Uncompressed
	case "gzip":
		codec = compress.Codecs.Gzip
	case "brotli":
		codec = compress.Codecs.Brotli
	case "lz4":
		codec = compress.Codecs.Lz4
	case "lz4raw":
		codec = compress.Codecs.Lz4Raw
	case "lzo":
		codec = compress.Codecs.Lzo
	default:
		// warn
	}

	return append(writerProps, parquet.WithCompression(codec),
		parquet.WithCompressionLevel(compressionLevel))
}

func (p parquetFormat) WriteDataFile(ctx context.Context, fs iceio.WriteFileIO, partitionValues map[int]any, info WriteFileInfo, batches []arrow.RecordBatch) (_ iceberg.DataFile, err error) {
	fw, err := fs.Create(info.FileName)
	if err != nil {
		return nil, err
	}

	defer internal.CheckedClose(fw, &err)

	cntWriter := internal.CountingWriter{W: fw}
	mem := compute.GetAllocator(ctx)
	writerProps := parquet.NewWriterProperties(info.WriteProps.([]parquet.WriterProperty)...)
	arrProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(mem), pqarrow.WithStoreSchema())

	writer, err := pqarrow.NewFileWriter(batches[0].Schema(), &cntWriter, writerProps, arrProps)
	if err != nil {
		return nil, err
	}

	for _, batch := range batches {
		if err := writer.WriteBuffered(batch); err != nil {
			return nil, err
		}
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	filemeta, err := writer.FileMetadata()
	if err != nil {
		return nil, err
	}

	colMapping, err := p.PathToIDMapping(info.FileSchema)
	if err != nil {
		return nil, err
	}

	return p.DataFileStatsFromMeta(filemeta, info.StatsCols, colMapping).
		ToDataFile(info.FileSchema, info.Spec, info.FileName, iceberg.ParquetFile, cntWriter.Count, partitionValues), nil
}

type decAsIntAgg[T int32 | int64] struct {
	*statsAggregator[T]
}

func (s *decAsIntAgg[T]) MinAsBytes() ([]byte, error) {
	if s.curMin == nil {
		return nil, nil
	}

	lit := iceberg.DecimalLiteral(iceberg.Decimal{
		Val:   decimal128.FromI64(int64(s.curMin.Value())),
		Scale: s.primitiveType.(iceberg.DecimalType).Scale(),
	})
	if s.truncLen > 0 {
		return s.toBytes((&iceberg.TruncateTransform{Width: s.truncLen}).
			Apply(iceberg.Optional[iceberg.Literal]{Valid: true, Val: lit}).Val)
	}

	return s.toBytes(lit)
}

func (s *decAsIntAgg[T]) MaxAsBytes() ([]byte, error) {
	if s.curMax == nil {
		return nil, nil
	}

	lit := iceberg.DecimalLiteral(iceberg.Decimal{
		Val:   decimal128.FromI64(int64(s.curMax.Value())),
		Scale: s.primitiveType.(iceberg.DecimalType).Scale(),
	})
	if s.truncLen <= 0 {
		return s.toBytes(lit)
	}

	return nil, fmt.Errorf("%s cannot be truncated for upper bound", s.primitiveType)
}

type wrappedBinaryStats struct {
	*metadata.ByteArrayStatistics
}

func (w *wrappedBinaryStats) Min() []byte {
	return w.ByteArrayStatistics.Min()
}

func (w *wrappedBinaryStats) Max() []byte {
	return w.ByteArrayStatistics.Max()
}

type wrappedStringStats struct {
	*metadata.ByteArrayStatistics
}

func (w *wrappedStringStats) Min() string {
	data := w.ByteArrayStatistics.Min()

	return unsafe.String(unsafe.SliceData(data), len(data))
}

func (w *wrappedStringStats) Max() string {
	data := w.ByteArrayStatistics.Max()

	return unsafe.String(unsafe.SliceData(data), len(data))
}

type wrappedUUIDStats struct {
	*metadata.FixedLenByteArrayStatistics
}

func (w *wrappedUUIDStats) Min() uuid.UUID {
	uid, err := uuid.FromBytes(w.FixedLenByteArrayStatistics.Min())
	if err != nil {
		panic(err)
	}

	return uid
}

func (w *wrappedUUIDStats) Max() uuid.UUID {
	uid, err := uuid.FromBytes(w.FixedLenByteArrayStatistics.Max())
	if err != nil {
		panic(err)
	}

	return uid
}

type wrappedFLBAStats struct {
	*metadata.FixedLenByteArrayStatistics
}

func (w *wrappedFLBAStats) Min() []byte {
	return w.FixedLenByteArrayStatistics.Min()
}

func (w *wrappedFLBAStats) Max() []byte {
	return w.FixedLenByteArrayStatistics.Max()
}

type wrappedDecStats struct {
	*metadata.FixedLenByteArrayStatistics
	scale int
}

func (w wrappedDecStats) Min() iceberg.Decimal {
	dec, err := BigEndianToDecimal(w.FixedLenByteArrayStatistics.Min())
	if err != nil {
		panic(err)
	}

	return iceberg.Decimal{Val: dec, Scale: w.scale}
}

func (w wrappedDecStats) Max() iceberg.Decimal {
	dec, err := BigEndianToDecimal(w.FixedLenByteArrayStatistics.Max())
	if err != nil {
		panic(err)
	}

	return iceberg.Decimal{Val: dec, Scale: w.scale}
}

func (p parquetFormat) DataFileStatsFromMeta(meta Metadata, statsCols map[int]StatisticsCollector, colMapping map[string]int) *DataFileStatistics {
	pqmeta := meta.(*metadata.FileMetaData)
	var (
		colSizes        = make(map[int]int64)
		valueCounts     = make(map[int]int64)
		splitOffsets    = make([]int64, 0)
		nullValueCounts = make(map[int]int64)
		nanValueCounts  = make(map[int]int64)
		invalidateCol   = make(map[int]struct{})
		colAggs         = make(map[int]StatsAgg)
	)

	for rg := range pqmeta.NumRowGroups() {
		// reference: https://github.com/apache/iceberg-python/blob/main/pyiceberg/io/pyarrow.py#L2285
		rowGroup := pqmeta.RowGroup(rg)
		colChunk, err := rowGroup.ColumnChunk(0)
		if err != nil {
			panic(err)
		}

		dataOffset, dictOffset := colChunk.DataPageOffset(), colChunk.DictionaryPageOffset()
		if colChunk.HasDictionaryPage() && dictOffset < dataOffset {
			splitOffsets = append(splitOffsets, dictOffset)
		} else {
			splitOffsets = append(splitOffsets, dataOffset)
		}

		for pos := range rowGroup.NumColumns() {
			colChunk, err = rowGroup.ColumnChunk(pos)
			if err != nil {
				panic(err)
			}

			fieldID := colMapping[colChunk.PathInSchema().String()]
			statsCol := statsCols[fieldID]
			if statsCol.Mode.Typ == MetricModeNone {
				continue
			}

			colSizes[fieldID] += colChunk.TotalCompressedSize()
			valueCounts[fieldID] += colChunk.NumValues()
			set, err := colChunk.StatsSet()
			if err != nil {
				panic(err)
			}

			if !set {
				invalidateCol[fieldID] = struct{}{}

				continue
			}

			stats, err := colChunk.Statistics()
			if err != nil || stats == nil {
				invalidateCol[fieldID] = struct{}{}

				continue
			}

			if stats.HasNullCount() {
				nullValueCounts[fieldID] += stats.NullCount()
			}

			if statsCol.Mode.Typ == MetricModeCounts || !stats.HasMinMax() {
				continue
			}

			agg, ok := colAggs[fieldID]
			if !ok {
				agg, err = p.createStatsAgg(statsCol.IcebergTyp, stats.Type().String(), statsCol.Mode.Len)
				if err != nil {
					panic(err)
				}

				colAggs[fieldID] = agg
			}

			switch t := statsCol.IcebergTyp.(type) {
			case iceberg.BinaryType:
				stats = &wrappedBinaryStats{stats.(*metadata.ByteArrayStatistics)}
			case iceberg.UUIDType:
				stats = &wrappedUUIDStats{stats.(*metadata.FixedLenByteArrayStatistics)}
			case iceberg.StringType:
				stats = &wrappedStringStats{stats.(*metadata.ByteArrayStatistics)}
			case iceberg.FixedType:
				stats = &wrappedFLBAStats{stats.(*metadata.FixedLenByteArrayStatistics)}
			case iceberg.DecimalType:
				stats = &wrappedDecStats{stats.(*metadata.FixedLenByteArrayStatistics), t.Scale()}
			}

			agg.Update(stats)
		}

	}

	slices.Sort(splitOffsets)
	maps.DeleteFunc(nullValueCounts, func(fieldID int, _ int64) bool {
		_, ok := invalidateCol[fieldID]

		return ok
	})
	maps.DeleteFunc(colAggs, func(fieldID int, _ StatsAgg) bool {
		_, ok := invalidateCol[fieldID]

		return ok
	})

	return &DataFileStatistics{
		RecordCount:     pqmeta.GetNumRows(),
		ColSizes:        colSizes,
		ValueCounts:     valueCounts,
		NullValueCounts: nullValueCounts,
		NanValueCounts:  nanValueCounts,
		SplitOffsets:    splitOffsets,
		ColAggs:         colAggs,
	}
}

type ParquetFileSource struct {
	mem  memory.Allocator
	fs   iceio.IO
	file iceberg.DataFile
}

type wrapPqArrowReader struct {
	*pqarrow.FileReader
}

func (w wrapPqArrowReader) Metadata() Metadata {
	return w.ParquetReader().MetaData()
}

func (w wrapPqArrowReader) SourceFileSize() int64 {
	return w.ParquetReader().MetaData().GetSourceFileSize()
}

func (w wrapPqArrowReader) Close() error {
	return w.ParquetReader().Close()
}

func (w wrapPqArrowReader) PrunedSchema(projectedIDs map[int]struct{}, mapping iceberg.NameMapping) (*arrow.Schema, []int, error) {
	return pruneParquetColumns(w.Manifest, projectedIDs, false, mapping)
}

func (w wrapPqArrowReader) GetRecords(ctx context.Context, cols []int, tester any) (array.RecordReader, error) {
	var (
		testRg func(*metadata.RowGroupMetaData, []int) (bool, error)
		ok     bool
	)

	if tester != nil {
		testRg, ok = tester.(func(*metadata.RowGroupMetaData, []int) (bool, error))
		if !ok {
			return nil, fmt.Errorf("%w: invalid tester function", iceberg.ErrInvalidArgument)
		}
	}

	var rgList []int
	if testRg != nil {
		rgList = make([]int, 0)
		fileMeta, numRg := w.ParquetReader().MetaData(), w.ParquetReader().NumRowGroups()
		for rg := 0; rg < numRg; rg++ {
			rgMeta := fileMeta.RowGroup(rg)
			use, err := testRg(rgMeta, cols)
			if err != nil {
				return nil, err
			}

			if use {
				rgList = append(rgList, rg)
			}
		}
	}

	return w.GetRecordReader(ctx, cols, rgList)
}

func (pfs *ParquetFileSource) GetReader(ctx context.Context) (FileReader, error) {
	pf, err := pfs.fs.Open(pfs.file.FilePath())
	if err != nil {
		return nil, err
	}

	rdr, err := file.NewParquetReader(pf,
		file.WithReadProps(parquet.NewReaderProperties(pfs.mem)))
	if err != nil {
		return nil, err
	}

	// TODO: grab these from the context
	arrProps := pqarrow.ArrowReadProperties{
		Parallel:  true,
		BatchSize: 1 << 17,
	}

	if pfs.file.ContentType() == iceberg.EntryContentPosDeletes {
		// for dictionary for filepath col
		arrProps.SetReadDict(0, true)
	}

	fr, err := pqarrow.NewFileReader(rdr, arrProps, pfs.mem)
	if err != nil {
		return nil, err
	}

	return wrapPqArrowReader{fr}, nil
}

type manifestVisitor[T any] interface {
	Manifest(*pqarrow.SchemaManifest, []T, *iceberg.MappedField) T
	Field(pqarrow.SchemaField, T, *iceberg.MappedField) T
	Struct(pqarrow.SchemaField, []T, *iceberg.MappedField) T
	List(pqarrow.SchemaField, T, *iceberg.MappedField) T
	Map(pqarrow.SchemaField, T, T, *iceberg.MappedField) T
	Primitive(pqarrow.SchemaField, *iceberg.MappedField) T
}

func visitParquetManifest[T any](manifest *pqarrow.SchemaManifest, visitor manifestVisitor[T], mapping *iceberg.MappedField) (res T, err error) {
	if manifest == nil {
		err = fmt.Errorf("%w: cannot visit nil manifest", iceberg.ErrInvalidArgument)

		return res, err
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%s", r)
		}
	}()

	var fieldMap *iceberg.MappedField

	results := make([]T, len(manifest.Fields))
	for i, f := range manifest.Fields {
		if mapping != nil {
			fieldMap = mapping.GetField(f.Field.Name)
		}
		res := visitManifestField(f, visitor, fieldMap)
		results[i] = visitor.Field(f, res, fieldMap)
	}

	return visitor.Manifest(manifest, results, mapping), nil
}

func visitParquetManifestStruct[T any](field pqarrow.SchemaField, visitor manifestVisitor[T], mapping *iceberg.MappedField) T {
	results := make([]T, len(field.Children))
	var fieldMap *iceberg.MappedField
	for i, f := range field.Children {
		if mapping != nil {
			fieldMap = mapping.GetField(f.Field.Name)
		}
		res := visitManifestField(f, visitor, fieldMap)
		results[i] = visitor.Field(f, res, fieldMap)
	}

	return visitor.Struct(field, results, mapping)
}

func visitManifestList[T any](field pqarrow.SchemaField, visitor manifestVisitor[T], mapping *iceberg.MappedField) T {
	elemField := field.Children[0]
	var elemMapping *iceberg.MappedField
	if mapping != nil {
		elemMapping = mapping.GetField("element")
	}
	res := visitManifestField(elemField, visitor, elemMapping)

	return visitor.List(field, res, mapping)
}

func visitManifestMap[T any](field pqarrow.SchemaField, visitor manifestVisitor[T], mapping *iceberg.MappedField) T {
	kvfield := field.Children[0]
	keyField, valField := kvfield.Children[0], kvfield.Children[1]
	var keyMapping, valMapping *iceberg.MappedField
	if mapping != nil {
		keyMapping = mapping.GetField("key")
		valMapping = mapping.GetField("value")
	}

	return visitor.Map(field, visitManifestField(keyField, visitor, keyMapping), visitManifestField(valField, visitor, valMapping), mapping)
}

func visitManifestField[T any](field pqarrow.SchemaField, visitor manifestVisitor[T], mapping *iceberg.MappedField) T {
	switch field.Field.Type.(type) {
	case *arrow.StructType:
		return visitParquetManifestStruct(field, visitor, mapping)
	case *arrow.MapType:
		return visitManifestMap(field, visitor, mapping)
	case arrow.ListLikeType:
		return visitManifestList(field, visitor, mapping)
	default:
		return visitor.Primitive(field, mapping)
	}
}

func pruneParquetColumns(manifest *pqarrow.SchemaManifest, selected map[int]struct{}, selectFullTypes bool, mapping iceberg.NameMapping) (*arrow.Schema, []int, error) {
	visitor := &pruneParquetSchema{
		selected:  selected,
		manifest:  manifest,
		fullTypes: selectFullTypes,
		indices:   []int{},
	}

	result, err := visitParquetManifest(manifest, visitor, &iceberg.MappedField{Fields: mapping})
	if err != nil {
		return nil, nil, err
	}

	return arrow.NewSchema(result.Type.(*arrow.StructType).Fields(), &result.Metadata),
		visitor.indices, nil
}

func getFieldID(f arrow.Field) *int {
	if !f.HasMetadata() {
		return nil
	}

	fieldIDStr, ok := f.Metadata.GetValue("PARQUET:field_id")
	if !ok {
		return nil
	}

	id, err := strconv.Atoi(fieldIDStr)
	if err != nil {
		return nil
	}

	return &id
}

type pruneParquetSchema struct {
	selected  map[int]struct{}
	fullTypes bool
	manifest  *pqarrow.SchemaManifest

	indices []int
}

func (p *pruneParquetSchema) fieldID(field arrow.Field, mapping *iceberg.MappedField) int {
	if mapping != nil {
		if mapping.FieldID != nil {
			return *mapping.FieldID
		}
	}

	if id := getFieldID(field); id != nil {
		return *id
	}

	panic(fmt.Errorf("%w: cannot convert %s to Iceberg field, missing field_id",
		iceberg.ErrInvalidSchema, field))
}

func (p *pruneParquetSchema) Manifest(manifest *pqarrow.SchemaManifest, fields []arrow.Field, _ *iceberg.MappedField) arrow.Field {
	finalFields := slices.DeleteFunc(fields, func(f arrow.Field) bool { return f.Type == nil })
	result := arrow.Field{
		Type: arrow.StructOf(finalFields...),
	}
	if manifest.SchemaMeta != nil {
		result.Metadata = *manifest.SchemaMeta
	}

	return result
}

func (p *pruneParquetSchema) Struct(field pqarrow.SchemaField, children []arrow.Field, _ *iceberg.MappedField) arrow.Field {
	selected, fields := []arrow.Field{}, field.Children
	sameType := true

	for i, t := range children {
		field := fields[i]
		if arrow.TypeEqual(field.Field.Type, t.Type) {
			selected = append(selected, *field.Field)
		} else if t.Type == nil {
			sameType = false
			// type has changed, create a new field with the projected type
			selected = append(selected, arrow.Field{
				Name:     field.Field.Name,
				Type:     field.Field.Type,
				Nullable: field.Field.Nullable,
				Metadata: field.Field.Metadata,
			})
		}
	}

	if len(selected) > 0 {
		if len(selected) == len(fields) && sameType {
			// nothing changed, return the original
			return *field.Field
		} else {
			result := *field.Field
			result.Type = arrow.StructOf(selected...)

			return result
		}
	}

	return arrow.Field{}
}

func (p *pruneParquetSchema) Field(field pqarrow.SchemaField, result arrow.Field, mapping *iceberg.MappedField) arrow.Field {
	_, ok := p.selected[p.fieldID(*field.Field, mapping)]
	if !ok {
		if result.Type != nil {
			return result
		}

		return arrow.Field{}
	}

	if p.fullTypes {
		return *field.Field
	}

	if _, ok := field.Field.Type.(*arrow.StructType); ok {
		result := *field.Field
		result.Type = p.projectSelectedStruct(result.Type)

		return result
	}

	if !field.IsLeaf() {
		panic(errors.New("cannot explicitly project list or map types"))
	}

	p.indices = append(p.indices, field.ColIndex)

	return *field.Field
}

func (p *pruneParquetSchema) List(field pqarrow.SchemaField, elemResult arrow.Field, mapping *iceberg.MappedField) arrow.Field {
	var elemMapping *iceberg.MappedField
	if mapping != nil {
		elemMapping = mapping.GetField("element")
	}

	_, ok := p.selected[p.fieldID(*field.Children[0].Field, elemMapping)]
	if !ok {
		if elemResult.Type != nil {
			result := *field.Field
			result.Type = p.projectList(field.Field.Type.(arrow.ListLikeType), elemResult.Type)

			return result
		}

		return arrow.Field{}
	}

	if p.fullTypes {
		return *field.Field
	}

	_, ok = field.Children[0].Field.Type.(*arrow.StructType)
	if field.Children[0].Field.Type != nil && ok {
		result := *field.Field
		projected := p.projectSelectedStruct(elemResult.Type)
		result.Type = p.projectList(field.Field.Type.(arrow.ListLikeType), projected)

		return result
	}

	if !field.Children[0].IsLeaf() {
		panic(errors.New("cannot explicitly project list or map types"))
	}

	p.indices = append(p.indices, field.Children[0].ColIndex)

	return *field.Field
}

func (p *pruneParquetSchema) Map(field pqarrow.SchemaField, keyResult, valResult arrow.Field, mapping *iceberg.MappedField) arrow.Field {
	var valMapping *iceberg.MappedField
	if mapping != nil {
		valMapping = mapping.GetField("value")
	}

	_, ok := p.selected[p.fieldID(*field.Children[0].Children[1].Field, valMapping)]
	if !ok {
		if valResult.Type != nil {
			result := *field.Field
			result.Type = p.projectMap(field.Field.Type.(*arrow.MapType), valResult.Type)

			return result
		}

		return arrow.Field{}
	}

	if p.fullTypes {
		return *field.Field
	}

	_, ok = field.Children[0].Children[1].Field.Type.(*arrow.StructType)
	if ok {
		result := *field.Field
		projected := p.projectSelectedStruct(valResult.Type)
		result.Type = p.projectMap(field.Field.Type.(*arrow.MapType), projected)

		return result
	}

	if !field.Children[0].Children[1].IsLeaf() {
		panic("cannot explicitly project list or map types")
	}

	p.indices = append(p.indices, field.Children[0].Children[0].ColIndex)
	p.indices = append(p.indices, field.Children[0].Children[1].ColIndex)

	return *field.Field
}

func (p *pruneParquetSchema) Primitive(_ pqarrow.SchemaField, _ *iceberg.MappedField) arrow.Field {
	return arrow.Field{}
}

func (p *pruneParquetSchema) projectSelectedStruct(projected arrow.DataType) *arrow.StructType {
	if projected == nil {
		return &arrow.StructType{}
	}

	if ty, ok := projected.(*arrow.StructType); ok {
		return ty
	}

	panic("expected a struct")
}

func (p *pruneParquetSchema) projectList(listType arrow.ListLikeType, elemResult arrow.DataType) arrow.ListLikeType {
	if arrow.TypeEqual(listType.Elem(), elemResult) {
		return listType
	}

	origField := listType.ElemField()
	origField.Type = elemResult

	switch listType.(type) {
	case *arrow.ListType:
		return arrow.ListOfField(origField)
	case *arrow.LargeListType:
		return arrow.LargeListOfField(origField)
	case *arrow.ListViewType:
		return arrow.ListViewOfField(origField)
	}

	n := listType.(*arrow.FixedSizeListType).Len()

	return arrow.FixedSizeListOfField(n, origField)
}

func (p *pruneParquetSchema) projectMap(m *arrow.MapType, valResult arrow.DataType) *arrow.MapType {
	if arrow.TypeEqual(m.ItemType(), valResult) {
		return m
	}

	return arrow.MapOf(m.KeyType(), valResult)
}

type id2ParquetPath struct {
	fieldID int
	path    string
}

type id2ParquetPathVisitor struct {
	fieldID int
	path    []string
}

func (v *id2ParquetPathVisitor) Schema(_ *iceberg.Schema, res func() []id2ParquetPath) []id2ParquetPath {
	return res()
}

func (v *id2ParquetPathVisitor) Struct(_ iceberg.StructType, results []func() []id2ParquetPath) []id2ParquetPath {
	result := make([]id2ParquetPath, 0, len(results))
	for _, res := range results {
		result = append(result, res()...)
	}

	return result
}

func (v *id2ParquetPathVisitor) Field(field iceberg.NestedField, res func() []id2ParquetPath) []id2ParquetPath {
	v.fieldID = field.ID
	v.path = append(v.path, field.Name)
	result := res()
	v.path = v.path[:len(v.path)-1]

	return result
}

func (v *id2ParquetPathVisitor) List(listType iceberg.ListType, elemResult func() []id2ParquetPath) []id2ParquetPath {
	v.fieldID = listType.ElementID
	v.path = append(v.path, "list")
	result := elemResult()
	v.path = v.path[:len(v.path)-1]

	return result
}

func (v *id2ParquetPathVisitor) Map(m iceberg.MapType, keyResult func() []id2ParquetPath, valResult func() []id2ParquetPath) []id2ParquetPath {
	v.fieldID = m.KeyID
	v.path = append(v.path, "key_value")
	keyRes := keyResult()
	v.path = v.path[:len(v.path)-1]

	v.fieldID = m.ValueID
	v.path = append(v.path, "key_value")
	valRes := valResult()
	v.path = v.path[:len(v.path)-1]

	return append(keyRes, valRes...)
}

func (v *id2ParquetPathVisitor) Primitive(iceberg.PrimitiveType) []id2ParquetPath {
	return []id2ParquetPath{{fieldID: v.fieldID, path: strings.Join(v.path, ".")}}
}
