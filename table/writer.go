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
	"maps"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/config"
	"github.com/apache/iceberg-go/io"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
)

// WriteTask writes all its Batches into one file. The rolling writer bypasses
// it, naming files via dataFileName directly.
type WriteTask struct {
	Uuid        uuid.UUID
	ID          int
	PartitionID int // PartitionID is the partition identifier used in data file naming.
	FileCount   int // FileCount is a sequential counter for files written by this task.
	Schema      *iceberg.Schema
	Batches     []arrow.RecordBatch
	// SortOrderID claims Batches are globally sorted by that order; writeFile
	// records it unverified. Must stay zero for position deletes.
	SortOrderID int
}

func (w WriteTask) GenerateDataFileName(extension string) string {
	return dataFileName(w.Uuid, w.ID, w.PartitionID, w.FileCount, extension)
}

// dataFileName builds a data file name, mirroring the Java API:
// https://github.com/apache/iceberg/blob/03ed4ba9af4e47d32bdb22b7e3d033eb2a4b2c83/core/src/main/java/org/apache/iceberg/io/OutputFileFactory.java#L93
// Format: {partitionId:05d}-{taskId}-{operationId}-{fileCount:05d}.{extension}
func dataFileName(writeUUID uuid.UUID, taskID, partitionID, fileCount int, extension string) string {
	return fmt.Sprintf("%05d-%d-%s-%05d.%s", partitionID, taskID, writeUUID, fileCount, extension)
}

type defaultDataFileWriter struct {
	loc              LocationProvider
	fs               io.WriteFileIO
	fileSchema       *iceberg.Schema
	propertySchema   *iceberg.Schema
	fileFormat       iceberg.FileFormat
	format           tblutils.FileFormat
	props            iceberg.Properties
	content          iceberg.ManifestEntryContent
	equalityFieldIDs []int
	invariants       dataWriterInvariants
}

// dataWriterInvariants contains the write configuration shared by every task
// handled by a defaultDataFileWriter.
type dataWriterInvariants struct {
	statsCols       map[int]tblutils.StatisticsCollector
	colMapping      map[string]int
	variantFieldIDs map[int]struct{}
	writeProps      any
	rowGroupBytes   int64
	spec            iceberg.PartitionSpec
	extension       string
	arrowSchema     *arrow.Schema
	schemaOpts      SchemaOptions
}

type dataFileWriterOption func(writer *defaultDataFileWriter)

func withFormat(format tblutils.FileFormat) dataFileWriterOption {
	return func(writer *defaultDataFileWriter) {
		writer.format = format
	}
}

func withFileSchema(schema *iceberg.Schema) dataFileWriterOption {
	return func(writer *defaultDataFileWriter) {
		writer.fileSchema = schema
	}
}

func withContent(content iceberg.ManifestEntryContent) dataFileWriterOption {
	return func(writer *defaultDataFileWriter) {
		writer.content = content
	}
}

func withEqualityFieldIDs(ids []int) dataFileWriterOption {
	return func(writer *defaultDataFileWriter) {
		writer.equalityFieldIDs = ids
	}
}

func newDataFileWriter(rootLocation string, fs io.WriteFileIO, meta *MetadataBuilder, props iceberg.Properties, opts ...dataFileWriterOption) (*defaultDataFileWriter, error) {
	locProvider, err := LoadLocationProvider(rootLocation, props)
	if err != nil {
		return nil, err
	}

	fileFormat, err := iceberg.FileFormatFromString(
		props.Get(WriteFormatDefaultKey, WriteFormatDefaultDefault))
	if err != nil {
		return nil, err
	}
	w := defaultDataFileWriter{
		loc:            locProvider,
		fs:             fs,
		fileSchema:     meta.CurrentSchema(),
		propertySchema: meta.CurrentSchema(),
		fileFormat:     fileFormat,
		format:         tblutils.GetFileFormat(fileFormat),
		content:        iceberg.EntryContentData,
		props:          props,
	}
	for _, apply := range opts {
		apply(&w)
	}
	if w.fileFormat == iceberg.ParquetFile {
		w.props, err = parquetPropsFor(w.content, w.props)
		if err != nil {
			return nil, err
		}
	}

	statsCols, err := computeStatsPlan(w.fileSchema, metricsPropsFor(w.content, meta.props))
	if err != nil {
		return nil, err
	}

	currentSpec, err := meta.CurrentSpec()
	if err != nil {
		return nil, err
	}

	var rowGroupBytes int64
	if w.fileFormat == iceberg.ParquetFile {
		rowGroupBytes, err = tblutils.ParquetRowGroupTargetSizeBytes(w.props)
		if err != nil {
			return nil, err
		}
	}
	arrowSchema, err := SchemaToArrowSchemaWithOptions(w.fileSchema, ArrowSchemaOptions{
		IncludeFieldIDs: true,
		TableProperties: meta.props,
	})
	if err != nil {
		return nil, err
	}

	var colMapping map[string]int
	var variantFieldIDs map[int]struct{}
	writeProps := w.props
	if w.fileFormat == iceberg.ParquetFile {
		colMapping, err = w.format.PathToIDMapping(w.fileSchema)
		if err != nil {
			return nil, err
		}
		variantFieldIDs = tblutils.VariantFieldIDsFromSchema(w.fileSchema)
		writeProps = tblutils.ResolveParquetStatsColumnPaths(w.props, meta.CurrentSchema(), w.fileSchema, colMapping, w.content)
	}

	w.invariants = dataWriterInvariants{
		statsCols:       statsCols,
		colMapping:      colMapping,
		variantFieldIDs: variantFieldIDs,
		writeProps:      w.format.GetWriteProperties(writeProps),
		rowGroupBytes:   rowGroupBytes,
		spec:            *currentSpec,
		extension:       strings.ToLower(string(w.fileFormat)),
		arrowSchema:     arrowSchema,
		schemaOpts: SchemaOptions{
			DowncastTimestamp: true,
			IncludeFieldIDs:   true,
			UseWriteDefault:   true,
			TableProperties:   meta.props,
		},
	}

	return &w, nil
}

func (w *defaultDataFileWriter) writeFile(ctx context.Context, partitionValues map[int]any, task WriteTask) (iceberg.DataFile, error) {
	defer func() {
		for _, b := range task.Batches {
			b.Release()
		}
	}()

	// Reject here on the caller's goroutine, not deep in the writer's Close().
	if task.SortOrderID != UnsortedSortOrderID && w.content == iceberg.EntryContentPosDeletes {
		return nil, fmt.Errorf("position delete file claims sort order id %d; the spec requires a null sort order id for position deletes",
			task.SortOrderID)
	}

	batches := make([]arrow.RecordBatch, len(task.Batches))
	for i, b := range task.Batches {
		rec, err := toRequestedSchema(ctx, w.fileSchema,
			task.Schema, b, w.invariants.schemaOpts, w.invariants.arrowSchema)
		if err != nil {
			return nil, err
		}
		batches[i] = rec
		defer rec.Release()
	}

	filePath := w.loc.NewDataLocation(
		task.GenerateDataFileName(w.invariants.extension), "")

	return w.format.WriteDataFile(ctx, w.fs, partitionValues, tblutils.WriteFileInfo{
		FileSchema:       w.fileSchema,
		PropertySchema:   w.propertySchema,
		Content:          w.content,
		FileName:         filePath,
		StatsCols:        w.invariants.statsCols,
		ColMapping:       w.invariants.colMapping,
		VariantFieldIDs:  w.invariants.variantFieldIDs,
		WriteProps:       w.invariants.writeProps,
		RowGroupBytes:    w.invariants.rowGroupBytes,
		Spec:             w.invariants.spec,
		EqualityFieldIDs: w.equalityFieldIDs,
		SortOrderID:      task.SortOrderID,
	}, batches)
}

type dataFileWriter interface {
	writeFile(ctx context.Context, partitionValues map[int]any, task WriteTask) (iceberg.DataFile, error)
}

// positionDeleteMetricsProps mirrors Java's MetricsConfig.forPositionDelete:
// position delete files keep full file_path and pos bounds whatever the table
// metrics settings are, so readers and conflict checks can resolve the data
// file a delete file targets from equal file_path bounds.
//
// The default mode is none on purpose, as in Java: any column besides
// file_path and pos, such as a future row column, gets no value counts, null
// counts, column sizes or bounds. The table's write.metadata.metrics.*
// settings do not apply to position delete files.
//
// The map is shared and handed to computeStatsPlan as is; treat it as
// read-only.
var positionDeleteMetricsProps = iceberg.Properties{
	DefaultWriteMetricsModeKey:                 string(tblutils.MetricModeNone),
	MetricsModeColumnConfPrefix + ".file_path": string(tblutils.MetricModeFull),
	MetricsModeColumnConfPrefix + ".pos":       string(tblutils.MetricModeFull),
}

// metricsPropsFor returns the properties that drive column metrics for files
// of the given content type.
func metricsPropsFor(content iceberg.ManifestEntryContent, props iceberg.Properties) iceberg.Properties {
	if content == iceberg.EntryContentPosDeletes {
		return positionDeleteMetricsProps
	}

	return props
}

// parquetPropsFor mirrors Java's deleteContext: delete settings override the
// corresponding data settings. Absent overrides inherit the data values, except
// that a different delete codec uses its own default compression level.
// Metrics use the original table properties independently of this writer config.
func parquetPropsFor(content iceberg.ManifestEntryContent, props iceberg.Properties) (iceberg.Properties, error) {
	if content == iceberg.EntryContentData {
		return props, tblutils.ValidateParquetWriteProperties(props)
	}

	effective := maps.Clone(props)
	overrides := map[string]string{
		DeleteParquetRowGroupSizeBytesKey: ParquetRowGroupSizeBytesKey,
		DeleteParquetPageSizeBytesKey:     ParquetPageSizeBytesKey,
		DeleteParquetPageRowLimitKey:      ParquetPageRowLimitKey,
		DeleteParquetDictSizeBytesKey:     ParquetDictSizeBytesKey,
		DeleteParquetPageVersionKey:       ParquetPageVersionKey,
		DeleteParquetCompressionKey:       ParquetCompressionKey,
		DeleteParquetCompressionLevelKey:  ParquetCompressionLevelKey,
	}
	for deleteKey, dataKey := range overrides {
		if value, ok := props[deleteKey]; ok {
			effective[dataKey] = value
		}
	}
	if value, overridden := props[DeleteParquetCompressionKey]; overridden {
		codec, ok := tblutils.ParquetCompressionCodec(value)
		if !ok {
			return nil, fmt.Errorf("%w: unsupported %s value %q", iceberg.ErrInvalidArgument, DeleteParquetCompressionKey, value)
		}
		if _, err := compress.GetCodec(codec); err != nil {
			return nil, fmt.Errorf("%w: unsupported %s value %q: %v", iceberg.ErrInvalidArgument, DeleteParquetCompressionKey, value, err)
		}
		dataCodec, _ := tblutils.ParquetCompressionCodec(props.Get(ParquetCompressionKey, ParquetCompressionDefault))
		if _, hasLevel := props[DeleteParquetCompressionLevelKey]; !hasLevel && codec != dataCodec {
			effective[ParquetCompressionLevelKey] = strconv.Itoa(ParquetCompressionLevelDefault)
		}
	}
	if version, overridden := props[DeleteParquetPageVersionKey]; overridden {
		switch strings.ToLower(version) {
		case "1", "2", "v1", "v2":
		default:
			return nil, fmt.Errorf("%w: unsupported %s value %q", iceberg.ErrInvalidArgument, DeleteParquetPageVersionKey, version)
		}
	}
	// Java does not apply data-column Bloom filters or dictionary overrides to
	// delete files, whose schema may differ from the data schema. Drop Bloom
	// settings before validation, including settings supported by other writers.
	maps.DeleteFunc(effective, func(key, _ string) bool {
		return strings.HasPrefix(key, "write.parquet.bloom-filter-") ||
			strings.HasPrefix(key, tblutils.ParquetDictEncodingColumnEnabledKeyPrefix+".")
	})
	for deleteKey, dataKey := range overrides {
		value, overridden := props[deleteKey]
		if !overridden || dataKey == ParquetCompressionKey || dataKey == ParquetPageVersionKey {
			continue
		}
		setting := iceberg.Properties{dataKey: value}
		if dataKey == ParquetCompressionLevelKey {
			setting[ParquetCompressionKey] = effective.Get(ParquetCompressionKey, ParquetCompressionDefault)
		}
		if err := tblutils.ValidateParquetWriteProperties(setting); err != nil {
			return nil, fmt.Errorf("%s: %w", deleteKey, err)
		}
	}
	if err := tblutils.ValidateParquetWriteProperties(effective); err != nil {
		return nil, err
	}

	return effective, nil
}

func newPositionDeleteWriter(rootLocation string, fs io.WriteFileIO, meta *MetadataBuilder, props iceberg.Properties, opts ...dataFileWriterOption) (*defaultDataFileWriter, error) {
	// Always enforce the file schema to be the Positional Delete Schema by appending the option at the very end
	return newDataFileWriter(rootLocation, fs, meta, props, append(opts, withFileSchema(iceberg.PositionalDeleteSchema), withContent(iceberg.EntryContentPosDeletes))...)
}

func newEqualityDeleteWriter(rootLocation string, fs io.WriteFileIO, meta *MetadataBuilder, props iceberg.Properties, opts ...dataFileWriterOption) (*defaultDataFileWriter, error) {
	return newDataFileWriter(rootLocation, fs, meta, props,
		append(opts, withContent(iceberg.EntryContentEqDeletes))...)
}

func newEqualityDeleteWriterMaker(deleteSchema *iceberg.Schema, equalityFieldIDs []int) dataFileWriterMaker {
	return func(rootLocation string, fs io.WriteFileIO, meta *MetadataBuilder, props iceberg.Properties, opts ...dataFileWriterOption) (dataFileWriter, error) {
		return newEqualityDeleteWriter(rootLocation, fs, meta, props,
			append(opts, withFileSchema(deleteSchema), withEqualityFieldIDs(equalityFieldIDs))...)
	}
}

type dataFileWriterMaker func(rootLocation string, fs io.WriteFileIO, meta *MetadataBuilder, props iceberg.Properties, opts ...dataFileWriterOption) (dataFileWriter, error)

type concurrentDataFileWriter struct {
	newDataFileWriter dataFileWriterMaker
	sanitizeSchema    bool
}

type concurrentDataFileWriterOption func(w *concurrentDataFileWriter)

func withSchemaSanitization(enabled bool) concurrentDataFileWriterOption {
	return func(w *concurrentDataFileWriter) {
		w.sanitizeSchema = enabled
	}
}

func newConcurrentDataFileWriter(newDataFileWriter dataFileWriterMaker, opts ...concurrentDataFileWriterOption) *concurrentDataFileWriter {
	w := concurrentDataFileWriter{
		newDataFileWriter: newDataFileWriter,
		sanitizeSchema:    true,
	}
	for _, apply := range opts {
		apply(&w)
	}

	return &w
}

func (w *concurrentDataFileWriter) writeFiles(ctx context.Context, rootLocation string, fs io.WriteFileIO, meta *MetadataBuilder, props iceberg.Properties, partitionValues map[int]any, tasks iter.Seq[WriteTask]) iter.Seq2[iceberg.DataFile, error] {
	fileSchema := meta.CurrentSchema()
	if w.sanitizeSchema {
		sanitized, err := iceberg.SanitizeColumnNames(fileSchema)
		if err != nil {
			return func(yield func(iceberg.DataFile, error) bool) {
				yield(nil, err)
			}
		}

		// if the schema needs to be transformed, use the transformed schema
		// and adjust the arrow schema appropriately. otherwise we just
		// use the original schema.
		if !sanitized.Equals(fileSchema) {
			fileSchema = sanitized
		}
	}

	fw, err := w.newDataFileWriter(rootLocation, fs, meta, props, withFileSchema(fileSchema))
	if err != nil {
		return func(yield func(iceberg.DataFile, error) bool) {
			yield(nil, err)
		}
	}

	return tblutils.MapExecWithDiscard(ctx, config.EnvConfig.MaxWorkers, tasks, func(t WriteTask) (iceberg.DataFile, error) {
		return fw.writeFile(ctx, partitionValues, t)
	}, func(t WriteTask) { releasePackedRecords(t.Batches) })
}
