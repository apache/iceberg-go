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
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/config"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
)

type WriteTask struct {
	Uuid        uuid.UUID
	ID          int
	PartitionID int // PartitionID is the partition identifier used in data file naming.
	FileCount   int // FileCount is a sequential counter for files written by this task.
	Schema      *iceberg.Schema
	Batches     []arrow.RecordBatch
	SortOrderID int
}

func (w WriteTask) GenerateDataFileName(extension string) string {
	// Mimics the behavior in the Java API:
	// https://github.com/apache/iceberg/blob/03ed4ba9af4e47d32bdb22b7e3d033eb2a4b2c83/core/src/main/java/org/apache/iceberg/io/OutputFileFactory.java#L93
	// Format: {partitionId:05d}-{taskId}-{operationId}-{fileCount:05d}.{extension}
	return fmt.Sprintf("%05d-%d-%s-%05d.%s", w.PartitionID, w.ID, w.Uuid, w.FileCount, extension)
}

type defaultDataFileWriter struct {
	loc        LocationProvider
	fs         io.WriteFileIO
	fileSchema *iceberg.Schema
	format     internal.FileFormat
	props      iceberg.Properties
	content    iceberg.ManifestEntryContent
	meta       *MetadataBuilder
}

type dataFileWriterOption func(writer *defaultDataFileWriter)

func withFormat(format internal.FileFormat) dataFileWriterOption {
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

func newDataFileWriter(rootLocation string, fs io.WriteFileIO, meta *MetadataBuilder, props iceberg.Properties, opts ...dataFileWriterOption) (*defaultDataFileWriter, error) {
	locProvider, err := LoadLocationProvider(rootLocation, props)
	if err != nil {
		return nil, err
	}
	w := defaultDataFileWriter{
		loc:        locProvider,
		fs:         fs,
		fileSchema: meta.CurrentSchema(),
		format:     internal.GetFileFormat(iceberg.ParquetFile),
		content:    iceberg.EntryContentData,
		props:      props,
		meta:       meta,
	}
	for _, apply := range opts {
		apply(&w)
	}

	return &w, nil
}

func (w *defaultDataFileWriter) writeFile(ctx context.Context, partitionValues map[int]any, task WriteTask) (iceberg.DataFile, error) {
	defer func() {
		for _, b := range task.Batches {
			b.Release()
		}
	}()

	batches := make([]arrow.RecordBatch, len(task.Batches))
	for i, b := range task.Batches {
		rec, err := ToRequestedSchema(ctx, w.fileSchema,
			task.Schema, b, false, true, false)
		if err != nil {
			return nil, err
		}
		batches[i] = rec
		defer rec.Release()
	}

	statsCols, err := computeStatsPlan(w.fileSchema, w.meta.props)
	if err != nil {
		return nil, err
	}

	filePath := w.loc.NewDataLocation(
		task.GenerateDataFileName("parquet"))

	currentSpec, err := w.meta.CurrentSpec()
	if err != nil {
		return nil, err
	}

	return w.format.WriteDataFile(ctx, w.fs, partitionValues, internal.WriteFileInfo{
		FileSchema: w.fileSchema,
		Content:    w.content,
		FileName:   filePath,
		StatsCols:  statsCols,
		WriteProps: w.format.GetWriteProperties(w.props),
		Spec:       *currentSpec,
	}, batches)
}

type dataFileWriter interface {
	writeFile(ctx context.Context, partitionValues map[int]any, task WriteTask) (iceberg.DataFile, error)
}

func newPositionDeleteWriter(rootLocation string, fs io.WriteFileIO, meta *MetadataBuilder, props iceberg.Properties, opts ...dataFileWriterOption) (*defaultDataFileWriter, error) {
	// Always enforce the file schema to be the Positional Delete Schema by appending the option at the very end
	return newDataFileWriter(rootLocation, fs, meta, props, append(opts, withFileSchema(iceberg.PositionalDeleteSchema), withContent(iceberg.EntryContentPosDeletes))...)
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

	return internal.MapExec(config.EnvConfig.MaxWorkers, tasks, func(t WriteTask) (iceberg.DataFile, error) {
		return fw.writeFile(ctx, partitionValues, t)
	})
}
