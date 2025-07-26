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

package deletes

import (
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// PositionDeleteWriter provides an interface for writing position delete files.
type PositionDeleteWriter interface {
	// Write writes a position delete record to the file.
	Write(ctx context.Context, filePath string, position int64) error

	// WritePositionDelete writes a PositionDelete to the file.
	WritePositionDelete(ctx context.Context, delete *PositionDelete) error

	// WriteBatch writes multiple position deletes to the file.
	WriteBatch(ctx context.Context, deletes []*PositionDelete) error

	// WriteIndex writes all position deletes from an index to the file.
	WriteIndex(ctx context.Context, index PositionDeleteIndex) error

	// Close closes the writer and flushes any remaining data.
	Close() error

	// DataFile returns the data file metadata for the written delete file.
	DataFile() iceberg.DataFile

	// WrittenRecords returns the number of records written.
	WrittenRecords() int64

	// WrittenBytes returns the number of bytes written.
	WrittenBytes() int64
}

// PositionDeleteWriterConfig contains configuration for position delete writers.
type PositionDeleteWriterConfig struct {
	// Schema is the schema for the position delete file (file_path, pos)
	Schema *iceberg.Schema

	// FileIO is the file IO interface for writing files
	FileIO iceio.IO

	// OutputPath is the path where the delete file will be written
	OutputPath string

	// PartitionSpec is the partition spec for the table
	PartitionSpec iceberg.PartitionSpec

	// SortColumns specifies columns to sort the output by (optional)
	SortColumns []string

	// FileFormat is the format to write (defaults to Parquet)
	FileFormat iceberg.FileFormat

	// Properties contains additional writer properties
	Properties map[string]string

	// Allocator for Arrow memory management
	Allocator memory.Allocator
}

// BasePositionDeleteWriter provides a base implementation for position delete writers.
type BasePositionDeleteWriter struct {
	config        PositionDeleteWriterConfig
	writer        io.WriteCloser
	parquetWriter *pqarrow.FileWriter
	records       []*PositionDelete
	recordCount   int64
	bytesWritten  int64
	closed        bool
}

// NewPositionDeleteWriter creates a new position delete writer with the given configuration.
func NewPositionDeleteWriter(config PositionDeleteWriterConfig) (PositionDeleteWriter, error) {
	if config.Schema == nil {
		config.Schema = &iceberg.PositionalDeleteSchema
	}

	if config.FileFormat == "" {
		config.FileFormat = iceberg.FileFormatParquet
	}

	if config.Allocator == nil {
		config.Allocator = memory.DefaultAllocator
	}

	writer := &BasePositionDeleteWriter{
		config:  config,
		records: make([]*PositionDelete, 0),
	}

	if err := writer.initialize(); err != nil {
		return nil, err
	}

	return writer, nil
}

// initialize sets up the underlying file writer.
func (w *BasePositionDeleteWriter) initialize() error {
	file, err := w.config.FileIO.Create(w.config.OutputPath)
	if err != nil {
		return fmt.Errorf("failed to create delete file: %w", err)
	}

	w.writer = file

	// Convert Iceberg schema to Arrow schema
	arrowSchema, err := w.schemaToArrow()
	if err != nil {
		w.writer.Close()
		return fmt.Errorf("failed to convert schema: %w", err)
	}

	// Set up Parquet writer properties
	props := parquet.NewWriterProperties()
	if compression, ok := w.config.Properties["compression"]; ok {
		switch compression {
		case "snappy":
			props = parquet.NewWriterProperties(parquet.WithCompression(parquet.Compressions.Snappy))
		case "gzip":
			props = parquet.NewWriterProperties(parquet.WithCompression(parquet.Compressions.Gzip))
		case "lz4":
			props = parquet.NewWriterProperties(parquet.WithCompression(parquet.Compressions.Lz4))
		case "zstd":
			props = parquet.NewWriterProperties(parquet.WithCompression(parquet.Compressions.Zstd))
		}
	}

	arrowProps := pqarrow.NewArrowWriterProperties()

	parquetWriter, err := pqarrow.NewFileWriter(arrowSchema, w.writer, props, arrowProps)
	if err != nil {
		w.writer.Close()
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}

	w.parquetWriter = parquetWriter
	return nil
}

// schemaToArrow converts the Iceberg schema to Arrow schema.
func (w *BasePositionDeleteWriter) schemaToArrow() (*arrow.Schema, error) {
	fields := []arrow.Field{
		{Name: "file_path", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "pos", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}

	return arrow.NewSchema(fields, nil), nil
}

// Write writes a position delete record to the file.
func (w *BasePositionDeleteWriter) Write(ctx context.Context, filePath string, position int64) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	delete := NewPositionDelete(filePath, position)
	w.records = append(w.records, delete)
	w.recordCount++

	return nil
}

// WritePositionDelete writes a PositionDelete to the file.
func (w *BasePositionDeleteWriter) WritePositionDelete(ctx context.Context, delete *PositionDelete) error {
	if delete == nil {
		return nil
	}
	return w.Write(ctx, delete.FilePath, delete.Position)
}

// WriteBatch writes multiple position deletes to the file.
func (w *BasePositionDeleteWriter) WriteBatch(ctx context.Context, deletes []*PositionDelete) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	for _, delete := range deletes {
		if delete != nil {
			w.records = append(w.records, delete)
			w.recordCount++
		}
	}

	return nil
}

// WriteIndex writes all position deletes from an index to the file.
func (w *BasePositionDeleteWriter) WriteIndex(ctx context.Context, index PositionDeleteIndex) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	for filePath := range index.Files() {
		for position := range index.Deletes(filePath) {
			delete := NewPositionDelete(filePath, position)
			w.records = append(w.records, delete)
			w.recordCount++
		}
	}

	return nil
}

// Close closes the writer and flushes any remaining data.
func (w *BasePositionDeleteWriter) Close() error {
	if w.closed {
		return nil
	}

	defer func() {
		w.closed = true
		if w.writer != nil {
			w.writer.Close()
		}
	}()

	// Sort records for consistent output (optional, based on sort columns)
	if len(w.config.SortColumns) > 0 || len(w.records) > 0 {
		w.sortRecords()
	}

	// Write all accumulated records
	if len(w.records) > 0 {
		if err := w.writeRecords(); err != nil {
			return fmt.Errorf("failed to write records: %w", err)
		}
	}

	// Close parquet writer
	if w.parquetWriter != nil {
		if err := w.parquetWriter.Close(); err != nil {
			return fmt.Errorf("failed to close parquet writer: %w", err)
		}
	}

	return nil
}

// sortRecords sorts the accumulated records.
func (w *BasePositionDeleteWriter) sortRecords() {
	sort.Slice(w.records, func(i, j int) bool {
		return w.records[i].Compare(w.records[j]) < 0
	})
}

// writeRecords writes the accumulated records to the parquet file.
func (w *BasePositionDeleteWriter) writeRecords() error {
	if len(w.records) == 0 {
		return nil
	}

	// Build Arrow arrays
	filePathBuilder := array.NewStringBuilder(w.config.Allocator)
	positionBuilder := array.NewInt64Builder(w.config.Allocator)

	filePathBuilder.Reserve(len(w.records))
	positionBuilder.Reserve(len(w.records))

	for _, record := range w.records {
		filePathBuilder.Append(record.FilePath)
		positionBuilder.Append(record.Position)
	}

	filePathArray := filePathBuilder.NewArray()
	positionArray := positionBuilder.NewArray()

	filePathBuilder.Release()
	positionBuilder.Release()

	// Create record batch
	schema, _ := w.schemaToArrow()
	recordBatch := array.NewRecord(schema, []arrow.Array{filePathArray, positionArray}, int64(len(w.records)))

	// Write record batch
	err := w.parquetWriter.Write(recordBatch)
	if err != nil {
		recordBatch.Release()
		filePathArray.Release()
		positionArray.Release()
		return err
	}

	// Calculate bytes written (approximate)
	w.bytesWritten += int64(len(w.records) * (8 + 16)) // rough estimate

	// Clean up
	recordBatch.Release()
	filePathArray.Release()
	positionArray.Release()

	return nil
}

// DataFile returns the data file metadata for the written delete file.
func (w *BasePositionDeleteWriter) DataFile() iceberg.DataFile {
	return iceberg.DataFileBuilder{}.
		ContentType(iceberg.EntryContentPosDeletes).
		Path(w.config.OutputPath).
		Format(w.config.FileFormat).
		RecordCount(w.recordCount).
		FileSizeBytes(w.bytesWritten).
		PartitionData(make(map[string]any)). // Empty partition data for delete files
		Build()
}

// WrittenRecords returns the number of records written.
func (w *BasePositionDeleteWriter) WrittenRecords() int64 {
	return w.recordCount
}

// WrittenBytes returns the number of bytes written.
func (w *BasePositionDeleteWriter) WrittenBytes() int64 {
	return w.bytesWritten
} 