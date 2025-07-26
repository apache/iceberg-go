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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/internal"
)

// DeleteWriter is an interface for writing delete files.
type DeleteWriter interface {
	io.Closer
	// Write adds a delete record to the writer
	Write(ctx context.Context, delete interface{}) error
	// Flush writes pending deletes to storage
	Flush(ctx context.Context) ([]iceberg.DataFile, error)
	// GetResult returns the final delete files created
	GetResult() []iceberg.DataFile
}

// EqualityDeleteWriter writes equality delete files.
type EqualityDeleteWriter struct {
	fs             iceio.WriteFileIO
	locProvider    LocationProvider
	schema         *iceberg.Schema
	equalityFields []int
	fileFormat     internal.FileFormat
	writeProps     interface{}
	mem            memory.Allocator
	
	// Current batch being built
	currentBatch   []arrow.Record
	currentPath    string
	writtenFiles   []iceberg.DataFile
	maxBatchSize   int
}

// LocationProvider provides file locations for delete files.
type LocationProvider interface {
	NewDeleteLocation(extension string) string
}

// NewEqualityDeleteWriter creates a new equality delete writer.
func NewEqualityDeleteWriter(
	fs iceio.WriteFileIO,
	locProvider LocationProvider,
	tableSchema *iceberg.Schema,
	equalityFields []int,
	fileFormat internal.FileFormat,
	writeProps interface{},
	mem memory.Allocator,
) *EqualityDeleteWriter {
	return &EqualityDeleteWriter{
		fs:             fs,
		locProvider:    locProvider,
		schema:         iceberg.EqualityDeleteSchema(equalityFields, tableSchema),
		equalityFields: equalityFields,
		fileFormat:     fileFormat,
		writeProps:     writeProps,
		mem:            mem,
		maxBatchSize:   1000, // Default batch size
		currentBatch:   make([]arrow.Record, 0),
		writtenFiles:   make([]iceberg.DataFile, 0),
	}
}

// Write adds an equality delete to the writer.
func (w *EqualityDeleteWriter) Write(ctx context.Context, delete interface{}) error {
	eq, ok := delete.(*EqualityDelete)
	if !ok {
		return fmt.Errorf("expected *EqualityDelete, got %T", delete)
	}
	
	// Convert equality delete to arrow record
	record, err := w.equalityDeleteToRecord(eq)
	if err != nil {
		return fmt.Errorf("converting equality delete to record: %w", err)
	}
	
	w.currentBatch = append(w.currentBatch, record)
	
	// Flush if batch is full
	if len(w.currentBatch) >= w.maxBatchSize {
		if _, err := w.Flush(ctx); err != nil {
			return err
		}
	}
	
	return nil
}

// Flush writes the current batch to a delete file.
func (w *EqualityDeleteWriter) Flush(ctx context.Context) ([]iceberg.DataFile, error) {
	if len(w.currentBatch) == 0 {
		return nil, nil
	}
	
	filePath := w.locProvider.NewDeleteLocation("parquet")
	
	dataFile, err := w.fileFormat.WriteDataFile(ctx, w.fs, internal.WriteFileInfo{
		FileSchema: w.schema,
		FileName:   filePath,
		StatsCols:  make(map[int]internal.StatisticsCollector), // TODO: Add stats collectors
		WriteProps: w.writeProps,
	}, w.currentBatch)
	
	if err != nil {
		return nil, fmt.Errorf("writing equality delete file: %w", err)
	}
	
	// Convert to equality delete file
	eqDeleteFile := &equalityDataFile{
		DataFile:       dataFile,
		equalityFields: w.equalityFields,
	}
	
	w.writtenFiles = append(w.writtenFiles, eqDeleteFile)
	
	// Clear the current batch
	for _, record := range w.currentBatch {
		record.Release()
	}
	w.currentBatch = w.currentBatch[:0]
	
	return []iceberg.DataFile{eqDeleteFile}, nil
}

// GetResult returns all written delete files.
func (w *EqualityDeleteWriter) GetResult() []iceberg.DataFile {
	return w.writtenFiles
}

// Close flushes any remaining data and closes the writer.
func (w *EqualityDeleteWriter) Close() error {
	// Release any remaining records in the current batch
	for _, record := range w.currentBatch {
		record.Release()
	}
	w.currentBatch = nil
	return nil
}

// equalityDeleteToRecord converts an EqualityDelete to an Arrow record.
func (w *EqualityDeleteWriter) equalityDeleteToRecord(eq *EqualityDelete) (arrow.Record, error) {
	fields := w.schema.Fields()
	builders := make([]array.Builder, len(fields))
	
	// Create builders for each field
	for i, field := range fields {
		arrowType, err := table.TypeToArrowType(field.Type, false, false)
		if err != nil {
			return nil, fmt.Errorf("converting field type to arrow: %w", err)
		}
		builders[i] = array.NewBuilder(w.mem, arrowType)
	}
	defer func() {
		for _, builder := range builders {
			builder.Release()
		}
	}()
	
	// Add file_path (first field)
	builders[0].(*array.StringBuilder).Append(eq.FilePath)
	
	// Add equality field values
	for i := 1; i < len(fields); i++ {
		fieldID := fields[i].ID
		if value, ok := eq.Values[fieldID]; ok {
			if err := appendValueToBuilder(builders[i], value, fields[i].Type); err != nil {
				return nil, err
			}
		} else {
			builders[i].AppendNull()
		}
	}
	
	// Build arrays
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
	}
	defer func() {
		for _, arr := range arrays {
			arr.Release()
		}
	}()
	
	// Create record
	schema, err := table.SchemaToArrowSchema(w.schema, nil, false, false)
	if err != nil {
		return nil, err
	}
	
	return array.NewRecord(schema, arrays, 1), nil
}

// appendValueToBuilder appends a value to an arrow builder based on the field type.
func appendValueToBuilder(builder array.Builder, value interface{}, fieldType iceberg.Type) error {
	// This is a simplified implementation - a full implementation would handle all Iceberg types
	switch builder := builder.(type) {
	case *array.StringBuilder:
		if str, ok := value.(string); ok {
			builder.Append(str)
		} else {
			builder.Append(fmt.Sprintf("%v", value))
		}
	case *array.Int32Builder:
		if i32, ok := value.(int32); ok {
			builder.Append(i32)
		} else if i, ok := value.(int); ok {
			builder.Append(int32(i))
		} else {
			return fmt.Errorf("cannot convert %T to int32", value)
		}
	case *array.Int64Builder:
		if i64, ok := value.(int64); ok {
			builder.Append(i64)
		} else if i, ok := value.(int); ok {
			builder.Append(int64(i))
		} else {
			return fmt.Errorf("cannot convert %T to int64", value)
		}
	case *array.Float32Builder:
		if f32, ok := value.(float32); ok {
			builder.Append(f32)
		} else if f64, ok := value.(float64); ok {
			builder.Append(float32(f64))
		} else {
			return fmt.Errorf("cannot convert %T to float32", value)
		}
	case *array.Float64Builder:
		if f64, ok := value.(float64); ok {
			builder.Append(f64)
		} else if f32, ok := value.(float32); ok {
			builder.Append(float64(f32))
		} else {
			return fmt.Errorf("cannot convert %T to float64", value)
		}
	case *array.BooleanBuilder:
		if b, ok := value.(bool); ok {
			builder.Append(b)
		} else {
			return fmt.Errorf("cannot convert %T to bool", value)
		}
	default:
		// For other types, append null for now
		builder.AppendNull()
	}
	return nil
}

// equalityDataFile wraps a DataFile to indicate it's an equality delete file.
type equalityDataFile struct {
	iceberg.DataFile
	equalityFields []int
}

// ContentType returns that this is an equality delete file.
func (e *equalityDataFile) ContentType() iceberg.ManifestEntryContent {
	return iceberg.EntryContentEqDeletes
}

// EqualityFieldIDs returns the field IDs used for equality comparison.
func (e *equalityDataFile) EqualityFieldIDs() []int {
	return e.equalityFields
}

// RollingEqualityDeleteWriter writes equality deletes across multiple files with size limits.
type RollingEqualityDeleteWriter struct {
	currentWriter *EqualityDeleteWriter
	fs            iceio.WriteFileIO
	locProvider   LocationProvider
	tableSchema   *iceberg.Schema
	equalityFields []int
	fileFormat    internal.FileFormat
	writeProps    interface{}
	mem           memory.Allocator
	maxFileSize   int64
	currentSize   int64
	writtenFiles  []iceberg.DataFile
}

// NewRollingEqualityDeleteWriter creates a new rolling equality delete writer.
func NewRollingEqualityDeleteWriter(
	fs iceio.WriteFileIO,
	locProvider LocationProvider,
	tableSchema *iceberg.Schema,
	equalityFields []int,
	fileFormat internal.FileFormat,
	writeProps interface{},
	mem memory.Allocator,
	maxFileSize int64,
) *RollingEqualityDeleteWriter {
	return &RollingEqualityDeleteWriter{
		fs:             fs,
		locProvider:    locProvider,
		tableSchema:    tableSchema,
		equalityFields: equalityFields,
		fileFormat:     fileFormat,
		writeProps:     writeProps,
		mem:            mem,
		maxFileSize:    maxFileSize,
		writtenFiles:   make([]iceberg.DataFile, 0),
	}
}

// Write adds an equality delete, potentially rolling to a new file if size limit is reached.
func (w *RollingEqualityDeleteWriter) Write(ctx context.Context, delete interface{}) error {
	if w.currentWriter == nil {
		w.currentWriter = NewEqualityDeleteWriter(
			w.fs, w.locProvider, w.tableSchema, w.equalityFields,
			w.fileFormat, w.writeProps, w.mem,
		)
	}
	
	if err := w.currentWriter.Write(ctx, delete); err != nil {
		return err
	}
	
	// Estimate size increase (simplified)
	w.currentSize += 100 // Rough estimate per record
	
	// Roll to new file if size limit reached
	if w.currentSize >= w.maxFileSize {
		if err := w.rollFile(ctx); err != nil {
			return err
		}
	}
	
	return nil
}

// rollFile finishes the current file and starts a new one.
func (w *RollingEqualityDeleteWriter) rollFile(ctx context.Context) error {
	if w.currentWriter == nil {
		return nil
	}
	
	files, err := w.currentWriter.Flush(ctx)
	if err != nil {
		return err
	}
	
	w.writtenFiles = append(w.writtenFiles, files...)
	
	if err := w.currentWriter.Close(); err != nil {
		return err
	}
	
	w.currentWriter = nil
	w.currentSize = 0
	return nil
}

// Flush writes any pending deletes.
func (w *RollingEqualityDeleteWriter) Flush(ctx context.Context) ([]iceberg.DataFile, error) {
	if err := w.rollFile(ctx); err != nil {
		return nil, err
	}
	return w.writtenFiles, nil
}

// GetResult returns all written delete files.
func (w *RollingEqualityDeleteWriter) GetResult() []iceberg.DataFile {
	return w.writtenFiles
}

// Close closes the writer and any current file.
func (w *RollingEqualityDeleteWriter) Close() error {
	if w.currentWriter != nil {
		return w.currentWriter.Close()
	}
	return nil
} 