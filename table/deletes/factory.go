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

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/internal"
)

// DeleteWriterType represents the type of delete writer to create.
type DeleteWriterType int

const (
	// BasicDeleteWriter creates a simple equality delete writer
	BasicDeleteWriter DeleteWriterType = iota
	// RollingDeleteWriter creates a rolling equality delete writer with file size limits
	RollingDeleteWriter
	// ClusteredDeleteWriter creates a clustered equality delete writer (placeholder for future implementation)
	ClusteredDeleteWriter
)

// DeleteWriterConfig contains configuration for creating delete writers.
type DeleteWriterConfig struct {
	// WriterType specifies the type of delete writer to create
	WriterType DeleteWriterType
	// FileSystem for writing delete files
	FileSystem iceio.WriteFileIO
	// LocationProvider for generating delete file locations
	LocationProvider LocationProvider
	// TableSchema is the schema of the table being written to
	TableSchema *iceberg.Schema
	// EqualityFields are the field IDs that define equality for delete records
	EqualityFields []int
	// FileFormat specifies the format for delete files (e.g., Parquet)
	FileFormat internal.FileFormat
	// WriteProperties contains format-specific write properties
	WriteProperties interface{}
	// MemoryAllocator for Arrow operations
	MemoryAllocator memory.Allocator
	// MaxFileSize for rolling writers (in bytes)
	MaxFileSize int64
}

// DeleteWriterFactory creates delete writers based on configuration.
type DeleteWriterFactory struct {
	config DeleteWriterConfig
}

// NewDeleteWriterFactory creates a new delete writer factory with the given configuration.
func NewDeleteWriterFactory(config DeleteWriterConfig) *DeleteWriterFactory {
	return &DeleteWriterFactory{config: config}
}

// CreateEqualityDeleteWriter creates an equality delete writer based on the factory configuration.
func (f *DeleteWriterFactory) CreateEqualityDeleteWriter() (DeleteWriter, error) {
	if len(f.config.EqualityFields) == 0 {
		return nil, fmt.Errorf("equality fields must be specified for equality delete writer")
	}

	switch f.config.WriterType {
	case BasicDeleteWriter:
		return NewEqualityDeleteWriter(
			f.config.FileSystem,
			f.config.LocationProvider,
			f.config.TableSchema,
			f.config.EqualityFields,
			f.config.FileFormat,
			f.config.WriteProperties,
			f.config.MemoryAllocator,
		), nil

	case RollingDeleteWriter:
		maxFileSize := f.config.MaxFileSize
		if maxFileSize <= 0 {
			maxFileSize = 128 * 1024 * 1024 // Default 128MB
		}
		return NewRollingEqualityDeleteWriter(
			f.config.FileSystem,
			f.config.LocationProvider,
			f.config.TableSchema,
			f.config.EqualityFields,
			f.config.FileFormat,
			f.config.WriteProperties,
			f.config.MemoryAllocator,
			maxFileSize,
		), nil

	case ClusteredDeleteWriter:
		// Placeholder for future clustered implementation
		return nil, fmt.Errorf("clustered delete writer not yet implemented")

	default:
		return nil, fmt.Errorf("unknown delete writer type: %d", f.config.WriterType)
	}
}

// CreatePositionDeleteWriter creates a position delete writer (placeholder for future implementation).
func (f *DeleteWriterFactory) CreatePositionDeleteWriter() (DeleteWriter, error) {
	return nil, fmt.Errorf("position delete writer not yet implemented")
}

// DefaultDeleteWriterConfig creates a default configuration for delete writers.
func DefaultDeleteWriterConfig(
	fs iceio.WriteFileIO,
	locProvider LocationProvider,
	tableSchema *iceberg.Schema,
	equalityFields []int,
	mem memory.Allocator,
) DeleteWriterConfig {
	return DeleteWriterConfig{
		WriterType:       BasicDeleteWriter,
		FileSystem:       fs,
		LocationProvider: locProvider,
		TableSchema:      tableSchema,
		EqualityFields:   equalityFields,
		FileFormat:       internal.GetFileFormat(iceberg.ParquetFile),
		WriteProperties:  internal.GetFileFormat(iceberg.ParquetFile).GetWriteProperties(nil),
		MemoryAllocator:  mem,
		MaxFileSize:      128 * 1024 * 1024, // 128MB default
	}
}

// SimpleLocationProvider provides basic file location generation for delete files.
type SimpleLocationProvider struct {
	baseLocation string
	fileCounter  int
}

// NewSimpleLocationProvider creates a simple location provider.
func NewSimpleLocationProvider(baseLocation string) *SimpleLocationProvider {
	return &SimpleLocationProvider{
		baseLocation: baseLocation,
		fileCounter:  0,
	}
}

// NewDeleteLocation generates a new location for a delete file.
func (p *SimpleLocationProvider) NewDeleteLocation(extension string) string {
	p.fileCounter++
	return fmt.Sprintf("%s/delete-%d.%s", p.baseLocation, p.fileCounter, extension)
}

// EqualityDeleteBatchWriter provides a high-level interface for writing batches of equality deletes.
type EqualityDeleteBatchWriter struct {
	writer    DeleteWriter
	batchSize int
	batch     []*EqualityDelete
}

// NewEqualityDeleteBatchWriter creates a batch writer for equality deletes.
func NewEqualityDeleteBatchWriter(writer DeleteWriter, batchSize int) *EqualityDeleteBatchWriter {
	if batchSize <= 0 {
		batchSize = 1000 // Default batch size
	}
	return &EqualityDeleteBatchWriter{
		writer:    writer,
		batchSize: batchSize,
		batch:     make([]*EqualityDelete, 0, batchSize),
	}
}

// Add adds an equality delete to the current batch.
func (w *EqualityDeleteBatchWriter) Add(delete *EqualityDelete) error {
	w.batch = append(w.batch, delete)
	if len(w.batch) >= w.batchSize {
		return w.Flush(context.Background())
	}
	return nil
}

// Flush writes the current batch of deletes.
func (w *EqualityDeleteBatchWriter) Flush(ctx context.Context) error {
	if len(w.batch) == 0 {
		return nil
	}

	for _, delete := range w.batch {
		if err := w.writer.Write(ctx, delete); err != nil {
			return fmt.Errorf("writing equality delete: %w", err)
		}
	}

	if _, err := w.writer.Flush(ctx); err != nil {
		return fmt.Errorf("flushing delete writer: %w", err)
	}

	// Clear the batch
	w.batch = w.batch[:0]
	return nil
}

// Close flushes any remaining deletes and closes the underlying writer.
func (w *EqualityDeleteBatchWriter) Close() error {
	if err := w.Flush(context.Background()); err != nil {
		return err
	}
	return w.writer.Close()
}

// GetResult returns the delete files created by this writer.
func (w *EqualityDeleteBatchWriter) GetResult() []iceberg.DataFile {
	return w.writer.GetResult()
} 