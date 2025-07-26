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
	"sort"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// SortingPositionOnlyDeleteWriter is a position delete writer that sorts deletes
// before writing them to ensure optimal file organization and query performance.
type SortingPositionOnlyDeleteWriter struct {
	baseWriter    PositionDeleteWriter
	pendingWrites []*PositionDelete
	sortStrategy  SortStrategy
	batchSize     int
	closed        bool
}

// SortStrategy defines how position deletes should be sorted.
type SortStrategy int

const (
	// SortByFilePathThenPosition sorts first by file path, then by position
	SortByFilePathThenPosition SortStrategy = iota
	// SortByPositionThenFilePath sorts first by position, then by file path
	SortByPositionThenFilePath
	// SortByFilePathOnly sorts only by file path
	SortByFilePathOnly
	// SortByPositionOnly sorts only by position
	SortByPositionOnly
)

// String returns the string representation of the SortStrategy.
func (s SortStrategy) String() string {
	switch s {
	case SortByFilePathThenPosition:
		return "FILE_PATH_THEN_POSITION"
	case SortByPositionThenFilePath:
		return "POSITION_THEN_FILE_PATH"
	case SortByFilePathOnly:
		return "FILE_PATH_ONLY"
	case SortByPositionOnly:
		return "POSITION_ONLY"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(s))
	}
}

// SortingPositionDeleteWriterConfig contains configuration for the sorting writer.
type SortingPositionDeleteWriterConfig struct {
	// Base configuration for the underlying writer
	BaseConfig PositionDeleteWriterConfig

	// SortStrategy defines how to sort the position deletes
	SortStrategy SortStrategy

	// BatchSize defines how many records to accumulate before sorting and writing
	// If 0, all records are accumulated until Close() is called
	BatchSize int

	// SortInMemory controls whether sorting is done in memory (true) or
	// using external sorting for large datasets (false)
	SortInMemory bool
}

// NewSortingPositionOnlyDeleteWriter creates a new sorting position delete writer.
func NewSortingPositionOnlyDeleteWriter(config SortingPositionDeleteWriterConfig) (PositionDeleteWriter, error) {
	// Set default values
	if config.BatchSize <= 0 {
		config.BatchSize = 10000 // Default batch size
	}

	// Create base writer
	baseWriter, err := NewPositionDeleteWriter(config.BaseConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create base writer: %w", err)
	}

	return &SortingPositionOnlyDeleteWriter{
		baseWriter:    baseWriter,
		pendingWrites: make([]*PositionDelete, 0, config.BatchSize),
		sortStrategy:  config.SortStrategy,
		batchSize:     config.BatchSize,
		closed:        false,
	}, nil
}

// Write writes a position delete record, potentially triggering a sort and flush.
func (w *SortingPositionOnlyDeleteWriter) Write(ctx context.Context, filePath string, position int64) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	delete := NewPositionDelete(filePath, position)
	return w.WritePositionDelete(ctx, delete)
}

// WritePositionDelete writes a PositionDelete, potentially triggering a sort and flush.
func (w *SortingPositionOnlyDeleteWriter) WritePositionDelete(ctx context.Context, delete *PositionDelete) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	if delete == nil {
		return nil
	}

	w.pendingWrites = append(w.pendingWrites, delete)

	// Check if we need to flush the batch
	if len(w.pendingWrites) >= w.batchSize {
		return w.flushPendingWrites(ctx)
	}

	return nil
}

// WriteBatch writes multiple position deletes, potentially triggering sorts and flushes.
func (w *SortingPositionOnlyDeleteWriter) WriteBatch(ctx context.Context, deletes []*PositionDelete) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	for _, delete := range deletes {
		if delete != nil {
			w.pendingWrites = append(w.pendingWrites, delete)

			// Check if we need to flush the batch
			if len(w.pendingWrites) >= w.batchSize {
				if err := w.flushPendingWrites(ctx); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// WriteIndex writes all position deletes from an index, potentially triggering sorts and flushes.
func (w *SortingPositionOnlyDeleteWriter) WriteIndex(ctx context.Context, index PositionDeleteIndex) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	for filePath := range index.Files() {
		for position := range index.Deletes(filePath) {
			delete := NewPositionDelete(filePath, position)
			w.pendingWrites = append(w.pendingWrites, delete)

			// Check if we need to flush the batch
			if len(w.pendingWrites) >= w.batchSize {
				if err := w.flushPendingWrites(ctx); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// flushPendingWrites sorts and writes the accumulated position deletes.
func (w *SortingPositionOnlyDeleteWriter) flushPendingWrites(ctx context.Context) error {
	if len(w.pendingWrites) == 0 {
		return nil
	}

	// Sort the pending writes according to the strategy
	w.sortPendingWrites()

	// Write the sorted deletes to the base writer
	err := w.baseWriter.WriteBatch(ctx, w.pendingWrites)
	if err != nil {
		return fmt.Errorf("failed to write sorted batch: %w", err)
	}

	// Clear the pending writes
	w.pendingWrites = w.pendingWrites[:0]

	return nil
}

// sortPendingWrites sorts the pending writes according to the configured strategy.
func (w *SortingPositionOnlyDeleteWriter) sortPendingWrites() {
	switch w.sortStrategy {
	case SortByFilePathThenPosition:
		sort.Slice(w.pendingWrites, func(i, j int) bool {
			return w.pendingWrites[i].Compare(w.pendingWrites[j]) < 0
		})
	case SortByPositionThenFilePath:
		sort.Slice(w.pendingWrites, func(i, j int) bool {
			if w.pendingWrites[i].Position != w.pendingWrites[j].Position {
				return w.pendingWrites[i].Position < w.pendingWrites[j].Position
			}
			return w.pendingWrites[i].FilePath < w.pendingWrites[j].FilePath
		})
	case SortByFilePathOnly:
		sort.Slice(w.pendingWrites, func(i, j int) bool {
			return w.pendingWrites[i].FilePath < w.pendingWrites[j].FilePath
		})
	case SortByPositionOnly:
		sort.Slice(w.pendingWrites, func(i, j int) bool {
			return w.pendingWrites[i].Position < w.pendingWrites[j].Position
		})
	}
}

// Close closes the writer, flushing any remaining data.
func (w *SortingPositionOnlyDeleteWriter) Close() error {
	if w.closed {
		return nil
	}

	defer func() {
		w.closed = true
	}()

	// Flush any remaining pending writes
	if len(w.pendingWrites) > 0 {
		if err := w.flushPendingWrites(context.Background()); err != nil {
			// Still try to close the base writer
			w.baseWriter.Close()
			return fmt.Errorf("failed to flush pending writes: %w", err)
		}
	}

	// Close the base writer
	return w.baseWriter.Close()
}

// DataFile returns the data file metadata for the written delete file.
func (w *SortingPositionOnlyDeleteWriter) DataFile() iceberg.DataFile {
	return w.baseWriter.DataFile()
}

// WrittenRecords returns the number of records written.
func (w *SortingPositionOnlyDeleteWriter) WrittenRecords() int64 {
	return w.baseWriter.WrittenRecords() + int64(len(w.pendingWrites))
}

// WrittenBytes returns the number of bytes written.
func (w *SortingPositionOnlyDeleteWriter) WrittenBytes() int64 {
	return w.baseWriter.WrittenBytes()
}

// FlushPendingWrites flushes any pending writes without closing the writer.
// This can be useful for controlling memory usage in long-running operations.
func (w *SortingPositionOnlyDeleteWriter) FlushPendingWrites(ctx context.Context) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	return w.flushPendingWrites(ctx)
}

// PendingWriteCount returns the number of position deletes waiting to be flushed.
func (w *SortingPositionOnlyDeleteWriter) PendingWriteCount() int {
	return len(w.pendingWrites)
} 