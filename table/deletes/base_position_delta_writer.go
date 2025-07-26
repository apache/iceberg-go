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
	"sync"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// BasePositionDeltaWriter handles position delta operations efficiently by
// maintaining an in-memory delta index and applying bulk operations.
// This is particularly useful for scenarios with frequent position delete
// operations that need to be applied atomically.
type BasePositionDeltaWriter struct {
	config      PositionDeltaWriterConfig
	deltaIndex  PositionDeleteIndex
	baseWriter  PositionDeleteWriter
	operations  []DeltaOperation
	mutex       sync.RWMutex
	totalOps    int64
	closed      bool
}

// PositionDeltaWriterConfig contains configuration for position delta writers.
type PositionDeltaWriterConfig struct {
	// Base configuration for the underlying writer
	BaseConfig PositionDeleteWriterConfig

	// IndexType specifies which index implementation to use
	IndexType DeleteIndexType

	// BatchSize for bulk operations
	BatchSize int

	// AutoCommitThreshold triggers automatic commit when reached
	AutoCommitThreshold int64

	// EnableTransactions for atomic operations
	EnableTransactions bool

	// DeltaCompressionThreshold for compressing delta operations
	DeltaCompressionThreshold int

	// OptimizationStrategy for delta index optimization
	OptimizationStrategy OptimizationStrategy
}

// OptimizationStrategy defines how delta operations are optimized.
type OptimizationStrategy int

const (
	// NoOptimization performs no optimization
	NoOptimization OptimizationStrategy = iota
	// CompactDeltas removes redundant delta operations
	CompactDeltas
	// MergeAdjacentDeltas merges adjacent position deletes
	MergeAdjacentDeltas
	// OptimizeForReads optimizes delta structure for read performance
	OptimizeForReads
)

// String returns the string representation of the OptimizationStrategy.
func (s OptimizationStrategy) String() string {
	switch s {
	case NoOptimization:
		return "NO_OPTIMIZATION"
	case CompactDeltas:
		return "COMPACT_DELTAS"
	case MergeAdjacentDeltas:
		return "MERGE_ADJACENT_DELTAS"
	case OptimizeForReads:
		return "OPTIMIZE_FOR_READS"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(s))
	}
}

// DeltaOperation represents a single delta operation on position deletes.
type DeltaOperation struct {
	Type      DeltaOperationType
	FilePath  string
	Position  int64
	Timestamp int64 // For ordering operations
}

// DeltaOperationType defines the type of delta operation.
type DeltaOperationType int

const (
	// AddDelete adds a position delete
	AddDelete DeltaOperationType = iota
	// RemoveDelete removes a position delete
	RemoveDelete
	// UpdateDelete updates a position delete (combination of remove + add)
	UpdateDelete
)

// String returns the string representation of the DeltaOperationType.
func (t DeltaOperationType) String() string {
	switch t {
	case AddDelete:
		return "ADD_DELETE"
	case RemoveDelete:
		return "REMOVE_DELETE"
	case UpdateDelete:
		return "UPDATE_DELETE"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(t))
	}
}

// DeltaStats provides statistics about delta operations.
type DeltaStats struct {
	TotalOperations   int64
	AddOperations     int64
	RemoveOperations  int64
	UpdateOperations  int64
	PendingOperations int
	DeltaIndexSize    int
	OptimizationCount int64
}

// NewBasePositionDeltaWriter creates a new base position delta writer.
func NewBasePositionDeltaWriter(config PositionDeltaWriterConfig) (*BasePositionDeltaWriter, error) {
	// Set default values
	if config.BatchSize <= 0 {
		config.BatchSize = 1000
	}

	if config.AutoCommitThreshold <= 0 {
		config.AutoCommitThreshold = 10000
	}

	if config.DeltaCompressionThreshold <= 0 {
		config.DeltaCompressionThreshold = 5000
	}

	// Create base writer
	baseWriter, err := NewPositionDeleteWriter(config.BaseConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create base writer: %w", err)
	}

	// Create delta index
	factory := NewPositionDeleteIndexFactory()
	deltaIndex := factory.Create(config.IndexType)

	return &BasePositionDeltaWriter{
		config:     config,
		deltaIndex: deltaIndex,
		baseWriter: baseWriter,
		operations: make([]DeltaOperation, 0),
		closed:     false,
	}, nil
}

// Write writes a position delete record as a delta operation.
func (w *BasePositionDeltaWriter) Write(ctx context.Context, filePath string, position int64) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	return w.AddDelta(ctx, filePath, position)
}

// WritePositionDelete writes a PositionDelete as a delta operation.
func (w *BasePositionDeltaWriter) WritePositionDelete(ctx context.Context, delete *PositionDelete) error {
	if delete == nil {
		return nil
	}
	return w.Write(ctx, delete.FilePath, delete.Position)
}

// WriteBatch writes multiple position deletes as delta operations.
func (w *BasePositionDeltaWriter) WriteBatch(ctx context.Context, deletes []*PositionDelete) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	for _, delete := range deletes {
		if delete != nil {
			w.addDeltaOperation(AddDelete, delete.FilePath, delete.Position)
		}
	}

	// Check if we need to auto-commit
	if w.config.AutoCommitThreshold > 0 && w.totalOps >= w.config.AutoCommitThreshold {
		return w.commitInternal(ctx)
	}

	return nil
}

// WriteIndex writes all position deletes from an index as delta operations.
func (w *BasePositionDeltaWriter) WriteIndex(ctx context.Context, index PositionDeleteIndex) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	for filePath := range index.Files() {
		for position := range index.Deletes(filePath) {
			w.addDeltaOperation(AddDelete, filePath, position)
		}
	}

	// Check if we need to auto-commit
	if w.config.AutoCommitThreshold > 0 && w.totalOps >= w.config.AutoCommitThreshold {
		return w.commitInternal(ctx)
	}

	return nil
}

// AddDelta adds a position delete as a delta operation.
func (w *BasePositionDeltaWriter) AddDelta(ctx context.Context, filePath string, position int64) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.addDeltaOperation(AddDelete, filePath, position)

	// Check if we need to auto-commit
	if w.config.AutoCommitThreshold > 0 && w.totalOps >= w.config.AutoCommitThreshold {
		return w.commitInternal(ctx)
	}

	return nil
}

// RemoveDelta removes a position delete as a delta operation.
func (w *BasePositionDeltaWriter) RemoveDelta(ctx context.Context, filePath string, position int64) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.addDeltaOperation(RemoveDelete, filePath, position)

	// Check if we need to auto-commit
	if w.config.AutoCommitThreshold > 0 && w.totalOps >= w.config.AutoCommitThreshold {
		return w.commitInternal(ctx)
	}

	return nil
}

// UpdateDelta updates a position delete (remove old, add new) as a delta operation.
func (w *BasePositionDeltaWriter) UpdateDelta(ctx context.Context, oldFilePath string, oldPosition int64, newFilePath string, newPosition int64) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Add remove operation for old position
	w.addDeltaOperation(RemoveDelete, oldFilePath, oldPosition)
	// Add add operation for new position
	w.addDeltaOperation(AddDelete, newFilePath, newPosition)

	// Check if we need to auto-commit
	if w.config.AutoCommitThreshold > 0 && w.totalOps >= w.config.AutoCommitThreshold {
		return w.commitInternal(ctx)
	}

	return nil
}

// addDeltaOperation adds a delta operation to the pending operations list.
func (w *BasePositionDeltaWriter) addDeltaOperation(opType DeltaOperationType, filePath string, position int64) {
	operation := DeltaOperation{
		Type:      opType,
		FilePath:  filePath,
		Position:  position,
		Timestamp: w.totalOps,
	}

	w.operations = append(w.operations, operation)
	w.totalOps++

	// Apply operation to delta index
	switch opType {
	case AddDelete:
		w.deltaIndex.Add(filePath, position)
	case RemoveDelete:
		w.deltaIndex.Remove(filePath, position)
	case UpdateDelete:
		// For update operations, this method is called twice (remove + add)
		// so no special handling needed here
	}

	// Check if we need to optimize delta operations
	if len(w.operations) >= w.config.DeltaCompressionThreshold {
		w.optimizeDeltaOperations()
	}
}

// optimizeDeltaOperations applies optimization strategies to delta operations.
func (w *BasePositionDeltaWriter) optimizeDeltaOperations() {
	switch w.config.OptimizationStrategy {
	case CompactDeltas:
		w.compactDeltaOperations()
	case MergeAdjacentDeltas:
		w.mergeAdjacentDeltas()
	case OptimizeForReads:
		w.optimizeForReads()
	}
}

// compactDeltaOperations removes redundant delta operations.
func (w *BasePositionDeltaWriter) compactDeltaOperations() {
	if len(w.operations) <= 1 {
		return
	}

	// Sort operations by file path, position, and timestamp
	sort.Slice(w.operations, func(i, j int) bool {
		if w.operations[i].FilePath != w.operations[j].FilePath {
			return w.operations[i].FilePath < w.operations[j].FilePath
		}
		if w.operations[i].Position != w.operations[j].Position {
			return w.operations[i].Position < w.operations[j].Position
		}
		return w.operations[i].Timestamp < w.operations[j].Timestamp
	})

	// Remove redundant operations (keep only the latest operation for each position)
	compacted := make([]DeltaOperation, 0, len(w.operations))
	for i := 0; i < len(w.operations); i++ {
		current := w.operations[i]
		
		// Look ahead to see if there's a later operation for the same position
		isLatest := true
		for j := i + 1; j < len(w.operations); j++ {
			if w.operations[j].FilePath == current.FilePath && w.operations[j].Position == current.Position {
				isLatest = false
				break
			}
		}

		if isLatest {
			compacted = append(compacted, current)
		}
	}

	w.operations = compacted
}

// mergeAdjacentDeltas merges adjacent position deletes for the same file.
func (w *BasePositionDeltaWriter) mergeAdjacentDeltas() {
	// This is a placeholder for more sophisticated adjacent merge logic
	// In practice, this would identify sequences of adjacent positions
	// and potentially create range-based operations for efficiency
	w.compactDeltaOperations()
}

// optimizeForReads reorganizes delta operations for optimal read performance.
func (w *BasePositionDeltaWriter) optimizeForReads() {
	// Sort operations by file path to improve read locality
	sort.Slice(w.operations, func(i, j int) bool {
		if w.operations[i].FilePath != w.operations[j].FilePath {
			return w.operations[i].FilePath < w.operations[j].FilePath
		}
		return w.operations[i].Position < w.operations[j].Position
	})
}

// Commit applies all pending delta operations to the base writer.
func (w *BasePositionDeltaWriter) Commit(ctx context.Context) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	w.mutex.Lock()
	defer w.mutex.Unlock()

	return w.commitInternal(ctx)
}

// commitInternal applies all pending delta operations (internal, assumes lock is held).
func (w *BasePositionDeltaWriter) commitInternal(ctx context.Context) error {
	if len(w.operations) == 0 {
		return nil
	}

	// Optimize operations before committing
	w.optimizeDeltaOperations()

	// Convert delta index to position deletes and write them
	deletes := make([]*PositionDelete, 0)
	for filePath := range w.deltaIndex.Files() {
		for position := range w.deltaIndex.Deletes(filePath) {
			deletes = append(deletes, NewPositionDelete(filePath, position))
		}
	}

	if len(deletes) > 0 {
		err := w.baseWriter.WriteBatch(ctx, deletes)
		if err != nil {
			return fmt.Errorf("failed to commit delta operations: %w", err)
		}
	}

	// Clear pending operations and delta index
	w.operations = w.operations[:0]
	w.deltaIndex.Clear()

	return nil
}

// Rollback discards all pending delta operations.
func (w *BasePositionDeltaWriter) Rollback() {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.operations = w.operations[:0]
	w.deltaIndex.Clear()
}

// GetDeltaIndex returns a copy of the current delta index.
func (w *BasePositionDeltaWriter) GetDeltaIndex() PositionDeleteIndex {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	// Create a copy of the delta index
	factory := NewPositionDeleteIndexFactory()
	copy := factory.Create(w.config.IndexType)

	for filePath := range w.deltaIndex.Files() {
		for position := range w.deltaIndex.Deletes(filePath) {
			copy.Add(filePath, position)
		}
	}

	return copy
}

// GetDeltaStats returns statistics about delta operations.
func (w *BasePositionDeltaWriter) GetDeltaStats() DeltaStats {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	stats := DeltaStats{
		TotalOperations:   w.totalOps,
		PendingOperations: len(w.operations),
		DeltaIndexSize:    w.deltaIndex.Count(),
	}

	// Count operation types
	for _, op := range w.operations {
		switch op.Type {
		case AddDelete:
			stats.AddOperations++
		case RemoveDelete:
			stats.RemoveOperations++
		case UpdateDelete:
			stats.UpdateOperations++
		}
	}

	return stats
}

// Close commits any pending operations and closes the writer.
func (w *BasePositionDeltaWriter) Close() error {
	if w.closed {
		return nil
	}

	defer func() {
		w.closed = true
	}()

	// Commit any pending operations
	if err := w.Commit(context.Background()); err != nil {
		// Still try to close the base writer
		w.baseWriter.Close()
		return fmt.Errorf("failed to commit pending operations: %w", err)
	}

	// Close the base writer
	return w.baseWriter.Close()
}

// DataFile returns the data file metadata from the base writer.
func (w *BasePositionDeltaWriter) DataFile() iceberg.DataFile {
	return w.baseWriter.DataFile()
}

// WrittenRecords returns the number of records written to the base writer.
func (w *BasePositionDeltaWriter) WrittenRecords() int64 {
	return w.baseWriter.WrittenRecords()
}

// WrittenBytes returns the number of bytes written by the base writer.
func (w *BasePositionDeltaWriter) WrittenBytes() int64 {
	return w.baseWriter.WrittenBytes()
} 