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
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// FileScopedPositionDeleteWriter organizes position deletes by file scope,
// creating separate delete files for different data files or file groups.
// This allows for more targeted delete file application during reads.
type FileScopedPositionDeleteWriter struct {
	config       FileScopedWriterConfig
	writers      map[string]PositionDeleteWriter
	writersMutex sync.RWMutex
	totalRecords int64
	totalBytes   int64
	closed       bool
}

// FileScopedWriterConfig contains configuration for file-scoped position delete writers.
type FileScopedWriterConfig struct {
	// Base configuration for the underlying writers
	BaseConfig PositionDeleteWriterConfig

	// ScopeStrategy defines how to group files into scopes
	ScopeStrategy FileScopeStrategy

	// MaxDeletesPerFile limits the number of deletes per delete file
	MaxDeletesPerFile int64

	// MaxFilesPerScope limits the number of data files per delete file scope
	MaxFilesPerScope int

	// OutputPathPattern is a pattern for generating output paths
	// Should include a placeholder like %s for scope identifier
	OutputPathPattern string

	// FileNamePrefix is added to the beginning of delete file names
	FileNamePrefix string

	// CompressionType for the delete files
	CompressionType string

	// SortStrategy for deletes within each scope
	SortStrategy SortStrategy
}

// FileScopeStrategy defines how data files are grouped into delete file scopes.
type FileScopeStrategy int

const (
	// OneDeleteFilePerDataFile creates one delete file per data file
	OneDeleteFilePerDataFile FileScopeStrategy = iota
	// GroupByPartition groups deletes by partition
	GroupByPartition
	// GroupByDirectory groups deletes by directory path
	GroupByDirectory
	// SingleDeleteFile puts all deletes in a single file
	SingleDeleteFile
)

// String returns the string representation of the FileScopeStrategy.
func (s FileScopeStrategy) String() string {
	switch s {
	case OneDeleteFilePerDataFile:
		return "ONE_DELETE_FILE_PER_DATA_FILE"
	case GroupByPartition:
		return "GROUP_BY_PARTITION"
	case GroupByDirectory:
		return "GROUP_BY_DIRECTORY"
	case SingleDeleteFile:
		return "SINGLE_DELETE_FILE"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(s))
	}
}

// NewFileScopedPositionDeleteWriter creates a new file-scoped position delete writer.
func NewFileScopedPositionDeleteWriter(config FileScopedWriterConfig) (PositionDeleteWriter, error) {
	// Set default values
	if config.MaxDeletesPerFile <= 0 {
		config.MaxDeletesPerFile = 100000 // Default max deletes per file
	}

	if config.MaxFilesPerScope <= 0 {
		config.MaxFilesPerScope = 1000 // Default max files per scope
	}

	if config.OutputPathPattern == "" {
		config.OutputPathPattern = "%s/delete_%s.parquet"
	}

	if config.FileNamePrefix == "" {
		config.FileNamePrefix = "pos_delete"
	}

	return &FileScopedPositionDeleteWriter{
		config:  config,
		writers: make(map[string]PositionDeleteWriter),
		closed:  false,
	}, nil
}

// Write writes a position delete record to the appropriate scoped writer.
func (w *FileScopedPositionDeleteWriter) Write(ctx context.Context, filePath string, position int64) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	delete := NewPositionDelete(filePath, position)
	return w.WritePositionDelete(ctx, delete)
}

// WritePositionDelete writes a PositionDelete to the appropriate scoped writer.
func (w *FileScopedPositionDeleteWriter) WritePositionDelete(ctx context.Context, delete *PositionDelete) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	if delete == nil {
		return nil
	}

	// Determine the scope for this delete
	scope := w.determineScope(delete.FilePath)

	// Get or create the writer for this scope
	writer, err := w.getOrCreateWriter(scope)
	if err != nil {
		return fmt.Errorf("failed to get writer for scope %s: %w", scope, err)
	}

	// Write to the scoped writer
	err = writer.WritePositionDelete(ctx, delete)
	if err != nil {
		return fmt.Errorf("failed to write to scoped writer %s: %w", scope, err)
	}

	w.totalRecords++
	return nil
}

// WriteBatch writes multiple position deletes to their appropriate scoped writers.
func (w *FileScopedPositionDeleteWriter) WriteBatch(ctx context.Context, deletes []*PositionDelete) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	// Group deletes by scope
	scopeDeletes := make(map[string][]*PositionDelete)
	for _, delete := range deletes {
		if delete != nil {
			scope := w.determineScope(delete.FilePath)
			scopeDeletes[scope] = append(scopeDeletes[scope], delete)
		}
	}

	// Write to each scoped writer
	for scope, scopedDeletes := range scopeDeletes {
		writer, err := w.getOrCreateWriter(scope)
		if err != nil {
			return fmt.Errorf("failed to get writer for scope %s: %w", scope, err)
		}

		err = writer.WriteBatch(ctx, scopedDeletes)
		if err != nil {
			return fmt.Errorf("failed to write batch to scoped writer %s: %w", scope, err)
		}

		w.totalRecords += int64(len(scopedDeletes))
	}

	return nil
}

// WriteIndex writes all position deletes from an index to their appropriate scoped writers.
func (w *FileScopedPositionDeleteWriter) WriteIndex(ctx context.Context, index PositionDeleteIndex) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	// Group deletes by scope
	scopeDeletes := make(map[string][]*PositionDelete)
	for filePath := range index.Files() {
		scope := w.determineScope(filePath)
		for position := range index.Deletes(filePath) {
			delete := NewPositionDelete(filePath, position)
			scopeDeletes[scope] = append(scopeDeletes[scope], delete)
		}
	}

	// Write to each scoped writer
	for scope, deletes := range scopeDeletes {
		writer, err := w.getOrCreateWriter(scope)
		if err != nil {
			return fmt.Errorf("failed to get writer for scope %s: %w", scope, err)
		}

		err = writer.WriteBatch(ctx, deletes)
		if err != nil {
			return fmt.Errorf("failed to write batch to scoped writer %s: %w", scope, err)
		}

		w.totalRecords += int64(len(deletes))
	}

	return nil
}

// determineScope determines the scope identifier for a given file path.
func (w *FileScopedPositionDeleteWriter) determineScope(filePath string) string {
	switch w.config.ScopeStrategy {
	case OneDeleteFilePerDataFile:
		// Use the file path itself as the scope (sanitized for file system)
		return w.sanitizeForFileName(filePath)
	case GroupByPartition:
		// Extract partition information from the file path
		// This is a simplified implementation - in practice, you'd use partition spec
		return w.extractPartitionFromPath(filePath)
	case GroupByDirectory:
		// Use the directory of the file
		dir := filepath.Dir(filePath)
		return w.sanitizeForFileName(dir)
	case SingleDeleteFile:
		// All deletes go to the same file
		return "all"
	default:
		// Default to single file
		return "all"
	}
}

// sanitizeForFileName removes or replaces characters that are not safe for file names.
func (w *FileScopedPositionDeleteWriter) sanitizeForFileName(path string) string {
	// Replace path separators and other unsafe characters
	sanitized := strings.ReplaceAll(path, "/", "_")
	sanitized = strings.ReplaceAll(sanitized, "\\", "_")
	sanitized = strings.ReplaceAll(sanitized, ":", "_")
	sanitized = strings.ReplaceAll(sanitized, "*", "_")
	sanitized = strings.ReplaceAll(sanitized, "?", "_")
	sanitized = strings.ReplaceAll(sanitized, "\"", "_")
	sanitized = strings.ReplaceAll(sanitized, "<", "_")
	sanitized = strings.ReplaceAll(sanitized, ">", "_")
	sanitized = strings.ReplaceAll(sanitized, "|", "_")

	// Limit length to avoid file system limits
	if len(sanitized) > 200 {
		sanitized = sanitized[:200]
	}

	return sanitized
}

// extractPartitionFromPath extracts partition information from a file path.
// This is a simplified implementation.
func (w *FileScopedPositionDeleteWriter) extractPartitionFromPath(filePath string) string {
	// Look for partition patterns like "year=2023/month=01/"
	parts := strings.Split(filePath, "/")
	var partitionParts []string

	for _, part := range parts {
		if strings.Contains(part, "=") {
			partitionParts = append(partitionParts, part)
		}
	}

	if len(partitionParts) > 0 {
		return strings.Join(partitionParts, "_")
	}

	// Fallback to directory-based grouping
	return w.sanitizeForFileName(filepath.Dir(filePath))
}

// getOrCreateWriter gets an existing writer for the scope or creates a new one.
func (w *FileScopedPositionDeleteWriter) getOrCreateWriter(scope string) (PositionDeleteWriter, error) {
	w.writersMutex.RLock()
	writer, exists := w.writers[scope]
	w.writersMutex.RUnlock()

	if exists {
		return writer, nil
	}

	w.writersMutex.Lock()
	defer w.writersMutex.Unlock()

	// Double-check after acquiring write lock
	if writer, exists := w.writers[scope]; exists {
		return writer, nil
	}

	// Create new writer for this scope
	outputPath := fmt.Sprintf(w.config.OutputPathPattern,
		filepath.Dir(w.config.BaseConfig.OutputPath),
		w.config.FileNamePrefix+"_"+scope)

	writerConfig := w.config.BaseConfig
	writerConfig.OutputPath = outputPath

	if w.config.CompressionType != "" {
		if writerConfig.Properties == nil {
			writerConfig.Properties = make(map[string]string)
		}
		writerConfig.Properties["compression"] = w.config.CompressionType
	}

	// Use sorting writer if sort strategy is specified
	var newWriter PositionDeleteWriter
	var err error

	if w.config.SortStrategy != SortByFilePathThenPosition {
		sortingConfig := SortingPositionDeleteWriterConfig{
			BaseConfig:   writerConfig,
			SortStrategy: w.config.SortStrategy,
			BatchSize:    int(w.config.MaxDeletesPerFile),
		}
		newWriter, err = NewSortingPositionOnlyDeleteWriter(sortingConfig)
	} else {
		newWriter, err = NewPositionDeleteWriter(writerConfig)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create writer for scope %s: %w", scope, err)
	}

	w.writers[scope] = newWriter
	return newWriter, nil
}

// Close closes all scoped writers and flushes any remaining data.
func (w *FileScopedPositionDeleteWriter) Close() error {
	if w.closed {
		return nil
	}

	defer func() {
		w.closed = true
	}()

	w.writersMutex.Lock()
	defer w.writersMutex.Unlock()

	var errors []string
	for scope, writer := range w.writers {
		if err := writer.Close(); err != nil {
			errors = append(errors, fmt.Sprintf("scope %s: %v", scope, err))
		} else {
			w.totalBytes += writer.WrittenBytes()
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to close some writers: %s", strings.Join(errors, "; "))
	}

	return nil
}

// DataFile returns a combined data file metadata for all written delete files.
// Note: This is a simplified implementation - in practice, you might want to
// return information about all the individual delete files.
func (w *FileScopedPositionDeleteWriter) DataFile() iceberg.DataFile {
	return iceberg.DataFileBuilder{}.
		ContentType(iceberg.EntryContentPosDeletes).
		Path(w.config.BaseConfig.OutputPath). // This might not be entirely accurate for multiple files
		Format(w.config.BaseConfig.FileFormat).
		RecordCount(w.totalRecords).
		FileSizeBytes(w.totalBytes).
		PartitionData(make(map[string]any)).
		Build()
}

// WrittenRecords returns the total number of records written across all scoped writers.
func (w *FileScopedPositionDeleteWriter) WrittenRecords() int64 {
	return w.totalRecords
}

// WrittenBytes returns the total number of bytes written across all scoped writers.
func (w *FileScopedPositionDeleteWriter) WrittenBytes() int64 {
	return w.totalBytes
}

// GetScopedWriters returns a copy of the current scoped writers map.
// This can be useful for inspecting the current state or accessing individual writers.
func (w *FileScopedPositionDeleteWriter) GetScopedWriters() map[string]PositionDeleteWriter {
	w.writersMutex.RLock()
	defer w.writersMutex.RUnlock()

	result := make(map[string]PositionDeleteWriter)
	for scope, writer := range w.writers {
		result[scope] = writer
	}
	return result
}

// GetScopeCount returns the number of active scopes (writers).
func (w *FileScopedPositionDeleteWriter) GetScopeCount() int {
	w.writersMutex.RLock()
	defer w.writersMutex.RUnlock()
	return len(w.writers)
}

// GetScopeNames returns the names of all active scopes.
func (w *FileScopedPositionDeleteWriter) GetScopeNames() []string {
	w.writersMutex.RLock()
	defer w.writersMutex.RUnlock()

	scopes := make([]string, 0, len(w.writers))
	for scope := range w.writers {
		scopes = append(scopes, scope)
	}
	slices.Sort(scopes)
	return scopes
} 