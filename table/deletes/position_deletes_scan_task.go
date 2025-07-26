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
	"iter"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// PositionDeletesScanTask represents a scan task for reading position delete files.
type PositionDeletesScanTask interface {
	// DeleteFiles returns the delete files to be scanned
	DeleteFiles() []iceberg.DataFile

	// Schema returns the schema for position delete records
	Schema() *iceberg.Schema

	// Scan executes the scan and returns an iterator over position delete records
	Scan(ctx context.Context) iter.Seq2[arrow.Record, error]

	// ScanToIndex executes the scan and loads position deletes into an index
	ScanToIndex(ctx context.Context, indexType DeleteIndexType) (PositionDeleteIndex, error)

	// EstimatedRecordCount returns the estimated number of records
	EstimatedRecordCount() int64

	// EstimatedDataSize returns the estimated data size in bytes
	EstimatedDataSize() int64

	// TargetDataFiles returns the data files that these deletes may apply to
	TargetDataFiles() []string

	// PartitionData returns partition information for the scan task
	PartitionData() map[string]any

	// String returns a string representation of the scan task
	String() string
}

// BasePositionDeletesScanTask provides a base implementation of PositionDeletesScanTask.
type BasePositionDeletesScanTask struct {
	deleteFiles      []iceberg.DataFile
	targetDataFiles  []string
	schema           *iceberg.Schema
	fileIO           iceio.IO
	partitionData    map[string]any
	estimatedRecords int64
	estimatedBytes   int64
}

// PositionDeletesScanTaskConfig contains configuration for position delete scan tasks.
type PositionDeletesScanTaskConfig struct {
	// DeleteFiles are the position delete files to scan
	DeleteFiles []iceberg.DataFile

	// TargetDataFiles are the data files that these deletes may apply to
	TargetDataFiles []string

	// FileIO for reading delete files
	FileIO iceio.IO

	// Schema for position delete records (optional, uses default if nil)
	Schema *iceberg.Schema

	// PartitionData for the scan task
	PartitionData map[string]any

	// EstimatedRecords for query planning
	EstimatedRecords int64

	// EstimatedBytes for query planning
	EstimatedBytes int64
}

// NewPositionDeletesScanTask creates a new position deletes scan task.
func NewPositionDeletesScanTask(config PositionDeletesScanTaskConfig) PositionDeletesScanTask {
	schema := config.Schema
	if schema == nil {
		schema = &iceberg.PositionalDeleteSchema
	}

	estimatedRecords := config.EstimatedRecords
	if estimatedRecords <= 0 {
		// Estimate based on delete files
		for _, df := range config.DeleteFiles {
			estimatedRecords += df.Count()
		}
	}

	estimatedBytes := config.EstimatedBytes
	if estimatedBytes <= 0 {
		// Estimate based on delete files
		for _, df := range config.DeleteFiles {
			estimatedBytes += df.FileSizeBytes()
		}
	}

	return &BasePositionDeletesScanTask{
		deleteFiles:      slices.Clone(config.DeleteFiles),
		targetDataFiles:  slices.Clone(config.TargetDataFiles),
		schema:           schema,
		fileIO:           config.FileIO,
		partitionData:    config.PartitionData,
		estimatedRecords: estimatedRecords,
		estimatedBytes:   estimatedBytes,
	}
}

// DeleteFiles returns the delete files to be scanned.
func (task *BasePositionDeletesScanTask) DeleteFiles() []iceberg.DataFile {
	return slices.Clone(task.deleteFiles)
}

// Schema returns the schema for position delete records.
func (task *BasePositionDeletesScanTask) Schema() *iceberg.Schema {
	return task.schema
}

// Scan executes the scan and returns an iterator over position delete records.
func (task *BasePositionDeletesScanTask) Scan(ctx context.Context) iter.Seq2[arrow.Record, error] {
	return func(yield func(arrow.Record, error) bool) {
		for _, deleteFile := range task.deleteFiles {
			err := task.scanDeleteFile(ctx, deleteFile, yield)
			if err != nil {
				yield(nil, err)
				return
			}
		}
	}
}

// scanDeleteFile scans a single delete file and yields records.
func (task *BasePositionDeletesScanTask) scanDeleteFile(
	ctx context.Context,
	deleteFile iceberg.DataFile,
	yield func(arrow.Record, error) bool) error {

	// Open the delete file
	file, err := task.fileIO.Open(deleteFile.FilePath())
	if err != nil {
		return fmt.Errorf("failed to open delete file %s: %w", deleteFile.FilePath(), err)
	}
	defer file.Close()

	// Get a reader for the file
	reader, err := file.GetReader(ctx)
	if err != nil {
		return fmt.Errorf("failed to get reader for delete file %s: %w", deleteFile.FilePath(), err)
	}
	defer reader.Close()

	// Read records and yield them
	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("failed to read from delete file %s: %w", deleteFile.FilePath(), err)
		}

		if !yield(record, nil) {
			break
		}
	}

	return nil
}

// ScanToIndex executes the scan and loads position deletes into an index.
func (task *BasePositionDeletesScanTask) ScanToIndex(ctx context.Context, indexType DeleteIndexType) (PositionDeleteIndex, error) {
	factory := NewPositionDeleteIndexFactory()
	index := factory.Create(indexType)

	// Scan all delete files and load into index
	for deleteFile, err := range task.Scan(ctx) {
		if err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}

		err = task.loadRecordIntoIndex(deleteFile, index)
		if err != nil {
			deleteFile.Release()
			return nil, fmt.Errorf("failed to load record into index: %w", err)
		}

		deleteFile.Release()
	}

	return index, nil
}

// loadRecordIntoIndex loads a single Arrow record into the position delete index.
func (task *BasePositionDeletesScanTask) loadRecordIntoIndex(record arrow.Record, index PositionDeleteIndex) error {
	schema := record.Schema()

	// Find file_path and pos column indices
	filePathColIdx := -1
	posColIdx := -1

	for i, field := range schema.Fields() {
		switch field.Name {
		case "file_path":
			filePathColIdx = i
		case "pos":
			posColIdx = i
		}
	}

	if filePathColIdx == -1 || posColIdx == -1 {
		return fmt.Errorf("record missing required columns file_path and/or pos")
	}

	// Extract position deletes from the record
	filePathArray := record.Column(filePathColIdx).(*array.String)
	posArray := record.Column(posColIdx)

	var posInt64Array *array.Int64
	switch posTyped := posArray.(type) {
	case *array.Int64:
		posInt64Array = posTyped
	case *array.Int32:
		// Convert Int32 to Int64
		builder := array.NewInt64Builder(memory.DefaultAllocator)
		for i := 0; i < posTyped.Len(); i++ {
			if posTyped.IsNull(i) {
				builder.AppendNull()
			} else {
				builder.Append(int64(posTyped.Value(i)))
			}
		}
		posInt64Array = builder.NewInt64Array()
		defer posInt64Array.Release()
	default:
		return fmt.Errorf("unsupported position column type: %T", posArray)
	}

	// Add position deletes to index
	for i := 0; i < filePathArray.Len(); i++ {
		if !filePathArray.IsNull(i) && !posInt64Array.IsNull(i) {
			filePath := filePathArray.Value(i)
			position := posInt64Array.Value(i)
			index.Add(filePath, position)
		}
	}

	return nil
}

// EstimatedRecordCount returns the estimated number of records.
func (task *BasePositionDeletesScanTask) EstimatedRecordCount() int64 {
	return task.estimatedRecords
}

// EstimatedDataSize returns the estimated data size in bytes.
func (task *BasePositionDeletesScanTask) EstimatedDataSize() int64 {
	return task.estimatedBytes
}

// TargetDataFiles returns the data files that these deletes may apply to.
func (task *BasePositionDeletesScanTask) TargetDataFiles() []string {
	return slices.Clone(task.targetDataFiles)
}

// PartitionData returns partition information for the scan task.
func (task *BasePositionDeletesScanTask) PartitionData() map[string]any {
	if task.partitionData == nil {
		return make(map[string]any)
	}
	
	result := make(map[string]any, len(task.partitionData))
	for k, v := range task.partitionData {
		result[k] = v
	}
	return result
}

// String returns a string representation of the scan task.
func (task *BasePositionDeletesScanTask) String() string {
	return fmt.Sprintf("PositionDeletesScanTask{files=%d, estimatedRecords=%d, estimatedBytes=%d}",
		len(task.deleteFiles), task.estimatedRecords, task.estimatedBytes)
}

// CombinedPositionDeletesScanTask combines multiple scan tasks into a single task.
type CombinedPositionDeletesScanTask struct {
	tasks           []PositionDeletesScanTask
	totalFiles      int
	totalRecords    int64
	totalBytes      int64
	combinedSchema  *iceberg.Schema
	fileIO          iceio.IO
}

// NewCombinedPositionDeletesScanTask creates a combined scan task from multiple tasks.
func NewCombinedPositionDeletesScanTask(tasks []PositionDeletesScanTask, fileIO iceio.IO) *CombinedPositionDeletesScanTask {
	totalFiles := 0
	totalRecords := int64(0)
	totalBytes := int64(0)

	for _, task := range tasks {
		totalFiles += len(task.DeleteFiles())
		totalRecords += task.EstimatedRecordCount()
		totalBytes += task.EstimatedDataSize()
	}

	schema := &iceberg.PositionalDeleteSchema
	if len(tasks) > 0 {
		schema = tasks[0].Schema()
	}

	return &CombinedPositionDeletesScanTask{
		tasks:          slices.Clone(tasks),
		totalFiles:     totalFiles,
		totalRecords:   totalRecords,
		totalBytes:     totalBytes,
		combinedSchema: schema,
		fileIO:         fileIO,
	}
}

// DeleteFiles returns all delete files from all combined tasks.
func (task *CombinedPositionDeletesScanTask) DeleteFiles() []iceberg.DataFile {
	var allFiles []iceberg.DataFile
	for _, t := range task.tasks {
		allFiles = append(allFiles, t.DeleteFiles()...)
	}
	return allFiles
}

// Schema returns the schema for position delete records.
func (task *CombinedPositionDeletesScanTask) Schema() *iceberg.Schema {
	return task.combinedSchema
}

// Scan executes the scan across all combined tasks.
func (task *CombinedPositionDeletesScanTask) Scan(ctx context.Context) iter.Seq2[arrow.Record, error] {
	return func(yield func(arrow.Record, error) bool) {
		for _, t := range task.tasks {
			for record, err := range t.Scan(ctx) {
				if !yield(record, err) {
					return
				}
			}
		}
	}
}

// ScanToIndex executes the scan across all tasks and loads into a single index.
func (task *CombinedPositionDeletesScanTask) ScanToIndex(ctx context.Context, indexType DeleteIndexType) (PositionDeleteIndex, error) {
	factory := NewPositionDeleteIndexFactory()
	combinedIndex := factory.Create(indexType)

	for _, t := range task.tasks {
		taskIndex, err := t.ScanToIndex(ctx, indexType)
		if err != nil {
			return nil, fmt.Errorf("failed to scan task to index: %w", err)
		}

		// Merge task index into combined index
		for filePath := range taskIndex.Files() {
			for position := range taskIndex.Deletes(filePath) {
				combinedIndex.Add(filePath, position)
			}
		}
	}

	return combinedIndex, nil
}

// EstimatedRecordCount returns the total estimated number of records.
func (task *CombinedPositionDeletesScanTask) EstimatedRecordCount() int64 {
	return task.totalRecords
}

// EstimatedDataSize returns the total estimated data size in bytes.
func (task *CombinedPositionDeletesScanTask) EstimatedDataSize() int64 {
	return task.totalBytes
}

// TargetDataFiles returns all target data files from all combined tasks.
func (task *CombinedPositionDeletesScanTask) TargetDataFiles() []string {
	var allTargets []string
	seenTargets := make(map[string]struct{})

	for _, t := range task.tasks {
		for _, target := range t.TargetDataFiles() {
			if _, seen := seenTargets[target]; !seen {
				allTargets = append(allTargets, target)
				seenTargets[target] = struct{}{}
			}
		}
	}

	return allTargets
}

// PartitionData returns combined partition information.
func (task *CombinedPositionDeletesScanTask) PartitionData() map[string]any {
	// For combined tasks, partition data is more complex
	// This is a simplified implementation
	return make(map[string]any)
}

// String returns a string representation of the combined scan task.
func (task *CombinedPositionDeletesScanTask) String() string {
	return fmt.Sprintf("CombinedPositionDeletesScanTask{tasks=%d, totalFiles=%d, estimatedRecords=%d, estimatedBytes=%d}",
		len(task.tasks), task.totalFiles, task.totalRecords, task.totalBytes)
}

// PositionDeletesScanTaskBuilder helps build position delete scan tasks.
type PositionDeletesScanTaskBuilder struct {
	deleteFiles     []iceberg.DataFile
	targetDataFiles []string
	fileIO          iceio.IO
	schema          *iceberg.Schema
	partitionData   map[string]any
}

// NewPositionDeletesScanTaskBuilder creates a new scan task builder.
func NewPositionDeletesScanTaskBuilder(fileIO iceio.IO) *PositionDeletesScanTaskBuilder {
	return &PositionDeletesScanTaskBuilder{
		deleteFiles:     make([]iceberg.DataFile, 0),
		targetDataFiles: make([]string, 0),
		fileIO:          fileIO,
		partitionData:   make(map[string]any),
	}
}

// AddDeleteFile adds a delete file to the scan task.
func (b *PositionDeletesScanTaskBuilder) AddDeleteFile(deleteFile iceberg.DataFile) *PositionDeletesScanTaskBuilder {
	b.deleteFiles = append(b.deleteFiles, deleteFile)
	return b
}

// AddDeleteFiles adds multiple delete files to the scan task.
func (b *PositionDeletesScanTaskBuilder) AddDeleteFiles(deleteFiles []iceberg.DataFile) *PositionDeletesScanTaskBuilder {
	b.deleteFiles = append(b.deleteFiles, deleteFiles...)
	return b
}

// AddTargetDataFile adds a target data file.
func (b *PositionDeletesScanTaskBuilder) AddTargetDataFile(dataFile string) *PositionDeletesScanTaskBuilder {
	b.targetDataFiles = append(b.targetDataFiles, dataFile)
	return b
}

// AddTargetDataFiles adds multiple target data files.
func (b *PositionDeletesScanTaskBuilder) AddTargetDataFiles(dataFiles []string) *PositionDeletesScanTaskBuilder {
	b.targetDataFiles = append(b.targetDataFiles, dataFiles...)
	return b
}

// WithSchema sets the schema for the scan task.
func (b *PositionDeletesScanTaskBuilder) WithSchema(schema *iceberg.Schema) *PositionDeletesScanTaskBuilder {
	b.schema = schema
	return b
}

// WithPartitionData sets partition data for the scan task.
func (b *PositionDeletesScanTaskBuilder) WithPartitionData(partitionData map[string]any) *PositionDeletesScanTaskBuilder {
	b.partitionData = partitionData
	return b
}

// Build creates the position delete scan task.
func (b *PositionDeletesScanTaskBuilder) Build() PositionDeletesScanTask {
	config := PositionDeletesScanTaskConfig{
		DeleteFiles:     b.deleteFiles,
		TargetDataFiles: b.targetDataFiles,
		FileIO:          b.fileIO,
		Schema:          b.schema,
		PartitionData:   b.partitionData,
	}

	return NewPositionDeletesScanTask(config)
} 