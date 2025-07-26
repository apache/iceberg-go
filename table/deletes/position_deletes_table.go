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
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// PositionDeletesTable provides metadata table functionality for position deletes,
// allowing users to query information about position delete files and their contents.
type PositionDeletesTable struct {
	baseTable     *table.Table
	metadataOnly  bool
	snapshotID    *int64
	schema        *iceberg.Schema
	deleteFiles   []iceberg.DataFile
	deleteIndex   PositionDeleteIndex
	lastUpdated   time.Time
}

// PositionDeletesTableConfig contains configuration for position deletes tables.
type PositionDeletesTableConfig struct {
	// BaseTable is the underlying Iceberg table
	BaseTable *table.Table

	// MetadataOnly indicates whether to include only metadata or actual delete data
	MetadataOnly bool

	// SnapshotID specifies which snapshot to read deletes from (nil for current)
	SnapshotID *int64

	// IncludeDeleteData indicates whether to load actual position delete data
	IncludeDeleteData bool

	// IndexType specifies the index type to use for delete data
	IndexType DeleteIndexType

	// MaxDeleteFilesToLoad limits the number of delete files to process
	MaxDeleteFilesToLoad int
}

// PositionDeletesTableSchema defines the schema for the position deletes metadata table.
var PositionDeletesTableSchema = iceberg.NewSchema(0,
	iceberg.NestedField{ID: 134217728, Type: iceberg.PrimitiveTypes.String, Name: "content", Required: true},
	iceberg.NestedField{ID: 134217729, Type: iceberg.PrimitiveTypes.String, Name: "file_path", Required: true},
	iceberg.NestedField{ID: 134217730, Type: iceberg.PrimitiveTypes.String, Name: "file_format", Required: true},
	iceberg.NestedField{ID: 134217731, Type: iceberg.PrimitiveTypes.Int64, Name: "spec_id", Required: false},
	iceberg.NestedField{ID: 134217732, Type: iceberg.PrimitiveTypes.Int64, Name: "record_count", Required: false},
	iceberg.NestedField{ID: 134217733, Type: iceberg.PrimitiveTypes.Int64, Name: "file_size_in_bytes", Required: false},
	iceberg.NestedField{ID: 134217734, Type: iceberg.PrimitiveTypes.Int64, Name: "data_sequence_number", Required: false},
	iceberg.NestedField{ID: 134217735, Type: iceberg.PrimitiveTypes.Int64, Name: "file_sequence_number", Required: false},
	iceberg.NestedField{ID: 134217736, Type: iceberg.MapType{
		KeyID: 136, KeyType: iceberg.PrimitiveTypes.String,
		ValueID: 137, ValueType: iceberg.PrimitiveTypes.String,
		ValueRequired: false,
	}, Name: "partition", Required: false},
	iceberg.NestedField{ID: 134217737, Type: iceberg.PrimitiveTypes.String, Name: "delete_file_path", Required: true},
	iceberg.NestedField{ID: 134217738, Type: iceberg.PrimitiveTypes.Int64, Name: "pos", Required: true},
)

// PositionDeleteRecord represents a single position delete record in the metadata table.
type PositionDeleteRecord struct {
	Content            string            `json:"content"`
	FilePath           string            `json:"file_path"`
	FileFormat         string            `json:"file_format"`
	SpecID             *int64            `json:"spec_id"`
	RecordCount        *int64            `json:"record_count"`
	FileSizeInBytes    *int64            `json:"file_size_in_bytes"`
	DataSequenceNumber *int64            `json:"data_sequence_number"`
	FileSequenceNumber *int64            `json:"file_sequence_number"`
	Partition          map[string]string `json:"partition"`
	DeleteFilePath     string            `json:"delete_file_path"`
	Position           int64             `json:"pos"`
}

// NewPositionDeletesTable creates a new position deletes metadata table.
func NewPositionDeletesTable(config PositionDeletesTableConfig) (*PositionDeletesTable, error) {
	if config.BaseTable == nil {
		return nil, fmt.Errorf("base table is required")
	}

	if config.MaxDeleteFilesToLoad <= 0 {
		config.MaxDeleteFilesToLoad = 1000 // Default limit
	}

	table := &PositionDeletesTable{
		baseTable:   config.BaseTable,
		metadataOnly: config.MetadataOnly,
		snapshotID:  config.SnapshotID,
		schema:      &PositionDeletesTableSchema,
		lastUpdated: time.Now(),
	}

	// Load delete files and data
	err := table.loadDeleteFiles(config)
	if err != nil {
		return nil, fmt.Errorf("failed to load delete files: %w", err)
	}

	if config.IncludeDeleteData {
		err = table.loadDeleteData(config.IndexType)
		if err != nil {
			return nil, fmt.Errorf("failed to load delete data: %w", err)
		}
	}

	return table, nil
}

// loadDeleteFiles loads position delete files from the specified snapshot.
func (pdt *PositionDeletesTable) loadDeleteFiles(config PositionDeletesTableConfig) error {
	var snapshot *iceberg.Snapshot
	var err error

	if config.SnapshotID != nil {
		snapshot, err = pdt.baseTable.Snapshot(*config.SnapshotID)
		if err != nil {
			return fmt.Errorf("failed to get snapshot %d: %w", *config.SnapshotID, err)
		}
	} else {
		snapshot = pdt.baseTable.CurrentSnapshot()
	}

	if snapshot == nil {
		return fmt.Errorf("no snapshot available")
	}

	// Collect position delete files from all manifests
	deleteFiles := make([]iceberg.DataFile, 0)
	manifestCount := 0

	for manifest := range snapshot.DataManifests() {
		if manifest.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}

		entries, err := manifest.FetchEntries(pdt.baseTable.GetIO(), true)
		if err != nil {
			return fmt.Errorf("failed to fetch manifest entries: %w", err)
		}

		for _, entry := range entries {
			if entry.Status() == iceberg.EntryStatusDELETED {
				continue
			}

			dataFile := entry.DataFile()
			if dataFile.ContentType() == iceberg.EntryContentPosDeletes {
				deleteFiles = append(deleteFiles, dataFile)

				// Respect the limit
				if len(deleteFiles) >= config.MaxDeleteFilesToLoad {
					break
				}
			}
		}

		manifestCount++
		if len(deleteFiles) >= config.MaxDeleteFilesToLoad {
			break
		}
	}

	pdt.deleteFiles = deleteFiles
	return nil
}

// loadDeleteData loads the actual position delete data into the index.
func (pdt *PositionDeletesTable) loadDeleteData(indexType DeleteIndexType) error {
	if len(pdt.deleteFiles) == 0 {
		factory := NewPositionDeleteIndexFactory()
		pdt.deleteIndex = factory.Create(indexType)
		return nil
	}

	// Create index for position deletes
	factory := NewPositionDeleteIndexFactory()
	index := factory.Create(indexType)

	// Load position deletes from each file
	for _, deleteFile := range pdt.deleteFiles {
		err := pdt.loadPositionDeletesFromFile(deleteFile, index)
		if err != nil {
			return fmt.Errorf("failed to load deletes from file %s: %w", deleteFile.FilePath(), err)
		}
	}

	pdt.deleteIndex = index
	return nil
}

// loadPositionDeletesFromFile loads position deletes from a specific delete file.
func (pdt *PositionDeletesTable) loadPositionDeletesFromFile(deleteFile iceberg.DataFile, index PositionDeleteIndex) error {
	// This is a simplified implementation. In practice, you would read the actual
	// parquet file using the file I/O interface from the base table.
	
	// For now, we'll create a placeholder implementation that would read the file
	// and extract position deletes. This would use the table's file I/O interface
	// to read the parquet file and parse the position delete records.
	
	// Placeholder: In a real implementation, this would:
	// 1. Get the file I/O from the base table
	// 2. Open the delete file 
	// 3. Read the parquet data
	// 4. Parse file_path and pos columns
	// 5. Add each position delete to the index
	
	return nil
}

// Schema returns the schema for the position deletes metadata table.
func (pdt *PositionDeletesTable) Schema() *iceberg.Schema {
	return pdt.schema
}

// GetDeleteFiles returns all position delete files.
func (pdt *PositionDeletesTable) GetDeleteFiles() []iceberg.DataFile {
	return slices.Clone(pdt.deleteFiles)
}

// GetDeleteIndex returns the position delete index (if loaded).
func (pdt *PositionDeletesTable) GetDeleteIndex() PositionDeleteIndex {
	return pdt.deleteIndex
}

// CountDeleteFiles returns the number of position delete files.
func (pdt *PositionDeletesTable) CountDeleteFiles() int {
	return len(pdt.deleteFiles)
}

// CountDeletes returns the total number of position deletes (if data is loaded).
func (pdt *PositionDeletesTable) CountDeletes() int {
	if pdt.deleteIndex == nil {
		return 0
	}
	return pdt.deleteIndex.Count()
}

// GetDeleteFilesForDataFile returns delete files that may contain deletes for the specified data file.
func (pdt *PositionDeletesTable) GetDeleteFilesForDataFile(dataFilePath string) []iceberg.DataFile {
	// This is a simplified implementation. In practice, you would use
	// partition information and file statistics to filter relevant delete files.
	return pdt.deleteFiles
}

// IsDeleted checks if a position is deleted in the specified data file.
func (pdt *PositionDeletesTable) IsDeleted(dataFilePath string, position int64) bool {
	if pdt.deleteIndex == nil {
		return false
	}
	return pdt.deleteIndex.IsDeleted(dataFilePath, position)
}

// GetDeletedPositions returns all deleted positions for a data file.
func (pdt *PositionDeletesTable) GetDeletedPositions(dataFilePath string) []int64 {
	if pdt.deleteIndex == nil {
		return nil
	}
	return pdt.deleteIndex.Get(dataFilePath)
}

// ToArrowTable converts the position deletes table to an Arrow table.
func (pdt *PositionDeletesTable) ToArrowTable(ctx context.Context) (arrow.Table, error) {
	mem := memory.DefaultAllocator
	
	// Build Arrow arrays based on whether we have metadata only or actual data
	if pdt.metadataOnly {
		return pdt.buildMetadataArrowTable(ctx, mem)
	}
	
	return pdt.buildDataArrowTable(ctx, mem)
}

// buildMetadataArrowTable builds an Arrow table with delete file metadata.
func (pdt *PositionDeletesTable) buildMetadataArrowTable(ctx context.Context, mem memory.Allocator) (arrow.Table, error) {
	numFiles := len(pdt.deleteFiles)
	if numFiles == 0 {
		return pdt.buildEmptyArrowTable(mem), nil
	}

	// Build arrays for metadata
	contentBuilder := array.NewStringBuilder(mem)
	filePathBuilder := array.NewStringBuilder(mem)
	fileFormatBuilder := array.NewStringBuilder(mem)
	specIDBuilder := array.NewInt64Builder(mem)
	recordCountBuilder := array.NewInt64Builder(mem)
	fileSizeBuilder := array.NewInt64Builder(mem)
	dataSeqBuilder := array.NewInt64Builder(mem)
	fileSeqBuilder := array.NewInt64Builder(mem)
	partitionBuilder := array.NewStringBuilder(mem) // Simplified - should be map type
	deleteFilePathBuilder := array.NewStringBuilder(mem)
	positionBuilder := array.NewInt64Builder(mem)

	// Reserve capacity
	contentBuilder.Reserve(numFiles)
	filePathBuilder.Reserve(numFiles)
	fileFormatBuilder.Reserve(numFiles)
	specIDBuilder.Reserve(numFiles)
	recordCountBuilder.Reserve(numFiles)
	fileSizeBuilder.Reserve(numFiles)
	dataSeqBuilder.Reserve(numFiles)
	fileSeqBuilder.Reserve(numFiles)
	partitionBuilder.Reserve(numFiles)
	deleteFilePathBuilder.Reserve(numFiles)
	positionBuilder.Reserve(numFiles)

	// Add one record per delete file (metadata only)
	for _, deleteFile := range pdt.deleteFiles {
		contentBuilder.Append("position_deletes")
		filePathBuilder.Append(deleteFile.FilePath())
		fileFormatBuilder.Append(deleteFile.FileFormat().String())
		specIDBuilder.Append(int64(deleteFile.SpecID()))
		recordCountBuilder.Append(deleteFile.Count())
		fileSizeBuilder.Append(deleteFile.FileSizeBytes())
		dataSeqBuilder.Append(0)  // Simplified
		fileSeqBuilder.Append(0)  // Simplified
		partitionBuilder.Append("{}") // Simplified partition representation
		deleteFilePathBuilder.Append(deleteFile.FilePath())
		positionBuilder.Append(-1) // No specific position for metadata-only
	}

	return pdt.buildArrowTableFromBuilders(mem, contentBuilder, filePathBuilder, 
		fileFormatBuilder, specIDBuilder, recordCountBuilder, fileSizeBuilder,
		dataSeqBuilder, fileSeqBuilder, partitionBuilder, deleteFilePathBuilder, positionBuilder)
}

// buildDataArrowTable builds an Arrow table with actual position delete data.
func (pdt *PositionDeletesTable) buildDataArrowTable(ctx context.Context, mem memory.Allocator) (arrow.Table, error) {
	if pdt.deleteIndex == nil || pdt.deleteIndex.IsEmpty() {
		return pdt.buildEmptyArrowTable(mem), nil
	}

	totalDeletes := pdt.deleteIndex.Count()

	// Build arrays for actual delete data
	contentBuilder := array.NewStringBuilder(mem)
	filePathBuilder := array.NewStringBuilder(mem)
	fileFormatBuilder := array.NewStringBuilder(mem)
	specIDBuilder := array.NewInt64Builder(mem)
	recordCountBuilder := array.NewInt64Builder(mem)
	fileSizeBuilder := array.NewInt64Builder(mem)
	dataSeqBuilder := array.NewInt64Builder(mem)
	fileSeqBuilder := array.NewInt64Builder(mem)
	partitionBuilder := array.NewStringBuilder(mem)
	deleteFilePathBuilder := array.NewStringBuilder(mem)
	positionBuilder := array.NewInt64Builder(mem)

	// Reserve capacity
	contentBuilder.Reserve(totalDeletes)
	filePathBuilder.Reserve(totalDeletes)
	fileFormatBuilder.Reserve(totalDeletes)
	specIDBuilder.Reserve(totalDeletes)
	recordCountBuilder.Reserve(totalDeletes)
	fileSizeBuilder.Reserve(totalDeletes)
	dataSeqBuilder.Reserve(totalDeletes)
	fileSeqBuilder.Reserve(totalDeletes)
	partitionBuilder.Reserve(totalDeletes)
	deleteFilePathBuilder.Reserve(totalDeletes)
	positionBuilder.Reserve(totalDeletes)

	// Add one record per position delete
	for dataFilePath := range pdt.deleteIndex.Files() {
		for position := range pdt.deleteIndex.Deletes(dataFilePath) {
			// Find the delete file that contains this delete (simplified)
			deleteFilePath := pdt.findDeleteFileForDataFile(dataFilePath)
			
			contentBuilder.Append("position_deletes")
			filePathBuilder.Append(dataFilePath)
			fileFormatBuilder.Append("PARQUET") // Simplified
			specIDBuilder.Append(0)             // Simplified
			recordCountBuilder.Append(1)        // Each record represents one delete
			fileSizeBuilder.Append(0)           // Simplified
			dataSeqBuilder.Append(0)            // Simplified
			fileSeqBuilder.Append(0)            // Simplified
			partitionBuilder.Append("{}")       // Simplified
			deleteFilePathBuilder.Append(deleteFilePath)
			positionBuilder.Append(position)
		}
	}

	return pdt.buildArrowTableFromBuilders(mem, contentBuilder, filePathBuilder,
		fileFormatBuilder, specIDBuilder, recordCountBuilder, fileSizeBuilder,
		dataSeqBuilder, fileSeqBuilder, partitionBuilder, deleteFilePathBuilder, positionBuilder)
}

// buildArrowTableFromBuilders creates an Arrow table from the provided builders.
func (pdt *PositionDeletesTable) buildArrowTableFromBuilders(
	mem memory.Allocator,
	contentBuilder, filePathBuilder, fileFormatBuilder *array.StringBuilder,
	specIDBuilder, recordCountBuilder, fileSizeBuilder, dataSeqBuilder, fileSeqBuilder *array.Int64Builder,
	partitionBuilder, deleteFilePathBuilder *array.StringBuilder,
	positionBuilder *array.Int64Builder) (arrow.Table, error) {

	// Build arrays
	contentArray := contentBuilder.NewArray()
	filePathArray := filePathBuilder.NewArray()
	fileFormatArray := fileFormatBuilder.NewArray()
	specIDArray := specIDBuilder.NewArray()
	recordCountArray := recordCountBuilder.NewArray()
	fileSizeArray := fileSizeBuilder.NewArray()
	dataSeqArray := dataSeqBuilder.NewArray()
	fileSeqArray := fileSeqBuilder.NewArray()
	partitionArray := partitionBuilder.NewArray()
	deleteFilePathArray := deleteFilePathBuilder.NewArray()
	positionArray := positionBuilder.NewArray()

	// Release builders
	contentBuilder.Release()
	filePathBuilder.Release()
	fileFormatBuilder.Release()
	specIDBuilder.Release()
	recordCountBuilder.Release()
	fileSizeBuilder.Release()
	dataSeqBuilder.Release()
	fileSeqBuilder.Release()
	partitionBuilder.Release()
	deleteFilePathBuilder.Release()
	positionBuilder.Release()

	// Create schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "content", Type: arrow.BinaryTypes.String},
		{Name: "file_path", Type: arrow.BinaryTypes.String},
		{Name: "file_format", Type: arrow.BinaryTypes.String},
		{Name: "spec_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "record_count", Type: arrow.PrimitiveTypes.Int64},
		{Name: "file_size_in_bytes", Type: arrow.PrimitiveTypes.Int64},
		{Name: "data_sequence_number", Type: arrow.PrimitiveTypes.Int64},
		{Name: "file_sequence_number", Type: arrow.PrimitiveTypes.Int64},
		{Name: "partition", Type: arrow.BinaryTypes.String}, // Simplified
		{Name: "delete_file_path", Type: arrow.BinaryTypes.String},
		{Name: "pos", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Create columns
	columns := []arrow.Column{
		arrow.NewColumn(schema.Field(0), arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{contentArray})),
		arrow.NewColumn(schema.Field(1), arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{filePathArray})),
		arrow.NewColumn(schema.Field(2), arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{fileFormatArray})),
		arrow.NewColumn(schema.Field(3), arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{specIDArray})),
		arrow.NewColumn(schema.Field(4), arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{recordCountArray})),
		arrow.NewColumn(schema.Field(5), arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{fileSizeArray})),
		arrow.NewColumn(schema.Field(6), arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{dataSeqArray})),
		arrow.NewColumn(schema.Field(7), arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{fileSeqArray})),
		arrow.NewColumn(schema.Field(8), arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{partitionArray})),
		arrow.NewColumn(schema.Field(9), arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{deleteFilePathArray})),
		arrow.NewColumn(schema.Field(10), arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{positionArray})),
	}

	return array.NewTable(schema, columns), nil
}

// buildEmptyArrowTable builds an empty Arrow table with the correct schema.
func (pdt *PositionDeletesTable) buildEmptyArrowTable(mem memory.Allocator) arrow.Table {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "content", Type: arrow.BinaryTypes.String},
		{Name: "file_path", Type: arrow.BinaryTypes.String},
		{Name: "file_format", Type: arrow.BinaryTypes.String},
		{Name: "spec_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "record_count", Type: arrow.PrimitiveTypes.Int64},
		{Name: "file_size_in_bytes", Type: arrow.PrimitiveTypes.Int64},
		{Name: "data_sequence_number", Type: arrow.PrimitiveTypes.Int64},
		{Name: "file_sequence_number", Type: arrow.PrimitiveTypes.Int64},
		{Name: "partition", Type: arrow.BinaryTypes.String},
		{Name: "delete_file_path", Type: arrow.BinaryTypes.String},
		{Name: "pos", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Create empty arrays
	emptyStringArray := array.NewStringBuilder(mem).NewArray()
	emptyInt64Array := array.NewInt64Builder(mem).NewArray()

	columns := make([]arrow.Column, 11)
	for i := range columns {
		if schema.Field(i).Type == arrow.BinaryTypes.String {
			columns[i] = arrow.NewColumn(schema.Field(i), arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{emptyStringArray}))
		} else {
			columns[i] = arrow.NewColumn(schema.Field(i), arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{emptyInt64Array}))
		}
	}

	return array.NewTable(schema, columns)
}

// findDeleteFileForDataFile finds the delete file that contains deletes for the given data file.
func (pdt *PositionDeletesTable) findDeleteFileForDataFile(dataFilePath string) string {
	// Simplified implementation - in practice, you would track which delete file
	// contains deletes for which data file during the loading process
	if len(pdt.deleteFiles) > 0 {
		return pdt.deleteFiles[0].FilePath()
	}
	return "unknown"
}

// Iterator returns an iterator over position delete records.
func (pdt *PositionDeletesTable) Iterator() iter.Seq[PositionDeleteRecord] {
	return func(yield func(PositionDeleteRecord) bool) {
		if pdt.deleteIndex == nil {
			return
		}

		for dataFilePath := range pdt.deleteIndex.Files() {
			for position := range pdt.deleteIndex.Deletes(dataFilePath) {
				record := PositionDeleteRecord{
					Content:            "position_deletes",
					FilePath:           dataFilePath,
					FileFormat:         "PARQUET",
					SpecID:             new(int64),
					RecordCount:        new(int64),
					FileSizeInBytes:    new(int64),
					DataSequenceNumber: new(int64),
					FileSequenceNumber: new(int64),
					Partition:          make(map[string]string),
					DeleteFilePath:     pdt.findDeleteFileForDataFile(dataFilePath),
					Position:           position,
				}

				if !yield(record) {
					return
				}
			}
		}
	}
}

// Refresh reloads the position deletes table from the current snapshot.
func (pdt *PositionDeletesTable) Refresh() error {
	config := PositionDeletesTableConfig{
		BaseTable:            pdt.baseTable,
		MetadataOnly:         pdt.metadataOnly,
		SnapshotID:          pdt.snapshotID,
		IncludeDeleteData:   pdt.deleteIndex != nil,
		IndexType:           BitmapIndexType, // Default
		MaxDeleteFilesToLoad: 1000,
	}

	err := pdt.loadDeleteFiles(config)
	if err != nil {
		return fmt.Errorf("failed to reload delete files: %w", err)
	}

	if config.IncludeDeleteData {
		err = pdt.loadDeleteData(config.IndexType)
		if err != nil {
			return fmt.Errorf("failed to reload delete data: %w", err)
		}
	}

	pdt.lastUpdated = time.Now()
	return nil
}

// LastUpdated returns the time when the table was last updated.
func (pdt *PositionDeletesTable) LastUpdated() time.Time {
	return pdt.lastUpdated
} 