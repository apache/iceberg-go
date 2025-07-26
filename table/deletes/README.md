# Position Deletes Implementation for Apache Iceberg Go

This package provides comprehensive support for Apache Iceberg position deletes, implementing full functionality comparable to the Java implementation.

## Overview

Position deletes in Apache Iceberg allow marking specific rows in data files as deleted without rewriting the entire file. Each position delete record contains:
- `file_path`: Path to the data file containing the deleted row
- `pos`: 0-based row position within that file that is deleted

This implementation provides efficient indexing, multiple writer strategies, and comprehensive utilities for managing position deletes.

## Features

### Core Data Structures
- **PositionDelete**: Represents a single position delete record
- **PositionDeleteIndex**: Interface for efficient indexing and querying
- **BitmapPositionDeleteIndex**: Bitmap-based implementation for dense deletes
- **SetPositionDeleteIndex**: Set-based implementation for sparse deletes

### Writers
- **BasePositionDeleteWriter**: Basic writer for position delete files
- **SortingPositionOnlyDeleteWriter**: Sorts deletes for optimal performance
- **FileScopedPositionDeleteWriter**: Organizes deletes by file scope
- **ClusteredPositionDeleteWriter**: Clusters deletes for optimal I/O
- **BasePositionDeltaWriter**: Handles delta operations efficiently

### Table & Metadata
- **PositionDeletesTable**: Metadata table for position delete information
- **PositionDeletesScanTask**: Scan task for reading position delete files
- **CombinedPositionDeletesScanTask**: Combines multiple scan tasks

### Utilities
- **PositionDeleteIndexUtil**: Merge, filter, and optimize operations
- Index type selection based on data characteristics
- Arrow table conversion for analysis

## Quick Start

### Basic Usage

```go
import "github.com/apache/iceberg-go/deletes"

// Create a position delete
delete := deletes.NewPositionDelete("/path/to/data.parquet", 42)

// Create an index and add deletes
index := deletes.NewSetPositionDeleteIndex()
index.Add("/path/to/data.parquet", 42)
index.Add("/path/to/data.parquet", 100)

// Check if a position is deleted
if index.IsDeleted("/path/to/data.parquet", 42) {
    // Handle deleted row
}
```

### Using Writers

```go
config := deletes.PositionDeleteWriterConfig{
    FileIO:     fileIO,
    OutputPath: "/path/to/deletes.parquet",
    FileFormat: iceberg.FileFormatParquet,
}

writer, err := deletes.NewPositionDeleteWriter(config)
if err != nil {
    // Handle error
}
defer writer.Close()

// Write position deletes
err = writer.Write(ctx, "/path/to/data.parquet", 42)
err = writer.WritePositionDelete(ctx, delete)
```

### Index Operations

```go
util := &deletes.PositionDeleteIndexUtil{}

// Merge multiple indexes
merged := util.Merge(index1, index2, index3)

// Filter deletes by file prefix
filtered := util.FilterByFilePrefix(index, "/path/to/partition")

// Convert to Arrow table
table := util.ToArrowTable(ctx, index)
```

## Index Selection

The package automatically chooses the most appropriate index type:

- **BitmapPositionDeleteIndex**: Used for dense deletes or many deletes per file
- **SetPositionDeleteIndex**: Used for sparse deletes or when memory efficiency is important

You can also explicitly choose using the factory:

```go
factory := deletes.NewPositionDeleteIndexFactory()
bitmapIndex := factory.Create(deletes.BitmapIndexType)
setIndex := factory.Create(deletes.SetIndexType)
```

## Writer Strategies

### Sorting Writer
Sorts deletes during writing for optimal query performance:

```go
sortingConfig := deletes.SortingPositionDeleteWriterConfig{
    BaseConfig:   writerConfig,
    SortStrategy: deletes.SortByFilePathThenPosition,
    BatchSize:    10000,
}
writer, err := deletes.NewSortingPositionOnlyDeleteWriter(sortingConfig)
```

### File-Scoped Writer
Organizes deletes by file scope to reduce read overhead:

```go
scopedConfig := deletes.FileScopedWriterConfig{
    BaseConfig:     writerConfig,
    ScopeStrategy:  deletes.GroupByPartition,
    SortStrategy:   deletes.SortByFilePathThenPosition,
}
writer, err := deletes.NewFileScopedPositionDeleteWriter(scopedConfig)
```

### Clustered Writer
Clusters deletes for optimal I/O and storage efficiency:

```go
clusteredConfig := deletes.ClusteredWriterConfig{
    BaseConfig:          writerConfig,
    ClusteringStrategy:  deletes.ClusterByPartition,
    MaxDeletesPerCluster: 50000,
    SortStrategy:        deletes.SortByFilePathThenPosition,
}
writer, err := deletes.NewClusteredPositionDeleteWriter(clusteredConfig)
```

### Delta Writer
Handles frequent position delete operations with batching and optimization:

```go
deltaConfig := deletes.PositionDeltaWriterConfig{
    BaseConfig:            writerConfig,
    IndexType:             deletes.BitmapIndexType,
    AutoCommitThreshold:   10000,
    OptimizationStrategy:  deletes.CompactDeltas,
}
writer, err := deletes.NewBasePositionDeltaWriter(deltaConfig)

// Perform delta operations
writer.AddDelta(ctx, "/path/to/file.parquet", 42)
writer.RemoveDelta(ctx, "/path/to/file.parquet", 24)
writer.UpdateDelta(ctx, "/old/path.parquet", 10, "/new/path.parquet", 15)

// Commit changes
writer.Commit(ctx)
```

## Advanced Features

### Scan Tasks
Efficiently scan position delete files:

```go
taskConfig := deletes.PositionDeletesScanTaskConfig{
    DeleteFiles:     deleteFiles,
    TargetDataFiles: dataFiles,
    FileIO:          fileIO,
}

task := deletes.NewPositionDeletesScanTask(taskConfig)

// Scan to index
index, err := task.ScanToIndex(ctx, deletes.BitmapIndexType)

// Or iterate over records
for record, err := range task.Scan(ctx) {
    if err != nil {
        // Handle error
    }
    // Process record
    record.Release()
}
```

### Position Deletes Table
Query position delete metadata:

```go
tableConfig := deletes.PositionDeletesTableConfig{
    BaseTable:         icebergTable,
    MetadataOnly:      false,
    IncludeDeleteData: true,
    IndexType:         deletes.BitmapIndexType,
}

posDeletesTable, err := deletes.NewPositionDeletesTable(tableConfig)

// Query delete information
fmt.Printf("Delete files: %d\n", posDeletesTable.CountDeleteFiles())
fmt.Printf("Total deletes: %d\n", posDeletesTable.CountDeletes())

// Check if position is deleted
isDeleted := posDeletesTable.IsDeleted("/path/to/data.parquet", 42)

// Convert to Arrow table for analysis
arrowTable, err := posDeletesTable.ToArrowTable(ctx)
```

## Performance Considerations

### Index Type Selection
- **Bitmap indexes**: More memory-efficient for dense deletes (many deletes per file)
- **Set indexes**: More memory-efficient for sparse deletes, consistent performance

### Writer Selection
- **Basic writer**: Simple, single-threaded writing
- **Sorting writer**: Better query performance through organization
- **File-scoped writer**: Reduces read overhead during queries
- **Clustered writer**: Optimal for high-throughput scenarios
- **Delta writer**: Best for frequent incremental updates

### Memory Management
- Use appropriate batch sizes for large datasets
- Consider clustering strategies for memory-constrained environments
- Optimize indexes based on usage patterns

## Thread Safety

- Most components are not thread-safe by default
- **FileScopedPositionDeleteWriter** and **ClusteredPositionDeleteWriter** are thread-safe for concurrent writes
- Use appropriate synchronization for shared indexes and writers

## Examples

See `examples_test.go` for comprehensive usage examples covering:
- Basic position delete operations
- Different index types and builders
- Various writer implementations
- Utility operations and workflows
- Complete end-to-end scenarios

## Integration

This package integrates seamlessly with the Apache Iceberg Go implementation:
- Uses existing schema and data file abstractions
- Compatible with Iceberg table metadata
- Supports all Iceberg file formats (Parquet, ORC, Avro)
- Works with existing partition specifications

## Testing

Run the test suite:
```bash
go test ./deletes/...
```

Run benchmarks:
```bash
go test -bench=. ./deletes/...
```

## Contributing

This implementation follows the Apache Iceberg specification for position deletes and maintains compatibility with the Java implementation. When contributing:

1. Ensure compatibility with Iceberg spec
2. Add comprehensive tests
3. Update documentation
4. Consider performance implications
5. Maintain thread-safety contracts 