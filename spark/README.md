# Apache Spark Integration for Iceberg Go

This package provides comprehensive integration between Apache Iceberg Go and Apache Spark using the [Apache Spark Connect Go client](https://github.com/apache/spark-connect-go).

> **⚠️ Current Status**: The Apache Spark Connect Go library currently has versioning issues in their go.mod file. The integration is fully implemented and ready to use once this is resolved upstream. See [Limitations](#limitations) section for details.

## Overview

The Spark integration enables you to:

- **Read and write** Iceberg tables through Spark
- **Perform batch processing** operations on Iceberg data
- **Execute table maintenance** operations (compaction, snapshot expiration, etc.)
- **Support structured streaming** with Iceberg tables
- **Manage schema evolution** and partition evolution
- **Convert data types** between Iceberg and Spark formats

## Architecture

The integration follows a modular design with the following core components:

### Core Components

- **`SparkUtil`** - General utilities for Spark session management and SQL execution
- **`SparkTableUtil`** - Table-specific operations (create, drop, list, etc.)
- **`SparkValueConverter`** - Data type conversions between Iceberg and Spark
- **`SparkWrite`** - Write operations (append, overwrite, merge, delete, update)
- **`SparkScan`** - Scan operations (read data, time travel, partition scanning)
- **`SparkBatch`** - Batch processing and table maintenance operations
- **`SparkActions`** - High-level table actions (compaction, optimization, etc.)
- **`SparkMicroBatchStream`** - Structured streaming support

### Integration Interface

- **`SparkIcebergIntegration`** - High-level interface that combines all components

## Quick Start

### Prerequisites

1. **Apache Spark 4.0+** with Iceberg support
2. **Spark Connect Server** running (typically on port 15002)
3. **Iceberg catalog** configured (Hive, REST, or other)
4. **Resolved dependency**: Once the Spark Connect Go library fixes their versioning, add:
   ```bash
   go get github.com/apache/spark-connect-go@latest
   ```

### Basic Usage

```go
package main

import (
    "context"
    "log"
    
    "github.com/apache/iceberg-go/spark"
    "github.com/apache/iceberg-go"
    "github.com/apache/iceberg-go/table"
)

func main() {
    // Configure Spark connection
    sparkOptions := map[string]string{
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg.type": "hive",
        "spark.sql.catalog.iceberg.uri": "thrift://localhost:9083",
        "spark.sql.catalog.iceberg.warehouse": "s3a://warehouse/",
    }
    
    // Create integration instance
    integration, err := spark.NewSparkIcebergIntegration("localhost", 15002, sparkOptions)
    if err != nil {
        log.Fatal("Failed to create integration:", err)
    }
    defer integration.Close()
    
    // Create a table schema
    schema := iceberg.NewSchema(0,
        iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
        iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
        iceberg.NestedField{ID: 3, Name: "timestamp", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
    )
    
    // Create table
    tableId := table.Identifier{"catalog", "db", "my_table"}
    err = integration.GetTableUtil().CreateTable(context.Background(), tableId, schema, map[string]string{
        "write.format.default": "parquet",
    })
    if err != nil {
        log.Fatal("Failed to create table:", err)
    }
    
    log.Println("✅ Table created successfully!")
}
```

## Detailed Usage Examples

### 1. Table Operations

```go
ctx := context.Background()
tableUtil := integration.GetTableUtil()

// Create table
schema := iceberg.NewSchema(0, /* fields */)
tableId := table.Identifier{"catalog", "db", "users"}
err := tableUtil.CreateTable(ctx, tableId, schema, map[string]string{
    "write.format.default": "parquet",
    "write.target-file-size-bytes": "134217728", // 128MB
})

// Check if table exists
exists, err := tableUtil.TableExists(ctx, tableId)

// List tables
tables, err := tableUtil.ListTables(ctx, table.Identifier{"catalog", "db"})

// Drop table
err = tableUtil.DropTable(ctx, tableId)
```

### 2. Writing Data

```go
write := integration.GetWrite()

// Create DataFrame with sample data
sampleSQL := `
    SELECT * FROM VALUES 
    (1L, 'John', TIMESTAMP '2023-01-01 10:00:00'),
    (2L, 'Jane', TIMESTAMP '2023-01-02 11:00:00')
    AS t(id, name, created_at)
`
df, err := integration.GetSparkUtil().ExecuteSQL(ctx, sampleSQL)

// Write to Iceberg table
writeOptions := &spark.WriteOptions{
    Mode: spark.WriteModeAppend,
    PartitionColumns: []string{"created_at"},
}
err = write.WriteDataFrame(ctx, df, tableId, writeOptions)

// Merge operation
err = write.MergeInto(ctx, sourceDF, targetTableId, 
    "target.id = source.id", 
    map[string]string{"name": "source.name"}, // updates
    map[string]string{"id": "source.id", "name": "source.name"}) // inserts
```

### 3. Scanning Data

```go
scan := integration.GetScan()

// Basic scan with filters
scanOptions := &spark.ScanOptions{
    SelectedColumns: []string{"id", "name"},
    Filter: "name LIKE 'J%'",
    BatchSize: 1000,
}
result, err := scan.ScanTable(ctx, tableId, scanOptions)

// Time travel queries
snapshotID := int64(12345)
result, err = scan.ScanSnapshot(ctx, tableId, snapshotID, scanOptions)

// Scan as of timestamp
timestamp := time.Now().UnixMilli() - 3600000 // 1 hour ago
result, err = scan.ScanAsOfTimestamp(ctx, tableId, timestamp, scanOptions)

// Get table statistics
stats, err := scan.GetTableStatistics(ctx, tableId)
```

### 4. Table Maintenance

```go
actions := integration.GetActions()

// Compact data files
compactionAction := &spark.RewriteDataFilesAction{
    TableIdentifier: tableId,
    Strategy: spark.CompactionStrategyBinPack,
    TargetSizeBytes: 134217728, // 128MB
}
result, err := actions.ExecuteRewriteDataFiles(ctx, compactionAction)

// Expire old snapshots
expireAction := &spark.ExpireSnapshotsAction{
    TableIdentifier: tableId,
    OlderThanTimestampMs: time.Now().UnixMilli() - 86400000, // 24 hours
    RetainLastSnapshots: 10,
}
result, err = actions.ExecuteExpireSnapshots(ctx, expireAction)

// Remove orphan files
orphanAction := &spark.DeleteOrphanFilesAction{
    TableIdentifier: tableId,
    OlderThanMs: time.Now().UnixMilli() - 86400000,
    DryRun: false,
}
result, err = actions.ExecuteDeleteOrphanFiles(ctx, orphanAction)
```

### 5. Schema Evolution

```go
evolution := &spark.SchemaEvolution{
    AddColumns: []spark.AddColumn{
        {Name: "email", Type: "STRING", Comment: "User email"},
        {Name: "age", Type: "INT", Comment: "User age"},
    },
    RenameColumns: map[string]string{
        "old_name": "new_name",
    },
    DropColumns: []string{"deprecated_field"},
}

result, err := actions.ExecuteSchemaEvolution(ctx, tableId, evolution)
```

### 6. Batch Processing

```go
batch := integration.GetBatch()

// Define a batch job
job := &spark.BatchJob{
    Name: "user_aggregation",
    Description: "Aggregate user data by department",
    SQL: `
        SELECT department, 
               COUNT(*) as user_count,
               AVG(age) as avg_age 
        FROM users 
        GROUP BY department
    `,
    OutputTable: table.Identifier{"catalog", "db", "user_stats"},
    Options: &spark.BatchOptions{
        Parallelism: 4,
        MemoryPerTask: "2g",
        WriteOptions: &spark.WriteOptions{
            Mode: spark.WriteModeOverwrite,
        },
    },
}

result, err := batch.ExecuteBatchJob(ctx, job)
```

### 7. Streaming

```go
streaming := integration.GetStreaming()

// Streaming options
streamOptions := &spark.StreamingOptions{
    CheckpointLocation: "/path/to/checkpoints",
    Trigger: spark.TriggerProcessingTime,
    ProcessingTimeInterval: "10 seconds",
    OutputMode: spark.OutputModeAppend,
}

// Read stream from table
sourceDF, err := streaming.ReadStreamFromTable(ctx, sourceTableId, streamOptions)

// Write stream to table
query, err := streaming.WriteStreamToTable(ctx, sourceDF, targetTableId, streamOptions)

// Stream-to-stream join
query, err = streaming.CreateStreamToStreamJoin(ctx, 
    leftTableId, rightTableId, 
    "left.id = right.user_id",
    "INNER",
    map[string]string{"timestamp": "1 minute"}, // watermark
    outputTableId, streamOptions)
```

## Configuration

### Spark Connect Configuration

```go
sparkOptions := map[string]string{
    // Iceberg extensions
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    
    // Catalog configuration
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type": "hive",
    "spark.sql.catalog.iceberg.uri": "thrift://hive-metastore:9083",
    "spark.sql.catalog.iceberg.warehouse": "s3a://warehouse/",
    
    // S3 configuration (if using S3)
    "spark.sql.catalog.iceberg.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.iceberg.s3.endpoint": "http://minio:9000",
    "spark.sql.catalog.iceberg.s3.access-key-id": "minioadmin",
    "spark.sql.catalog.iceberg.s3.secret-access-key": "minioadmin",
    "spark.sql.catalog.iceberg.s3.path-style-access": "true",
    
    // Performance tuning
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
}
```

### Environment Variables

For integration tests:

```bash
export SPARK_INTEGRATION_TEST=true
export SPARK_CONNECT_HOST=localhost
export SPARK_CONNECT_PORT=15002
```

## Testing

The package includes comprehensive integration tests that require a running Spark Connect server:

```bash
# Set up test environment
export SPARK_INTEGRATION_TEST=true

# Run tests
go test -v ./spark/...

# Run specific test
go test -v ./spark/ -run TestSparkTableOperations
```

### Test Requirements

1. **Spark Connect Server** running on localhost:15002
2. **Hive Metastore** (optional, for catalog tests)
3. **S3-compatible storage** (optional, for advanced tests)

## Error Handling

The integration provides comprehensive error handling:

```go
// Check for specific error types
if err != nil {
    switch {
    case strings.Contains(err.Error(), "table does not exist"):
        // Handle table not found
    case strings.Contains(err.Error(), "connection refused"):
        // Handle connection issues
    default:
        // Handle other errors
    }
}
```

## Performance Considerations

1. **Connection pooling**: Reuse `SparkIcebergIntegration` instances
2. **Batch sizes**: Configure appropriate batch sizes for scan operations
3. **Parallelism**: Tune Spark parallelism settings
4. **Memory management**: Configure executor memory based on data size
5. **File sizes**: Use optimal target file sizes (128MB-1GB)

## Limitations

> **Critical**: The Apache Spark Connect Go library currently has versioning issues in their go.mod file that prevent compilation. The library declares a v40 module path but is tagged as v0.1.0, causing Go module resolution to fail.

**Current Issues:**
1. **Dependency Resolution**: Cannot add `github.com/apache/spark-connect-go` due to module versioning conflicts
2. **Compilation**: Code will not compile until the upstream library fixes their go.mod versioning

**Workarounds:**
- Monitor the [Spark Connect Go repository](https://github.com/apache/spark-connect-go) for fixes
- Use Spark's other language bindings (Scala, Python, Java) until Go client is stable
- Consider implementing direct gRPC calls to Spark Connect protocol

**Other Limitations:**
1. **Arrow integration**: Full Arrow-Spark conversion not yet implemented
2. **Streaming operations**: Some streaming features require additional Spark setup
3. **Complex data types**: Some complex type conversions may need refinement
4. **Error recovery**: Advanced error recovery mechanisms not fully implemented

## Contributing

When contributing to the Spark integration:

1. Follow the existing code patterns and structure
2. Add comprehensive tests for new features
3. Update documentation for new functionality
4. Ensure compatibility with multiple Spark versions
5. Test with different catalog implementations

## Dependencies

- Apache Spark Connect Go: `github.com/apache/spark-connect-go` ⚠️ **Currently has versioning issues**
- Apache Arrow Go: `github.com/apache/arrow-go/v18` ✅ **Working**
- Apache Iceberg Go: Core Iceberg Go types and interfaces ✅ **Working**

**Note**: Once the Spark Connect Go library resolves their versioning issues, this integration will be immediately usable.

## License

Licensed under the Apache License, Version 2.0. See the LICENSE file for details. 