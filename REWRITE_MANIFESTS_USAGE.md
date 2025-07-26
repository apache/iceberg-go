# RewriteManifests Feature

The RewriteManifests feature in iceberg-go allows you to optimize table metadata by consolidating and rewriting manifest files. This is equivalent to the Java `org.apache.iceberg.RewriteManifests` and `org.apache.iceberg.BaseRewriteManifests` classes.

## Overview

Manifest files in Apache Iceberg contain metadata about data files in the table. Over time, as tables are modified through inserts, updates, and deletes, you may end up with many small manifest files. This can:

- Increase metadata scan overhead during query planning
- Reduce overall query performance
- Create inefficient storage patterns

The RewriteManifests feature solves these problems by:

- Consolidating small manifest files into larger ones
- Reorganizing manifest structure for better performance  
- Maintaining data integrity while optimizing metadata layout

## Core Components

### Interface: `RewriteManifests`

The main interface providing three key methods:

```go
type RewriteManifests interface {
    // RewriteManifests rewrites manifest files according to specified options
    RewriteManifests(ctx context.Context, options ManifestRewriteOptions, snapshotProps iceberg.Properties) error
    
    // RewriteManifestsBySpec rewrites manifests for specific partition specs
    RewriteManifestsBySpec(ctx context.Context, specIDs []int, options ManifestRewriteOptions, snapshotProps iceberg.Properties) error
    
    // RewriteManifestsByPredicate rewrites manifests that match a given predicate
    RewriteManifestsByPredicate(ctx context.Context, predicate func(iceberg.ManifestFile) bool, options ManifestRewriteOptions, snapshotProps iceberg.Properties) error
}
```

### Implementation: `BaseRewriteManifests`

The main implementation class with configuration options:

```go
type BaseRewriteManifests struct {
    txn               *Transaction
    conflictDetection bool
    caseSensitive     bool
    targetSizeBytes   int64
    minCountToRewrite int
}
```

### Configuration: `ManifestRewriteOptions`

Options to control how manifests are rewritten:

```go
type ManifestRewriteOptions struct {
    Strategy          ManifestRewriteStrategy // How to select manifests
    TargetSizeBytes   int64                   // Target size for new manifests
    MinCountToRewrite int                     // Minimum number to trigger rewriting
    MaxManifestCount  int                     // Maximum number after rewriting
    SnapshotID        int64                   // Which snapshot to rewrite (0 = current)
}
```

### Rewrite Strategies

Three strategies are available:

1. **`RewriteSmallManifests`** - Rewrites manifests smaller than target size
2. **`RewriteAllManifests`** - Rewrites all manifests regardless of size
3. **`RewriteByCount`** - Rewrites when there are too many small manifests

## Usage Examples

### Basic Usage - Rewrite Small Manifests

```go
package main

import (
    "context"
    "log"
    
    "github.com/apache/iceberg-go"
    "github.com/apache/iceberg-go/table"
)

func rewriteSmallManifests(tbl *table.Table) {
    ctx := context.Background()
    
    // Configure options to rewrite small manifests
    options := table.DefaultManifestRewriteOptions()
    options.Strategy = table.RewriteSmallManifests
    options.TargetSizeBytes = 8 * 1024 * 1024 // 8MB target size
    
    // Rewrite manifests - creates new snapshot
    newTable, err := tbl.RewriteManifests(ctx, options, iceberg.Properties{})
    if err != nil {
        log.Fatalf("Failed to rewrite manifests: %v", err)
    }
    
    log.Printf("Successfully rewrote manifests. New snapshot ID: %d", 
               newTable.CurrentSnapshot().SnapshotID())
}
```

### Advanced Usage - Transaction API

```go
func rewriteManifestsInTransaction(tbl *table.Table) {
    ctx := context.Background()
    
    // Start a transaction
    txn := tbl.NewTransaction()
    
    // Create rewriter with custom configuration
    rewriter := txn.NewRewriteManifests().
        WithTargetSizeBytes(16 * 1024 * 1024). // 16MB target
        WithMinCountToRewrite(5).               // At least 5 manifests
        WithConflictDetection(true).            // Enable conflict detection
        WithCaseSensitive(false)                // Case-insensitive column names
    
    // Configure rewrite options
    options := table.DefaultManifestRewriteOptions()
    options.Strategy = table.RewriteByCount
    options.MaxManifestCount = 10
    
    // Perform the rewrite
    err := rewriter.RewriteManifests(ctx, options, iceberg.Properties{
        "rewrite.reason": "optimization",
    })
    if err != nil {
        log.Fatalf("Failed to rewrite manifests: %v", err)
    }
    
    // Commit the transaction
    newTable, err := txn.Commit(ctx)
    if err != nil {
        log.Fatalf("Failed to commit transaction: %v", err)
    }
    
    log.Printf("Transaction committed. New table snapshot: %d", 
               newTable.CurrentSnapshot().SnapshotID())
}
```

### Rewrite by Partition Spec

```go
func rewriteManifestsBySpec(tbl *table.Table) {
    ctx := context.Background()
    
    // Get partition spec IDs you want to rewrite
    specIDs := []int{0, 1} // Rewrite manifests for specs 0 and 1
    
    options := table.DefaultManifestRewriteOptions()
    options.Strategy = table.RewriteAllManifests
    
    newTable, err := tbl.RewriteManifestsBySpec(ctx, specIDs, options, iceberg.Properties{})
    if err != nil {
        log.Fatalf("Failed to rewrite manifests by spec: %v", err)
    }
    
    log.Printf("Rewrote manifests for partition specs %v", specIDs)
}
```

### Rewrite with Custom Predicate

```go
func rewriteManifestsWithPredicate(tbl *table.Table) {
    ctx := context.Background()
    
    // Custom predicate: rewrite manifests larger than 1MB but smaller than 4MB
    predicate := func(mf iceberg.ManifestFile) bool {
        size := mf.Length()
        return size > 1*1024*1024 && size < 4*1024*1024
    }
    
    options := table.DefaultManifestRewriteOptions()
    options.TargetSizeBytes = 8 * 1024 * 1024 // Consolidate into 8MB manifests
    
    newTable, err := tbl.RewriteManifestsByPredicate(ctx, predicate, options, iceberg.Properties{})
    if err != nil {
        log.Fatalf("Failed to rewrite manifests with predicate: %v", err)
    }
    
    log.Printf("Rewrote manifests matching custom predicate")
}
```

### Error Handling

```go
func rewriteManifestsWithErrorHandling(tbl *table.Table) {
    ctx := context.Background()
    options := table.DefaultManifestRewriteOptions()
    
    newTable, err := tbl.RewriteManifests(ctx, options, iceberg.Properties{})
    if err != nil {
        switch {
        case errors.Is(err, table.ErrNoManifestsToRewrite):
            log.Println("No manifests needed rewriting - table already optimized")
            return
        case errors.Is(err, table.ErrManifestRewriteFailed):
            log.Printf("Manifest rewrite operation failed: %v", err)
            return
        default:
            log.Printf("Unexpected error during manifest rewrite: %v", err)
            return
        }
    }
    
    log.Printf("Successfully optimized table manifests")
}
```

## Configuration Options

### Default Settings

```go
// Get default options
opts := table.DefaultManifestRewriteOptions()
// Returns:
// Strategy: RewriteSmallManifests
// TargetSizeBytes: 8MB (from table properties)
// MinCountToRewrite: 100 (from table properties)  
// MaxManifestCount: 100
// SnapshotID: 0 (current snapshot)
```

### Customizing Behavior

```go
// Fine-tune rewriter behavior
rewriter := txn.NewRewriteManifests().
    WithTargetSizeBytes(32 * 1024 * 1024).    // 32MB manifests
    WithMinCountToRewrite(3).                  // Start rewriting with 3+ manifests  
    WithConflictDetection(false).              // Disable conflict detection for speed
    WithCaseSensitive(true)                    // Case-sensitive column resolution
```

## Best Practices

### 1. Monitor Manifest Health

```go
func checkManifestHealth(tbl *table.Table) {
    ctx := context.Background()
    
    // Get current manifests
    snapshot := tbl.CurrentSnapshot()
    if snapshot == nil {
        log.Println("Table has no data")
        return
    }
    
    fs, _ := tbl.FS(ctx)
    manifests, err := snapshot.Manifests(fs)
    if err != nil {
        log.Printf("Failed to get manifests: %v", err)
        return
    }
    
    smallCount := 0
    totalSize := int64(0)
    
    for _, mf := range manifests {
        totalSize += mf.Length()
        if mf.Length() < 4*1024*1024 { // Less than 4MB
            smallCount++
        }
    }
    
    log.Printf("Table has %d manifests, %d are small (<%dMB), total size: %dMB", 
               len(manifests), smallCount, 4, totalSize/(1024*1024))
    
    // Recommend rewrite if more than 50% are small manifests
    if smallCount > len(manifests)/2 {
        log.Println("Recommendation: Consider rewriting manifests for better performance")
    }
}
```

### 2. Integrate with Table Maintenance

```go
func maintainTable(tbl *table.Table) {
    ctx := context.Background()
    
    // First check if manifests need optimization
    checkManifestHealth(tbl)
    
    // Rewrite manifests as part of regular maintenance
    options := table.DefaultManifestRewriteOptions()
    options.Strategy = table.RewriteSmallManifests
    
    newTable, err := tbl.RewriteManifests(ctx, options, iceberg.Properties{
        "maintenance.timestamp": time.Now().Format(time.RFC3339),
        "maintenance.reason":    "scheduled_optimization",
    })
    
    if err != nil && !errors.Is(err, table.ErrNoManifestsToRewrite) {
        log.Printf("Manifest rewrite failed: %v", err)
        return
    }
    
    log.Println("Table maintenance completed successfully")
}
```

### 3. Use Appropriate Strategies

- **`RewriteSmallManifests`**: Best for regular maintenance
- **`RewriteAllManifests`**: Use sparingly, for major reorganization  
- **`RewriteByCount`**: Good for tables with predictable manifest patterns

### 4. Configure Target Sizes

- Smaller targets (1-4MB): Better for tables with frequent metadata scans
- Larger targets (8-32MB): Better for tables with less frequent metadata access
- Consider your query patterns and table access frequency

## Performance Considerations

1. **Conflict Detection**: Enable for concurrent environments, disable for batch processing
2. **Target Size**: Balance between metadata scan performance and storage efficiency  
3. **Transaction Scope**: Combine with other operations when possible to reduce commit overhead
4. **Timing**: Run during low-activity periods to minimize conflicts

## Troubleshooting

### Common Errors

```go
// No manifests to rewrite
if errors.Is(err, table.ErrNoManifestsToRewrite) {
    // This is normal - table is already optimized
}

// Manifest rewrite failed  
if errors.Is(err, table.ErrManifestRewriteFailed) {
    // Check underlying cause and retry if appropriate
}

// Snapshot not found
if strings.Contains(err.Error(), "snapshot not found") {
    // Invalid snapshot ID specified in options
}
```

### Debug Information

```go
func debugManifestRewrite(tbl *table.Table) {
    // Enable debug logging to understand what's happening
    log.SetLevel(log.DebugLevel)
    
    ctx := context.Background()
    options := table.DefaultManifestRewriteOptions()
    
    log.Printf("Starting manifest rewrite with options: %+v", options)
    
    newTable, err := tbl.RewriteManifests(ctx, options, iceberg.Properties{})
    if err != nil {
        log.Printf("Rewrite failed: %v", err)
        return
    }
    
    log.Printf("Rewrite completed. Old snapshot: %d, New snapshot: %d",
               tbl.CurrentSnapshot().SnapshotID(),
               newTable.CurrentSnapshot().SnapshotID())
}
```

This feature provides powerful table optimization capabilities while maintaining full data integrity and following Apache Iceberg specifications. 