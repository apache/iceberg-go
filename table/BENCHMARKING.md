# Benchmarking Framework

This document describes the comprehensive benchmarking framework for the Apache Iceberg Go `table` package, designed to track performance characteristics and prevent performance regressions.

## Overview

The benchmarking framework provides systematic performance measurements across all critical operations in the Iceberg table package, following Go's standard benchmarking conventions. It enables developers to:

- Track performance over time
- Identify performance regressions
- Compare the impact of code changes
- Optimize critical code paths
- Ensure consistent performance characteristics

## Benchmark Categories

### 1. Table Creation Benchmarks

These benchmarks measure the performance of creating Iceberg tables with different schema complexities.

- **`BenchmarkTableCreationSimple`**: Creates tables with basic schemas (2 fields)
- **`BenchmarkTableCreationComplex`**: Creates tables with complex schemas (8+ fields, nested types)
- **`BenchmarkTableCreationPartitioned`**: Creates tables with partition specifications

**Example output:**
```
BenchmarkTableCreationSimple-12     805   301073 ns/op   43156 B/op   443 allocs/op
BenchmarkTableCreationComplex-12    704   341323 ns/op   59716 B/op   663 allocs/op
BenchmarkTableCreationPartitioned-12 764  312279 ns/op   52607 B/op   542 allocs/op
```

### 2. Data Writing Benchmarks

These benchmarks measure the performance of writing data to Iceberg tables.

- **`BenchmarkDataWritingSmall`**: Writes small datasets (100 records)
- **`BenchmarkDataWritingMedium`**: Writes medium datasets (10,000 records)
- **`BenchmarkDataWritingBatch`**: Writes multiple files in batches

**Key metrics:**
- Throughput (MB/s)
- Latency (ns/op)
- Memory allocations

**Example output:**
```
BenchmarkDataWritingSmall-12      398   583518 ns/op   4.11 MB/s   1817103 B/op   1916 allocs/op
BenchmarkDataWritingMedium-12     385   626651 ns/op   382.99 MB/s 1823289 B/op   1926 allocs/op
BenchmarkDataWritingBatch-12      228   1042078 ns/op  230.31 MB/s 2180650 B/op   5669 allocs/op
```

### 3. Data Reading Benchmarks

These benchmarks measure the performance of reading data from Iceberg tables.

- **`BenchmarkDataReadingSmall`**: Reads small datasets
- **`BenchmarkDataReadingWithFilter`**: Reads with row filters applied
- **`BenchmarkDataReadingPlanFiles`**: Measures file planning performance

**Example output:**
```
BenchmarkDataReadingSmall-12           229   1061960 ns/op   22.60 MB/s   8012873 B/op   11169 allocs/op
BenchmarkDataReadingWithFilter-12      164   1456628 ns/op             9992243 B/op   19330 allocs/op
BenchmarkDataReadingPlanFiles-12       422   546159 ns/op              788025 B/op    10127 allocs/op
```

### 4. Schema Operation Benchmarks

These benchmarks measure the performance of schema operations.

- **`BenchmarkSchemaFieldLookup`**: Field lookup by name
- **`BenchmarkSchemaFieldLookupCaseInsensitive`**: Case-insensitive field lookup
- **`BenchmarkSchemaEquality`**: Schema equality comparisons

**Example output:**
```
BenchmarkSchemaFieldLookup-12                  17146582   13.83 ns/op   0 B/op   0 allocs/op
BenchmarkSchemaFieldLookupCaseInsensitive-12   11532408   20.68 ns/op   0 B/op   0 allocs/op
BenchmarkSchemaEquality-12                      8900454   26.79 ns/op   0 B/op   0 allocs/op
```

### 5. Transaction Benchmarks

These benchmarks measure transaction-related operations.

- **`BenchmarkTransactionCreation`**: Creating new transactions
- **`BenchmarkTransactionScan`**: Creating scans from transactions

### 6. Partition Operation Benchmarks

These benchmarks measure partition specification operations.

- **`BenchmarkPartitionSpecFieldIteration`**: Iterating over partition fields
- **`BenchmarkPartitionSpecEquality`**: Partition spec equality comparisons

### 7. Metadata and Properties Benchmarks

These benchmarks measure metadata access patterns.

- **`BenchmarkPropertyAccess`**: Accessing table properties
- **`BenchmarkMetadataAccess`**: Accessing table metadata

### 8. End-to-End Benchmarks

These benchmarks measure complete workflows.

- **`BenchmarkEndToEndWorkflow`**: Complete table creation, data writing, and reading workflow

## Usage

### Running All Benchmarks

```bash
go test -bench=. -benchtime=200ms
```

### Running Specific Benchmark Categories

```bash
# Table creation benchmarks
go test -bench=BenchmarkTableCreation -benchtime=200ms

# Data writing benchmarks
go test -bench=BenchmarkDataWriting -benchtime=200ms

# Schema operation benchmarks
go test -bench=BenchmarkSchema -benchtime=200ms
```

### Running with Memory Profiling

```bash
go test -bench=. -benchmem -memprofile=mem.prof
```

### Running with CPU Profiling

```bash
go test -bench=. -cpuprofile=cpu.prof
```

### Comparing Performance

To compare performance between versions or changes:

```bash
# Baseline
go test -bench=. -benchtime=500ms > baseline.txt

# After changes
go test -bench=. -benchtime=500ms > new.txt

# Compare with benchcmp (if available)
benchcmp baseline.txt new.txt
```

## Benchmark Design Principles

### 1. Isolation

Each benchmark iteration uses isolated resources:
- Unique table names per iteration
- Fresh catalog instances
- Temporary directories

### 2. Realistic Workloads

Benchmarks simulate realistic usage patterns:
- Various data sizes
- Different schema complexities
- Common query patterns

### 3. Stable Measurements

Benchmarks are designed for stable, repeatable measurements:
- Proper setup and teardown
- Adequate warm-up periods
- Resource cleanup

### 4. Comprehensive Coverage

The framework covers all major operations:
- Table lifecycle (create, read, write)
- Schema operations
- Partition operations
- Transaction handling
- Metadata access

## Interpreting Results

### Key Metrics

- **ns/op**: Nanoseconds per operation (lower is better)
- **MB/s**: Throughput in megabytes per second (higher is better)
- **B/op**: Bytes allocated per operation (lower is better)
- **allocs/op**: Allocations per operation (lower is better)

### Performance Targets

Based on the current implementation:

| Operation | Target Performance |
|-----------|-------------------|
| Simple table creation | < 500μs |
| Schema field lookup | < 20ns |
| Small data write | > 4 MB/s |
| Medium data write | > 300 MB/s |
| Data read | > 20 MB/s |

### Regression Detection

Monitor for:
- **Latency increases** > 20% in critical paths
- **Throughput decreases** > 15% in data operations
- **Memory allocation increases** > 25% in any operation
- **Allocation count increases** > 30% in hot paths

## CI/CD Integration

### Automated Performance Testing

Include benchmarks in CI/CD pipelines:

```yaml
- name: Run Benchmarks
  run: |
    go test -bench=. -benchtime=200ms -benchmem > bench_results.txt
    # Upload results for analysis
```

### Performance Monitoring

Set up automated performance monitoring:
- Track benchmark results over time
- Alert on performance regressions
- Generate performance reports

### Performance Gates

Consider implementing performance gates:
- Fail builds if performance degrades significantly
- Require performance analysis for large changes
- Validate performance improvements

## Best Practices

### For Developers

1. **Run benchmarks before and after changes**
2. **Focus on hot paths and critical operations**
3. **Use profiling to identify bottlenecks**
4. **Consider memory allocations, not just speed**
5. **Test with realistic data sizes**

### For Maintainers

1. **Review benchmark results in pull requests**
2. **Maintain performance targets**
3. **Update benchmarks when adding new features**
4. **Document performance expectations**
5. **Investigate and address regressions promptly**

### For Users

1. **Use benchmarks to understand performance characteristics**
2. **Run benchmarks in your environment**
3. **Report performance issues with benchmark data**
4. **Compare performance across different configurations**

## Extension and Customization

### Adding New Benchmarks

To add a new benchmark:

1. Follow the `BenchmarkXxx` naming convention
2. Use the `BenchmarkSetup` helper for common setup
3. Reset the timer before the operation being measured
4. Report allocations with `b.ReportAllocs()`
5. Set bytes processed with `b.SetBytes()` for throughput calculations

Example:
```go
func BenchmarkNewOperation(b *testing.B) {
    setup := newBenchmarkSetup(b)
    // ... setup code ...
    
    b.ResetTimer()
    b.ReportAllocs()
    b.SetBytes(int64(dataSize))
    
    for i := 0; i < b.N; i++ {
        // ... operation being measured ...
    }
}
```

### Custom Scenarios

Create custom benchmarks for specific use cases:
- Different table sizes
- Various query patterns
- Specific hardware configurations
- Custom data distributions

## Troubleshooting

### Common Issues

1. **Inconsistent results**: Ensure proper isolation and stable test environment
2. **High variance**: Increase benchmark time or check for external interference
3. **Memory leaks**: Verify proper resource cleanup in benchmarks
4. **Platform differences**: Run benchmarks on target platforms

### Debugging Performance Issues

1. **Use CPU profiling**: `go test -bench=. -cpuprofile=cpu.prof`
2. **Use memory profiling**: `go test -bench=. -memprofile=mem.prof`
3. **Analyze with pprof**: `go tool pprof cpu.prof`
4. **Check for allocation hotspots**: Focus on `allocs/op` metrics

## Conclusion

The benchmarking framework provides comprehensive performance tracking for the Iceberg Go table package. Regular use of these benchmarks helps maintain high performance standards and prevents performance regressions, ensuring the library remains efficient and scalable for production use.

For questions or suggestions about the benchmarking framework, please open an issue in the project repository. 