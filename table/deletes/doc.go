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

// Package deletes provides comprehensive support for Apache Iceberg position deletes,
// including data structures, indexing, writers, and utilities for managing deleted rows
// in Iceberg tables.
//
// # Position Deletes Overview
//
// Position deletes in Apache Iceberg are a mechanism for marking specific rows in data files
// as deleted without rewriting the entire file. Each position delete record contains:
//   - file_path: The path to the data file containing the deleted row
//   - pos: The 0-based row position within that file that is deleted
//
// This package implements full position delete support comparable to the Java implementation,
// including efficient indexing, multiple writer strategies, and comprehensive utilities.
//
// # Core Data Structures
//
// The package provides several key data structures:
//
//   - PositionDelete: Represents a single position delete record
//   - PositionDeleteIndex: Interface for efficient position delete indexing and querying
//   - BitmapPositionDeleteIndex: Bitmap-based implementation for dense deletes
//   - SetPositionDeleteIndex: Set-based implementation for sparse deletes
//
// # Writers
//
// Multiple writer implementations are provided for different use cases:
//
//   - BasePositionDeleteWriter: Basic writer for position delete files
//   - SortingPositionOnlyDeleteWriter: Writer that sorts deletes for optimal performance
//   - FileScopedPositionDeleteWriter: Writer that organizes deletes by file scope
//
// # Usage Examples
//
// ## Basic Usage
//
//	// Create a position delete
//	delete := deletes.NewPositionDelete("/path/to/data.parquet", 42)
//
//	// Create an index and add deletes
//	index := deletes.NewSetPositionDeleteIndex()
//	index.Add("/path/to/data.parquet", 42)
//	index.Add("/path/to/data.parquet", 100)
//
//	// Check if a position is deleted
//	if index.IsDeleted("/path/to/data.parquet", 42) {
//	    // Handle deleted row
//	}
//
// ## Using Writers
//
//	config := deletes.PositionDeleteWriterConfig{
//	    FileIO:     fileIO,
//	    OutputPath: "/path/to/deletes.parquet",
//	    FileFormat: iceberg.FileFormatParquet,
//	}
//
//	writer, err := deletes.NewPositionDeleteWriter(config)
//	if err != nil {
//	    // Handle error
//	}
//	defer writer.Close()
//
//	// Write position deletes
//	err = writer.Write(ctx, "/path/to/data.parquet", 42)
//	err = writer.WritePositionDelete(ctx, delete)
//
// ## Using Index Utilities
//
//	util := &deletes.PositionDeleteIndexUtil{}
//
//	// Merge multiple indexes
//	merged := util.Merge(index1, index2, index3)
//
//	// Filter deletes by file prefix
//	filtered := util.FilterByFilePrefix(index, "/path/to/partition")
//
//	// Convert to Arrow table
//	table := util.ToArrowTable(ctx, index)
//
// # Index Selection
//
// The package automatically chooses the most appropriate index type based on data characteristics:
//
//   - BitmapPositionDeleteIndex: Used for dense deletes or when there are many deletes per file
//   - SetPositionDeleteIndex: Used for sparse deletes or when memory efficiency is important
//
// You can also explicitly choose the index type using the PositionDeleteIndexFactory.
//
// # Performance Considerations
//
// - Bitmap indexes are more memory-efficient for dense deletes but may use more memory for sparse deletes
// - Set indexes are more memory-efficient for sparse deletes and provide consistent performance
// - Sorted writers improve query performance by organizing deletes optimally
// - File-scoped writers reduce the overhead of applying deletes during reads
//
// # Thread Safety
//
// Most components in this package are not thread-safe by default. The FileScopedPositionDeleteWriter
// is thread-safe for concurrent writes, but individual indexes and basic writers should be used
// from a single goroutine or protected with appropriate synchronization.
package deletes 