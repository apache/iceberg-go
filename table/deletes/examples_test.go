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

package deletes_test

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/deletes"
	iceio "github.com/apache/iceberg-go/io"
)

// Example_basicUsage demonstrates basic position delete functionality.
func Example_basicUsage() {
	// Create a position delete
	delete := deletes.NewPositionDelete("/path/to/data.parquet", 42)
	fmt.Printf("Created position delete: %s\n", delete)

	// Create an index and add deletes
	index := deletes.NewSetPositionDeleteIndex()
	index.Add("/path/to/data.parquet", 42)
	index.Add("/path/to/data.parquet", 100)
	index.Add("/path/to/other.parquet", 25)

	// Check if positions are deleted
	fmt.Printf("Position 42 deleted: %t\n", index.IsDeleted("/path/to/data.parquet", 42))
	fmt.Printf("Position 50 deleted: %t\n", index.IsDeleted("/path/to/data.parquet", 50))

	// Get all deleted positions for a file
	positions := index.Get("/path/to/data.parquet")
	fmt.Printf("Deleted positions in data.parquet: %v\n", positions)

	// Count total deletes
	fmt.Printf("Total deletes: %d\n", index.Count())

	// Output:
	// Created position delete: PositionDelete{file=/path/to/data.parquet, pos=42}
	// Position 42 deleted: true
	// Position 50 deleted: false
	// Deleted positions in data.parquet: [42 100]
	// Total deletes: 3
}

// Example_indexTypes demonstrates different index types and their selection.
func Example_indexTypes() {
	factory := deletes.NewPositionDeleteIndexFactory()

	// Create different index types
	bitmapIndex := factory.Create(deletes.BitmapIndexType)
	setIndex := factory.Create(deletes.SetIndexType)

	// Add the same deletes to both indexes
	for i := int64(0); i < 1000; i += 2 { // Every other position
		bitmapIndex.Add("/path/to/dense.parquet", i)
		setIndex.Add("/path/to/dense.parquet", i)
	}

	fmt.Printf("Bitmap index: %s\n", bitmapIndex.String())
	fmt.Printf("Set index: %s\n", setIndex.String())

	// Both should have the same count
	fmt.Printf("Bitmap count: %d, Set count: %d\n", bitmapIndex.Count(), setIndex.Count())

	// Output:
	// Bitmap index: BitmapPositionDeleteIndex{files=1, totalDeletes=500, deletes=[/path/to/dense.parquet:500]}
	// Set index: SetPositionDeleteIndex{files=1, totalDeletes=500, deletes=[/path/to/dense.parquet:500]}
	// Bitmap count: 500, Set count: 500
}

// Example_builders demonstrates using builders for creating indexes.
func Example_builders() {
	// Using bitmap builder
	bitmapBuilder := deletes.NewBitmapPositionDeleteIndexBuilder()
	bitmapIndex := bitmapBuilder.
		Add("/path/to/file1.parquet", 10).
		Add("/path/to/file1.parquet", 20).
		AddAll("/path/to/file2.parquet", []int64{5, 15, 25}).
		Build()

	fmt.Printf("Built index: %s\n", bitmapIndex.String())
	fmt.Printf("Total deletes: %d\n", bitmapIndex.Count())

	// Output:
	// Built index: BitmapPositionDeleteIndex{files=2, totalDeletes=5, deletes=[/path/to/file1.parquet:2, /path/to/file2.parquet:3]}
	// Total deletes: 5
}

// Example_writers demonstrates various writer implementations.
func Example_writers() {
	ctx := context.Background()

	// This is a conceptual example - in practice you'd use actual file I/O
	var fileIO iceio.IO // This would be initialized with actual file I/O

	// Basic writer
	writerConfig := deletes.PositionDeleteWriterConfig{
		FileIO:     fileIO,
		OutputPath: "/path/to/deletes.parquet",
		FileFormat: iceberg.FileFormatParquet,
	}

	writer, err := deletes.NewPositionDeleteWriter(writerConfig)
	if err != nil {
		log.Fatal(err)
	}

	// Write some deletes
	err = writer.Write(ctx, "/path/to/data.parquet", 42)
	if err != nil {
		log.Fatal(err)
	}

	// Write multiple deletes
	deletesToWrite := []*deletes.PositionDelete{
		deletes.NewPositionDelete("/path/to/data.parquet", 100),
		deletes.NewPositionDelete("/path/to/other.parquet", 25),
	}
	err = writer.WriteBatch(ctx, deletesToWrite)
	if err != nil {
		log.Fatal(err)
	}

	// Close and get metadata
	err = writer.Close()
	if err != nil {
		log.Fatal(err)
	}

	dataFile := writer.DataFile()
	fmt.Printf("Written %d records, %d bytes\n", dataFile.Count(), dataFile.FileSizeBytes())
}

// Example_sortingWriter demonstrates the sorting position delete writer.
func Example_sortingWriter() {
	ctx := context.Background()
	var fileIO iceio.IO // This would be initialized with actual file I/O

	// Create sorting writer configuration
	sortingConfig := deletes.SortingPositionDeleteWriterConfig{
		BaseConfig: deletes.PositionDeleteWriterConfig{
			FileIO:     fileIO,
			OutputPath: "/path/to/sorted_deletes.parquet",
			FileFormat: iceberg.FileFormatParquet,
		},
		SortStrategy: deletes.SortByFilePathThenPosition,
		BatchSize:    1000,
	}

	writer, err := deletes.NewSortingPositionOnlyDeleteWriter(sortingConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer writer.Close()

	// Write deletes in random order - they'll be sorted automatically
	unsortedDeletes := []*deletes.PositionDelete{
		deletes.NewPositionDelete("/path/to/z.parquet", 100),
		deletes.NewPositionDelete("/path/to/a.parquet", 50),
		deletes.NewPositionDelete("/path/to/z.parquet", 10),
		deletes.NewPositionDelete("/path/to/a.parquet", 200),
	}

	err = writer.WriteBatch(ctx, unsortedDeletes)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Written %d sorted deletes\n", writer.WrittenRecords())
}

// Example_clusteredWriter demonstrates the clustered position delete writer.
func Example_clusteredWriter() {
	ctx := context.Background()
	var fileIO iceio.IO // This would be initialized with actual file I/O

	// Create clustered writer configuration
	clusteredConfig := deletes.ClusteredWriterConfig{
		BaseConfig: deletes.PositionDeleteWriterConfig{
			FileIO:     fileIO,
			OutputPath: "/path/to/clustered",
			FileFormat: iceberg.FileFormatParquet,
		},
		ClusteringStrategy:    deletes.ClusterByPartition,
		MaxDeletesPerCluster: 10000,
		MaxClustersInMemory:  50,
		SortStrategy:         deletes.SortByFilePathThenPosition,
	}

	writer, err := deletes.NewClusteredPositionDeleteWriter(clusteredConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer writer.Close()

	// Write deletes from different partitions
	partitionDeletes := []*deletes.PositionDelete{
		deletes.NewPositionDelete("/data/year=2023/month=01/file1.parquet", 10),
		deletes.NewPositionDelete("/data/year=2023/month=01/file2.parquet", 20),
		deletes.NewPositionDelete("/data/year=2023/month=02/file1.parquet", 30),
		deletes.NewPositionDelete("/data/year=2023/month=02/file2.parquet", 40),
	}

	err = writer.WriteBatch(ctx, partitionDeletes)
	if err != nil {
		log.Fatal(err)
	}

	// Get clustering metrics
	metrics := writer.(*deletes.ClusteredPositionDeleteWriter).GetMetrics()
	fmt.Printf("Active clusters: %d, Total deletes: %d\n", 
		metrics.ActiveClusters, metrics.TotalDeletes)
}

// Example_deltaWriter demonstrates the delta writer functionality.
func Example_deltaWriter() {
	ctx := context.Background()
	var fileIO iceio.IO // This would be initialized with actual file I/O

	// Create delta writer configuration
	deltaConfig := deletes.PositionDeltaWriterConfig{
		BaseConfig: deletes.PositionDeleteWriterConfig{
			FileIO:     fileIO,
			OutputPath: "/path/to/delta_deletes.parquet",
			FileFormat: iceberg.FileFormatParquet,
		},
		IndexType:               deletes.BitmapIndexType,
		AutoCommitThreshold:     1000,
		OptimizationStrategy:    deletes.CompactDeltas,
		DeltaCompressionThreshold: 500,
	}

	writer, err := deletes.NewBasePositionDeltaWriter(deltaConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer writer.Close()

	// Perform various delta operations
	err = writer.AddDelta(ctx, "/path/to/file.parquet", 10)
	if err != nil {
		log.Fatal(err)
	}

	err = writer.AddDelta(ctx, "/path/to/file.parquet", 20)
	if err != nil {
		log.Fatal(err)
	}

	// Remove a delete
	err = writer.RemoveDelta(ctx, "/path/to/file.parquet", 10)
	if err != nil {
		log.Fatal(err)
	}

	// Update a delete (move from one position to another)
	err = writer.UpdateDelta(ctx, "/path/to/file.parquet", 20, "/path/to/file.parquet", 30)
	if err != nil {
		log.Fatal(err)
	}

	// Get delta statistics
	stats := writer.GetDeltaStats()
	fmt.Printf("Total operations: %d, Pending: %d, Index size: %d\n",
		stats.TotalOperations, stats.PendingOperations, stats.DeltaIndexSize)

	// Manually commit changes
	err = writer.Commit(ctx)
	if err != nil {
		log.Fatal(err)
	}
}

// Example_indexUtilities demonstrates position delete index utilities.
func Example_indexUtilities() {
	util := &deletes.PositionDeleteIndexUtil{}

	// Create two indexes
	index1 := deletes.NewSetPositionDeleteIndex()
	index1.Add("/path/to/file1.parquet", 10)
	index1.Add("/path/to/file1.parquet", 20)
	index1.Add("/path/to/shared.parquet", 5)

	index2 := deletes.NewSetPositionDeleteIndex()
	index2.Add("/path/to/file2.parquet", 30)
	index2.Add("/path/to/file2.parquet", 40)
	index2.Add("/path/to/shared.parquet", 5) // Same delete as in index1

	// Merge indexes
	merged := util.Merge(index1, index2)
	fmt.Printf("Merged index count: %d\n", merged.Count())

	// Find intersection
	intersection := util.Intersect(index1, index2)
	fmt.Printf("Intersection count: %d\n", intersection.Count())

	// Filter by file prefix
	filtered := util.FilterByFilePrefix(merged, "/path/to/file1")
	fmt.Printf("Filtered count: %d\n", filtered.Count())

	// Filter by position range
	rangeFiltered := util.FilterByPositionRange(merged, 10, 30)
	fmt.Printf("Range filtered count: %d\n", rangeFiltered.Count())

	// Subtract one index from another
	subtracted := util.Subtract(merged, index2)
	fmt.Printf("Subtracted count: %d\n", subtracted.Count())

	// Output:
	// Merged index count: 5
	// Intersection count: 1
	// Filtered count: 2
	// Range filtered count: 4
	// Subtracted count: 2
}

// Example_scanTasks demonstrates position delete scan tasks.
func Example_scanTasks() {
	ctx := context.Background()
	var fileIO iceio.IO       // This would be initialized with actual file I/O
	var deleteFiles []iceberg.DataFile // This would contain actual delete files

	// Create a scan task
	taskConfig := deletes.PositionDeletesScanTaskConfig{
		DeleteFiles:     deleteFiles,
		TargetDataFiles: []string{"/path/to/data1.parquet", "/path/to/data2.parquet"},
		FileIO:          fileIO,
	}

	task := deletes.NewPositionDeletesScanTask(taskConfig)

	fmt.Printf("Scan task: %s\n", task.String())
	fmt.Printf("Estimated records: %d\n", task.EstimatedRecordCount())

	// Scan to index
	index, err := task.ScanToIndex(ctx, deletes.BitmapIndexType)
	if err != nil {
		log.Printf("Scan error: %v", err)
		return
	}

	fmt.Printf("Scanned %d deletes into index\n", index.Count())

	// Create a combined task from multiple tasks
	tasks := []deletes.PositionDeletesScanTask{task}
	combined := deletes.NewCombinedPositionDeletesScanTask(tasks, fileIO)
	fmt.Printf("Combined task: %s\n", combined.String())
}

// Example_completeWorkflow demonstrates a complete position delete workflow.
func Example_completeWorkflow() {
	ctx := context.Background()

	// 1. Create position deletes in memory
	fmt.Println("=== Creating Position Deletes ===")
	factory := deletes.NewPositionDeleteIndexFactory()
	index := factory.Create(deletes.BitmapIndexType)

	// Add various deletes
	deleteData := map[string][]int64{
		"/data/year=2023/month=01/file1.parquet": {10, 25, 42, 100},
		"/data/year=2023/month=01/file2.parquet": {5, 15, 77},
		"/data/year=2023/month=02/file1.parquet": {33, 88},
	}

	for filePath, positions := range deleteData {
		for _, pos := range positions {
			index.Add(filePath, pos)
		}
	}

	fmt.Printf("Created index with %d deletes across %d files\n", 
		index.Count(), len(deleteData))

	// 2. Use utilities to analyze the deletes
	fmt.Println("\n=== Analyzing Deletes ===")
	util := &deletes.PositionDeleteIndexUtil{}

	// Filter deletes for January partition
	januaryDeletes := util.FilterByFilePrefix(index, "/data/year=2023/month=01")
	fmt.Printf("January deletes: %d\n", januaryDeletes.Count())

	// Filter deletes in position range 20-50
	midRangeDeletes := util.FilterByPositionRange(index, 20, 50)
	fmt.Printf("Deletes in range 20-50: %d\n", midRangeDeletes.Count())

	// 3. Convert to Arrow table for analysis
	fmt.Println("\n=== Converting to Arrow Table ===")
	table := util.ToArrowTable(ctx, index)
	fmt.Printf("Arrow table: %d rows, %d columns\n", 
		table.NumRows(), table.Schema().NumFields())
	table.Release()

	// 4. Optimize the index
	fmt.Println("\n=== Optimizing Index ===")
	optimized := util.OptimizeIndex(index)
	fmt.Printf("Optimized index: %s\n", optimized.String())

	fmt.Println("\n=== Workflow Complete ===")

	// Output:
	// === Creating Position Deletes ===
	// Created index with 9 deletes across 3 files
	//
	// === Analyzing Deletes ===
	// January deletes: 7
	// Deletes in range 20-50: 5
	//
	// === Converting to Arrow Table ===
	// Arrow table: 9 rows, 2 columns
	//
	// === Optimizing Index ===
	// Optimized index: BitmapPositionDeleteIndex{files=3, totalDeletes=9, deletes=[/data/year=2023/month=01/file1.parquet:4, /data/year=2023/month=01/file2.parquet:3, /data/year=2023/month=02/file1.parquet:2]}
	//
	// === Workflow Complete ===
} 