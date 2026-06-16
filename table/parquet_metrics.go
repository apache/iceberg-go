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

package table

import (
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/iceberg-go"
	tblutils "github.com/apache/iceberg-go/table/internal"
)

// ParquetDataFileArgs collects everything needed to compute a fully
// populated [iceberg.DataFile] (data, equality-delete, or positional-
// delete) from a parquet file's in-heap [metadata.FileMetaData] without
// performing any filesystem reads.
type ParquetDataFileArgs struct {
	Schema           *iceberg.Schema
	Spec             iceberg.PartitionSpec
	ParquetMetadata  *metadata.FileMetaData
	FilePath         string
	FileSize         int64
	Content          iceberg.ManifestEntryContent
	PartitionValues  map[int]any
	SortOrderID      int
	Properties       iceberg.Properties
	EqualityFieldIDs []int
}

// DataFileFromParquetMetadata builds a fully populated [iceberg.DataFile]
// from a parquet file's in-heap [metadata.FileMetaData], extracting per-
// column statistics (column_sizes, value_counts, null_value_counts,
// lower_bounds, upper_bounds), split offsets (= row-group start offsets),
// and the manifest-entry shape required for the requested content type.
// No filesystem reads are performed.
func DataFileFromParquetMetadata(args ParquetDataFileArgs) (iceberg.DataFile, error) {
	if args.Schema == nil {
		return nil, errors.New("DataFileFromParquetMetadata: Schema is required")
	}
	if args.ParquetMetadata == nil {
		return nil, errors.New("DataFileFromParquetMetadata: ParquetMetadata is required")
	}
	if args.FilePath == "" {
		return nil, errors.New("DataFileFromParquetMetadata: FilePath is required")
	}
	if args.FileSize < 0 {
		return nil, fmt.Errorf("DataFileFromParquetMetadata: FileSize must be >= 0, got %d", args.FileSize)
	}

	switch args.Content {
	case iceberg.EntryContentData,
		iceberg.EntryContentEqDeletes,
		iceberg.EntryContentPosDeletes:
	default:
		return nil, fmt.Errorf("DataFileFromParquetMetadata: invalid Content %v", args.Content)
	}

	if args.Content == iceberg.EntryContentEqDeletes && len(args.EqualityFieldIDs) == 0 {
		return nil, errors.New("DataFileFromParquetMetadata: EqualityFieldIDs is required for equality-delete files")
	}
	if args.Content != iceberg.EntryContentEqDeletes && len(args.EqualityFieldIDs) > 0 {
		return nil, fmt.Errorf("DataFileFromParquetMetadata: EqualityFieldIDs must be empty for content %s", args.Content)
	}

	format := tblutils.GetFileFormat(iceberg.ParquetFile)
	if format == nil {
		return nil, errors.New("DataFileFromParquetMetadata: parquet file format unavailable")
	}

	statsPlan, err := computeStatsPlan(args.Schema, args.Properties)
	if err != nil {
		return nil, fmt.Errorf("DataFileFromParquetMetadata: build stats plan: %w", err)
	}

	pathMapping, err := format.PathToIDMapping(args.Schema)
	if err != nil {
		return nil, fmt.Errorf("DataFileFromParquetMetadata: build path-to-id mapping: %w", err)
	}

	stats := format.DataFileStatsFromMeta(
		args.ParquetMetadata,
		statsPlan,
		pathMapping,
		tblutils.VariantFieldIDsFromSchema(args.Schema),
	)
	if stats == nil {
		return nil, errors.New("DataFileFromParquetMetadata: failed to compute file statistics")
	}

	// EqualityFieldIDs is consumed by DataFileStatistics.ToDataFile and
	// written into the manifest entry only when Content is EqDeletes.
	if len(args.EqualityFieldIDs) > 0 {
		stats.EqualityFieldIDs = append(stats.EqualityFieldIDs, args.EqualityFieldIDs...)
	}

	partitionValues := args.PartitionValues
	if partitionValues == nil {
		partitionValues = make(map[int]any)
	}

	df := stats.ToDataFile(tblutils.DataFileOpts{
		Schema:          args.Schema,
		Spec:            args.Spec,
		Path:            args.FilePath,
		Format:          iceberg.ParquetFile,
		Content:         args.Content,
		FileSize:        args.FileSize,
		PartitionValues: partitionValues,
		SortOrderID:     args.SortOrderID,
	})

	return df, nil
}
