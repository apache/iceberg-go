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
	"context"
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
)

// IncrementalScan provides the main API interface for incremental scanning of Iceberg tables.
// It allows reading only the data that was added between two snapshots.
type IncrementalScan interface {
	// UseStartSnapshotID sets the starting snapshot ID for the incremental scan.
	// Data added after this snapshot will be included in the scan.
	UseStartSnapshotID(snapshotID int64) IncrementalScan

	// UseEndSnapshotID sets the ending snapshot ID for the incremental scan.
	// Data added up to and including this snapshot will be included in the scan.
	UseEndSnapshotID(snapshotID int64) IncrementalScan

	// UseStartSnapshotExclusive sets whether the start snapshot should be exclusive.
	// If true, data from the start snapshot itself is excluded.
	UseStartSnapshotExclusive(exclusive bool) IncrementalScan

	// Filter applies a row filter to the incremental scan.
	Filter(expr iceberg.BooleanExpression) IncrementalScan

	// Select specifies which columns to include in the scan.
	Select(columns ...string) IncrementalScan

	// Project selects columns using a schema projection.
	Project(schema *iceberg.Schema) IncrementalScan

	// UseRef specifies a named reference to use as the end snapshot.
	UseRef(ref string) (IncrementalScan, error)

	// Option sets scan options.
	Option(key, value string) IncrementalScan

	// CaseSensitive sets whether column name matching should be case sensitive.
	CaseSensitive(caseSensitive bool) IncrementalScan

	// IncludeColumnStats sets whether to include column statistics in the scan.
	IncludeColumnStats() IncrementalScan

	// PlanFiles returns the files that need to be scanned for this incremental scan.
	PlanFiles(ctx context.Context) ([]FileScanTask, error)

	// ToArrowRecords returns the arrow schema and an iterator over arrow records.
	ToArrowRecords(ctx context.Context) (*arrow.Schema, iter.Seq2[arrow.Record, error], error)

	// ToArrowTable returns all records as an arrow table.
	ToArrowTable(ctx context.Context) (arrow.Table, error)

	// Schema returns the schema that will be used for this scan.
	Schema() (*iceberg.Schema, error)

	// StartSnapshotID returns the starting snapshot ID, if set.
	StartSnapshotID() *int64

	// EndSnapshotID returns the ending snapshot ID, if set.
	EndSnapshotID() *int64

	// IsStartSnapshotExclusive returns whether the start snapshot is exclusive.
	IsStartSnapshotExclusive() bool
}

// IncrementalAppendScan provides API for incremental scanning of append-only operations.
// This is optimized for tables that only have data appended (no deletes or updates).
type IncrementalAppendScan interface {
	IncrementalScan

	// FromSnapshotInclusive creates an incremental scan from the given snapshot (inclusive)
	// to the current snapshot.
	FromSnapshotInclusive(snapshotID int64) IncrementalAppendScan

	// FromSnapshotExclusive creates an incremental scan from after the given snapshot
	// to the current snapshot.
	FromSnapshotExclusive(snapshotID int64) IncrementalAppendScan

	// ToSnapshot creates an incremental scan up to the given snapshot.
	ToSnapshot(snapshotID int64) IncrementalAppendScan

	// AsOfTime creates an incremental scan up to the snapshot at the given time.
	AsOfTime(timestampMs int64) (IncrementalAppendScan, error)
}

// IncrementalChangelogScan provides API for incremental scanning that includes
// change information (inserted, updated, deleted records).
type IncrementalChangelogScan interface {
	IncrementalScan

	// FromSnapshotInclusive creates an incremental changelog scan from the given snapshot (inclusive).
	FromSnapshotInclusive(snapshotID int64) IncrementalChangelogScan

	// FromSnapshotExclusive creates an incremental changelog scan from after the given snapshot.
	FromSnapshotExclusive(snapshotID int64) IncrementalChangelogScan

	// ToSnapshot creates an incremental changelog scan up to the given snapshot.
	ToSnapshot(snapshotID int64) IncrementalChangelogScan
}

// IncrementalScanEvent represents an event during incremental scanning.
type IncrementalScanEvent struct {
	// ScanID is a unique identifier for this scan operation.
	ScanID string
	// TableName is the name of the table being scanned.
	TableName string
	// StartSnapshotID is the starting snapshot ID for the scan.
	StartSnapshotID *int64
	// EndSnapshotID is the ending snapshot ID for the scan.
	EndSnapshotID *int64
	// FilesScanned is the number of files that were scanned.
	FilesScanned int
	// RecordsScanned is the number of records that were scanned.
	RecordsScanned int64
	// ScanDurationMs is the duration of the scan in milliseconds.
	ScanDurationMs int64
} 