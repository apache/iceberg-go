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
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
)

// InspectTable exposes a table's metadata (snapshots, history, manifests, and
// so on) as queryable Arrow tables. It mirrors the metadata tables offered by
// the Java, PyIceberg, and Rust clients.
//
// Obtain one via Table.Inspect. Each method returns an array.RecordReader that
// the caller is responsible for releasing.
type InspectTable struct {
	tbl Table
}

// Inspect returns an InspectTable for reading this table's metadata tables.
func (t Table) Inspect() InspectTable {
	return InspectTable{tbl: t}
}

// History returns the chronological log of every snapshot that was ever the
// table's current snapshot, one row per snapshot-log entry. Rolled-back
// snapshots remain visible but are flagged via is_current_ancestor.
//
// Columns:
//   - made_current_at (timestamptz, required): when the snapshot became current
//   - snapshot_id (long, required): the snapshot that became current
//   - parent_id (long, optional): the snapshot's parent, null when the snapshot
//     has no parent or has since been expired
//   - is_current_ancestor (boolean, required): whether the snapshot is an
//     ancestor of the current snapshot; false for rolled-back snapshots
//
// is_current_ancestor is derived by walking the current snapshot's parent
// chain. If an intermediate ancestor has been expired (removed from the
// snapshot list), that walk is truncated and snapshots below the gap are
// reported as non-ancestors. Well-formed tables never hit this: ExpireSnapshots
// keeps the current snapshot and its full ancestry intact. This matches the
// silent-truncation behavior of the Java, PyIceberg, and Rust clients.
//
// The returned reader holds a single record batch. The caller must Release it.
func (i InspectTable) History(ctx context.Context) (array.RecordReader, error) {
	sc := historySchema()

	arrowSchema, err := SchemaToArrowSchema(sc, nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("inspect history: build arrow schema: %w", err)
	}

	// Walk the current snapshot's parent chain once. Membership in this set
	// is what tells the live lineage apart from rolled-back history entries.
	// AncestorsOf already guards against cycles in malformed metadata, so a
	// corrupt parent chain cannot hang the scan.
	ancestors := make(map[int64]struct{})
	if current := i.tbl.metadata.CurrentSnapshot(); current != nil {
		for _, snap := range AncestorsOf(current.SnapshotID, i.tbl.metadata.SnapshotByID) {
			ancestors[snap.SnapshotID] = struct{}{}
		}
	}

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer bldr.Release()

	// Field positions and concrete builder types follow historySchema; the
	// assertions are safe as long as the two stay in sync.
	madeCurrentAt := bldr.Field(0).(*array.TimestampBuilder)
	snapshotID := bldr.Field(1).(*array.Int64Builder)
	parentID := bldr.Field(2).(*array.Int64Builder)
	isCurrentAncestor := bldr.Field(3).(*array.BooleanBuilder)

	for entry := range i.tbl.metadata.SnapshotLogs() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Iceberg stores timestamps as epoch milliseconds; Arrow uses
		// microseconds for the timestamptz type.
		madeCurrentAt.Append(arrow.Timestamp(entry.TimestampMs * 1000))
		snapshotID.Append(entry.SnapshotID)

		// parent_id resolves through the live snapshot table, so an entry
		// referencing an expired snapshot renders a null parent.
		if snap := i.tbl.metadata.SnapshotByID(entry.SnapshotID); snap != nil && snap.ParentSnapshotID != nil {
			parentID.Append(*snap.ParentSnapshotID)
		} else {
			parentID.AppendNull()
		}

		_, ok := ancestors[entry.SnapshotID]
		isCurrentAncestor.Append(ok)
	}

	rec := bldr.NewRecordBatch()
	// NewRecordReader retains rec, so release our reference unconditionally:
	// on success the reader owns it, and on error NewRecordReader has already
	// released its own retain. This avoids a deferred double-release.
	rr, err := array.NewRecordReader(arrowSchema, []arrow.RecordBatch{rec})
	rec.Release()
	if err != nil {
		return nil, fmt.Errorf("inspect history: new record reader: %w", err)
	}

	return rr, nil
}

// historySchema returns the Iceberg schema of the history metadata table. The
// field IDs match the Java, PyIceberg, and Rust clients for cross-client parity.
func historySchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "made_current_at", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
		iceberg.NestedField{ID: 2, Name: "snapshot_id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 3, Name: "parent_id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 4, Name: "is_current_ancestor", Type: iceberg.PrimitiveTypes.Bool, Required: true},
	)
}
