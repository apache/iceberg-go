// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
)

// AllManifests returns every manifest referenced by every snapshot currently
// tracked by the table. A manifest referenced by multiple snapshots produces
// one row per reference, identified by reference_snapshot_id.
func (i InspectTable) AllManifests(ctx context.Context) (array.RecordReader, error) {
	arrowSchema, err := SchemaToArrowSchema(AllManifestsSchema(), nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("inspect all manifests: build arrow schema: %w", err)
	}

	bldr := array.NewRecordBuilder(i.alloc, arrowSchema)
	defer bldr.Release()

	snapshots := i.tbl.metadata.Snapshots()
	if len(snapshots) > 0 {
		if i.tbl.fsF == nil {
			return nil, errors.New("inspect all manifests: table file IO is not configured")
		}
		fs, err := i.tbl.fsF(ctx)
		if err != nil {
			return nil, fmt.Errorf("inspect all manifests: %w", err)
		}

		for _, snapshot := range snapshots {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			manifests, err := snapshot.Manifests(fs)
			if err != nil {
				return nil, fmt.Errorf("inspect all manifests: read snapshot %d manifests: %w",
					snapshot.SnapshotID, err)
			}
			referenceSnapshotID := snapshot.SnapshotID
			if err := i.appendManifestRows(ctx, bldr, manifests, &referenceSnapshotID); err != nil {
				return nil, fmt.Errorf("inspect all manifests: snapshot %d: %w", snapshot.SnapshotID, err)
			}
		}
	}

	rr, err := singleBatchReader(arrowSchema, bldr)
	if err != nil {
		return nil, fmt.Errorf("inspect all manifests: %w", err)
	}

	return rr, nil
}

// AllManifestsSchema returns the schema of the all_manifests metadata table.
func AllManifestsSchema() *iceberg.Schema {
	fields := ManifestsSchema().Fields()
	fields = append(fields,
		iceberg.NestedField{
			ID: 18, Name: "reference_snapshot_id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
		},
		iceberg.NestedField{
			ID: 19, Name: "key_metadata", Type: iceberg.PrimitiveTypes.Binary, Required: false,
		},
	)

	return iceberg.NewSchema(0, fields...)
}
