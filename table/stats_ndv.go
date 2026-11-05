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
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table/stats"
)

// NDVResult is the number-of-distinct-values sketch computed for one column.
type NDVResult struct {
	// FieldID is the Iceberg field ID of the column.
	FieldID int
	// NDV is the estimated number of distinct non-null values.
	NDV int64
	// Sketch is the serialized DataSketches Theta sketch, i.e. the body of an
	// apache-datasketches-theta-v1 statistics blob.
	Sketch []byte
}

// ComputeNDVs scans a snapshot and computes a DataSketches Theta sketch per
// top-level primitive column, returning the estimated number of distinct values
// keyed by field ID.
//
// When snapshotID is 0 the current snapshot is used. When columns is empty every
// top-level primitive column is analyzed, matching Iceberg-Java's
// ComputeTableStats default; named non-primitive or unknown columns are skipped.
//
// Values are fed to the sketch using Iceberg's single-value serialization (the
// same encoding used for manifest bounds), so estimates are comparable with
// sketches produced by other Iceberg implementations. Null values are not
// counted.
func (t Table) ComputeNDVs(ctx context.Context, snapshotID int64, columns ...string) (map[int]NDVResult, error) {
	opts := []ScanOption{WithSnapshotID(snapshotID)}
	if len(columns) > 0 {
		opts = append(opts, WithSelectedFields(columns...))
	}
	scan := t.Scan(opts...)

	// Resolve the snapshot so an empty or missing target is a clear error rather
	// than a silently empty result.
	if _, err := scan.ResolveSnapshot(); err != nil {
		return nil, fmt.Errorf("compute NDVs: %w", err)
	}

	projected, err := scan.Projection()
	if err != nil {
		return nil, fmt.Errorf("compute NDVs: %w", err)
	}

	// Restrict to top-level primitive fields; nested fields are not sketched,
	// matching the reference implementation.
	primitives := make(map[string]iceberg.NestedField)
	for _, f := range projected.Fields() {
		if _, ok := f.Type.(iceberg.PrimitiveType); ok {
			primitives[f.Name] = f
		}
	}
	if len(primitives) == 0 {
		return map[int]NDVResult{}, nil
	}

	arrowSchema, records, err := scan.ToArrowRecords(ctx)
	if err != nil {
		return nil, fmt.Errorf("compute NDVs: %w", err)
	}

	// Map each arrow column index to the target field and its sketch, matching
	// by name against the projected schema (the returned arrow schema carries no
	// field-id metadata).
	targets := make(map[int]*ndvTarget)
	for i, f := range arrowSchema.Fields() {
		nf, ok := primitives[f.Name]
		if !ok {
			continue
		}
		targets[i] = &ndvTarget{field: nf, sketch: stats.NewSketch()}
	}

	for rec, err := range records {
		if err != nil {
			return nil, fmt.Errorf("compute NDVs: %w", err)
		}
		if err := addRecordToSketches(rec, targets); err != nil {
			rec.Release()

			return nil, fmt.Errorf("compute NDVs: %w", err)
		}
		rec.Release()
	}

	results := make(map[int]NDVResult, len(targets))
	for _, tgt := range targets {
		results[tgt.field.ID] = NDVResult{
			FieldID: tgt.field.ID,
			NDV:     tgt.sketch.NDV(),
			Sketch:  tgt.sketch.Serialize(),
		}
	}

	return results, nil
}

// ndvTarget pairs a primitive column with the sketch accumulating its values.
type ndvTarget struct {
	field  iceberg.NestedField
	sketch *stats.Sketch
}

// addRecordToSketches feeds every non-null value of each targeted column into
// its sketch, using Iceberg single-value serialization for the hashed bytes.
func addRecordToSketches(rec arrow.RecordBatch, targets map[int]*ndvTarget) error {
	numRows := int(rec.NumRows())
	for col := range int(rec.NumCols()) {
		tgt, ok := targets[col]
		if !ok {
			continue
		}
		arr := rec.Column(col)
		for row := range numRows {
			lit, err := getArrowValueAsIcebergLiteral(arr, row, tgt.field.Type)
			if err != nil {
				return fmt.Errorf("field %q: row %d: %w", tgt.field.Name, row, err)
			}
			if lit == nil {
				continue // null value: not counted
			}
			b, err := lit.MarshalBinary()
			if err != nil {
				return fmt.Errorf("field %q: serialize value: %w", tgt.field.Name, err)
			}
			tgt.sketch.Add(b)
		}
	}

	return nil
}
