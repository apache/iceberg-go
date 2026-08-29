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
	"fmt"
	"sort"
	"testing"

	"github.com/apache/iceberg-go"
	iceberginternal "github.com/apache/iceberg-go/internal"
	"github.com/apache/iceberg-go/table/internal"
)

var equalityDeleteConflictFilterBenchmarkSink iceberg.BooleanExpression

func BenchmarkEqDeletePartitionsToFilter(b *testing.B) {
	for _, fieldCount := range []int{1, 4} {
		for _, tc := range []struct {
			fileCount      int
			partitionCount int
		}{
			{fileCount: 1_000, partitionCount: 100},
			{fileCount: 10_000, partitionCount: 100},
			{fileCount: 1_000, partitionCount: 1_000},
		} {
			name := fmt.Sprintf(
				"fields=%d/files=%d/partitions=%d",
				fieldCount,
				tc.fileCount,
				tc.partitionCount,
			)
			b.Run(name, func(b *testing.B) {
				meta, files := equalityDeleteConflictBenchmarkInput(
					b, fieldCount, tc.fileCount, tc.partitionCount)

				b.Run("before", func(b *testing.B) {
					benchmarkEqDeletePartitionFilter(b, files, meta, eqDeletePartitionsToFilterBeforeDedup)
				})
				b.Run("after", func(b *testing.B) {
					benchmarkEqDeletePartitionFilter(b, files, meta, eqDeletePartitionsToFilter)
				})
			})
		}
	}
}

type eqDeletePartitionFilterBuilder func(
	[]iceberg.DataFile, Metadata,
) (iceberg.BooleanExpression, error)

func benchmarkEqDeletePartitionFilter(
	b *testing.B,
	files []iceberg.DataFile,
	meta Metadata,
	build eqDeletePartitionFilterBuilder,
) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		filter, err := build(files, meta)
		if err != nil {
			b.Fatal(err)
		}
		equalityDeleteConflictFilterBenchmarkSink = filter
	}
}

func equalityDeleteConflictBenchmarkInput(
	b *testing.B,
	fieldCount, fileCount, partitionCount int,
) (Metadata, []iceberg.DataFile) {
	b.Helper()

	schemaFields := make([]iceberg.NestedField, fieldCount)
	partitionFields := make([]iceberg.PartitionField, fieldCount)
	for i := range fieldCount {
		fieldID := i + 1
		fieldName := fmt.Sprintf("partition_%d", i)
		schemaFields[i] = iceberg.NestedField{
			ID:       fieldID,
			Name:     fieldName,
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		}
		partitionFields[i] = iceberg.PartitionField{
			FieldID:   1000 + i,
			SourceIDs: []int{fieldID},
			Name:      fieldName,
			Transform: iceberg.IdentityTransform{},
		}
	}

	schema := iceberg.NewSchema(0, schemaFields...)
	spec := iceberg.NewPartitionSpec(partitionFields...)
	meta, err := NewMetadata(
		schema,
		&spec,
		UnsortedSortOrder,
		"file:///tmp/eq-delete-conflict-benchmark",
		iceberg.Properties{PropertyFormatVersion: "2"},
	)
	if err != nil {
		b.Fatal(err)
	}

	files := make([]iceberg.DataFile, fileCount)
	for i := range fileCount {
		partition := make(map[int]any, fieldCount)
		partitionID := i % partitionCount
		for field := range fieldCount {
			partition[1000+field] = fmt.Sprintf("partition-%d-field-%d", partitionID, field)
		}

		builder, err := iceberg.NewDataFileBuilder(
			spec,
			iceberg.EntryContentEqDeletes,
			fmt.Sprintf("eq-delete-%d.parquet", i),
			iceberg.ParquetFile,
			partition,
			nil,
			nil,
			1,
			1024,
		)
		if err != nil {
			b.Fatal(err)
		}
		files[i] = builder.Build()
	}

	return meta, files
}

// eqDeletePartitionsToFilterBeforeDedup mirrors the pre-optimization path so
// the benchmark measures the cost of the production change directly.
func eqDeletePartitionsToFilterBeforeDedup(
	files []iceberg.DataFile,
	meta Metadata,
) (iceberg.BooleanExpression, error) {
	terms := make([]iceberg.BooleanExpression, 0, len(files))
	for _, f := range files {
		p := iceberginternal.BorrowedDataFilePartition(f)
		if len(p) == 0 {
			return iceberg.AlwaysTrue{}, nil
		}

		spec := meta.PartitionSpecByID(int(f.SpecID()))
		if spec == nil {
			return nil, fmt.Errorf("partition spec ID %d not found in metadata", f.SpecID())
		}

		partFieldByID := make(map[int]iceberg.PartitionField, spec.NumFields())
		for _, pf := range spec.Fields() {
			partFieldByID[pf.FieldID] = pf
		}

		fieldIDs := make([]int, 0, len(p))
		for id := range p {
			fieldIDs = append(fieldIDs, id)
		}
		sort.Ints(fieldIDs)

		identityOnly := true
		for _, fid := range fieldIDs {
			if pf, ok := partFieldByID[fid]; ok {
				if _, isIdentity := pf.Transform.(iceberg.IdentityTransform); !isIdentity {
					identityOnly = false

					break
				}
			}
		}
		if !identityOnly {
			terms = append(terms, iceberg.AlwaysTrue{})

			continue
		}

		conjuncts := make([]iceberg.BooleanExpression, 0, len(p))
		for _, partFieldID := range fieldIDs {
			pf, ok := partFieldByID[partFieldID]
			if !ok {
				return nil, fmt.Errorf("partition field ID %d not found in spec %d", partFieldID, f.SpecID())
			}

			sourceField, ok := meta.CurrentSchema().FindFieldByID(pf.SourceID())
			if !ok {
				return nil, fmt.Errorf("source field ID %d (partition field %q) not found in schema", pf.SourceID(), pf.Name)
			}

			value := p[partFieldID]
			if value == nil {
				conjuncts = append(conjuncts, iceberg.IsNull(iceberg.Reference(sourceField.Name)))

				continue
			}

			lit, err := internal.LiteralForPartitionValue(value)
			if err != nil {
				return nil, fmt.Errorf("partition field %q: %w", sourceField.Name, err)
			}

			conjuncts = append(conjuncts, iceberg.LiteralPredicate(iceberg.OpEQ, iceberg.Reference(sourceField.Name), lit))
		}

		if len(conjuncts) == 1 {
			terms = append(terms, conjuncts[0])
		} else {
			terms = append(terms, iceberg.NewAnd(conjuncts[0], conjuncts[1], conjuncts[2:]...))
		}
	}

	if len(terms) == 0 {
		return iceberg.AlwaysTrue{}, nil
	}

	if len(terms) == 1 {
		return terms[0], nil
	}

	return iceberg.NewOr(terms[0], terms[1], terms[2:]...), nil
}
