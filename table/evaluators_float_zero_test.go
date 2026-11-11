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
	"math"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestSignedZeroBoundsDoNotPruneMatchingRows(t *testing.T) {
	tests := []struct {
		name     string
		typ      iceberg.Type
		negative iceberg.Literal
		positive iceberg.Literal
	}{
		{
			name:     "float",
			typ:      iceberg.PrimitiveTypes.Float32,
			negative: iceberg.Float32Literal(math.Float32frombits(1 << 31)),
			positive: iceberg.Float32Literal(0),
		},
		{
			name:     "double",
			typ:      iceberg.PrimitiveTypes.Float64,
			negative: iceberg.Float64Literal(math.Float64frombits(1 << 63)),
			positive: iceberg.Float64Literal(0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := iceberg.NewSchema(1, iceberg.NestedField{
				ID: 1, Name: "x", Type: tt.typ, Required: true,
			})
			negativeBound, err := tt.negative.MarshalBinary()
			require.NoError(t, err)
			positiveBound, err := tt.positive.MarshalBinary()
			require.NoError(t, err)

			newFile := func(bound []byte) iceberg.DataFile {
				t.Helper()
				builder, err := iceberg.NewDataFileBuilder(
					iceberg.NewPartitionSpec(), iceberg.EntryContentData,
					"s3://bucket/zeros.parquet", iceberg.ParquetFile,
					nil, nil, nil, 1, 100,
				)
				require.NoError(t, err)

				return builder.
					ValueCounts(map[int]int64{1: 1}).
					NullValueCounts(map[int]int64{1: 0}).
					NaNValueCounts(map[int]int64{1: 0}).
					LowerBoundValues(map[int][]byte{1: bound}).
					UpperBoundValues(map[int][]byte{1: bound}).
					Build()
			}
			negativeFile := newFile(negativeBound)
			positiveFile := newFile(positiveBound)

			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "x_part", Transform: iceberg.IdentityTransform{},
			})
			for _, literal := range []iceberg.Literal{tt.negative, tt.positive} {
				for _, file := range []iceberg.DataFile{negativeFile, positiveFile} {
					for _, op := range []iceberg.Operation{iceberg.OpEQ, iceberg.OpNEQ, iceberg.OpLT, iceberg.OpLTEQ, iceberg.OpGT, iceberg.OpGTEQ} {
						expected := op == iceberg.OpEQ || op == iceberg.OpLTEQ || op == iceberg.OpGTEQ
						predicate := iceberg.LiteralPredicate(op, iceberg.Reference("x"), literal)
						inclusive, err := newInclusiveMetricsEvaluator(schema, predicate, true, false)
						require.NoError(t, err)
						matches, err := inclusive(file)
						require.NoError(t, err)
						if expected {
							require.True(t, matches, "%s must retain either zero", op)
						}
						strict, err := newStrictMetricsEvaluator(schema, predicate, true, false)
						require.NoError(t, err)
						allMatch, err := strict(file)
						require.NoError(t, err)
						require.Equal(t, expected, allMatch, "%s must follow Arrow zero equality", op)

						bound := file.LowerBoundValues()[1]
						manifest := iceberg.NewManifestBuilder(1, "s3://bucket/manifest.avro", 100, 0, 1).
							Partitions([]iceberg.FieldSummary{{ContainsNull: false, LowerBound: &bound, UpperBound: &bound}}).Build()
						evaluator, err := newManifestEvaluator(spec, schema, iceberg.LiteralPredicate(op, iceberg.Reference("x_part"), literal), true)
						require.NoError(t, err)
						matches, err = evaluator(manifest)
						require.NoError(t, err)
						if expected {
							require.True(t, matches, "%s must retain either zero in manifests", op)
						}
					}
				}
			}
		})
	}
}
