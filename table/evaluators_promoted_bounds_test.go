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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestPromotedNumericBoundsInManifestAndFileMetrics(t *testing.T) {
	tests := []struct {
		name     string
		typ      iceberg.Type
		oldBound iceberg.Literal
		matching iceberg.BooleanExpression
		outside  iceberg.BooleanExpression
	}{
		{
			name:     "int to long",
			typ:      iceberg.PrimitiveTypes.Int64,
			oldBound: iceberg.Int32Literal(-7),
			matching: iceberg.EqualTo(iceberg.Reference("value"), int64(-7)),
			outside:  iceberg.EqualTo(iceberg.Reference("value"), int64(99)),
		},
		{
			name:     "float to double",
			typ:      iceberg.PrimitiveTypes.Float64,
			oldBound: iceberg.Float32Literal(-1.25),
			matching: iceberg.EqualTo(iceberg.Reference("value"), float64(-1.25)),
			outside:  iceberg.EqualTo(iceberg.Reference("value"), float64(99)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := tt.oldBound.MarshalBinary()
			require.NoError(t, err)
			require.Len(t, raw, 4)

			schema := iceberg.NewSchema(1, iceberg.NestedField{
				ID: 1, Name: "value", Type: tt.typ,
			})
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1},
				FieldID:   1000,
				Name:      "value",
				Transform: iceberg.IdentityTransform{},
			})
			manifest := iceberg.NewManifestBuilder(2, "manifest.avro", 1, 0, 1).
				AddedFiles(1).
				Partitions([]iceberg.FieldSummary{{LowerBound: &raw, UpperBound: &raw}}).
				Build()

			builder, err := iceberg.NewDataFileBuilder(
				iceberg.NewPartitionSpec(), iceberg.EntryContentData, "file.parquet",
				iceberg.ParquetFile, nil, nil, nil, 1, 1,
			)
			require.NoError(t, err)
			file := builder.
				ValueCounts(map[int]int64{1: 1}).
				NullValueCounts(map[int]int64{1: 0}).
				LowerBoundValues(map[int][]byte{1: raw}).
				UpperBoundValues(map[int][]byte{1: raw}).
				Build()

			for _, testCase := range []struct {
				name string
				expr iceberg.BooleanExpression
				want bool
			}{
				{"matching value is retained", tt.matching, true},
				{"outside value is pruned", tt.outside, false},
			} {
				t.Run(testCase.name, func(t *testing.T) {
					manifestEval, err := newManifestEvaluator(spec, schema, testCase.expr, true)
					require.NoError(t, err)
					manifestMatches, err := manifestEval(manifest)
					require.NoError(t, err)
					require.Equal(t, testCase.want, manifestMatches, "manifest bounds")

					fileEval, err := newInclusiveMetricsEvaluator(schema, testCase.expr, true, false)
					require.NoError(t, err)
					fileMatches, err := fileEval(file)
					require.NoError(t, err)
					require.Equal(t, testCase.want, fileMatches, "file bounds")
				})
			}
		})
	}
}
