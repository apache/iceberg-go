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

	"github.com/DataDog/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestMalformedFixedBoundsAreConservative(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "value", Type: iceberg.FixedTypeOf(4),
	})
	expr := iceberg.EqualTo(iceberg.Reference("value"), []byte{1, 2, 3, 4})
	malformed := []byte{1, 2, 3, 4, 5}

	t.Run("manifest evaluator keeps the manifest", func(t *testing.T) {
		spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
			SourceIDs: []int{1}, FieldID: 1000, Name: "value", Transform: iceberg.IdentityTransform{},
		})
		eval, err := newManifestEvaluator(spec, schema, expr, true)
		require.NoError(t, err)

		manifest := iceberg.NewManifestFile(2, "manifest.avro", 1, 0, 1).
			AddedFiles(1).
			Partitions([]iceberg.FieldSummary{{LowerBound: &malformed, UpperBound: &malformed}}).
			Build()
		matches, err := eval(manifest)
		require.NoError(t, err)
		require.True(t, matches)
	})

	builder, err := iceberg.NewDataFileBuilder(
		iceberg.NewPartitionSpec(), iceberg.EntryContentData, "file.parquet",
		iceberg.ParquetFile, nil, nil, nil, 1, 1,
	)
	require.NoError(t, err)
	dataFile := builder.
		ValueCounts(map[int]int64{1: 1}).
		NullValueCounts(map[int]int64{1: 0}).
		LowerBoundValues(map[int][]byte{1: malformed}).
		UpperBoundValues(map[int][]byte{1: malformed}).
		Build()

	t.Run("inclusive evaluator keeps the file", func(t *testing.T) {
		eval, err := newInclusiveMetricsEvaluator(schema, expr, true, false)
		require.NoError(t, err)
		matches, err := eval(dataFile)
		require.NoError(t, err)
		require.True(t, matches)
	})

	t.Run("strict evaluator does not prove a match", func(t *testing.T) {
		eval, err := newStrictMetricsEvaluator(schema, expr, true, false)
		require.NoError(t, err)
		matches, err := eval(dataFile)
		require.NoError(t, err)
		require.False(t, matches)
	})
}
