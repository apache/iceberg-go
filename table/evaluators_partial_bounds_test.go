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

// A summary with only one bound is invalid, so it cannot prove that the
// manifest has no matches.
func TestManifestEvaluatorPartialPartitionBounds(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.String,
	})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "value",
		Transform: iceberg.IdentityTransform{},
	})
	lower := []byte("m")
	upper := []byte("t")
	ref := iceberg.Reference("value")

	filters := []struct {
		name string
		expr iceberg.BooleanExpression
	}{
		{"less than", iceberg.LessThan(ref, "o")},
		{"less than equal", iceberg.LessThanEqual(ref, "o")},
		{"greater than", iceberg.GreaterThan(ref, "o")},
		{"greater than equal", iceberg.GreaterThanEqual(ref, "o")},
		{"equal", iceberg.EqualTo(ref, "o")},
		{"in", iceberg.IsIn(ref, "o", "p")},
		{"in below lower bound", iceberg.IsIn(ref, "a", "b")},
		{"not null", iceberg.NotNull(ref)},
		{"starts with", iceberg.StartsWith(ref, "o")},
	}

	for _, tc := range []struct {
		name    string
		summary iceberg.FieldSummary
	}{
		{"lower bound only", iceberg.FieldSummary{ContainsNull: true, LowerBound: &lower}},
		{"upper bound only", iceberg.FieldSummary{ContainsNull: true, UpperBound: &upper}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manifest := iceberg.NewManifestBuilder(2, "manifest.avro", 1, 0, 1).
				AddedFiles(1).
				Partitions([]iceberg.FieldSummary{tc.summary}).
				Build()

			for _, filter := range filters {
				t.Run(filter.name, func(t *testing.T) {
					eval, err := newManifestEvaluator(spec, schema, filter.expr, true)
					require.NoError(t, err)

					matches, err := eval(manifest)
					require.NoError(t, err)
					require.True(t, matches, "an omitted bound cannot prove the manifest does not match")
				})
			}
		})
	}
}

func TestManifestEvaluatorPartialBoundsOnSecondPartitionField(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "first", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "second", Type: iceberg.PrimitiveTypes.String},
	)
	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{SourceIDs: []int{1}, FieldID: 1000, Name: "first", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: "second", Transform: iceberg.IdentityTransform{}},
	)
	firstLower, firstUpper := []byte("a"), []byte("z")
	secondLower := []byte("m")
	manifest := iceberg.NewManifestBuilder(2, "manifest.avro", 1, 0, 1).
		AddedFiles(1).
		Partitions([]iceberg.FieldSummary{
			{LowerBound: &firstLower, UpperBound: &firstUpper},
			{ContainsNull: true, LowerBound: &secondLower},
		}).Build()

	eval, err := newManifestEvaluator(spec, schema,
		iceberg.EqualTo(iceberg.Reference("second"), "o"), true)
	require.NoError(t, err)

	matches, err := eval(manifest)
	require.NoError(t, err)
	require.True(t, matches, "a one-bound summary on the second field cannot exclude the manifest")
}

func TestManifestEvaluatorMissingBoundsPruneNullOnlyPartition(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.String,
	})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "value",
		Transform: iceberg.IdentityTransform{},
	})
	manifest := iceberg.NewManifestBuilder(2, "null-only.avro", 1, 0, 1).
		AddedFiles(1).
		Partitions([]iceberg.FieldSummary{{ContainsNull: true}}).
		Build()
	ref := iceberg.Reference("value")
	filters := []struct {
		name string
		expr iceberg.BooleanExpression
	}{
		{"equal", iceberg.EqualTo(ref, "o")},
		{"less than", iceberg.LessThan(ref, "o")},
		{"less than equal", iceberg.LessThanEqual(ref, "o")},
		{"greater than", iceberg.GreaterThan(ref, "o")},
		{"greater than equal", iceberg.GreaterThanEqual(ref, "o")},
		{"in", iceberg.IsIn(ref, "o", "p")},
		{"starts with", iceberg.StartsWith(ref, "o")},
	}

	for _, filter := range filters {
		t.Run(filter.name, func(t *testing.T) {
			eval, err := newManifestEvaluator(spec, schema, filter.expr, true)
			require.NoError(t, err)

			matches, err := eval(manifest)
			require.NoError(t, err)
			require.False(t, matches, "a complete null-only summary cannot match a non-null predicate")
		})
	}
}

func TestManifestEvaluatorPartialBoundsDoNotProveAllNaN(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.Float64,
	})
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "value",
		Transform: iceberg.IdentityTransform{},
	})
	upper, err := iceberg.Float64Literal(20).MarshalBinary()
	require.NoError(t, err)
	containsNaN := true
	manifest := iceberg.NewManifestBuilder(2, "manifest.avro", 1, 0, 1).
		AddedFiles(1).
		Partitions([]iceberg.FieldSummary{{
			ContainsNaN: &containsNaN,
			UpperBound:  &upper,
		}}).
		Build()

	eval, err := newManifestEvaluator(spec, schema, iceberg.NotNaN(iceberg.Reference("value")), true)
	require.NoError(t, err)

	matches, err := eval(manifest)
	require.NoError(t, err)
	require.True(t, matches, "an upper bound proves that at least one partition value is not NaN")
}
