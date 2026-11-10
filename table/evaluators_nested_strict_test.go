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
)

func TestStrictMetricsNestedColumnDoesNotProveAllRowsMatch(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "payload", Required: false,
		Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 2, Name: "value", Required: false, Type: iceberg.PrimitiveTypes.Int32},
		}},
	})

	// One row has a null parent, and the other has payload.value = 3.
	// The child counts exclude the null parent, so they cannot establish
	// that both table rows match even though the only child value is 3.
	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		"file.parquet", iceberg.ParquetFile, nil, nil, nil, 2, 1024,
	)
	if err != nil {
		t.Fatal(err)
	}
	three, err := iceberg.Int32Literal(3).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	file := builder.ValueCounts(map[int]int64{2: 1}).
		NullValueCounts(map[int]int64{2: 0}).
		LowerBoundValues(map[int][]byte{2: three}).
		UpperBoundValues(map[int][]byte{2: three}).
		Build()

	for _, tc := range []struct {
		name string
		expr iceberg.BooleanExpression
	}{
		{"equal", iceberg.EqualTo(iceberg.Reference("payload.value"), int32(3))},
		{"not equal", iceberg.NotEqualTo(iceberg.Reference("payload.value"), int32(4))},
		{"not null", iceberg.NotNull(iceberg.Reference("payload.value"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eval, err := newStrictMetricsEvaluator(schema, tc.expr, true, false)
			if err != nil {
				t.Fatal(err)
			}
			matches, err := eval(file)
			if err != nil {
				t.Fatal(err)
			}
			if matches {
				t.Fatal("nested metrics must not prove that every table row matches")
			}
		})
	}
}

func TestStrictMetricsTopLevelColumnStillProvesMatch(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 2, Name: "value", Required: true, Type: iceberg.PrimitiveTypes.Int32,
	})
	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentData,
		"file.parquet", iceberg.ParquetFile, nil, nil, nil, 2, 1024,
	)
	if err != nil {
		t.Fatal(err)
	}
	three, err := iceberg.Int32Literal(3).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	file := builder.ValueCounts(map[int]int64{2: 2}).
		NullValueCounts(map[int]int64{2: 0}).
		LowerBoundValues(map[int][]byte{2: three}).
		UpperBoundValues(map[int][]byte{2: three}).
		Build()

	eval, err := newStrictMetricsEvaluator(
		schema, iceberg.EqualTo(iceberg.Reference("value"), int32(3)), true, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	matches, err := eval(file)
	if err != nil {
		t.Fatal(err)
	}
	if !matches {
		t.Fatal("top-level metrics should still prove a constant column matches")
	}
}
