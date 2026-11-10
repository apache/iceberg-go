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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"testing"

	"github.com/apache/iceberg-go"
)

func TestInclusiveMetricsNotStartsWithUnknownNullCounts(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "value", Type: iceberg.PrimitiveTypes.String,
	})
	filter, err := iceberg.BindExpr(schema,
		iceberg.NotStartsWith(iceberg.Reference("value"), "ab"), true)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		nullCounts map[int]int64
		upperBound []byte
		wantRead   bool
	}{
		{name: "no null statistics", wantRead: true, upperBound: []byte("abd")},
		{name: "empty null statistics", nullCounts: map[int]int64{}, wantRead: true, upperBound: []byte("abd")},
		{name: "other column has statistics", nullCounts: map[int]int64{2: 0}, wantRead: true, upperBound: []byte("abd")},
		{name: "known zero nulls", nullCounts: map[int]int64{1: 0}, wantRead: false, upperBound: []byte("abd")},
		{name: "known positive nulls", nullCounts: map[int]int64{1: 1}, wantRead: true, upperBound: []byte("abd")},
		{name: "known zero nulls without upper bound", nullCounts: map[int]int64{1: 0}, wantRead: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evaluator := &inclusiveMetricsEval{
				metricsEvaluator: metricsEvaluator{
					nullCounts:  tt.nullCounts,
					lowerBounds: map[int][]byte{1: []byte("abc")},
					upperBounds: map[int][]byte{1: tt.upperBound},
				},
			}

			shouldRead, err := iceberg.VisitExpr(filter, evaluator)
			if err != nil {
				t.Fatal(err)
			}
			if shouldRead != tt.wantRead {
				t.Errorf("shouldRead = %t, want %t (null counts: %v)",
					shouldRead, tt.wantRead, tt.nullCounts)
			}
		})
	}
}
