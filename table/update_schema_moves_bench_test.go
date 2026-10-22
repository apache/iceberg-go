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
	"testing"

	"github.com/apache/iceberg-go"
)

var moveFieldsBenchmarkSink []iceberg.NestedField

func BenchmarkMoveFieldsBulkReorder(b *testing.B) {
	cases := []struct {
		fieldCount int
		moveCount  int
		mixed      bool
	}{
		{16, 2, false},
		{64, 8, false},
		{256, 64, false},
		{1024, 1024, false},
		{4096, 4096, false},
		{256, 64, true},
		{1024, 1024, true},
	}
	for _, tc := range cases {
		fields := makeMoveTestFields(tc.fieldCount)
		moves := make([]move, tc.moveCount)
		for i := range moves {
			moves[i] = move{FieldID: tc.fieldCount - i%tc.fieldCount, Op: MoveOpFirst}
			if tc.mixed {
				moves[i].FieldID = 1 + i%tc.fieldCount
				moves[i].RelativeTo = 1 + (i*7+3)%tc.fieldCount
				if moves[i].FieldID == moves[i].RelativeTo {
					moves[i].RelativeTo = 1 + moves[i].RelativeTo%tc.fieldCount
				}
				switch i % 3 {
				case 1:
					moves[i].Op = MoveOpBefore
				case 2:
					moves[i].Op = MoveOpAfter
				}
			}
		}
		shape := "first"
		if tc.mixed {
			shape = "mixed"
		}
		name := fmt.Sprintf("%s/%d_fields_%d_moves", shape, tc.fieldCount, tc.moveCount)
		b.Run(name, func(b *testing.B) {
			for _, impl := range []struct {
				name string
				fn   func([]iceberg.NestedField, []move) []iceberg.NestedField
			}{{"linear", moveFieldsLinear}, {"indexed", moveFields}} {
				b.Run(impl.name, func(b *testing.B) {
					b.ReportAllocs()
					for range b.N {
						moveFieldsBenchmarkSink = impl.fn(fields, moves)
					}
				})
			}
		})
	}
}
