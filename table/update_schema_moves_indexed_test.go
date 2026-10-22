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
	"math/rand"
	"reflect"
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
)

func makeMoveTestFields(count int) []iceberg.NestedField {
	fields := make([]iceberg.NestedField, count)
	for i := range fields {
		fields[i] = iceberg.NestedField{ID: i + 1, Name: fmt.Sprintf("col_%d", i)}
	}

	return fields
}

func TestMoveFieldsIndexedMatchesLinear(t *testing.T) {
	for _, count := range []int{1, 16, 63, 64, 128, 256, 1024} {
		for _, moveCount := range []int{0, 1, 7, 8, 64, 256} {
			t.Run(fmt.Sprintf("%d_fields_%d_moves", count, moveCount), func(t *testing.T) {
				fields := makeMoveTestFields(count)
				original := slices.Clone(fields)
				for seed := range int64(10) {
					rng := rand.New(rand.NewSource(seed))
					moves := make([]move, moveCount)
					for i := range moves {
						moves[i].FieldID = 1 + rng.Intn(count)
						moves[i].RelativeTo = 1 + rng.Intn(count)
						if moves[i].RelativeTo == moves[i].FieldID {
							moves[i].RelativeTo = 1 + moves[i].RelativeTo%count
						}
						switch rng.Intn(3) {
						case 0:
							moves[i].Op = MoveOpFirst
						case 1:
							moves[i].Op = MoveOpBefore
						case 2:
							moves[i].Op = MoveOpAfter
						}
						if count == 1 {
							moves[i].Op = MoveOpFirst
						}
					}
					want := moveFieldsLinear(fields, moves)
					got := moveFields(fields, moves)
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("seed %d: move order mismatch: got %v, want %v", seed, got, want)
					}
					if !reflect.DeepEqual(fields, original) {
						t.Fatal("moveFields mutated input fields")
					}
				}
			})
		}
	}
}

func TestMoveFieldsIndexedFallbacks(t *testing.T) {
	fields := makeMoveTestFields(128)
	t.Run("missing source", func(t *testing.T) {
		moves := make([]move, 8)
		for i := range moves {
			moves[i] = move{FieldID: 99999, Op: MoveOpFirst}
		}
		assertSameMovePanic(t, fields, moves)
	})
	t.Run("missing target", func(t *testing.T) {
		moves := make([]move, 8)
		for i := range moves {
			moves[i] = move{FieldID: i + 1, RelativeTo: 99999, Op: MoveOpAfter}
		}
		assertSameMovePanic(t, fields, moves)
	})
	t.Run("self relative", func(t *testing.T) {
		moves := make([]move, 8)
		for i := range moves {
			moves[i] = move{FieldID: i + 1, RelativeTo: i + 1, Op: MoveOpBefore}
		}
		assertSameMovePanic(t, fields, moves)
	})
	t.Run("duplicate IDs", func(t *testing.T) {
		duplicate := slices.Clone(fields)
		duplicate[1].ID = duplicate[0].ID
		moves := []move{
			{FieldID: 1, Op: MoveOpFirst},
			{FieldID: 3, Op: MoveOpFirst},
			{FieldID: 4, Op: MoveOpFirst},
			{FieldID: 5, Op: MoveOpFirst},
			{FieldID: 6, Op: MoveOpFirst},
			{FieldID: 7, Op: MoveOpFirst},
			{FieldID: 8, Op: MoveOpFirst},
			{FieldID: 9, Op: MoveOpFirst},
		}
		if got, want := moveFields(duplicate, moves), moveFieldsLinear(duplicate, moves); !reflect.DeepEqual(got, want) {
			t.Fatalf("duplicate field IDs: got %v, want %v", got, want)
		}
	})
	t.Run("unknown operation", func(t *testing.T) {
		moves := make([]move, 8)
		for i := range moves {
			moves[i] = move{FieldID: i + 1, Op: MoveOp("unknown")}
		}
		if got, want := moveFields(fields, moves), moveFieldsLinear(fields, moves); !reflect.DeepEqual(got, want) {
			t.Fatalf("unknown operation: got %v, want %v", got, want)
		}
	})
}

func assertSameMovePanic(t *testing.T, fields []iceberg.NestedField, moves []move) {
	t.Helper()
	getPanic := func(fn func([]iceberg.NestedField, []move) []iceberg.NestedField) (panicValue any) {
		defer func() { panicValue = recover() }()
		fn(fields, moves)

		return nil
	}
	got, want := getPanic(moveFields), getPanic(moveFieldsLinear)
	if got == nil || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("panic mismatch: got %v, want %v", got, want)
	}
}
