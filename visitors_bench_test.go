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

package iceberg

import (
	"fmt"
	"testing"
)

type evaluatorBenchmarkRow []any

func (r evaluatorBenchmarkRow) Size() int       { return len(r) }
func (r evaluatorBenchmarkRow) Get(pos int) any { return r[pos] }
func (r evaluatorBenchmarkRow) Set(pos int, val any) {
	r[pos] = val
}

var expressionEvaluatorBenchmarkSink bool

func BenchmarkExpressionEvaluatorTraversal(b *testing.B) {
	for _, tc := range []struct {
		name         string
		op           Operation
		shortCircuit bool
	}{
		{name: "and-left-false", op: OpAnd, shortCircuit: true},
		{name: "or-left-true", op: OpOr, shortCircuit: true},
		{name: "and-no-short-circuit", op: OpAnd},
		{name: "or-no-short-circuit", op: OpOr},
	} {
		for _, fieldCount := range []int{8, 32, 128} {
			b.Run(fmt.Sprintf("%s/fields=%d", tc.name, fieldCount), func(b *testing.B) {
				bound, row := evaluatorBenchmarkInput(fieldCount, tc.op, tc.shortCircuit)

				b.Run("full-traversal", func(b *testing.B) {
					benchmarkExpressionTraversal(b, bound, row, false)
				})
				b.Run("short-circuit", func(b *testing.B) {
					benchmarkExpressionTraversal(b, bound, row, true)
				})
			})
		}
	}
}

func evaluatorBenchmarkInput(fieldCount int, op Operation, shortCircuit bool) (BooleanExpression, StructLike) {
	fields := make([]NestedField, fieldCount)
	predicates := make([]BooleanExpression, fieldCount)
	values := make(evaluatorBenchmarkRow, fieldCount)

	for i := range fieldCount {
		name := fmt.Sprintf("field_%d", i)
		fields[i] = NestedField{ID: i + 1, Name: name, Type: PrimitiveTypes.Int32, Required: true}
		predicates[i] = EqualTo(Reference(name), int32(1))
		values[i] = int32(0)
	}

	if op == OpAnd {
		if shortCircuit {
			values[0] = int32(0)
			for i := 1; i < fieldCount; i++ {
				values[i] = int32(1)
			}
		} else {
			for i := range values {
				values[i] = int32(1)
			}
		}
	} else if shortCircuit {
		values[0] = int32(1)
	}

	rest := predicates[fieldCount-1]
	for i := fieldCount - 2; i >= 1; i-- {
		if op == OpAnd {
			rest = NewAnd(predicates[i], rest)
		} else {
			rest = NewOr(predicates[i], rest)
		}
	}

	var expr BooleanExpression
	if op == OpAnd {
		expr = NewAnd(predicates[0], rest)
	} else {
		expr = NewOr(predicates[0], rest)
	}

	bound, err := BindExpr(NewSchema(0, fields...), expr, true)
	if err != nil {
		panic(err)
	}

	return bound, values
}

func benchmarkExpressionTraversal(b *testing.B, bound BooleanExpression, row StructLike, shortCircuit bool) {
	evaluator := exprEvaluator{bound: bound, st: row}
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		var (
			result bool
			err    error
		)
		if shortCircuit {
			result, err = VisitExprEvaluator(bound, &evaluator)
		} else {
			result, err = VisitExpr(bound, &evaluator)
		}
		if err != nil {
			b.Fatal(err)
		}
		expressionEvaluatorBenchmarkSink = result
	}
}
