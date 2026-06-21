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

// This file is a PROPOSED public API surface for REST scan-planning
// expression JSON (apache/iceberg-go#1178). The bodies are intentionally
// unimplemented; the file exists so the API shape can be reviewed.
//
// The codec lives in the root iceberg package because expression internals
// are defined here and are not exported. Compatibility with Java's
// ExpressionParser is correctness-critical and every encoding must be
// confirmed against checked-in Java golden fixtures.
//
// Design decision: MarshalExpressionJSON emits Java ExpressionParser wire
// format, including bare JSON booleans for AlwaysTrue/AlwaysFalse (`true` and
// `false`). The Java REST reference uses the same parser for planTableScan
// request filters, even though the REST OpenAPI Expression schema also models
// true/false as objects (`{"type":"true"}` and `{"type":"false"}`).
// UnmarshalExpressionJSON must accept both forms so clients can read Java-
// compatible responses and strictly spec-shaped payloads.

package iceberg

import "fmt"

// MarshalExpressionJSON serializes a boolean expression to the JSON format
// produced by Java's ExpressionParser, for use as a REST scan-planning filter.
func MarshalExpressionJSON(expr BooleanExpression) ([]byte, error) {
	return nil, fmt.Errorf("%w: expression JSON marshal", ErrNotImplemented)
}

// UnmarshalExpressionJSON parses a Java ExpressionParser-format expression,
// binding literal terms against schema. schema is required for full
// date/time/decimal/fixed fidelity and to bind residual filters returned by a
// REST server.
func UnmarshalExpressionJSON(data []byte, schema *Schema) (BooleanExpression, error) {
	return nil, fmt.Errorf("%w: expression JSON unmarshal", ErrNotImplemented)
}
