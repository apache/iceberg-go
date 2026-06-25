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
// format. AlwaysTrue/AlwaysFalse are bare JSON booleans (`true`/`false`) — what
// ExpressionParser.toJson emits and what the Java REST reference accepts on
// planTableScan request filters (it parses the filter through the same parser).
//
// The REST OpenAPI Expression schema documents true/false as objects
// (`{"type":"true"}` / `{"type":"false"}`), but the Java reference parser does
// not implement that form: it rejects `{"type":"true"}` and accepts only bare
// booleans or the literal form `{"type":"literal","value":true}`. That is why
// marshal emits bare booleans. UnmarshalExpressionJSON stays permissive — it
// accepts bare booleans and the `{"type":"literal","value":...}` form, and also
// the OpenAPI object form for a strictly spec-shaped server, even though no
// known implementation emits it.

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
