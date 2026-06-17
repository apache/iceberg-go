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
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// JSON serialization for boolean expressions, used for the "filter" field of a
// REST scan-planning request. Mirrors Java's ExpressionParser.

// exprKeyTransform is the "type" value identifying a transform term.
const exprKeyTransform = "transform"

// opToJSON maps an Operation to its wire string (OpLTEQ -> "lt-eq"). OpTrue and
// OpFalse are handled separately: they serialize as bare JSON booleans.
var opToJSON = map[Operation]string{
	OpIsNull:        "is-null",
	OpNotNull:       "not-null",
	OpIsNan:         "is-nan",
	OpNotNan:        "not-nan",
	OpLT:            "lt",
	OpLTEQ:          "lt-eq",
	OpGT:            "gt",
	OpGTEQ:          "gt-eq",
	OpEQ:            "eq",
	OpNEQ:           "not-eq",
	OpStartsWith:    "starts-with",
	OpNotStartsWith: "not-starts-with",
	OpIn:            "in",
	OpNotIn:         "not-in",
	OpNot:           "not",
	OpAnd:           "and",
	OpOr:            "or",
}

var jsonToOp = func() map[string]Operation {
	m := make(map[string]Operation, len(opToJSON))
	for op, s := range opToJSON {
		m[s] = op
	}

	return m
}()

// exprNode is the wire form of an expression node. omitempty leaves only the
// keys relevant to a given node; field order matches Java's output.
type exprNode struct {
	Type   string            `json:"type"`
	Child  json.RawMessage   `json:"child,omitempty"`
	Left   json.RawMessage   `json:"left,omitempty"`
	Right  json.RawMessage   `json:"right,omitempty"`
	Term   json.RawMessage   `json:"term,omitempty"`
	Value  json.RawMessage   `json:"value,omitempty"`
	Values []json.RawMessage `json:"values,omitempty"`
}

// transformNode is the wire form of a transform term, e.g.
// {"type":"transform","transform":"bucket[16]","term":"id"}.
type transformNode struct {
	Type      string `json:"type"`
	Transform string `json:"transform"`
	Term      string `json:"term"`
}

// MarshalJSON emits the REST JSON form, so an expression can be used directly
// as a request's "filter" field. Tag such fields omitempty to drop a nil one.
func (e AlwaysTrue) MarshalJSON() ([]byte, error)  { return encodeExpr(e) }
func (e AlwaysFalse) MarshalJSON() ([]byte, error) { return encodeExpr(e) }
func (e NotExpr) MarshalJSON() ([]byte, error)     { return encodeExpr(e) }
func (e AndExpr) MarshalJSON() ([]byte, error)     { return encodeExpr(e) }
func (e OrExpr) MarshalJSON() ([]byte, error)      { return encodeExpr(e) }

func (p *unboundUnaryPredicate) MarshalJSON() ([]byte, error)   { return encodeExpr(p) }
func (p *unboundLiteralPredicate) MarshalJSON() ([]byte, error) { return encodeExpr(p) }
func (p *unboundSetPredicate) MarshalJSON() ([]byte, error)     { return encodeExpr(p) }

// Bound predicates delegate to the same encoder; without these, json.Marshal of
// a bound expression would fall through to {} since their fields are unexported.
func (p *boundUnaryPredicate[T]) MarshalJSON() ([]byte, error)   { return encodeExpr(p) }
func (p *boundLiteralPredicate[T]) MarshalJSON() ([]byte, error) { return encodeExpr(p) }
func (p *boundSetPredicate[T]) MarshalJSON() ([]byte, error)     { return encodeExpr(p) }

// ParseExpr parses an expression from its REST JSON form (a request "filter" or
// a task's residual filter).
//
// With a schema, literals take the referenced field's type (e.g. "2022-08-14"
// on a date column becomes a DateLiteral). Without one they fall back to the
// base JSON kind: Int64Literal, Float64Literal, StringLiteral, or BoolLiteral.
func ParseExpr(data []byte, schema *Schema) (BooleanExpression, error) {
	return decodeExpr(json.RawMessage(data), schema)
}

func encodeExpr(e BooleanExpression) (json.RawMessage, error) {
	switch v := e.(type) {
	case AlwaysTrue:
		return json.RawMessage("true"), nil
	case AlwaysFalse:
		return json.RawMessage("false"), nil
	case NotExpr:
		child, err := encodeExpr(v.child)
		if err != nil {
			return nil, err
		}

		return json.Marshal(exprNode{Type: opToJSON[OpNot], Child: child})
	case AndExpr:
		left, err := encodeExpr(v.left)
		if err != nil {
			return nil, err
		}
		right, err := encodeExpr(v.right)
		if err != nil {
			return nil, err
		}

		return json.Marshal(exprNode{Type: opToJSON[OpAnd], Left: left, Right: right})
	case OrExpr:
		left, err := encodeExpr(v.left)
		if err != nil {
			return nil, err
		}
		right, err := encodeExpr(v.right)
		if err != nil {
			return nil, err
		}

		return json.Marshal(exprNode{Type: opToJSON[OpOr], Left: left, Right: right})
	}

	return encodePredicate(e)
}

func encodePredicate(e BooleanExpression) (json.RawMessage, error) {
	op := e.Op()
	js, ok := opToJSON[op]
	if !ok {
		return nil, fmt.Errorf("%w: cannot serialize expression with operation %s", ErrInvalidArgument, op)
	}

	var (
		term Term
		err  error
	)
	switch p := e.(type) {
	case UnboundPredicate:
		term = p.Term()
	case BoundPredicate:
		term = p.Term()
	default:
		return nil, fmt.Errorf("%w: cannot serialize expression of type %T", ErrInvalidArgument, e)
	}

	// A bound term carries the field type, which a timestamptz literal needs to
	// emit its +00:00 offset. Unbound terms leave it nil.
	var typ Type
	if bt, ok := term.(BoundTerm); ok {
		typ = bt.Type()
	}

	node := exprNode{Type: js}
	if node.Term, err = encodeTerm(term); err != nil {
		return nil, err
	}

	switch {
	case op >= OpIsNull && op <= OpNotNan:
		// unary predicate: no value or values field
	case op >= OpLT && op <= OpNotStartsWith:
		lit, err := literalOf(e)
		if err != nil {
			return nil, err
		}
		if node.Value, err = encodeLiteral(lit, typ); err != nil {
			return nil, err
		}
	case op >= OpIn && op <= OpNotIn:
		lits, err := literalsOf(e)
		if err != nil {
			return nil, err
		}
		node.Values = make([]json.RawMessage, 0, len(lits))
		for _, l := range lits {
			v, err := encodeLiteral(l, typ)
			if err != nil {
				return nil, err
			}
			node.Values = append(node.Values, v)
		}
		// A set has no order; sort the encoded values for deterministic output.
		sort.Slice(node.Values, func(i, j int) bool {
			return bytes.Compare(node.Values[i], node.Values[j]) < 0
		})
	default:
		return nil, fmt.Errorf("%w: cannot serialize expression with operation %s", ErrInvalidArgument, op)
	}

	return json.Marshal(node)
}

func encodeTerm(term Term) (json.RawMessage, error) {
	switch t := term.(type) {
	case Reference:
		return json.Marshal(string(t))
	case BoundReference:
		return json.Marshal(t.Field().Name)
	case *BoundTransform:
		return json.Marshal(transformNode{
			Type:      exprKeyTransform,
			Transform: t.transform.String(),
			Term:      t.term.Ref().Field().Name,
		})
	default:
		return nil, fmt.Errorf("%w: cannot serialize term of type %T", ErrInvalidArgument, term)
	}
}

// encodeLiteral writes a non-null literal in the JSON form for its Iceberg type
// (see Java's SingleValueParser). typ is the resolved field type, used to tell a
// timestamptz literal from a plain timestamp; it may be nil for an unbound term.
func encodeLiteral(lit Literal, typ Type) (json.RawMessage, error) {
	switch l := lit.(type) {
	case BoolLiteral:
		return json.Marshal(bool(l))
	case Int32Literal:
		return json.Marshal(int32(l))
	case Int64Literal:
		return json.Marshal(int64(l))
	case Float32Literal:
		if f := float64(l); math.IsInf(f, 0) || math.IsNaN(f) {
			return nil, fmt.Errorf("%w: cannot serialize non-finite float %v", ErrInvalidArgument, f)
		}

		return json.Marshal(float32(l))
	case Float64Literal:
		if f := float64(l); math.IsInf(f, 0) || math.IsNaN(f) {
			return nil, fmt.Errorf("%w: cannot serialize non-finite float %v", ErrInvalidArgument, f)
		}

		return json.Marshal(float64(l))
	case StringLiteral:
		return json.Marshal(string(l))
	case DateLiteral:
		return json.Marshal(Date(l).ToTime().Format("2006-01-02"))
	case TimeLiteral:
		// "9"s trim trailing fractional zeros (and the point when zero), as Java does.
		return json.Marshal(time.UnixMicro(int64(l)).UTC().Format("15:04:05.999999"))
	case TimestampLiteral:
		t := Timestamp(l).ToTime()
		// timestamptz gets a +00:00 offset ("-07:00" prints it, "Z07:00" wouldn't).
		if _, ok := typ.(TimestampTzType); ok {
			return json.Marshal(t.UTC().Format("2006-01-02T15:04:05.999999-07:00"))
		}

		return json.Marshal(t.Format("2006-01-02T15:04:05.999999"))
	case TimestampNsLiteral:
		t := TimestampNano(l).ToTime()
		if _, ok := typ.(TimestampTzNsType); ok {
			return json.Marshal(t.UTC().Format("2006-01-02T15:04:05.999999999-07:00"))
		}

		return json.Marshal(t.Format("2006-01-02T15:04:05.999999999"))
	case UUIDLiteral:
		return json.Marshal(uuid.UUID(l).String())
	case FixedLiteral:
		return json.Marshal(strings.ToUpper(hex.EncodeToString([]byte(l))))
	case BinaryLiteral:
		return json.Marshal(strings.ToUpper(hex.EncodeToString([]byte(l))))
	case DecimalLiteral:
		return json.Marshal(Decimal(l).String())
	default:
		return nil, fmt.Errorf("%w: cannot serialize literal of type %s", ErrInvalidArgument, lit.Type())
	}
}

func literalOf(e BooleanExpression) (Literal, error) {
	switch p := e.(type) {
	case *unboundLiteralPredicate:
		return p.lit, nil
	case BoundLiteralPredicate:
		return p.Literal(), nil
	default:
		return nil, fmt.Errorf("%w: expected a literal predicate, got %T", ErrInvalidArgument, e)
	}
}

func literalsOf(e BooleanExpression) ([]Literal, error) {
	switch p := e.(type) {
	case *unboundSetPredicate:
		return p.lits.Members(), nil
	case BoundSetPredicate:
		return p.Literals().Members(), nil
	default:
		return nil, fmt.Errorf("%w: expected a set predicate, got %T", ErrInvalidArgument, e)
	}
}

func decodeExpr(raw json.RawMessage, schema *Schema) (BooleanExpression, error) {
	b := bytes.TrimSpace(raw)
	if len(b) == 0 {
		return nil, fmt.Errorf("%w: cannot parse expression from empty input", ErrInvalidArgument)
	}

	// A bare boolean is AlwaysTrue/AlwaysFalse.
	if b[0] == 't' || b[0] == 'f' {
		var bv bool
		if err := json.Unmarshal(b, &bv); err != nil {
			return nil, fmt.Errorf("%w: cannot parse expression: %s", ErrInvalidArgument, err)
		}
		if bv {
			return AlwaysTrue{}, nil
		}

		return AlwaysFalse{}, nil
	}

	var node exprNode
	if err := json.Unmarshal(b, &node); err != nil {
		return nil, fmt.Errorf("%w: cannot parse expression: %s", ErrInvalidArgument, err)
	}

	// {"type":"literal","value":<bool>} is an alternate spelling of a constant.
	if node.Type == "literal" {
		var bv bool
		if err := json.Unmarshal(node.Value, &bv); err != nil {
			return nil, fmt.Errorf("%w: cannot parse literal expression: %s", ErrInvalidArgument, err)
		}
		if bv {
			return AlwaysTrue{}, nil
		}

		return AlwaysFalse{}, nil
	}

	op, ok := jsonToOp[node.Type]
	if !ok {
		return nil, fmt.Errorf("%w: unknown expression type %q", ErrInvalidArgument, node.Type)
	}

	switch op {
	case OpNot:
		child, err := decodeExpr(node.Child, schema)
		if err != nil {
			return nil, err
		}

		return NewNot(child), nil
	case OpAnd, OpOr:
		left, err := decodeExpr(node.Left, schema)
		if err != nil {
			return nil, err
		}
		right, err := decodeExpr(node.Right, schema)
		if err != nil {
			return nil, err
		}
		if op == OpAnd {
			return NewAnd(left, right), nil
		}

		return NewOr(left, right), nil
	}

	return decodePredicate(op, node, schema)
}

func decodePredicate(op Operation, node exprNode, schema *Schema) (BooleanExpression, error) {
	term, err := decodeTerm(node.Term)
	if err != nil {
		return nil, err
	}

	// Resolve the field type once; nil means schema-less.
	var typ Type
	if schema != nil {
		bound, err := term.Bind(schema, false)
		if err != nil {
			return nil, err
		}
		typ = bound.Type()
	}

	switch {
	case op >= OpIsNull && op <= OpNotNan:
		if len(node.Value) != 0 || node.Values != nil {
			return nil, fmt.Errorf("%w: unary predicate %s must not have a value", ErrInvalidArgument, node.Type)
		}

		return UnaryPredicate(op, term), nil
	case op >= OpLT && op <= OpNotStartsWith:
		if len(node.Value) == 0 {
			return nil, fmt.Errorf("%w: predicate %s is missing a value", ErrInvalidArgument, node.Type)
		}
		if node.Values != nil {
			return nil, fmt.Errorf("%w: predicate %s must not have values", ErrInvalidArgument, node.Type)
		}
		lit, err := decodeValue(node.Value, typ)
		if err != nil {
			return nil, err
		}

		return LiteralPredicate(op, term, lit), nil
	case op >= OpIn && op <= OpNotIn:
		if node.Values == nil {
			return nil, fmt.Errorf("%w: predicate %s is missing values", ErrInvalidArgument, node.Type)
		}
		if len(node.Value) != 0 {
			return nil, fmt.Errorf("%w: predicate %s must not have a value", ErrInvalidArgument, node.Type)
		}
		lits := make([]Literal, 0, len(node.Values))
		for _, v := range node.Values {
			lit, err := decodeValue(v, typ)
			if err != nil {
				return nil, err
			}
			lits = append(lits, lit)
		}

		return SetPredicate(op, term, lits), nil
	default:
		return nil, fmt.Errorf("%w: unsupported predicate operation %s", ErrInvalidArgument, op)
	}
}

func decodeTerm(raw json.RawMessage) (UnboundTerm, error) {
	b := bytes.TrimSpace(raw)
	if len(b) == 0 {
		return nil, fmt.Errorf("%w: predicate is missing a term", ErrInvalidArgument)
	}

	if b[0] == '"' {
		var name string
		if err := json.Unmarshal(b, &name); err != nil {
			return nil, fmt.Errorf("%w: cannot parse term: %s", ErrInvalidArgument, err)
		}

		return Reference(name), nil
	}

	var t transformNode
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, fmt.Errorf("%w: cannot parse term: %s", ErrInvalidArgument, err)
	}
	switch t.Type {
	case "reference":
		// {"type":"reference","term":"name"}
		return Reference(t.Term), nil
	case exprKeyTransform:
		return nil, fmt.Errorf("%w: transform terms are not supported when parsing expressions", ErrNotImplemented)
	default:
		return nil, fmt.Errorf("%w: cannot parse term with type %q", ErrInvalidArgument, t.Type)
	}
}

// decodeValue parses one literal value. A nil type yields the base literal kind
// for the JSON token; otherwise the value is converted to that type.
func decodeValue(raw json.RawMessage, typ Type) (Literal, error) {
	base, err := asObject(raw)
	if err != nil {
		return nil, err
	}

	if typ == nil {
		return base, nil
	}

	// Binary and fixed arrive as hex strings; decode them rather than treating
	// the string as raw bytes.
	switch typ.(type) {
	case BinaryType, FixedType:
		s, ok := base.(StringLiteral)
		if !ok {
			return nil, fmt.Errorf("%w: expected a string for %s value", ErrInvalidArgument, typ)
		}
		decoded, err := hex.DecodeString(string(s))
		if err != nil {
			return nil, fmt.Errorf("%w: invalid hex for %s value: %s", ErrInvalidArgument, typ, err)
		}
		if _, ok := typ.(FixedType); ok {
			return FixedLiteral(decoded), nil
		}

		return BinaryLiteral(decoded), nil
	}

	return base.To(typ)
}

// asObject parses a JSON scalar to its base literal kind (Int64, Float64,
// String, or Bool), as the reference does without a schema.
func asObject(raw json.RawMessage) (Literal, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("%w: cannot parse literal value: %s", ErrInvalidArgument, err)
	}

	switch x := v.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return Int64Literal(i), nil
		}
		f, err := x.Float64()
		if err != nil {
			return nil, fmt.Errorf("%w: cannot parse number literal %q: %s", ErrInvalidArgument, x.String(), err)
		}

		return Float64Literal(f), nil
	case string:
		return StringLiteral(x), nil
	case bool:
		return BoolLiteral(x), nil
	default:
		return nil, fmt.Errorf("%w: cannot parse literal value from %s", ErrInvalidArgument, raw)
	}
}
