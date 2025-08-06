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
	"math"
	"slices"

	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

const (
	rowsMightMatch, rowsMustMatch      = true, true
	rowsCannotMatch, rowsMightNotMatch = false, false
	inPredicateLimit                   = 200
)

// newManifestEvaluator returns a function that can be used to evaluate whether a particular
// manifest file has rows that might or might not match a given partition filter by using
// the stats provided in the partitions (UpperBound/LowerBound/ContainsNull/ContainsNaN).
func newManifestEvaluator(spec iceberg.PartitionSpec, schema *iceberg.Schema, partitionFilter iceberg.BooleanExpression, caseSensitive bool) (func(iceberg.ManifestFile) (bool, error), error) {
	partType := spec.PartitionType(schema)
	partSchema := iceberg.NewSchema(0, partType.FieldList...)
	filter, err := iceberg.RewriteNotExpr(partitionFilter)
	if err != nil {
		return nil, err
	}

	boundFilter, err := iceberg.BindExpr(partSchema, filter, caseSensitive)
	if err != nil {
		return nil, err
	}

	return (&manifestEvalVisitor{partitionFilter: boundFilter}).Eval, nil
}

type manifestEvalVisitor struct {
	partitionFields []iceberg.FieldSummary
	partitionFilter iceberg.BooleanExpression
}

func (m *manifestEvalVisitor) Eval(manifest iceberg.ManifestFile) (bool, error) {
	if parts := manifest.Partitions(); len(parts) > 0 {
		m.partitionFields = parts

		return iceberg.VisitExpr(m.partitionFilter, m)
	}

	return rowsMightMatch, nil
}

func removeBoundCmp[T iceberg.LiteralType](bound iceberg.Literal, vals []iceberg.Literal, cmpToDelete int) []iceberg.Literal {
	val := bound.(iceberg.TypedLiteral[T])
	cmp := val.Comparator()

	return slices.DeleteFunc(vals, func(v iceberg.Literal) bool {
		return cmp(val.Value(), v.(iceberg.TypedLiteral[T]).Value()) == cmpToDelete
	})
}

func removeBoundCheck(bound iceberg.Literal, vals []iceberg.Literal, toDelete int) []iceberg.Literal {
	switch bound.Type().(type) {
	case iceberg.BooleanType:
		return removeBoundCmp[bool](bound, vals, toDelete)
	case iceberg.Int32Type:
		return removeBoundCmp[int32](bound, vals, toDelete)
	case iceberg.Int64Type:
		return removeBoundCmp[int64](bound, vals, toDelete)
	case iceberg.Float32Type:
		return removeBoundCmp[float32](bound, vals, toDelete)
	case iceberg.Float64Type:
		return removeBoundCmp[float64](bound, vals, toDelete)
	case iceberg.DateType:
		return removeBoundCmp[iceberg.Date](bound, vals, toDelete)
	case iceberg.TimeType:
		return removeBoundCmp[iceberg.Time](bound, vals, toDelete)
	case iceberg.TimestampType, iceberg.TimestampTzType:
		return removeBoundCmp[iceberg.Timestamp](bound, vals, toDelete)
	case iceberg.BinaryType, iceberg.FixedType:
		return removeBoundCmp[[]byte](bound, vals, toDelete)
	case iceberg.StringType:
		return removeBoundCmp[string](bound, vals, toDelete)
	case iceberg.UUIDType:
		return removeBoundCmp[uuid.UUID](bound, vals, toDelete)
	case iceberg.DecimalType:
		return removeBoundCmp[iceberg.Decimal](bound, vals, toDelete)
	}
	panic("unrecognized type")
}

func allBoundCmp[T iceberg.LiteralType](bound iceberg.Literal, set iceberg.Set[iceberg.Literal], want int) bool {
	val := bound.(iceberg.TypedLiteral[T])
	cmp := val.Comparator()

	return set.All(func(e iceberg.Literal) bool {
		return cmp(val.Value(), e.(iceberg.TypedLiteral[T]).Value()) == want
	})
}

func allBoundCheck(bound iceberg.Literal, set iceberg.Set[iceberg.Literal], want int) bool {
	switch bound.Type().(type) {
	case iceberg.BooleanType:
		return allBoundCmp[bool](bound, set, want)
	case iceberg.Int32Type:
		return allBoundCmp[int32](bound, set, want)
	case iceberg.Int64Type:
		return allBoundCmp[int64](bound, set, want)
	case iceberg.Float32Type:
		return allBoundCmp[float32](bound, set, want)
	case iceberg.Float64Type:
		return allBoundCmp[float64](bound, set, want)
	case iceberg.DateType:
		return allBoundCmp[iceberg.Date](bound, set, want)
	case iceberg.TimeType:
		return allBoundCmp[iceberg.Time](bound, set, want)
	case iceberg.TimestampType, iceberg.TimestampTzType:
		return allBoundCmp[iceberg.Timestamp](bound, set, want)
	case iceberg.BinaryType, iceberg.FixedType:
		return allBoundCmp[[]byte](bound, set, want)
	case iceberg.StringType:
		return allBoundCmp[string](bound, set, want)
	case iceberg.UUIDType:
		return allBoundCmp[uuid.UUID](bound, set, want)
	case iceberg.DecimalType:
		return allBoundCmp[iceberg.Decimal](bound, set, want)
	}
	panic(iceberg.ErrType)
}

func (m *manifestEvalVisitor) VisitIn(term iceberg.BoundTerm, literals iceberg.Set[iceberg.Literal]) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.LowerBound == nil {
		return rowsCannotMatch
	}

	if literals.Len() > inPredicateLimit {
		return rowsMightMatch
	}

	lower, err := iceberg.LiteralFromBytes(term.Type(), *field.LowerBound)
	if err != nil {
		panic(err)
	}

	if allBoundCheck(lower, literals, 1) {
		return rowsCannotMatch
	}

	if field.UpperBound != nil {
		upper, err := iceberg.LiteralFromBytes(term.Type(), *field.UpperBound)
		if err != nil {
			panic(err)
		}

		if allBoundCheck(upper, literals, -1) {
			return rowsCannotMatch
		}
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitNotIn(term iceberg.BoundTerm, literals iceberg.Set[iceberg.Literal]) bool {
	// because the bounds are not necessarily a min or max value, this cannot be answered using them
	// notIn(col, {X, ...}) with (X, Y) doesn't guarantee that X is a value in col
	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitIsNan(term iceberg.BoundTerm) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.ContainsNaN != nil && !*field.ContainsNaN {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitNotNan(term iceberg.BoundTerm) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.ContainsNaN != nil && *field.ContainsNaN && !field.ContainsNull && field.LowerBound == nil {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitIsNull(term iceberg.BoundTerm) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if !field.ContainsNull {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitNotNull(term iceberg.BoundTerm) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	// ContainsNull encodes whether at least one partition value is null
	// lowerBound is null if all partition values are null
	allNull := field.ContainsNull && field.LowerBound == nil
	if allNull && (term.Ref().Type().Equals(iceberg.PrimitiveTypes.Float32) || term.Ref().Type().Equals(iceberg.PrimitiveTypes.Float64)) {
		// floating point types may include NaN values, which we check separately
		// in case bounds don't include NaN values, ContainsNaN needsz to be checked
		allNull = field.ContainsNaN != nil && !*field.ContainsNaN
	}

	if allNull {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func getCmp[T iceberg.LiteralType](b iceberg.TypedLiteral[T]) func(iceberg.Literal, iceberg.Literal) int {
	cmp := b.Comparator()

	return func(l1, l2 iceberg.Literal) int {
		return cmp(l1.(iceberg.TypedLiteral[T]).Value(), l2.(iceberg.TypedLiteral[T]).Value())
	}
}

func getCmpLiteral(boundary iceberg.Literal) func(iceberg.Literal, iceberg.Literal) int {
	switch l := boundary.(type) {
	case iceberg.TypedLiteral[bool]:
		return getCmp(l)
	case iceberg.TypedLiteral[int32]:
		return getCmp(l)
	case iceberg.TypedLiteral[int64]:
		return getCmp(l)
	case iceberg.TypedLiteral[float32]:
		return getCmp(l)
	case iceberg.TypedLiteral[float64]:
		return getCmp(l)
	case iceberg.TypedLiteral[iceberg.Date]:
		return getCmp(l)
	case iceberg.TypedLiteral[iceberg.Time]:
		return getCmp(l)
	case iceberg.TypedLiteral[iceberg.Timestamp]:
		return getCmp(l)
	case iceberg.TypedLiteral[[]byte]:
		return getCmp(l)
	case iceberg.TypedLiteral[string]:
		return getCmp(l)
	case iceberg.TypedLiteral[uuid.UUID]:
		return getCmp(l)
	case iceberg.TypedLiteral[iceberg.Decimal]:
		return getCmp(l)
	}
	panic(iceberg.ErrType)
}

func (m *manifestEvalVisitor) VisitEqual(term iceberg.BoundTerm, lit iceberg.Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.LowerBound == nil || field.UpperBound == nil {
		// values are all null and literal cannot contain null
		return rowsCannotMatch
	}

	lower, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.LowerBound)
	if err != nil {
		panic(err)
	}

	cmp := getCmpLiteral(lower)
	if cmp(lower, lit) == 1 {
		return rowsCannotMatch
	}

	upper, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.UpperBound)
	if err != nil {
		panic(err)
	}

	if cmp(lit, upper) == 1 {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitNotEqual(term iceberg.BoundTerm, lit iceberg.Literal) bool {
	// because bounds are not necessarily a min or max, this cannot be answered
	// using them. notEq(col, X) with (X, Y) doesn't guarantee X is a value in col
	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitGreaterEqual(term iceberg.BoundTerm, lit iceberg.Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.UpperBound == nil {
		return rowsCannotMatch
	}

	upper, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.UpperBound)
	if err != nil {
		panic(err)
	}

	if getCmpLiteral(upper)(lit, upper) == 1 {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitGreater(term iceberg.BoundTerm, lit iceberg.Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.UpperBound == nil {
		return rowsCannotMatch
	}

	upper, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.UpperBound)
	if err != nil {
		panic(err)
	}

	if getCmpLiteral(upper)(lit, upper) >= 0 {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitLessEqual(term iceberg.BoundTerm, lit iceberg.Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.LowerBound == nil {
		return rowsCannotMatch
	}

	lower, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.LowerBound)
	if err != nil {
		panic(err)
	}

	if getCmpLiteral(lower)(lit, lower) == -1 {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitLess(term iceberg.BoundTerm, lit iceberg.Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.LowerBound == nil {
		return rowsCannotMatch
	}

	lower, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.LowerBound)
	if err != nil {
		panic(err)
	}

	if getCmpLiteral(lower)(lit, lower) <= 0 {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitStartsWith(term iceberg.BoundTerm, lit iceberg.Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	var prefix string
	if val, ok := lit.(iceberg.TypedLiteral[string]); ok {
		prefix = val.Value()
	} else {
		prefix = string(lit.(iceberg.TypedLiteral[[]byte]).Value())
	}

	lenPrefix := len(prefix)

	if field.LowerBound == nil {
		return rowsCannotMatch
	}

	lower, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.LowerBound)
	if err != nil {
		panic(err)
	}

	// truncate lower bound so that it's length is not greater than the length of prefix
	var v string
	switch l := lower.(type) {
	case iceberg.TypedLiteral[string]:
		v = l.Value()
		if len(v) > lenPrefix {
			v = v[:lenPrefix]
		}
	case iceberg.TypedLiteral[[]byte]:
		v = string(l.Value())
		if len(v) > lenPrefix {
			v = v[:lenPrefix]
		}
	}

	if v > prefix {
		return rowsCannotMatch
	}

	if field.UpperBound == nil {
		return rowsCannotMatch
	}

	upper, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.UpperBound)
	if err != nil {
		panic(err)
	}

	switch u := upper.(type) {
	case iceberg.TypedLiteral[string]:
		v = u.Value()
		if len(v) > lenPrefix {
			v = v[:lenPrefix]
		}
	case iceberg.TypedLiteral[[]byte]:
		v = string(u.Value())
		if len(v) > lenPrefix {
			v = v[:lenPrefix]
		}
	}

	if v < prefix {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitNotStartsWith(term iceberg.BoundTerm, lit iceberg.Literal) bool {
	pos := term.Ref().Pos()
	field := m.partitionFields[pos]

	if field.ContainsNull || field.LowerBound == nil || field.UpperBound == nil {
		return rowsMightMatch
	}

	// NotStartsWith will match unless ALL values must start with the prefix.
	// this happens when the lower and upper bounds BOTH start with the prefix
	lower, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.LowerBound)
	if err != nil {
		panic(err)
	}

	upper, err := iceberg.LiteralFromBytes(term.Ref().Type(), *field.UpperBound)
	if err != nil {
		panic(err)
	}

	var prefix, lowerBound, upperBound string
	if val, ok := lit.(iceberg.TypedLiteral[string]); ok {
		prefix = val.Value()
		lowerBound, upperBound = lower.(iceberg.TypedLiteral[string]).Value(), upper.(iceberg.TypedLiteral[string]).Value()
	} else {
		prefix = string(lit.(iceberg.TypedLiteral[[]byte]).Value())
		lowerBound = string(lower.(iceberg.TypedLiteral[[]byte]).Value())
		upperBound = string(upper.(iceberg.TypedLiteral[[]byte]).Value())
	}

	lenPrefix := len(prefix)
	if len(lowerBound) < lenPrefix {
		return rowsMightMatch
	}

	if lowerBound[:lenPrefix] == prefix {
		// if upper is shorter then upper can't start with the prefix
		if len(upperBound) < lenPrefix {
			return rowsMightMatch
		}

		if upperBound[:lenPrefix] == prefix {
			return rowsCannotMatch
		}
	}

	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitTrue() bool {
	return rowsMightMatch
}

func (m *manifestEvalVisitor) VisitFalse() bool {
	return rowsCannotMatch
}

func (m *manifestEvalVisitor) VisitUnbound(iceberg.UnboundPredicate) bool {
	panic("need bound predicate")
}

func (m *manifestEvalVisitor) VisitBound(pred iceberg.BoundPredicate) bool {
	return iceberg.VisitBoundPredicate(pred, m)
}

func (m *manifestEvalVisitor) VisitNot(child bool) bool       { return !child }
func (m *manifestEvalVisitor) VisitAnd(left, right bool) bool { return left && right }
func (m *manifestEvalVisitor) VisitOr(left, right bool) bool  { return left || right }

type projectionEvaluator struct {
	spec          iceberg.PartitionSpec
	schema        *iceberg.Schema
	caseSensitive bool
}

func (*projectionEvaluator) VisitTrue() iceberg.BooleanExpression  { return iceberg.AlwaysTrue{} }
func (*projectionEvaluator) VisitFalse() iceberg.BooleanExpression { return iceberg.AlwaysFalse{} }
func (*projectionEvaluator) VisitNot(child iceberg.BooleanExpression) iceberg.BooleanExpression {
	panic(fmt.Errorf("%w: cannot project 'not' expression, should be rewritten %s",
		iceberg.ErrInvalidArgument, child))
}

func (*projectionEvaluator) VisitAnd(left, right iceberg.BooleanExpression) iceberg.BooleanExpression {
	return iceberg.NewAnd(left, right)
}

func (*projectionEvaluator) VisitOr(left, right iceberg.BooleanExpression) iceberg.BooleanExpression {
	return iceberg.NewOr(left, right)
}

func (*projectionEvaluator) VisitUnbound(pred iceberg.UnboundPredicate) iceberg.BooleanExpression {
	panic(fmt.Errorf("%w: cannot project unbound predicate: %s", iceberg.ErrInvalidArgument, pred))
}

type inclusiveProjection struct{ projectionEvaluator }

func (p *inclusiveProjection) Project(expr iceberg.BooleanExpression) (iceberg.BooleanExpression, error) {
	expr, err := iceberg.RewriteNotExpr(expr)
	if err != nil {
		return nil, err
	}

	bound, err := iceberg.BindExpr(p.schema, expr, p.caseSensitive)
	if err != nil {
		return nil, err
	}

	return iceberg.VisitExpr(bound, p)
}

func (p *inclusiveProjection) VisitBound(pred iceberg.BoundPredicate) iceberg.BooleanExpression {
	parts := p.spec.FieldsBySourceID(pred.Term().Ref().Field().ID)

	var result iceberg.BooleanExpression = iceberg.AlwaysTrue{}
	for _, part := range parts {
		// consider (d = 2019-01-01) with bucket(7, d) and bucket(5, d)
		// projections: b1 = bucket(7, '2019-01-01') = 5, b2 = bucket(5, '2019-01-01') = 0
		// any value where b1 != 5 or any value where b2 != 0 cannot be the '2019-01-01'
		//
		// similarly, if partitioning by day(ts) and hour(ts), the more restrictive
		// projection should be used. ts = 2019-01-01T01:00:00 produces day=2019-01-01 and
		// hour=2019-01-01-01. the value will be in 2019-01-01-01 and not in 2019-01-01-02.
		inclProjection, err := part.Transform.Project(part.Name, pred)
		if err != nil {
			panic(err)
		}
		if inclProjection != nil {
			result = iceberg.NewAnd(result, inclProjection)
		}
	}

	return result
}

func newInclusiveProjection(s *iceberg.Schema, spec iceberg.PartitionSpec, caseSensitive bool) func(iceberg.BooleanExpression) (iceberg.BooleanExpression, error) {
	return (&inclusiveProjection{
		projectionEvaluator: projectionEvaluator{
			schema:        s,
			spec:          spec,
			caseSensitive: caseSensitive,
		},
	}).Project
}

type metricsEvaluator struct {
	valueCounts map[int]int64
	nullCounts  map[int]int64
	nanCounts   map[int]int64
	lowerBounds map[int][]byte
	upperBounds map[int][]byte
}

func (m *metricsEvaluator) VisitTrue() bool  { return rowsMightMatch }
func (m *metricsEvaluator) VisitFalse() bool { return rowsCannotMatch }
func (m *metricsEvaluator) VisitNot(child bool) bool {
	panic(fmt.Errorf("%w: NOT should be rewritten %v", iceberg.ErrInvalidArgument, child))
}
func (m *metricsEvaluator) VisitAnd(left, right bool) bool { return left && right }
func (m *metricsEvaluator) VisitOr(left, right bool) bool  { return left || right }

func (m *metricsEvaluator) containsNullsOnly(id int) bool {
	valCount, ok := m.valueCounts[id]
	if !ok {
		return false
	}

	nullCount, ok := m.nullCounts[id]
	if !ok {
		return false
	}

	return valCount == nullCount
}

func (m *metricsEvaluator) containsNansOnly(id int) bool {
	nanCount, ok := m.nanCounts[id]
	if !ok {
		return false
	}

	valCount, ok := m.valueCounts[id]
	if !ok {
		return false
	}

	return nanCount == valCount
}

func (m *metricsEvaluator) isNan(v iceberg.Literal) bool {
	switch v := v.(type) {
	case iceberg.Float32Literal:
		return math.IsNaN(float64(v))
	case iceberg.Float64Literal:
		return math.IsNaN(float64(v))
	default:
		return false
	}
}

func newInclusiveMetricsEvaluator(s *iceberg.Schema, expr iceberg.BooleanExpression,
	caseSensitive bool, includeEmptyFiles bool,
) (func(iceberg.DataFile) (bool, error), error) {
	rewritten, err := iceberg.RewriteNotExpr(expr)
	if err != nil {
		return nil, err
	}

	bound, err := iceberg.BindExpr(s, rewritten, caseSensitive)
	if err != nil {
		return nil, err
	}

	return (&inclusiveMetricsEval{
		st:                s.AsStruct(),
		includeEmptyFiles: includeEmptyFiles,
		expr:              bound,
	}).Eval, nil
}

func newParquetRowGroupStatsEvaluator(fileSchema *iceberg.Schema, expr iceberg.BooleanExpression,
	includeEmptyFiles bool,
) (func(*metadata.RowGroupMetaData, []int) (bool, error), error) {
	rewritten, err := iceberg.RewriteNotExpr(expr)
	if err != nil {
		return nil, err
	}

	return (&inclusiveMetricsEval{
		st:                fileSchema.AsStruct(),
		includeEmptyFiles: includeEmptyFiles,
		expr:              rewritten,
	}).TestRowGroup, nil
}

type inclusiveMetricsEval struct {
	metricsEvaluator

	st                iceberg.StructType
	expr              iceberg.BooleanExpression
	includeEmptyFiles bool
}

func (m *inclusiveMetricsEval) TestRowGroup(rgmeta *metadata.RowGroupMetaData, colIndices []int) (bool, error) {
	if !m.includeEmptyFiles && rgmeta.NumRows() == 0 {
		return rowsCannotMatch, nil
	}

	m.valueCounts = make(map[int]int64)
	m.nullCounts = make(map[int]int64)
	m.nanCounts = nil
	m.lowerBounds = make(map[int][]byte)
	m.upperBounds = make(map[int][]byte)

	for _, c := range colIndices {
		colMeta, err := rgmeta.ColumnChunk(c)
		if err != nil {
			return false, err
		}

		if ok, err := colMeta.StatsSet(); !ok || err != nil {
			continue
		}

		stats, err := colMeta.Statistics()
		if err != nil {
			return false, err
		}

		if stats == nil {
			continue
		}

		fieldID := int(stats.Descr().SchemaNode().FieldID())
		m.valueCounts[fieldID] = stats.NumValues()
		if stats.HasNullCount() {
			m.nullCounts[fieldID] = stats.NullCount()
		}
		if stats.HasMinMax() {
			m.lowerBounds[fieldID] = stats.EncodeMin()
			m.upperBounds[fieldID] = stats.EncodeMax()
		}
	}

	return iceberg.VisitExpr(m.expr, m)
}

func (m *inclusiveMetricsEval) Eval(file iceberg.DataFile) (bool, error) {
	if !m.includeEmptyFiles && file.Count() == 0 {
		return rowsCannotMatch, nil
	}

	// avoid race condition while maintaining existing state
	ev := inclusiveMetricsEval{
		st:                m.st,
		includeEmptyFiles: m.includeEmptyFiles,
		expr:              m.expr,
	}

	ev.valueCounts, ev.nullCounts = file.ValueCounts(), file.NullValueCounts()
	ev.nanCounts = file.NaNValueCounts()
	ev.lowerBounds, ev.upperBounds = file.LowerBoundValues(), file.UpperBoundValues()

	return iceberg.VisitExpr(m.expr, &ev)
}

func (m *inclusiveMetricsEval) mayContainNull(fieldID int) bool {
	if m.nullCounts == nil {
		return true
	}

	_, ok := m.nullCounts[fieldID]

	return ok
}

func (m *inclusiveMetricsEval) VisitUnbound(iceberg.UnboundPredicate) bool {
	panic("need bound predicate")
}

func (m *inclusiveMetricsEval) VisitBound(pred iceberg.BoundPredicate) bool {
	return iceberg.VisitBoundPredicate(pred, m)
}

func (m *inclusiveMetricsEval) VisitIsNull(t iceberg.BoundTerm) bool {
	fieldID := t.Ref().Field().ID
	if cnt, exists := m.nullCounts[fieldID]; exists && cnt == 0 {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *inclusiveMetricsEval) VisitNotNull(t iceberg.BoundTerm) bool {
	// no need to check whether the field is required because binding evaluates
	// that case if the column has no non-null values, the expression cannot match
	fieldID := t.Ref().Field().ID
	if m.containsNullsOnly(fieldID) {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *inclusiveMetricsEval) VisitIsNan(t iceberg.BoundTerm) bool {
	fieldID := t.Ref().Field().ID
	if cnt, exists := m.nanCounts[fieldID]; exists && cnt == 0 {
		return rowsCannotMatch
	}
	// when there's no nancounts information but we already know the column
	// contains null it's guaranteed that there's no nan value
	if m.containsNullsOnly(fieldID) {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *inclusiveMetricsEval) VisitNotNan(t iceberg.BoundTerm) bool {
	fieldID := t.Ref().Field().ID

	if m.containsNansOnly(fieldID) {
		return rowsCannotMatch
	}

	return rowsMightMatch
}

func (m *inclusiveMetricsEval) VisitLess(t iceberg.BoundTerm, lit iceberg.Literal) bool {
	field := t.Ref().Field()
	fieldID := field.ID

	if m.containsNullsOnly(fieldID) || m.containsNansOnly(fieldID) {
		return rowsCannotMatch
	}

	if _, ok := field.Type.(iceberg.PrimitiveType); !ok {
		panic(fmt.Errorf("%w: expected iceberg.PrimitiveType, got %s",
			iceberg.ErrInvalidTypeString, field.Type))
	}

	if lowerBoundBytes := m.lowerBounds[fieldID]; lowerBoundBytes != nil {
		lowerBound, err := iceberg.LiteralFromBytes(field.Type, lowerBoundBytes)
		if err != nil {
			panic(err)
		}

		if m.isNan(lowerBound) {
			// nan indicates unreliable bounds
			return rowsMightMatch
		}

		if getCmpLiteral(lowerBound)(lowerBound, lit) >= 0 {
			return rowsCannotMatch
		}
	}

	return rowsMightMatch
}

func (m *inclusiveMetricsEval) VisitLessEqual(t iceberg.BoundTerm, lit iceberg.Literal) bool {
	field := t.Ref().Field()
	fieldID := field.ID

	if m.containsNullsOnly(fieldID) || m.containsNansOnly(fieldID) {
		return rowsCannotMatch
	}

	if _, ok := field.Type.(iceberg.PrimitiveType); !ok {
		panic(fmt.Errorf("%w: expected iceberg.PrimitiveType, got %s",
			iceberg.ErrInvalidTypeString, field.Type))
	}

	if lowerBoundBytes := m.lowerBounds[fieldID]; lowerBoundBytes != nil {
		lowerBound, err := iceberg.LiteralFromBytes(field.Type, lowerBoundBytes)
		if err != nil {
			panic(err)
		}

		if m.isNan(lowerBound) {
			// nan indicates unreliable bounds
			return rowsMightMatch
		}

		if getCmpLiteral(lowerBound)(lowerBound, lit) > 0 {
			return rowsCannotMatch
		}
	}

	return rowsMightMatch
}

func (m *inclusiveMetricsEval) VisitGreater(t iceberg.BoundTerm, lit iceberg.Literal) bool {
	field := t.Ref().Field()
	fieldID := field.ID

	if m.containsNullsOnly(fieldID) || m.containsNansOnly(fieldID) {
		return rowsCannotMatch
	}

	if _, ok := field.Type.(iceberg.PrimitiveType); !ok {
		panic(fmt.Errorf("%w: expected iceberg.PrimitiveType, got %s",
			iceberg.ErrInvalidTypeString, field.Type))
	}

	if upperBoundBytes := m.upperBounds[fieldID]; upperBoundBytes != nil {
		upperBound, err := iceberg.LiteralFromBytes(field.Type, upperBoundBytes)
		if err != nil {
			panic(err)
		}

		if getCmpLiteral(upperBound)(upperBound, lit) <= 0 {
			if m.isNan(upperBound) {
				return rowsMightMatch
			}

			return rowsCannotMatch
		}
	}

	return rowsMightMatch
}

func (m *inclusiveMetricsEval) VisitGreaterEqual(t iceberg.BoundTerm, lit iceberg.Literal) bool {
	field := t.Ref().Field()
	fieldID := field.ID

	if m.containsNullsOnly(fieldID) || m.containsNansOnly(fieldID) {
		return rowsCannotMatch
	}

	if _, ok := field.Type.(iceberg.PrimitiveType); !ok {
		panic(fmt.Errorf("%w: expected iceberg.PrimitiveType, got %s",
			iceberg.ErrInvalidTypeString, field.Type))
	}

	if upperBoundBytes := m.upperBounds[fieldID]; upperBoundBytes != nil {
		upperBound, err := iceberg.LiteralFromBytes(field.Type, upperBoundBytes)
		if err != nil {
			panic(err)
		}

		if getCmpLiteral(upperBound)(upperBound, lit) < 0 {
			if m.isNan(upperBound) {
				return rowsMightMatch
			}

			return rowsCannotMatch
		}
	}

	return rowsMightMatch
}

func (m *inclusiveMetricsEval) VisitEqual(t iceberg.BoundTerm, lit iceberg.Literal) bool {
	field := t.Ref().Field()
	fieldID := field.ID

	if m.containsNullsOnly(fieldID) || m.containsNansOnly(fieldID) {
		return rowsCannotMatch
	}

	if _, ok := field.Type.(iceberg.PrimitiveType); !ok {
		panic(fmt.Errorf("%w: expected iceberg.PrimitiveType, got %s",
			iceberg.ErrInvalidTypeString, field.Type))
	}

	var cmp func(iceberg.Literal, iceberg.Literal) int
	if lowerBoundBytes := m.lowerBounds[fieldID]; lowerBoundBytes != nil {
		lowerBound, err := iceberg.LiteralFromBytes(field.Type, lowerBoundBytes)
		if err != nil {
			panic(err)
		}

		if m.isNan(lowerBound) {
			return rowsMightMatch
		}

		cmp = getCmpLiteral(lowerBound)
		if cmp(lowerBound, lit) == 1 {
			return rowsCannotMatch
		}
	}

	if upperBoundBytes := m.upperBounds[fieldID]; upperBoundBytes != nil {
		upperBound, err := iceberg.LiteralFromBytes(field.Type, upperBoundBytes)
		if err != nil {
			panic(err)
		}

		if m.isNan(upperBound) {
			return rowsMightMatch
		}

		cmp = getCmpLiteral(upperBound)
		if cmp(upperBound, lit) == -1 {
			return rowsCannotMatch
		}
	}

	return rowsMightMatch
}

func (m *inclusiveMetricsEval) VisitNotEqual(iceberg.BoundTerm, iceberg.Literal) bool {
	return rowsMightMatch
}

func (m *inclusiveMetricsEval) VisitIn(t iceberg.BoundTerm, s iceberg.Set[iceberg.Literal]) bool {
	field := t.Ref().Field()
	fieldID := field.ID

	if m.containsNullsOnly(fieldID) || m.containsNansOnly(fieldID) {
		return rowsCannotMatch
	}

	if s.Len() > inPredicateLimit {
		// skip evaluating the predicate if the number of values is too big
		return rowsMightMatch
	}

	if _, ok := field.Type.(iceberg.PrimitiveType); !ok {
		panic(fmt.Errorf("%w: expected iceberg.PrimitiveType, got %s",
			iceberg.ErrInvalidTypeString, field.Type))
	}

	values := s.Members()
	if lowerBoundBytes := m.lowerBounds[fieldID]; lowerBoundBytes != nil {
		lowerBound, err := iceberg.LiteralFromBytes(field.Type, lowerBoundBytes)
		if err != nil {
			panic(lowerBound)
		}

		if m.isNan(lowerBound) {
			return rowsMightMatch
		}

		values = removeBoundCheck(lowerBound, values, 1)
		if len(values) == 0 {
			return rowsCannotMatch
		}
	}

	if upperBoundBytes := m.upperBounds[fieldID]; upperBoundBytes != nil {
		upperBound, err := iceberg.LiteralFromBytes(field.Type, upperBoundBytes)
		if err != nil {
			panic(err)
		}

		if m.isNan(upperBound) {
			return rowsMightMatch
		}

		values = removeBoundCheck(upperBound, values, -1)
		if len(values) == 0 {
			return rowsCannotMatch
		}
	}

	return rowsMightMatch
}

func (m *inclusiveMetricsEval) VisitNotIn(iceberg.BoundTerm, iceberg.Set[iceberg.Literal]) bool {
	// because the bounds are not necessarily a min or max value, this cannot be
	// answered using them. notIn(col, {X, ...}) with (XX, Y) doesn't guarantee that
	// X is a value in col
	return rowsMightMatch
}

func (m *inclusiveMetricsEval) VisitStartsWith(t iceberg.BoundTerm, lit iceberg.Literal) bool {
	field := t.Ref().Field()
	fieldID := field.ID

	if m.containsNullsOnly(fieldID) {
		return rowsCannotMatch
	}

	if _, ok := field.Type.(iceberg.PrimitiveType); !ok {
		panic(fmt.Errorf("%w: expected iceberg.PrimitiveType, got %s",
			iceberg.ErrInvalidTypeString, field.Type))
	}

	var prefix string
	if val, ok := lit.(iceberg.TypedLiteral[string]); ok {
		prefix = val.Value()
	} else {
		prefix = string(lit.(iceberg.TypedLiteral[[]byte]).Value())
	}

	lenPrefix := len(prefix)

	if lowerBoundBytes := m.lowerBounds[fieldID]; lowerBoundBytes != nil {
		lowerBound, err := iceberg.LiteralFromBytes(field.Type, lowerBoundBytes)
		if err != nil {
			panic(err)
		}

		var v string
		switch l := lowerBound.(type) {
		case iceberg.TypedLiteral[string]:
			v = l.Value()
		case iceberg.TypedLiteral[[]byte]:
			v = string(l.Value())
		}

		if len(v) > lenPrefix {
			v = v[:lenPrefix]
		}

		if len(v) > 0 && v > prefix {
			return rowsCannotMatch
		}
	}

	if upperBoundBytes := m.upperBounds[fieldID]; upperBoundBytes != nil {
		upperBound, err := iceberg.LiteralFromBytes(field.Type, upperBoundBytes)
		if err != nil {
			panic(err)
		}

		var v string
		switch u := upperBound.(type) {
		case iceberg.TypedLiteral[string]:
			v = u.Value()
		case iceberg.TypedLiteral[[]byte]:
			v = string(u.Value())
		}

		if len(v) > lenPrefix {
			v = v[:lenPrefix]
		}

		if len(v) > 0 && v < prefix {
			return rowsCannotMatch
		}
	}

	return rowsMightMatch
}

func (m *inclusiveMetricsEval) VisitNotStartsWith(t iceberg.BoundTerm, lit iceberg.Literal) bool {
	field := t.Ref().Field()
	fieldID := field.ID

	if m.mayContainNull(fieldID) {
		return rowsMightMatch
	}

	if _, ok := field.Type.(iceberg.PrimitiveType); !ok {
		panic(fmt.Errorf("%w: expected iceberg.PrimitiveType, got %s",
			iceberg.ErrInvalidTypeString, field.Type))
	}

	// not_starts_with will match unless all values must start with the prefix.
	// this happens when the lower and upper bounds both start with the prefix
	lowerBoundBytes, upperBoundBytes := m.lowerBounds[fieldID], m.upperBounds[fieldID]
	if lowerBoundBytes != nil && upperBoundBytes != nil {
		lowerBound, err := iceberg.LiteralFromBytes(field.Type, lowerBoundBytes)
		if err != nil {
			panic(err)
		}

		upperBound, err := iceberg.LiteralFromBytes(field.Type, upperBoundBytes)
		if err != nil {
			panic(err)
		}

		var prefix, lower, upper string
		if val, ok := lit.(iceberg.TypedLiteral[string]); ok {
			prefix = val.Value()
			lower, upper = lowerBound.(iceberg.TypedLiteral[string]).Value(), upperBound.(iceberg.TypedLiteral[string]).Value()
		} else {
			prefix = string(lit.(iceberg.TypedLiteral[[]byte]).Value())
			lower, upper = string(lowerBound.(iceberg.TypedLiteral[[]byte]).Value()), string(upperBound.(iceberg.TypedLiteral[[]byte]).Value())
		}

		lenPrefix := len(prefix)
		if len(lower) < lenPrefix {
			return rowsMightMatch
		}

		if lower[:lenPrefix] == prefix {
			if len(upper) < lenPrefix {
				return rowsMightMatch
			}

			if upper[:lenPrefix] == prefix {
				return rowsCannotMatch
			}
		}
	}

	return rowsMightMatch
}

func newStrictMetricsEvaluator(s *iceberg.Schema, expr iceberg.BooleanExpression,
	caseSensitive bool, includeEmptyFiles bool,
) (func(iceberg.DataFile) (bool, error), error) {
	rewritten, err := iceberg.RewriteNotExpr(expr)
	if err != nil {
		return nil, err
	}

	bound, err := iceberg.BindExpr(s, rewritten, caseSensitive)
	if err != nil {
		return nil, err
	}

	return (&strictMetricsEval{
		st:                s.AsStruct(),
		includeEmptyFiles: includeEmptyFiles,
		expr:              bound,
	}).Eval, nil
}

type strictMetricsEval struct {
	metricsEvaluator

	st                iceberg.StructType
	expr              iceberg.BooleanExpression
	includeEmptyFiles bool
}

func (m *strictMetricsEval) Eval(file iceberg.DataFile) (bool, error) {
	if !m.includeEmptyFiles && file.Count() <= 0 {
		return rowsMustMatch, nil
	}

	// avoid race condition while maintaining existing state
	ev := strictMetricsEval{
		st:                m.st,
		includeEmptyFiles: m.includeEmptyFiles,
		expr:              m.expr,
	}

	ev.valueCounts, ev.nullCounts = file.ValueCounts(), file.NullValueCounts()
	ev.nanCounts = file.NaNValueCounts()
	ev.lowerBounds, ev.upperBounds = file.LowerBoundValues(), file.UpperBoundValues()

	return iceberg.VisitExpr(m.expr, &ev)
}

func (m *strictMetricsEval) VisitUnbound(iceberg.UnboundPredicate) bool {
	panic("need bound predicate")
}

func (m *strictMetricsEval) VisitBound(pred iceberg.BoundPredicate) bool {
	return iceberg.VisitBoundPredicate(pred, m)
}

func (m *strictMetricsEval) VisitIsNull(t iceberg.BoundTerm) bool {
	fieldID := t.Ref().Field().ID
	if m.containsNullsOnly(fieldID) {
		return rowsMustMatch
	}

	return rowsMightNotMatch
}

func (m *strictMetricsEval) VisitNotNull(t iceberg.BoundTerm) bool {
	fieldID := t.Ref().Field().ID
	if cnt, exists := m.nullCounts[fieldID]; exists && cnt == 0 {
		return rowsMustMatch
	}

	return rowsMightNotMatch
}

func (m *strictMetricsEval) VisitIsNan(t iceberg.BoundTerm) bool {
	fieldID := t.Ref().Field().ID

	if m.containsNansOnly(fieldID) {
		return rowsMustMatch
	}

	return rowsMightNotMatch
}

func (m *strictMetricsEval) VisitNotNan(t iceberg.BoundTerm) bool {
	fieldID := t.Ref().Field().ID

	if cnt, exists := m.nanCounts[fieldID]; exists && cnt == 0 {
		return rowsMustMatch
	}

	if m.containsNullsOnly(fieldID) {
		return rowsMustMatch
	}

	return rowsMightNotMatch
}

func (m *strictMetricsEval) VisitLess(t iceberg.BoundTerm, lit iceberg.Literal) bool {
	field := t.Ref().Field()
	fieldID := field.ID

	if m.canContainNulls(fieldID) || m.canContainNans(fieldID) {
		return rowsMightNotMatch
	}

	if upperBoundBytes := m.upperBounds[fieldID]; upperBoundBytes != nil {
		upperBound, err := iceberg.LiteralFromBytes(field.Type, upperBoundBytes)
		if err != nil {
			panic(err)
		}

		if getCmpLiteral(upperBound)(upperBound, lit) < 0 {
			return rowsMustMatch
		}
	}

	return rowsMightNotMatch
}

func (m *strictMetricsEval) VisitLessEqual(t iceberg.BoundTerm, lit iceberg.Literal) bool {
	field := t.Ref().Field()
	fieldID := field.ID

	if m.canContainNulls(fieldID) || m.canContainNans(fieldID) {
		return rowsMightNotMatch
	}

	if upperBoundBytes := m.upperBounds[fieldID]; upperBoundBytes != nil {
		upperBound, err := iceberg.LiteralFromBytes(field.Type, upperBoundBytes)
		if err != nil {
			panic(err)
		}

		if getCmpLiteral(upperBound)(upperBound, lit) <= 0 {
			return rowsMustMatch
		}
	}

	return rowsMightNotMatch
}

func (m *strictMetricsEval) VisitGreater(t iceberg.BoundTerm, lit iceberg.Literal) bool {
	field := t.Ref().Field()
	fieldID := field.ID

	if m.canContainNulls(fieldID) || m.canContainNans(fieldID) {
		return rowsMightNotMatch
	}

	if lowerBoundBytes := m.lowerBounds[fieldID]; lowerBoundBytes != nil {
		lowerBound, err := iceberg.LiteralFromBytes(field.Type, lowerBoundBytes)
		if err != nil {
			panic(err)
		}

		if m.isNan(lowerBound) {
			// NaN indicates unreliable bounds.
			return rowsMightNotMatch
		}

		if getCmpLiteral(lowerBound)(lowerBound, lit) > 0 {
			return rowsMustMatch
		}
	}

	return rowsMightNotMatch
}

func (m *strictMetricsEval) VisitGreaterEqual(t iceberg.BoundTerm, lit iceberg.Literal) bool {
	field := t.Ref().Field()
	fieldID := field.ID

	if m.canContainNulls(fieldID) || m.canContainNans(fieldID) {
		return rowsMightNotMatch
	}

	if lowerBoundBytes := m.lowerBounds[fieldID]; lowerBoundBytes != nil {
		lowerBound, err := iceberg.LiteralFromBytes(field.Type, lowerBoundBytes)
		if err != nil {
			panic(err)
		}

		if m.isNan(lowerBound) {
			// NaN indicates unreliable bounds.
			return rowsMightNotMatch
		}

		if getCmpLiteral(lowerBound)(lowerBound, lit) >= 0 {
			return rowsMustMatch
		}
	}

	return rowsMightNotMatch
}

func (m *strictMetricsEval) VisitEqual(t iceberg.BoundTerm, lit iceberg.Literal) bool {
	field := t.Ref().Field()
	fieldID := field.ID

	if m.canContainNulls(fieldID) || m.canContainNans(fieldID) {
		return rowsMightNotMatch
	}

	lowerBytes := m.lowerBounds[fieldID]
	upperBytes := m.upperBounds[fieldID]

	if lowerBytes != nil && upperBytes != nil {
		lowerBound, err := iceberg.LiteralFromBytes(field.Type, lowerBytes)
		if err != nil {
			panic(err)
		}
		upperBound, err := iceberg.LiteralFromBytes(field.Type, upperBytes)
		if err != nil {
			panic(err)
		}
		if getCmpLiteral(lowerBound)(lowerBound, lit) != 0 || getCmpLiteral(upperBound)(upperBound, lit) != 0 {
			return rowsMightNotMatch
		} else {
			return rowsMustMatch
		}
	}

	return rowsMightNotMatch
}

func (m *strictMetricsEval) VisitNotEqual(t iceberg.BoundTerm, lit iceberg.Literal) bool {
	field := t.Ref().Field()
	fieldID := field.ID

	if m.canContainNulls(fieldID) || m.canContainNans(fieldID) {
		return rowsMustMatch
	}

	var cmp func(iceberg.Literal, iceberg.Literal) int
	if lowerBoundBytes := m.lowerBounds[fieldID]; lowerBoundBytes != nil {
		lowerBound, err := iceberg.LiteralFromBytes(field.Type, lowerBoundBytes)
		if err != nil {
			panic(err)
		}

		if m.isNan(lowerBound) {
			return rowsMightNotMatch
		}

		cmp = getCmpLiteral(lowerBound)
		if cmp(lowerBound, lit) == 1 {
			return rowsMustMatch
		}
	}

	if upperBoundBytes := m.upperBounds[fieldID]; upperBoundBytes != nil {
		upperBound, err := iceberg.LiteralFromBytes(field.Type, upperBoundBytes)
		if err != nil {
			panic(err)
		}

		if m.isNan(upperBound) {
			return rowsMightNotMatch
		}

		cmp = getCmpLiteral(upperBound)
		if cmp(upperBound, lit) == -1 {
			return rowsMustMatch
		}
	}

	return rowsMightNotMatch
}

func (m *strictMetricsEval) VisitIn(t iceberg.BoundTerm, s iceberg.Set[iceberg.Literal]) bool {
	field := t.Ref().Field()
	fieldID := field.ID

	if m.canContainNulls(fieldID) || m.canContainNans(fieldID) {
		return rowsMightNotMatch
	}

	lowerBytes := m.lowerBounds[fieldID]
	upperBytes := m.upperBounds[fieldID]

	if lowerBytes != nil && upperBytes != nil {
		lowerBound, err := iceberg.LiteralFromBytes(field.Type, lowerBytes)
		if err != nil {
			panic(err)
		}
		if !s.Contains(lowerBound) {
			return rowsMightNotMatch
		}

		upperBound, err := iceberg.LiteralFromBytes(field.Type, upperBytes)
		if err != nil {
			panic(err)
		}
		if !s.Contains(upperBound) {
			return rowsMightNotMatch
		}

		if getCmpLiteral(lowerBound)(lowerBound, upperBound) != 0 {
			return rowsMightNotMatch
		}

		return rowsMustMatch
	}

	return rowsMightNotMatch
}

func (m *strictMetricsEval) VisitNotIn(t iceberg.BoundTerm, s iceberg.Set[iceberg.Literal]) bool {
	field := t.Ref().Field()
	fieldID := field.ID

	if m.canContainNulls(fieldID) || m.canContainNans(fieldID) {
		return rowsMustMatch
	}

	values := s.Members()
	if lowerBoundBytes := m.lowerBounds[fieldID]; lowerBoundBytes != nil {
		lowerBound, err := iceberg.LiteralFromBytes(field.Type, lowerBoundBytes)
		if err != nil {
			panic(err)
		}

		if m.isNan(lowerBound) {
			return rowsMightNotMatch
		}

		values = removeBoundCheck(lowerBound, values, 1)
		if len(values) == 0 {
			return rowsMustMatch
		}
	}

	if upperBoundBytes := m.upperBounds[fieldID]; upperBoundBytes != nil {
		upperBound, err := iceberg.LiteralFromBytes(field.Type, upperBoundBytes)
		if err != nil {
			panic(err)
		}

		values = removeBoundCheck(upperBound, values, -1)
		if len(values) == 0 {
			return rowsMustMatch
		}
	}

	return rowsMightNotMatch
}

func (m *strictMetricsEval) VisitStartsWith(iceberg.BoundTerm, iceberg.Literal) bool {
	return rowsMightNotMatch
}

func (m *strictMetricsEval) VisitNotStartsWith(iceberg.BoundTerm, iceberg.Literal) bool {
	return rowsMightNotMatch
}

func (m *strictMetricsEval) canContainNulls(fieldID int) bool {
	cnt, exists := m.nullCounts[fieldID]

	return exists && cnt > 0
}

func (m *strictMetricsEval) canContainNans(fieldID int) bool {
	cnt, exists := m.nanCounts[fieldID]

	return exists && cnt > 0
}
