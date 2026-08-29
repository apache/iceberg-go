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
	"slices"

	"github.com/apache/iceberg-go"
)

// partitionResidualPlan partially evaluates a bound row filter using the
// values of identity partition fields for one partition spec. Predicates that
// cannot be proven from the partition remain in the residual.
type partitionResidualPlan struct {
	schema  *iceberg.Schema
	root    *partitionResidualNode
	sources map[int]partitionResidualSource
}

type partitionResidualSource struct {
	partitionFieldIDs []int
	path              []int
}

type partitionResidualNodeKind uint8

const (
	partitionResidualOpaque partitionResidualNodeKind = iota
	partitionResidualTrue
	partitionResidualFalse
	partitionResidualPredicateKind
	partitionResidualNot
	partitionResidualAnd
	partitionResidualOr
)

type partitionResidualNode struct {
	kind      partitionResidualNodeKind
	expr      iceberg.BooleanExpression
	predicate *partitionResidualPredicate
	left      *partitionResidualNode
	right     *partitionResidualNode
	child     *partitionResidualNode
}

type partitionResidualPredicate struct {
	sourceID int
	evaluate func(iceberg.StructLike) (bool, error)
}

type partitionResidualPlanBuilder struct {
	schema             *iceberg.Schema
	caseSensitive      bool
	identitySourceIDs  map[int][]int
	sources            map[int]partitionResidualSource
	identityPredicates int
}

func newPartitionResidualPlan(
	schema *iceberg.Schema,
	spec *iceberg.PartitionSpec,
	filter iceberg.BooleanExpression,
	caseSensitive bool,
) *partitionResidualPlan {
	if filter == nil || spec == nil {
		return nil
	}

	identitySourceIDs := make(map[int][]int)
	for _, field := range spec.Fields() {
		if !isIdentityPartitionField(field) {
			continue
		}

		sourceID := field.SourceID()
		if sourceID <= 0 {
			continue
		}

		identitySourceIDs[sourceID] = append(identitySourceIDs[sourceID], field.FieldID)
	}
	if len(identitySourceIDs) == 0 {
		return nil
	}

	builder := &partitionResidualPlanBuilder{
		schema:            schema,
		caseSensitive:     caseSensitive,
		identitySourceIDs: identitySourceIDs,
		sources:           make(map[int]partitionResidualSource),
	}
	root, err := iceberg.VisitExpr(filter, builder)
	if err != nil || builder.identityPredicates == 0 {
		return nil
	}

	return &partitionResidualPlan{
		schema:  schema,
		root:    root,
		sources: builder.sources,
	}
}

func isIdentityPartitionField(field iceberg.PartitionField) bool {
	if len(field.SourceIDs) != 1 {
		return false
	}

	switch transform := field.Transform.(type) {
	case iceberg.IdentityTransform:
		return true
	case *iceberg.IdentityTransform:
		return transform != nil
	default:
		return false
	}
}

func (b *partitionResidualPlanBuilder) VisitTrue() *partitionResidualNode {
	return &partitionResidualNode{kind: partitionResidualTrue, expr: iceberg.AlwaysTrue{}}
}

func (b *partitionResidualPlanBuilder) VisitFalse() *partitionResidualNode {
	return &partitionResidualNode{kind: partitionResidualFalse, expr: iceberg.AlwaysFalse{}}
}

func (b *partitionResidualPlanBuilder) VisitNot(child *partitionResidualNode) *partitionResidualNode {
	return &partitionResidualNode{
		kind:  partitionResidualNot,
		expr:  iceberg.NewNot(child.expr),
		child: child,
	}
}

func (b *partitionResidualPlanBuilder) VisitAnd(left, right *partitionResidualNode) *partitionResidualNode {
	return &partitionResidualNode{
		kind:  partitionResidualAnd,
		expr:  iceberg.NewAnd(left.expr, right.expr),
		left:  left,
		right: right,
	}
}

func (b *partitionResidualPlanBuilder) VisitOr(left, right *partitionResidualNode) *partitionResidualNode {
	return &partitionResidualNode{
		kind:  partitionResidualOr,
		expr:  iceberg.NewOr(left.expr, right.expr),
		left:  left,
		right: right,
	}
}

func (b *partitionResidualPlanBuilder) VisitUnbound(pred iceberg.UnboundPredicate) *partitionResidualNode {
	return &partitionResidualNode{kind: partitionResidualOpaque, expr: pred}
}

func (b *partitionResidualPlanBuilder) VisitBound(pred iceberg.BoundPredicate) *partitionResidualNode {
	ref, ok := pred.Term().(iceberg.BoundReference)
	if !ok {
		return &partitionResidualNode{kind: partitionResidualOpaque, expr: pred}
	}

	partitionFieldIDs, ok := b.identitySourceIDs[ref.Field().ID]
	if !ok || !partitionResidualPredicateSupported(pred) {
		return &partitionResidualNode{kind: partitionResidualOpaque, expr: pred}
	}

	unbound, err := iceberg.TranslateColumnNames(pred, b.schema)
	if err != nil {
		return &partitionResidualNode{kind: partitionResidualOpaque, expr: pred}
	}

	evaluate, err := iceberg.ExpressionEvaluator(b.schema, unbound, b.caseSensitive)
	if err != nil {
		return &partitionResidualNode{kind: partitionResidualOpaque, expr: pred}
	}

	path := ref.PosPath()
	if !partitionResidualPathSupported(b.schema, path) {
		return &partitionResidualNode{kind: partitionResidualOpaque, expr: pred}
	}

	source, exists := b.sources[ref.Field().ID]
	if !exists {
		source = partitionResidualSource{path: slices.Clone(path)}
	}
	if !slices.Equal(source.path, path) {
		return &partitionResidualNode{kind: partitionResidualOpaque, expr: pred}
	}
	for _, partitionFieldID := range partitionFieldIDs {
		if !slices.Contains(source.partitionFieldIDs, partitionFieldID) {
			source.partitionFieldIDs = append(source.partitionFieldIDs, partitionFieldID)
		}
	}
	b.sources[ref.Field().ID] = source
	b.identityPredicates++

	return &partitionResidualNode{
		kind: partitionResidualPredicateKind,
		expr: pred,
		predicate: &partitionResidualPredicate{
			sourceID: ref.Field().ID,
			evaluate: evaluate,
		},
	}
}

func partitionResidualPredicateSupported(pred iceberg.BoundPredicate) bool {
	switch pred.(type) {
	case iceberg.BoundUnaryPredicate, iceberg.BoundLiteralPredicate, iceberg.BoundSetPredicate:
		return true
	default:
		return false
	}
}

func partitionResidualPathSupported(schema *iceberg.Schema, path []int) bool {
	if len(path) == 0 {
		return false
	}

	fields := schema.Fields()
	for i, pos := range path {
		if pos < 0 || pos >= len(fields) {
			return false
		}
		if i == len(path)-1 {
			return true
		}

		nested, ok := fields[pos].Type.(*iceberg.StructType)
		if !ok || nested == nil {
			return false
		}
		fields = nested.FieldList
	}

	return false
}

// residual returns the task residual and whether at least one partition value
// was used. A false changed result means the caller should keep a nil task
// residual and use the scan filter as the conservative fallback.
func (p *partitionResidualPlan) residual(partition map[int]any) (iceberg.BooleanExpression, bool) {
	record := make(partitionSourceRecord, p.schema.NumFields())
	knownSources := make(map[int]struct{}, len(p.sources))
	for sourceID, source := range p.sources {
		value, ok := partitionValue(partition, source.partitionFieldIDs)
		if !ok || !setPartitionSourceValue(record, p.schema, source.path, value) {
			continue
		}

		knownSources[sourceID] = struct{}{}
	}

	residual, changed, _, _ := p.root.residual(knownSources, record)
	if !changed {
		return nil, false
	}

	return residual, true
}

func partitionValue(partition map[int]any, fieldIDs []int) (any, bool) {
	for _, fieldID := range fieldIDs {
		value, ok := partition[fieldID]
		if !ok {
			continue
		}
		if _, unknown := value.(iceberg.AboveMaxLiteral); unknown {
			return nil, false
		}
		if _, unknown := value.(iceberg.BelowMinLiteral); unknown {
			return nil, false
		}
		if literal, isLiteral := value.(iceberg.Literal); isLiteral {
			return literal.Any(), true
		}

		return value, true
	}

	return nil, false
}

type partitionSourceRecord []any

func (r partitionSourceRecord) Size() int              { return len(r) }
func (r partitionSourceRecord) Get(pos int) any        { return r[pos] }
func (r partitionSourceRecord) Set(pos int, value any) { r[pos] = value }

func setPartitionSourceValue(
	record partitionSourceRecord,
	schema *iceberg.Schema,
	path []int,
	value any,
) bool {
	if len(path) == 0 {
		return false
	}

	if len(path) == 1 {
		pos := path[0]
		if pos < 0 || pos >= schema.NumFields() {
			return false
		}
		record[pos] = value

		return true
	}

	fields := schema.Fields()
	current := record
	for i, pos := range path {
		if pos < 0 || pos >= len(fields) {
			return false
		}
		if i == len(path)-1 {
			current[pos] = value

			return true
		}

		nested, ok := fields[pos].Type.(*iceberg.StructType)
		if !ok || nested == nil {
			return false
		}

		child, ok := current[pos].(partitionSourceRecord)
		if !ok {
			child = make(partitionSourceRecord, len(nested.FieldList))
			current[pos] = child
		}
		current = child
		fields = nested.FieldList
	}

	return false
}

func (n *partitionResidualNode) residual(
	knownSources map[int]struct{},
	record iceberg.StructLike,
) (iceberg.BooleanExpression, bool, bool, bool) {
	switch n.kind {
	case partitionResidualOpaque:
		return n.expr, false, false, false
	case partitionResidualTrue:
		return iceberg.AlwaysTrue{}, false, true, true
	case partitionResidualFalse:
		return iceberg.AlwaysFalse{}, false, true, false
	case partitionResidualPredicateKind:
		if _, ok := knownSources[n.predicate.sourceID]; !ok {
			return n.expr, false, false, false
		}

		value, err := n.predicate.evaluate(record)
		if err != nil {
			return n.expr, false, false, false
		}
		if value {
			return iceberg.AlwaysTrue{}, true, true, true
		}

		return iceberg.AlwaysFalse{}, true, true, false
	case partitionResidualNot:
		child, changed, exact, value := n.child.residual(knownSources, record)
		if !exact {
			return iceberg.NewNot(child), changed, false, false
		}

		return boolExpressionForValue(!value), changed, true, !value
	case partitionResidualAnd:
		left, leftChanged, leftExact, leftValue := n.left.residual(knownSources, record)
		if leftExact && !leftValue {
			return iceberg.AlwaysFalse{}, leftChanged, true, false
		}

		right, rightChanged, rightExact, rightValue := n.right.residual(knownSources, record)
		changed := leftChanged || rightChanged
		switch {
		case leftExact && leftValue:
			return right, changed, rightExact, rightValue
		case rightExact && !rightValue:
			return iceberg.AlwaysFalse{}, changed, true, false
		case rightExact && rightValue:
			return left, changed, leftExact, leftValue
		case leftExact && rightExact:
			return iceberg.AlwaysTrue{}, changed, true, true
		default:
			return iceberg.NewAnd(left, right), changed, false, false
		}
	case partitionResidualOr:
		left, leftChanged, leftExact, leftValue := n.left.residual(knownSources, record)
		if leftExact && leftValue {
			return iceberg.AlwaysTrue{}, leftChanged, true, true
		}

		right, rightChanged, rightExact, rightValue := n.right.residual(knownSources, record)
		changed := leftChanged || rightChanged
		switch {
		case leftExact && !leftValue:
			return right, changed, rightExact, rightValue
		case rightExact && rightValue:
			return iceberg.AlwaysTrue{}, changed, true, true
		case rightExact && !rightValue:
			return left, changed, leftExact, leftValue
		case leftExact && rightExact:
			return iceberg.AlwaysFalse{}, changed, true, false
		default:
			return iceberg.NewOr(left, right), changed, false, false
		}
	}

	return n.expr, false, false, false
}

func boolExpressionForValue(value bool) iceberg.BooleanExpression {
	if value {
		return iceberg.AlwaysTrue{}
	}

	return iceberg.AlwaysFalse{}
}
