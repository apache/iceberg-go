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
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/compute/exprs"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/iceberg-go"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/apache/iceberg-go/table/substrait"
	"github.com/substrait-io/substrait-go/v8/expr"
)

// compiledFileFilterPlan contains immutable work derived from one Iceberg
// filter and one physical file schema. Row-group stats evaluators are created
// from statsFilter for each file because inclusiveMetricsEval stores mutable
// per-row-group maps.
type compiledFileFilterPlan struct {
	statsFilter iceberg.BooleanExpression
	bloomPreds  []tblutils.RowGroupBloomPred

	recordFilter      expr.Expression
	extensionRegistry *expr.ExtensionRegistry
	dropFile          bool
}

func (p *compiledFileFilterPlan) recordProcessor(ctx context.Context) recProcessFn {
	if p == nil || p.recordFilter == nil {
		return nil
	}

	ctx = exprs.WithExtensionIDSet(ctx, exprs.NewExtensionSetDefault(*p.extensionRegistry))

	return filterRecords(ctx, p.recordFilter)
}

func (p *compiledFileFilterPlan) statsEvaluator() func(*metadata.RowGroupMetaData, []int) (bool, error) {
	return newParquetRowGroupStatsEvaluatorFromRewritten(p.statsFilter, false)
}

type compiledFileFilterPlans struct {
	record  *compiledFileFilterPlan
	pruning *compiledFileFilterPlan
}

type compiledFileFilterPlanCache struct {
	mu    sync.RWMutex
	plans map[string]*compiledFileFilterPlans
}

// cachedFileFilterPlans returns plans for the scan's fixed filter(s). The
// schema string is a structural key: the schema ID is intentionally omitted
// because files from different schema versions can share the same physical
// layout and use the same compiled plan. Pruning plans are only built when the
// caller is reading a format that supports Parquet row-group pruning.
func (as *arrowScan) cachedFileFilterPlans(fileSchema *iceberg.Schema, includePruning bool) (*compiledFileFilterPlans, error) {
	if fileSchema == nil {
		return nil, fmt.Errorf("%w: cannot compile a filter plan for a nil file schema", iceberg.ErrInvalidArgument)
	}

	key, err := physicalSchemaKey(fileSchema)
	if err != nil {
		return nil, err
	}

	as.filterPlanCache.mu.RLock()
	plans, ok := as.filterPlanCache.plans[key]
	if ok && (!includePruning || plans.pruning != nil) {
		as.filterPlanCache.mu.RUnlock()

		return plans, nil
	}
	as.filterPlanCache.mu.RUnlock()

	as.filterPlanCache.mu.Lock()
	defer as.filterPlanCache.mu.Unlock()

	plans, ok = as.filterPlanCache.plans[key]
	if as.filterPlanCache.plans == nil {
		as.filterPlanCache.plans = make(map[string]*compiledFileFilterPlans)
	}

	if !ok {
		sharePruning := includePruning && as.rowGroupFilter == nil
		record, err := compileFileFilterPlan(fileSchema, as.boundRowFilter, as.caseSensitive, true, sharePruning)
		if err != nil {
			return nil, err
		}
		plans = &compiledFileFilterPlans{record: record}
		if sharePruning {
			plans.pruning = record
		}
		as.filterPlanCache.plans[key] = plans
	}
	if !includePruning || plans.pruning != nil {
		return plans, nil
	}

	pruningFilter := as.rowGroupFilter
	if pruningFilter == nil {
		pruningFilter = as.boundRowFilter
	}

	logicalSchema := as.filterSchema
	if logicalSchema == nil {
		logicalSchema = as.projectedSchema
	}
	hasMissingDefault, err := pruningFilterHasMissingInitialDefault(
		pruningFilter, logicalSchema, fileSchema)
	if err != nil {
		return nil, err
	}
	if hasMissingDefault {
		// TranslateColumnNames treats fields missing from the physical file as
		// null. That is unsafe for an initial default, so keep pruning disabled
		// just as the uncached path does.
		pruningFilter = iceberg.AlwaysTrue{}
	}

	pruning, err := compileFileFilterPlan(fileSchema, pruningFilter, as.caseSensitive, false, true)
	if err != nil {
		return nil, err
	}

	plans.pruning = pruning

	return plans, nil
}

func physicalSchemaKey(fileSchema *iceberg.Schema) (string, error) {
	// The key includes field names, IDs, requiredness, and type details. Schema
	// IDs, docs, and defaults do not affect filter translation or compilation.
	visitor := &physicalSchemaKeyVisitor{}
	_, err := iceberg.Visit(fileSchema, visitor)
	if err != nil {
		return "", fmt.Errorf("%w: cannot encode physical schema key: %v", iceberg.ErrInvalidSchema, err)
	}

	return visitor.builder.String(), nil
}

type physicalSchemaKeyVisitor struct {
	builder strings.Builder
	depth   int
}

func (v *physicalSchemaKeyVisitor) Schema(_ *iceberg.Schema, _ struct{}) struct{} { return struct{}{} }
func (v *physicalSchemaKeyVisitor) Struct(_ iceberg.StructType, _ []struct{}) struct{} {
	return struct{}{}
}
func (v *physicalSchemaKeyVisitor) Field(_ iceberg.NestedField, _ struct{}) struct{} {
	return struct{}{}
}
func (v *physicalSchemaKeyVisitor) List(_ iceberg.ListType, _ struct{}) struct{}  { return struct{}{} }
func (v *physicalSchemaKeyVisitor) Map(_ iceberg.MapType, _, _ struct{}) struct{} { return struct{}{} }
func (v *physicalSchemaKeyVisitor) Primitive(_ iceberg.PrimitiveType) struct{}    { return struct{}{} }
func (v *physicalSchemaKeyVisitor) Variant(_ iceberg.VariantType) struct{}        { return struct{}{} }

func (v *physicalSchemaKeyVisitor) BeforeField(field iceberg.NestedField) {
	if v.depth == 0 {
		writePhysicalFieldKey(&v.builder, field)
	}
	v.depth++
}

func (v *physicalSchemaKeyVisitor) AfterField(_ iceberg.NestedField) { v.depth-- }

func (v *physicalSchemaKeyVisitor) BeforeListElement(_ iceberg.NestedField) { v.depth++ }
func (v *physicalSchemaKeyVisitor) AfterListElement(_ iceberg.NestedField)  { v.depth-- }
func (v *physicalSchemaKeyVisitor) BeforeMapKey(_ iceberg.NestedField)      { v.depth++ }
func (v *physicalSchemaKeyVisitor) AfterMapKey(_ iceberg.NestedField)       { v.depth-- }
func (v *physicalSchemaKeyVisitor) BeforeMapValue(_ iceberg.NestedField)    { v.depth++ }
func (v *physicalSchemaKeyVisitor) AfterMapValue(_ iceberg.NestedField)     { v.depth-- }

func writePhysicalFieldKey(builder *strings.Builder, field iceberg.NestedField) {
	builder.WriteByte('f')
	writePhysicalInt(builder, field.ID)
	writePhysicalString(builder, field.Name)
	if field.Required {
		builder.WriteByte('1')
	} else {
		builder.WriteByte('0')
	}
	writePhysicalTypeKey(builder, field.Type)
}

func writePhysicalTypeKey(builder *strings.Builder, typ iceberg.Type) {
	switch t := typ.(type) {
	case *iceberg.StructType:
		builder.WriteByte('s')
		builder.WriteByte('{')
		for _, field := range t.FieldList {
			writePhysicalFieldKey(builder, field)
		}
		builder.WriteByte('}')
	case *iceberg.ListType:
		builder.WriteByte('l')
		writePhysicalInt(builder, t.ElementID)
		if t.ElementRequired {
			builder.WriteByte('1')
		} else {
			builder.WriteByte('0')
		}
		writePhysicalTypeKey(builder, t.Element)
	case *iceberg.MapType:
		builder.WriteByte('m')
		writePhysicalInt(builder, t.KeyID)
		writePhysicalTypeKey(builder, t.KeyType)
		writePhysicalInt(builder, t.ValueID)
		if t.ValueRequired {
			builder.WriteByte('1')
		} else {
			builder.WriteByte('0')
		}
		writePhysicalTypeKey(builder, t.ValueType)
	case nil:
		builder.WriteString("n")
	default:
		builder.WriteByte('p')
		writePhysicalString(builder, typ.String())
	}
}

func writePhysicalInt(builder *strings.Builder, value int) {
	var buf [20]byte
	builder.Write(strconv.AppendInt(buf[:0], int64(value), 10))
	builder.WriteByte(':')
}

func writePhysicalString(builder *strings.Builder, value string) {
	writePhysicalInt(builder, len(value))
	builder.WriteString(value)
	builder.WriteByte(':')
}

func compileFileFilterPlan(
	fileSchema *iceberg.Schema,
	rowFilter iceberg.BooleanExpression,
	caseSensitive, includeRecordFilter, includePruning bool,
) (*compiledFileFilterPlan, error) {
	if rowFilter == nil {
		rowFilter = iceberg.AlwaysTrue{}
	}

	translatedFilter, err := iceberg.TranslateColumnNames(rowFilter, fileSchema)
	if err != nil {
		return nil, err
	}

	boundFilter := translatedFilter
	if !translatedFilter.Equals(iceberg.AlwaysFalse{}) {
		boundFilter, err = iceberg.BindExpr(fileSchema, translatedFilter, caseSensitive)
		if err != nil {
			return nil, err
		}
	}

	plan := &compiledFileFilterPlan{}
	if includePruning {
		statsFilter, err := iceberg.RewriteNotExpr(boundFilter)
		if err != nil {
			return nil, err
		}
		bloomPreds, err := newBloomFilterPredicatesFromRewritten(statsFilter)
		if err != nil {
			return nil, err
		}

		plan.statsFilter = statsFilter
		plan.bloomPreds = bloomPreds
	}
	if !includeRecordFilter {
		return plan, nil
	}
	if boundFilter.Equals(iceberg.AlwaysFalse{}) {
		plan.dropFile = true

		return plan, nil
	}
	if boundFilter.Equals(iceberg.AlwaysTrue{}) {
		return plan, nil
	}

	extSet, recordFilter, err := substrait.ConvertExpr(fileSchema, boundFilter, caseSensitive)
	if err != nil {
		return nil, err
	}
	plan.recordFilter = recordFilter
	plan.extensionRegistry = extSet

	return plan, nil
}
