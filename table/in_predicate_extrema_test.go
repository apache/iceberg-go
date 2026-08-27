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
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManifestEvaluatorInPredicateExtrema(t *testing.T) {
	decimal := func(value int64) iceberg.Decimal {
		return iceberg.Decimal{Val: decimal128.FromI64(value), Scale: 2}
	}

	tests := []struct {
		name       string
		typ        iceberg.Type
		expr       iceberg.BooleanExpression
		lower      iceberg.Literal
		upper      iceberg.Literal
		expectRead bool
	}{
		{
			name:       "decimal below lower bound",
			typ:        iceberg.DecimalTypeOf(12, 2),
			expr:       iceberg.IsIn(iceberg.Reference("value"), decimal(100), decimal(200), decimal(300)),
			lower:      iceberg.NewLiteral(decimal(400)),
			upper:      iceberg.NewLiteral(decimal(500)),
			expectRead: false,
		},
		{
			name:       "decimal above upper bound",
			typ:        iceberg.DecimalTypeOf(12, 2),
			expr:       iceberg.IsIn(iceberg.Reference("value"), decimal(100), decimal(200), decimal(300)),
			lower:      iceberg.NewLiteral(decimal(-100)),
			upper:      iceberg.NewLiteral(decimal(0)),
			expectRead: false,
		},
		{
			name:       "decimal overlaps bound",
			typ:        iceberg.DecimalTypeOf(12, 2),
			expr:       iceberg.IsIn(iceberg.Reference("value"), decimal(100), decimal(200), decimal(300)),
			lower:      iceberg.NewLiteral(decimal(200)),
			upper:      iceberg.NewLiteral(decimal(250)),
			expectRead: true,
		},
		{
			name:       "timestamp nanos below lower bound",
			typ:        iceberg.PrimitiveTypes.TimestampNs,
			expr:       iceberg.IsIn(iceberg.Reference("value"), iceberg.TimestampNano(100), iceberg.TimestampNano(200), iceberg.TimestampNano(300)),
			lower:      iceberg.NewLiteral(iceberg.TimestampNano(400)),
			upper:      iceberg.NewLiteral(iceberg.TimestampNano(500)),
			expectRead: false,
		},
		{
			name:       "timestamp nanos above upper bound",
			typ:        iceberg.PrimitiveTypes.TimestampNs,
			expr:       iceberg.IsIn(iceberg.Reference("value"), iceberg.TimestampNano(100), iceberg.TimestampNano(200), iceberg.TimestampNano(300)),
			lower:      iceberg.NewLiteral(iceberg.TimestampNano(-100)),
			upper:      iceberg.NewLiteral(iceberg.TimestampNano(99)),
			expectRead: false,
		},
		{
			name:       "timestamp nanos overlaps bound",
			typ:        iceberg.PrimitiveTypes.TimestampNs,
			expr:       iceberg.IsIn(iceberg.Reference("value"), iceberg.TimestampNano(100), iceberg.TimestampNano(200), iceberg.TimestampNano(300)),
			lower:      iceberg.NewLiteral(iceberg.TimestampNano(200)),
			upper:      iceberg.NewLiteral(iceberg.TimestampNano(250)),
			expectRead: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "value", Type: tt.typ})
			spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
				SourceIDs: []int{1}, FieldID: 1000, Name: "value", Transform: iceberg.IdentityTransform{},
			})
			eval, err := newManifestEvaluator(spec, schema, tt.expr, true)
			require.NoError(t, err)

			lower, err := tt.lower.MarshalBinary()
			require.NoError(t, err)
			upper, err := tt.upper.MarshalBinary()
			require.NoError(t, err)
			manifest := iceberg.NewManifestFile(2, "manifest.avro", 1, 0, 1).Partitions(
				[]iceberg.FieldSummary{{LowerBound: &lower, UpperBound: &upper}},
			).Build()

			result, err := eval(manifest)
			require.NoError(t, err)
			assert.Equal(t, tt.expectRead, result)
		})
	}
}
