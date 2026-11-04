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

package internal

import (
	"bytes"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

// newORCBounds reuses Iceberg's existing type-aware bounds and truncation
// handling. No bounds are recorded for fields excluded by the metrics plan.
func newORCBounds(field iceberg.NestedField, plans map[int]StatisticsCollector) StatsAgg {
	typ, ok := field.Type.(iceberg.PrimitiveType)
	if !ok {
		return nil
	}

	truncate := 0
	if plans != nil {
		plan, ok := plans[field.ID]
		if !ok || (plan.Mode.Typ != MetricModeFull && plan.Mode.Typ != MetricModeTruncate) {
			return nil
		}
		if plan.Mode.Typ == MetricModeTruncate {
			switch typ.(type) {
			case iceberg.StringType, iceberg.BinaryType:
				truncate = plan.Mode.Len
			}
		}
	}

	switch typ.(type) {
	case iceberg.BooleanType:
		return newStatAgg[bool](typ, 0)
	case iceberg.Int32Type, iceberg.DateType:
		return newStatAgg[int32](typ, 0)
	case iceberg.Int64Type, iceberg.TimeType, iceberg.TimestampType,
		iceberg.TimestampTzType, iceberg.TimestampNsType, iceberg.TimestampTzNsType:
		return newStatAgg[int64](typ, 0)
	case iceberg.Float32Type:
		return newStatAgg[float32](typ, 0)
	case iceberg.Float64Type:
		return newStatAgg[float64](typ, 0)
	case iceberg.StringType:
		return newStatAgg[string](typ, truncate)
	case iceberg.BinaryType, iceberg.FixedType:
		return newStatAgg[[]byte](typ, truncate)
	case iceberg.UUIDType:
		return newStatAgg[uuid.UUID](typ, 0)
	case iceberg.DecimalType:
		return newStatAgg[iceberg.Decimal](typ, 0)
	default:
		return nil
	}
}

func addORCBound[T iceberg.LiteralType](agg StatsAgg, val T) {
	bounds := agg.(*statsAggregator[T])
	bounds.updateMin(val)
	bounds.updateMax(val)
}

// trackBounds runs only after a primitive value has passed the ORC writer's
// validation. Skip NaNs: Iceberg bounds cover non-null, non-NaN values.
func (c *orcColumn) trackBounds(values arrow.Array, row int) {
	if c.bounds == nil {
		return
	}

	switch a := values.(type) {
	case *array.Boolean:
		addORCBound(c.bounds, a.Value(row))
	case *array.Int32:
		addORCBound(c.bounds, a.Value(row))
	case *array.Int64:
		addORCBound(c.bounds, a.Value(row))
	case *array.Date32:
		addORCBound(c.bounds, int32(a.Value(row)))
	case *array.Time64:
		addORCBound(c.bounds, int64(a.Value(row)))
	case *array.Timestamp:
		addORCBound(c.bounds, int64(a.Value(row)))
	case *array.Float32:
		if v := a.Value(row); !math.IsNaN(float64(v)) {
			addORCBound(c.bounds, v)
		}
	case *array.Float64:
		if v := a.Value(row); !math.IsNaN(v) {
			addORCBound(c.bounds, v)
		}
	case *array.String:
		addORCBound(c.bounds, a.Value(row))
	case *array.LargeString:
		addORCBound(c.bounds, a.Value(row))
	case *array.Binary:
		addORCBound(c.bounds, bytes.Clone(a.Value(row)))
	case *array.LargeBinary:
		addORCBound(c.bounds, bytes.Clone(a.Value(row)))
	case *array.FixedSizeBinary:
		addORCBound(c.bounds, bytes.Clone(a.Value(row)))
	case *extensions.UUIDArray:
		addORCBound(c.bounds, uuid.UUID(a.Value(row)))
	case *array.Decimal128:
		decimal := c.field.Type.(iceberg.DecimalType)
		addORCBound(c.bounds, iceberg.Decimal{Val: a.Value(row), Scale: decimal.Scale()})
	}
}
