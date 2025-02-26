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

	"github.com/apache/iceberg-go/internal"
	"github.com/hamba/avro/v2"
)

func partitionTypeToAvroSchema(t *StructType) (avro.Schema, error) {
	fields := make([]*avro.Field, len(t.FieldList))
	for i, f := range t.FieldList {
		var sc avro.Schema
		switch typ := f.Type.(type) {
		case Int32Type:
			sc = internal.IntSchema
		case Int64Type:
			sc = internal.LongSchema
		case Float32Type:
			sc = internal.FloatSchema
		case Float64Type:
			sc = internal.DoubleSchema
		case StringType:
			sc = internal.StringSchema
		case DateType:
			sc = internal.DateSchema
		case TimeType:
			sc = internal.TimeSchema
		case TimestampType:
			sc = internal.TimestampSchema
		case TimestampTzType:
			sc = internal.TimestampTzSchema
		case UUIDType:
			sc = internal.UUIDSchema
		case BooleanType:
			sc = internal.BoolSchema
		case BinaryType:
			sc = internal.BinarySchema
		case FixedType:
			sc = internal.Must(avro.NewFixedSchema("fixed", "", typ.len, nil))
		case DecimalType:
			sc = internal.DecimalSchema(typ.precision, typ.scale)
		default:
			return nil, fmt.Errorf("unsupported partition type: %s", f.Type.String())
		}

		fields[i], _ = avro.NewField(f.Name, sc, internal.WithFieldID(f.ID))
	}

	return avro.NewRecordSchema("r102", "", fields)
}
