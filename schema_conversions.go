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
	"strings"

	"github.com/apache/iceberg-go/internal"
	"github.com/twmb/avro"
)

func partitionTypeToAvroSchema(t *StructType) (*avro.Schema, error) {
	if len(t.FieldList) == 0 {
		return avro.Parse(`{"type":"record","name":"r102","fields":[]}`)
	}

	definedNames := make(map[string]bool)

	fieldJSONs := make([]string, 0, len(t.FieldList))
	for _, f := range t.FieldList {
		typeJSON, err := partitionFieldTypeJSON(f.Type, definedNames)
		if err != nil {
			return nil, err
		}
		fieldJSONs = append(fieldJSONs, fmt.Sprintf(`{"name":%q,"type":%s,"field-id":%d,"default":null}`,
			f.Name, typeJSON, f.ID))
	}

	schemaJSON := fmt.Sprintf(`{"type":"record","name":"r102","fields":[%s]}`,
		strings.Join(fieldJSONs, ","))

	return avro.Parse(schemaJSON)
}

// partitionFieldTypeJSON returns the nullable union JSON for the given partition field type.
func partitionFieldTypeJSON(typ Type, definedNames map[string]bool) (string, error) {
	switch t := typ.(type) {
	case Int32Type:
		return `["null","int"]`, nil
	case Int64Type:
		return `["null","long"]`, nil
	case Float32Type:
		return `["null","float"]`, nil
	case Float64Type:
		return `["null","double"]`, nil
	case StringType:
		return `["null","string"]`, nil
	case DateType:
		return `["null",{"type":"int","logicalType":"date"}]`, nil
	case TimeType:
		return `["null",{"type":"long","logicalType":"time-micros"}]`, nil
	case TimestampType:
		return `["null",{"type":"long","logicalType":"timestamp-micros","adjust-to-utc":false}]`, nil
	case TimestampTzType:
		return `["null",{"type":"long","logicalType":"timestamp-micros","adjust-to-utc":true}]`, nil
	case UUIDType:
		if !definedNames["uuid"] {
			definedNames["uuid"] = true
			return `["null",{"type":"fixed","name":"uuid","size":16,"logicalType":"uuid"}]`, nil
		}

		return `["null","uuid"]`, nil
	case BooleanType:
		return `["null","boolean"]`, nil
	case BinaryType:
		return `["null","bytes"]`, nil
	case FixedType:
		fixedName := fmt.Sprintf("fixed_%d", t.len)
		if !definedNames[fixedName] {
			definedNames[fixedName] = true
			return fmt.Sprintf(`["null",{"type":"fixed","name":"%s","size":%d}]`, fixedName, t.len), nil
		}

		return fmt.Sprintf(`["null","%s"]`, fixedName), nil
	case DecimalType:
		size := internal.DecimalRequiredBytes(t.precision)
		decName := fmt.Sprintf("fixed_%d_%d", t.precision, t.scale)
		if !definedNames[decName] {
			definedNames[decName] = true
			return fmt.Sprintf(`["null",{"type":"fixed","name":"%s","size":%d,"logicalType":"decimal","precision":%d,"scale":%d}]`,
				decName, size, t.precision, t.scale), nil
		}

		return fmt.Sprintf(`["null","%s"]`, decName), nil
	default:
		return "", fmt.Errorf("unsupported partition type: %s", typ.String())
	}
}
