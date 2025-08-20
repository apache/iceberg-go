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

package glue

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
)

func schemasToGlueColumns(metadata table.Metadata) []types.Column {
	results := make(map[string]types.Column)

	for _, field := range schemaToGlueColumns(metadata.CurrentSchema(), true) {
		results[aws.ToString(field.Name)] = field
	}

	for _, schema := range metadata.Schemas() {
		if schema.ID == metadata.CurrentSchema().ID {
			continue
		}

		for _, field := range schemaToGlueColumns(schema, false) {
			if _, ok := results[aws.ToString(field.Name)]; !ok {
				results[aws.ToString(field.Name)] = field
			}
		}
	}

	return slices.Collect(maps.Values(results))
}

// schemaToGlueColumns converts an Iceberg schema to a list of Glue columns.
func schemaToGlueColumns(schema *iceberg.Schema, isCurrent bool) []types.Column {
	var columns []types.Column
	for _, field := range schema.Fields() {
		columns = append(columns, fieldToGlueColumn(field, isCurrent))
	}

	return columns
}

// fieldToGlueColumn converts an Iceberg nested field to a Glue column.
func fieldToGlueColumn(field iceberg.NestedField, isCurrent bool) types.Column {
	column := types.Column{
		Name:    aws.String(field.Name),
		Comment: aws.String(field.Doc),
		Type:    aws.String(icebergTypeToGlueType(field.Type)),
		Parameters: map[string]string{
			icebergFieldIDKey:       strconv.Itoa(field.ID),
			icebergFieldOptionalKey: strconv.FormatBool(!field.Required),
			icebergFieldCurrentKey:  strconv.FormatBool(isCurrent),
		},
	}

	return column
}

// icebergTypeToGlueType converts an Iceberg type to a Glue type string representation.
// It handles primitive types as well as nested types like structs, lists, and maps.
// Reference: https://docs.aws.amazon.com/glue/latest/dg/glue-types.html#glue-types-cataloghttps://cwiki.apache.org/confluence/display/hive/languagemanual+types%23LanguageManualTypes-Date/TimeTypes
// Apache Hive type: https://cwiki.apache.org/confluence/display/hive/languagemanual+types
func icebergTypeToGlueType(typ iceberg.Type) string {
	switch t := typ.(type) {
	case iceberg.BooleanType:
		return "boolean"
	case iceberg.Int32Type:
		return "int"
	case iceberg.Int64Type:
		return "bigint"
	case iceberg.Float32Type:
		return "float"
	case iceberg.Float64Type:
		return "double"
	case iceberg.DateType:
		return "date"
	case iceberg.TimeType:
		return "string"
	case iceberg.TimestampType:
		return "timestamp"
	case iceberg.TimestampTzType:
		return "timestamp"
	case iceberg.StringType:
		return "string"
	case iceberg.UUIDType:
		return "string" // Represent UUID as string
	case iceberg.BinaryType:
		return "binary"
	case iceberg.DecimalType:
		return fmt.Sprintf("decimal(%d,%d)", t.Precision(), t.Scale())
	case iceberg.FixedType:
		return fmt.Sprintf("binary(%d)", t.Len())
	case *iceberg.StructType:
		// For struct types, create a struct<field1:type1,field2:type2,...> representation
		var fieldStrings []string
		for _, field := range t.Fields() {
			fieldStrings = append(fieldStrings,
				fmt.Sprintf("%s:%s", field.Name, icebergTypeToGlueType(field.Type)))
		}

		return fmt.Sprintf("struct<%s>", strings.Join(fieldStrings, ","))
	case *iceberg.ListType:
		// For list types, create an array<type> representation
		elementField := t.ElementField()

		return fmt.Sprintf("array<%s>", icebergTypeToGlueType(elementField.Type))
	case *iceberg.MapType:
		// For map types, create a map<keyType,valueType> representation
		keyField := t.KeyField()
		valueField := t.ValueField()

		return fmt.Sprintf("map<%s,%s>",
			icebergTypeToGlueType(keyField.Type),
			icebergTypeToGlueType(valueField.Type))
	default:
		return "string"
	}
}
