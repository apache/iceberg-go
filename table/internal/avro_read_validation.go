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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package internal

import (
	"fmt"

	"github.com/apache/iceberg-go"
	avro "github.com/twmb/avro"
)

// validateAvroReadSchema checks unsafe unions and limits schema depth. Nested
// records, lists and maps are supported by the twmb/avro Arrow conversion.
func validateAvroReadSchema(schemaJSON string) error {
	schema, err := avro.Parse(schemaJSON)
	if err != nil {
		return fmt.Errorf("%w: parse Avro writer schema: %v", iceberg.ErrInvalidSchema, err)
	}

	return validateAvroReadNode(*schema.Root().ExpandReferences(), false, false)
}

const maxAvroReadSchemaDepth = 256

func validateAvroReadNode(node avro.SchemaNode, _, _ bool) error {
	return validateAvroReadNodeDepth(node, 1)
}

func validateAvroReadNodeDepth(node avro.SchemaNode, depth int) error {
	if depth > maxAvroReadSchemaDepth {
		return fmt.Errorf("%w: Avro schema exceeds maximum nesting depth %d", iceberg.ErrInvalidSchema, maxAvroReadSchemaDepth)
	}
	if node.Type == "union" {
		if len(node.Branches) != 2 {
			return fmt.Errorf("%w: Avro unions must contain null and one value type", iceberg.ErrNotImplemented)
		}
		switch {
		case node.Branches[0].Type == "null" && node.Branches[1].Type != "null":
			return validateAvroReadNodeDepth(node.Branches[1], depth+1)
		case node.Branches[1].Type == "null" && node.Branches[0].Type != "null":
			return validateAvroReadNodeDepth(node.Branches[0], depth+1)
		default:
			return fmt.Errorf("%w: invalid Avro nullable union", iceberg.ErrNotImplemented)
		}
	}
	switch node.Type {
	case "record":
		for _, f := range node.Fields {
			if err := validateAvroReadNodeDepth(f.Type, depth+1); err != nil {
				return err
			}
		}
	case "array":
		if node.Items != nil {
			return validateAvroReadNodeDepth(*node.Items, depth+1)
		}
	case "map":
		if node.Values != nil {
			return validateAvroReadNodeDepth(*node.Values, depth+1)
		}
	}

	return nil
}
