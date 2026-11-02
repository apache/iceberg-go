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
	"fmt"

	"github.com/apache/iceberg-go"
	avro "github.com/twmb/avro"
)

// validateAvroReadSchema prevents the Arrow reader from silently losing data
// for nullable records and nested containers that it cannot decode correctly.
func validateAvroReadSchema(schemaJSON string) error {
	schema, err := avro.Parse(schemaJSON)
	if err != nil {
		return fmt.Errorf("%w: parse Avro writer schema: %v", iceberg.ErrInvalidSchema, err)
	}

	return validateAvroReadNode(*schema.Root().ExpandReferences(), false, false)
}

// Prevent untrusted Avro schemas from exhausting the call stack during
// validation, Arrow schema decoration, or record batch retagging.
const maxAvroReadSchemaDepth = 256

func validateAvroReadNode(node avro.SchemaNode, nullable, inMap bool) error {
	return validateAvroReadNodeDepth(node, nullable, inMap, 1)
}

func validateAvroReadNodeDepth(node avro.SchemaNode, nullable, inMap bool, depth int) error {
	if depth > maxAvroReadSchemaDepth {
		return fmt.Errorf("%w: Avro schema exceeds maximum nesting depth %d", iceberg.ErrInvalidSchema, maxAvroReadSchemaDepth)
	}
	if node.Type == "union" {
		if len(node.Branches) != 2 {
			return fmt.Errorf("%w: Avro unions must contain null and one value type", iceberg.ErrNotImplemented)
		}
		var nonNull *avro.SchemaNode
		hasNull := false
		for i := range node.Branches {
			branch := &node.Branches[i]
			if branch.Type == "null" {
				hasNull = true
			} else if nonNull == nil {
				nonNull = branch
			} else {
				return fmt.Errorf("%w: Avro unions must contain null and one value type", iceberg.ErrNotImplemented)
			}
		}
		if !hasNull || nonNull == nil {
			return fmt.Errorf("%w: Avro unions must contain null and one value type", iceberg.ErrNotImplemented)
		}

		return validateAvroReadNodeDepth(*nonNull, true, inMap, depth+1)
	}
	if nullable && node.Type == "record" {
		return fmt.Errorf("%w: Avro nullable records are not supported", iceberg.ErrNotImplemented)
	}
	if node.Type == "map" && inMap {
		return fmt.Errorf("%w: nested Avro maps are not supported", iceberg.ErrNotImplemented)
	}
	if (node.Type == "array" || node.Type == "map") && avroHasComplexChild(node) {
		return fmt.Errorf("%w: Avro containers with nested complex values are not supported", iceberg.ErrNotImplemented)
	}
	switch node.Type {
	case "record":
		for _, field := range node.Fields {
			if err := validateAvroReadNodeDepth(field.Type, false, inMap, depth+1); err != nil {
				return err
			}
		}
	case "array":
		if node.Items != nil {
			return validateAvroReadNodeDepth(*node.Items, false, inMap, depth+1)
		}
	case "map":
		if node.Values != nil {
			return validateAvroReadNodeDepth(*node.Values, false, true, depth+1)
		}
	}

	return nil
}

func avroHasComplexChild(node avro.SchemaNode) bool {
	var child *avro.SchemaNode
	switch node.Type {
	case "array":
		child = node.Items
	case "map":
		child = node.Values
	}
	if child == nil {
		return false
	}
	if child.Type == "union" {
		for _, branch := range child.Branches {
			if branch.Type == "record" || branch.Type == "map" || branch.Type == "array" {
				return true
			}
		}

		return false
	}

	return child.Type == "record" || child.Type == "map" || child.Type == "array"
}
