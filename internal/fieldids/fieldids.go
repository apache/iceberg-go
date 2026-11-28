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

// Package fieldids checks the field IDs of a schema. It is shared by the
// table and view packages.
package fieldids

import (
	"fmt"

	"github.com/apache/iceberg-go"
)

// CheckUnique returns an error if two fields share an ID, including 0 and
// negative IDs, walking struct, list and map children.
func CheckUnique(fields []iceberg.NestedField) error {
	return checkUnique(make(map[int]string), fields)
}

func checkUnique(seen map[int]string, fields []iceberg.NestedField) error {
	for _, f := range fields {
		if prev, ok := seen[f.ID]; ok {
			return fmt.Errorf("%w: multiple fields for id %d: %s and %s",
				iceberg.ErrInvalidSchema, f.ID, prev, f.Name)
		}
		seen[f.ID] = f.Name

		var children []iceberg.NestedField
		switch t := f.Type.(type) {
		case *iceberg.StructType:
			if t != nil {
				children = t.Fields()
			}
		case *iceberg.ListType:
			if t != nil {
				children = []iceberg.NestedField{t.ElementField()}
			}
		case *iceberg.MapType:
			if t != nil {
				children = []iceberg.NestedField{t.KeyField(), t.ValueField()}
			}
		}

		if err := checkUnique(seen, children); err != nil {
			return err
		}
	}

	return nil
}
