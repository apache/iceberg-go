// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
)

// The ORC decoder reports null dates as ordinary epoch dates. Reject date
// columns at any nesting level until it can preserve their null bitmap.
func validateORCDates(schema *arrow.Schema) error {
	var check func(arrow.DataType) error
	check = func(t arrow.DataType) error {
		switch typ := t.(type) {
		case *arrow.Date32Type:

			return fmt.Errorf("%w: ORC date decoding does not preserve nulls", iceberg.ErrNotImplemented)
		case *arrow.StructType:
			for _, field := range typ.Fields() {
				if err := check(field.Type); err != nil {
					return err
				}
			}
		case *arrow.ListType:

			return check(typ.ElemField().Type)
		case *arrow.MapType:
			if err := check(typ.KeyField().Type); err != nil {
				return err
			}

			return check(typ.ItemField().Type)
		}

		return nil
	}
	for _, field := range schema.Fields() {
		if err := check(field.Type); err != nil {
			return fmt.Errorf("ORC field %q: %w", field.Name, err)
		}
	}

	return nil
}
