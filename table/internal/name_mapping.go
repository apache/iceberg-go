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
)

// ValidateNameMappingProperty returns an error wrapping errInvalid when props
// sets key but mapping, the name mapping parsed from that property, is nil
// because the value could not be parsed.
func ValidateNameMappingProperty(props iceberg.Properties, key string, mapping iceberg.NameMapping, errInvalid error) error {
	if _, exists := props[key]; exists && mapping == nil {
		return fmt.Errorf("%w: invalid %s", errInvalid, key)
	}

	return nil
}
