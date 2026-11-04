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
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
)

// SetLocation stages a change to the table's base location. The location is
// stored as provided. Future writes use it, but existing files are not moved.
// Call Commit to persist the change.
func (t *Transaction) SetLocation(location string) error {
	if strings.TrimSpace(location) == "" {
		return fmt.Errorf("%w: table location must not be empty", iceberg.ErrInvalidArgument)
	}

	return t.apply([]Update{NewSetLocationUpdate(location)}, nil)
}
