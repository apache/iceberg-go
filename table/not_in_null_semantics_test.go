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

package table_test

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

// A scan must use SQL three-valued logic for NotIn: a NULL term is neither in
// nor out of the set, so the row does not match, whatever the number of values.
func TestScanNotInDoesNotMatchNullRows(t *testing.T) {
	age := iceberg.Reference("age")

	for _, version := range []string{"2", "3"} {
		t.Run("v"+version, func(t *testing.T) {
			tbl := newCopyOnWriteNullRowsTable(t, version)

			// rows: id=1 {25}, id=2 {30}, id=3 {NULL}, id=4 {40}
			require.Equal(t, []int64{1},
				scanIDsMatching(t, tbl, iceberg.NotIn(age, int64(30), int64(40))))
			require.Equal(t, []int64{1, 4},
				scanIDsMatching(t, tbl, iceberg.NotIn(age, int64(30), int64(30))))
		})
	}
}
