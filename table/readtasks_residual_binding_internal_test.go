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
	"context"
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestReadTasksMixedResidualBindingPreservesInput(t *testing.T) {
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64})
	metadata, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "mem://mixed-residuals", nil)
	require.NoError(t, err)
	scan := New(Identifier{"db", "tbl"}, metadata, "metadata.json", func(context.Context) (iceio.IO, error) { return iceio.NewMemFS(), nil }, nil).Scan()
	unbound := iceberg.GreaterThan(iceberg.Reference("id"), int64(1))
	bound, err := iceberg.BindExpr(schema, unbound, true)
	require.NoError(t, err)
	for _, invalid := range []bool{false, true} {
		t.Run(map[bool]string{false: "reusable mixed plan", true: "invalid bound residual after binding"}[invalid], func(t *testing.T) {
			tasks := []FileScanTask{{Residual: bound}, {}, {Residual: unbound}, {Residual: bound}}
			if invalid {
				wrongSchema := iceberg.NewSchema(0, iceberg.NestedField{ID: 2, Name: "other", Type: iceberg.PrimitiveTypes.Int64})
				wrongBound, err := iceberg.BindExpr(wrongSchema, iceberg.EqualTo(iceberg.Reference("other"), int64(1)), true)
				require.NoError(t, err)
				tasks[len(tasks)-1].Residual = wrongBound
			}
			before := slices.Clone(tasks)
			for range 2 {
				_, _, err := scan.ReadTasks(t.Context(), tasks)
				if invalid {
					require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
					require.ErrorContains(t, err, "field ID 2")
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, before, tasks)
			}
		})
	}
}
