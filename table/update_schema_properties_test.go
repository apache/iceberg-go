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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestMetricsAndBloomFilterPropertiesFollowSchemaChanges(t *testing.T) {
	const metrics = MetricsModeColumnConfPrefix + "."
	const bloom = ParquetBloomFilterColumnEnabledKeyPrefix + "."

	for _, tc := range []struct {
		name    string
		props   iceberg.Properties
		change  func(*UpdateSchema) *UpdateSchema
		present iceberg.Properties
		absent  []string
	}{
		{
			name: "rename leaf",
			props: iceberg.Properties{
				metrics + "name":  "full",
				bloom + "name":    "true",
				metrics + "age":   "counts",
				bloom + "missing": "true",
			},
			change: func(u *UpdateSchema) *UpdateSchema {
				return u.RenameColumn([]string{"name"}, "full_name")
			},
			present: iceberg.Properties{
				metrics + "full_name": "full",
				bloom + "full_name":   "true",
				metrics + "age":       "counts",
				bloom + "missing":     "true",
			},
			absent: []string{metrics + "name", bloom + "name"},
		},
		{
			name: "rename nested parent",
			props: iceberg.Properties{
				metrics + "address.city": "full",
				bloom + "address.zip":    "true",
			},
			change: func(u *UpdateSchema) *UpdateSchema {
				return u.RenameColumn([]string{"address"}, "location")
			},
			present: iceberg.Properties{
				metrics + "location.city": "full",
				bloom + "location.zip":    "true",
			},
			absent: []string{metrics + "address.city", bloom + "address.zip"},
		},
		{
			name: "delete nested parent",
			props: iceberg.Properties{
				metrics + "address.city": "full",
				bloom + "address.zip":    "true",
				metrics + "name":         "counts",
				bloom + "name":           "false",
			},
			change: func(u *UpdateSchema) *UpdateSchema {
				return u.DeleteColumn([]string{"address"})
			},
			present: iceberg.Properties{
				metrics + "name": "counts",
				bloom + "name":   "false",
			},
			absent: []string{metrics + "address.city", bloom + "address.zip"},
		},
		{
			name: "delete and readd",
			props: iceberg.Properties{
				metrics + "name": "full",
				bloom + "name":   "true",
			},
			change: func(u *UpdateSchema) *UpdateSchema {
				return u.DeleteColumn([]string{"name"}).
					AddColumn([]string{"name"}, iceberg.PrimitiveTypes.String, "", false, nil)
			},
			absent: []string{metrics + "name", bloom + "name"},
		},
		{
			name: "rename into deleted name",
			props: iceberg.Properties{
				metrics + "name": "full",
				bloom + "name":   "false",
				metrics + "age":  "counts",
				bloom + "age":    "true",
			},
			change: func(u *UpdateSchema) *UpdateSchema {
				return u.DeleteColumn([]string{"age"}).RenameColumn([]string{"name"}, "age")
			},
			present: iceberg.Properties{
				metrics + "age": "full",
				bloom + "age":   "false",
			},
			absent: []string{metrics + "name", bloom + "name"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			meta, err := NewMetadata(originalSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "", tc.props)
			require.NoError(t, err)
			txn := New([]string{"table"}, meta, "", nil, nil).NewTransaction()
			require.NoError(t, tc.change(txn.UpdateSchema(true, true)).Commit())
			staged, err := txn.txnMeta()
			require.NoError(t, err)
			for key, value := range tc.present {
				require.Equal(t, value, staged.props[key], key)
			}
			for _, key := range tc.absent {
				require.NotContains(t, staged.props, key)
			}
		})
	}
}
