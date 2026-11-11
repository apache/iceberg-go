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

func TestParquetCompressionPropertiesFollowSchemaChanges(t *testing.T) {
	const codec = ParquetCompressionColumnCodecKeyPrefix + "."
	const level = ParquetCompressionColumnLevelKeyPrefix + "."

	for _, tc := range []struct {
		name    string
		props   iceberg.Properties
		change  func(*UpdateSchema) *UpdateSchema
		present iceberg.Properties
		absent  []string
	}{
		{
			name:    "rename leaf",
			props:   iceberg.Properties{codec + "name": "gzip", level + "name": "3", codec + "missing": "snappy"},
			change:  func(u *UpdateSchema) *UpdateSchema { return u.RenameColumn([]string{"name"}, "full_name") },
			present: iceberg.Properties{codec + "full_name": "gzip", level + "full_name": "3", codec + "missing": "snappy"},
			absent:  []string{codec + "name", level + "name"},
		},
		{
			name:    "rename nested parent",
			props:   iceberg.Properties{codec + "address.city": "gzip", level + "address.zip": "5"},
			change:  func(u *UpdateSchema) *UpdateSchema { return u.RenameColumn([]string{"address"}, "location") },
			present: iceberg.Properties{codec + "location.city": "gzip", level + "location.zip": "5"},
			absent:  []string{codec + "address.city", level + "address.zip"},
		},
		{
			name:    "delete nested parent",
			props:   iceberg.Properties{codec + "address.city": "gzip", level + "address.zip": "5", codec + "age": "snappy"},
			change:  func(u *UpdateSchema) *UpdateSchema { return u.DeleteColumn([]string{"address"}) },
			present: iceberg.Properties{codec + "age": "snappy"},
			absent:  []string{codec + "address.city", level + "address.zip"},
		},
		{
			name:  "delete and readd",
			props: iceberg.Properties{codec + "name": "gzip", level + "name": "4"},
			change: func(u *UpdateSchema) *UpdateSchema {
				return u.DeleteColumn([]string{"name"}).AddColumn([]string{"name"}, iceberg.PrimitiveTypes.String, "", false, nil)
			},
			absent: []string{codec + "name", level + "name"},
		},
		{
			name:  "rename into deleted name",
			props: iceberg.Properties{codec + "name": "gzip", codec + "age": "snappy", level + "age": "9"},
			change: func(u *UpdateSchema) *UpdateSchema {
				return u.DeleteColumn([]string{"age"}).RenameColumn([]string{"name"}, "age")
			},
			present: iceberg.Properties{codec + "age": "gzip"},
			absent:  []string{codec + "name", level + "age"},
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

func TestParquetCompressionSchemaChangeUsesStagedProperties(t *testing.T) {
	const codec = ParquetCompressionColumnCodecKeyPrefix + "."
	meta, err := NewMetadata(originalSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "", nil)
	require.NoError(t, err)
	txn := New([]string{"table"}, meta, "", nil, nil).NewTransaction()
	require.NoError(t, txn.SetProperties(iceberg.Properties{codec + "name": "gzip"}))
	require.NoError(t, txn.UpdateSchema(true, false).RenameColumn([]string{"name"}, "full_name").Commit())
	require.NoError(t, txn.SetProperties(iceberg.Properties{codec + "full_name": "snappy"}))
	staged, err := txn.txnMeta()
	require.NoError(t, err)
	require.NotContains(t, staged.props, codec+"name")
	require.Equal(t, "snappy", staged.props[codec+"full_name"])
}

func TestParquetCompressionSchemaRenameKeepsCanonicalListPath(t *testing.T) {
	const codec = ParquetCompressionColumnCodecKeyPrefix + "."
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "items", Type: iceberg.NewListType(2,
			iceberg.NewStructType(iceberg.NestedField{ID: 3, Name: "value", Type: iceberg.PrimitiveTypes.String}), false),
	})
	meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "", iceberg.Properties{
		codec + "items.element.value": "gzip",
		codec + "items.value":         "snappy",
	})
	require.NoError(t, err)
	txn := New([]string{"table"}, meta, "", nil, nil).NewTransaction()
	require.NoError(t, txn.UpdateSchema(true, false).RenameColumn([]string{"items"}, "renamed").Commit())
	staged, err := txn.txnMeta()
	require.NoError(t, err)
	require.Equal(t, "gzip", staged.props[codec+"renamed.element.value"])
	require.Equal(t, "snappy", staged.props[codec+"items.value"])
	require.NotContains(t, staged.props, codec+"items.element.value")
}
