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

func TestParquetStatisticsPropertiesFollowSchemaChanges(t *testing.T) {
	const prefix = ParquetColumnStatsEnabledKeyPrefix + "."
	for _, tc := range []struct {
		name   string
		before string
		after  string
		change func(*UpdateSchema) *UpdateSchema
	}{
		{"leaf rename", "name", "full_name", func(u *UpdateSchema) *UpdateSchema { return u.RenameColumn([]string{"name"}, "full_name") }},
		{"parent rename", "address.city", "location.city", func(u *UpdateSchema) *UpdateSchema { return u.RenameColumn([]string{"address"}, "location") }},
		{"list rename", "tags.element", "labels.element", func(u *UpdateSchema) *UpdateSchema { return u.RenameColumn([]string{"tags"}, "labels") }},
		{"map rename", "properties.value", "attributes.value", func(u *UpdateSchema) *UpdateSchema { return u.RenameColumn([]string{"properties"}, "attributes") }},
		{"parent delete", "address.city", "", func(u *UpdateSchema) *UpdateSchema { return u.DeleteColumn([]string{"address"}) }},
		{"delete and reuse", "name", "", func(u *UpdateSchema) *UpdateSchema {
			return u.DeleteColumn([]string{"name"}).AddColumn([]string{"name"}, iceberg.PrimitiveTypes.String, "", false, nil)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			meta, err := NewMetadata(originalSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "", iceberg.Properties{
				prefix + tc.before: "false", prefix + "unknown": "true", "unrelated": "kept",
			})
			require.NoError(t, err)
			tx := New([]string{"table"}, meta, "", nil, nil).NewTransaction()
			require.NoError(t, tc.change(tx.UpdateSchema(true, true)).Commit())
			staged, err := tx.txnMeta()
			require.NoError(t, err)
			require.NotContains(t, staged.props, prefix+tc.before)
			if tc.after != "" {
				require.Equal(t, "false", staged.props[prefix+tc.after])
			}
			require.Equal(t, "true", staged.props[prefix+"unknown"])
			require.Equal(t, "kept", staged.props["unrelated"])
		})
	}
}

func TestParquetStatisticsAndOtherColumnPropertiesMoveTogether(t *testing.T) {
	settings := []struct{ prefix, value string }{
		{MetricsModeColumnConfPrefix, "full"},
		{ParquetBloomFilterColumnEnabledKeyPrefix, "true"},
		{ParquetColumnStatsEnabledKeyPrefix, "false"},
		{ParquetCompressionColumnCodecKeyPrefix, "gzip"},
		{ParquetCompressionColumnLevelKeyPrefix, "4"},
	}
	props := iceberg.Properties{}
	for _, setting := range settings {
		props[setting.prefix+".name"] = setting.value
	}
	meta, err := NewMetadata(originalSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "", props)
	require.NoError(t, err)
	tx := New([]string{"table"}, meta, "", nil, nil).NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, true).RenameColumn([]string{"name"}, "full_name").Commit())
	staged, err := tx.txnMeta()
	require.NoError(t, err)
	for _, setting := range settings {
		require.NotContains(t, staged.props, setting.prefix+".name")
		require.Equal(t, setting.value, staged.props[setting.prefix+".full_name"])
	}
}

func TestParquetStatisticsRenameIntoDeletedName(t *testing.T) {
	const prefix = ParquetColumnStatsEnabledKeyPrefix + "."
	meta, err := NewMetadata(originalSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "", iceberg.Properties{
		prefix + "name": "false", prefix + "age": "true",
	})
	require.NoError(t, err)
	tx := New([]string{"table"}, meta, "", nil, nil).NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, true).DeleteColumn([]string{"age"}).RenameColumn([]string{"name"}, "age").Commit())
	staged, err := tx.txnMeta()
	require.NoError(t, err)
	require.NotContains(t, staged.props, prefix+"name")
	require.Equal(t, "false", staged.props[prefix+"age"])
}

func TestParquetStatisticsAndOtherColumnPropertiesRenameIntoDeletedName(t *testing.T) {
	settings := []struct{ prefix, renamed, deleted string }{
		{MetricsModeColumnConfPrefix, "full", "none"},
		{ParquetBloomFilterColumnEnabledKeyPrefix, "true", "false"},
		{ParquetColumnStatsEnabledKeyPrefix, "false", "true"},
		{ParquetCompressionColumnCodecKeyPrefix, "gzip", "zstd"},
		{ParquetCompressionColumnLevelKeyPrefix, "4", "2"},
	}
	props := iceberg.Properties{}
	for _, setting := range settings {
		props[setting.prefix+".name"] = setting.renamed
		props[setting.prefix+".age"] = setting.deleted
	}
	meta, err := NewMetadata(originalSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "", props)
	require.NoError(t, err)
	tx := New([]string{"table"}, meta, "", nil, nil).NewTransaction()
	require.NoError(t, tx.UpdateSchema(true, true).DeleteColumn([]string{"age"}).RenameColumn([]string{"name"}, "age").Commit())
	staged, err := tx.txnMeta()
	require.NoError(t, err)
	for _, setting := range settings {
		require.NotContains(t, staged.props, setting.prefix+".name")
		require.Equal(t, setting.renamed, staged.props[setting.prefix+".age"], setting.prefix)
	}
}

func TestParquetStatisticsRenameUsesStagedProperties(t *testing.T) {
	const prefix = ParquetColumnStatsEnabledKeyPrefix + "."
	meta, err := NewMetadata(originalSchema, iceberg.UnpartitionedSpec, UnsortedSortOrder, "", nil)
	require.NoError(t, err)
	tx := New([]string{"table"}, meta, "", nil, nil).NewTransaction()
	require.NoError(t, tx.SetProperties(iceberg.Properties{prefix + "name": "false"}))
	require.NoError(t, tx.UpdateSchema(true, false).RenameColumn([]string{"name"}, "full_name").Commit())
	staged, err := tx.txnMeta()
	require.NoError(t, err)
	require.NotContains(t, staged.props, prefix+"name")
	require.Equal(t, "false", staged.props[prefix+"full_name"])
}
