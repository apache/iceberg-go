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

package view

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestLoadMetadata(t *testing.T) {
	props := make(map[string]string)
	m, err := LoadMetadata(t.Context(), props, "testdata/view-metadata.json", "test", "test")
	require.NoError(t, err)

	require.Equal(t, "fa6506c3-7681-40c8-86dc-e36561f83385", m.ViewUUID())
	require.Equal(t, 1, m.FormatVersion())
	require.Equal(t, 2, len(m.(*metadata).VersionList))
	require.Equal(t, "Daily event counts", m.Properties()["comment"])
}

func TestNewMetadata(t *testing.T) {
	version := Version{
		VersionID:   1,
		SchemaID:    1,
		TimestampMs: 132,
		Summary:     nil,
		Representations: []SQLRepresentation{
			{
				Type:    "sql",
				SQL:     "select * from events",
				Dialect: "spark",
			},
		},
		DefaultCatalog:   nil,
		DefaultNamespace: []string{"namespace"},
	}
	schema := iceberg.NewSchema(1)
	props := iceberg.Properties{"comment": "Daily event counts"}
	meta, err := NewMetadata(schema, version, "s3://location", props)
	require.NoError(t, err)
	require.Equal(t, 1, meta.FormatVersion())
	require.Equal(t, props, meta.Properties())
	require.Equal(t, "s3://location", meta.Location())
	require.Equal(t, schema.ID, meta.CurrentVersion().SchemaID)
	expectedVersion := Version{
		VersionID:   1,
		SchemaID:    1,
		TimestampMs: 132,
		Summary:     nil,
		Representations: []SQLRepresentation{
			{
				Type:    "sql",
				SQL:     "select * from events",
				Dialect: "spark",
			},
		},
		DefaultCatalog:   nil,
		DefaultNamespace: []string{"namespace"},
	}

	require.Equal(t, expectedVersion, *meta.CurrentVersion())
}

func TestNewMetadataRejectsSchemaIDMismatch(t *testing.T) {
	version := Version{
		VersionID:   1,
		SchemaID:    0,
		TimestampMs: 132,
		Summary:     nil,
		Representations: []SQLRepresentation{
			{
				Type:    "sql",
				SQL:     "select * from events",
				Dialect: "spark",
			},
		},
		DefaultCatalog:   nil,
		DefaultNamespace: []string{"namespace"},
	}
	schema := iceberg.NewSchema(1)
	props := iceberg.Properties{"comment": "Daily event counts"}

	meta, err := NewMetadata(schema, version, "s3://location", props)
	require.Nil(t, meta)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.Contains(t, err.Error(), "version.SchemaID does not match schema.ID")
}
