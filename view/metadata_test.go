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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type clonable struct {
	foo []int
	bar int //lint:ignore U1000 false positive
}

func (c *clonable) Clone() *clonable {
	cloned := *c
	cloned.foo = slices.Clone(c.foo)

	return &cloned
}

// Test the Equals method on the Version struct
func TestVersionEquals(t *testing.T) {
	summary := VersionSummary{"foo.bar": "foobar"}
	representations := []Representation{
		{"sql", "SELECT * FROM my.table", "spark"},
		{"sql", "SELECT * FROM my.table", "trino"},
	}
	v1 := &Version{
		VersionID:       1,
		SchemaID:        1,
		TimestampMS:     0,
		Summary:         summary,
		Representations: slices.Clone(representations),
	}
	v2 := &Version{
		VersionID:       v1.VersionID + 1,
		SchemaID:        1,
		TimestampMS:     v1.TimestampMS + 1,
		Summary:         summary,
		Representations: slices.Clone(representations),
	}
	assert.True(t, v1.Equals(v2), fmt.Sprintf("Expected the same SchemaID, Summary, Representation for %v, got %v", v1, v2))
}

func TestNewMetadata(t *testing.T) {
	// VersionID of 3 should be overridden by the ViewMD ctor
	version := newTestVersion(3, LastAddedID,
		WithVersionSummary(VersionSummary{"summary-key": "summary-val"}),
		WithTimestampMS(1000))
	schema := newTestSchema(0)
	props := iceberg.Properties{"prop": "value"}

	md, err := NewMetadata(version, schema, "location", props)
	require.NoError(t, err)

	expectedVersion := version.Clone()
	// VersionID and SchemaID should be overridden to spec defaults for new View
	expectedVersion.VersionID = InitialVersionID
	expectedVersion.SchemaID = 0
	assert.Equal(t, expectedVersion, md.CurrentVersion())
	expectedSchema, err := iceberg.AssignFreshSchemaIDs(schema, nil)
	require.NoError(t, err)
	assert.True(t, expectedSchema.Equals(md.CurrentSchema()))
	assert.Equal(t, []VersionLogEntry{{TimestampMS: 1000, VersionID: 1}}, md.VersionLog())
}

func TestUnmarshalViewMetadata(t *testing.T) {
	md, err := ParseMetadataString(exampleViewJSON)
	require.NoError(t, err)
	expectedUUID, _ := uuid.Parse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
	assert.Equal(t, expectedUUID, md.ViewUUID())
	assert.Equal(t, "s3://bucket/test/location", md.Location())
	assert.EqualValues(t, 1, md.CurrentVersionID())
	assert.EqualValues(t, 1, md.CurrentVersion().VersionID)
	assert.Equal(t, 0, md.CurrentSchemaID())
	assert.Equal(t, 0, md.CurrentSchema().ID)
	assert.Equal(t, 0, md.CurrentVersion().SchemaID)
	assert.Equal(t, VersionSummary{"summaryProp": "summaryVal"}, md.CurrentVersion().Summary)
	assert.Equal(t, []VersionLogEntry{{TimestampMS: 1000, VersionID: 1}}, md.VersionLog())
	assert.Equal(t, []Representation{NewRepresentation("select * from ns.tbl", "trino")}, md.CurrentVersion().Representations)
	assert.Equal(t, iceberg.Properties{"prop": "value"}, md.Properties())
}

func TestValidMetadataDeserialization(t *testing.T) {
	validJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 1,
		"properties": {
			"comment": "Daily event counts"
		},
		"versions": [{
			"version-id": 1,
			"timestamp-ms": 1573518431292,
			"schema-id": 1,
			"default-catalog": "prod",
			"default-namespace": ["default"],
			"summary": {
				"operation": "create",
				"engine-name": "Spark",
				"engine-version": "3.3.2"
			},
			"representations": [{
				"type": "sql",
				"sql": "SELECT COUNT(*) FROM events",
				"dialect": "spark"
			}]
		}],
		"schemas": [{
			"schema-id": 1,
			"type": "struct",
			"fields": [{
				"id": 1,
				"name": "event_count",
				"required": false,
				"type": "long"
			}]
		}],
		"version-log": [{
			"timestamp-ms": 1573518431292,
			"version-id": 1
		}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(validJSON), &meta)
	require.NoError(t, err)

	expectedUUID, _ := uuid.Parse("fa6506c3-7681-40c8-86dc-e36561f83385")
	assert.Equal(t, expectedUUID, meta.ViewUUID())
	assert.Equal(t, 1, meta.FormatVersion())
	assert.Equal(t, "s3://bucket/warehouse/default.db/event_agg", meta.Location())
	assert.Equal(t, int64(1), meta.CurrentVersionIDValue)
}

func TestMissingViewUUID(t *testing.T) {
	invalidJSON := `{
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 1,
		"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
	assert.Contains(t, err.Error(), "view-uuid is required")
}

func TestMissingLocation(t *testing.T) {
	invalidJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"current-version-id": 1,
		"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
	assert.Contains(t, err.Error(), "location is required")
}

func TestMissingFormatVersion(t *testing.T) {
	invalidJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 1,
		"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadataFormatVersion))
	assert.Contains(t, err.Error(), "format-version is required")
}

func TestInvalidFormatVersion(t *testing.T) {
	testCases := []struct {
		name string
		json string
	}{
		{
			"version 0",
			`{
				"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
				"format-version": 0,
				"location": "s3://bucket/warehouse/default.db/event_agg",
				"current-version-id": 1,
				"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
				"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
				"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
			}`,
		},
		{
			"version 2",
			`{
				"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
				"format-version": 2,
				"location": "s3://bucket/warehouse/default.db/event_agg",
				"current-version-id": 1,
				"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
				"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
				"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
			}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var meta metadata
			err := json.Unmarshal([]byte(tc.json), &meta)
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrInvalidViewMetadataFormatVersion))
		})
	}
}

func TestMissingVersions(t *testing.T) {
	invalidJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 1,
		"versions": [],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": []
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
	assert.Contains(t, err.Error(), "at least one version is required")
}

func TestCurrentVersionNotFound(t *testing.T) {
	invalidJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 99,
		"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
	assert.Contains(t, err.Error(), "current-version-id 99 not found")
}

func TestVersionReferencesUnknownSchema(t *testing.T) {
	invalidJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 1,
		"versions": [{"version-id": 1, "schema-id": 99, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
	assert.Contains(t, err.Error(), "version 1 references unknown schema-id 99")
}

func TestMissingSchemas(t *testing.T) {
	invalidJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 1,
		"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
		"schemas": [],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
	assert.Contains(t, err.Error(), "at least one schema is required")
}

func TestDuplicateDialects(t *testing.T) {
	invalidJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 1,
		"versions": [{
			"version-id": 1,
			"schema-id": 1,
			"timestamp-ms": 1234567890,
			"representations": [
				{"type": "sql", "sql": "SELECT 1", "dialect": "spark"},
				{"type": "sql", "sql": "SELECT 2", "dialect": "SPARK"}
			]
		}],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
	assert.Contains(t, err.Error(), "duplicate dialect")
}

func TestNilFieldsInJSON(t *testing.T) {
	validJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 1,
		"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(validJSON), &meta)
	require.NoError(t, err)

	assert.NotNil(t, meta.Props)
	assert.Empty(t, meta.Props)
}

func TestMissingCurrentVersionID(t *testing.T) {
	invalidJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]}],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(invalidJSON), &meta)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
	assert.Contains(t, err.Error(), "current-version-id is required")
}

func TestMultipleVersionsValidation(t *testing.T) {
	validJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 2,
		"versions": [
			{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}]},
			{"version-id": 2, "schema-id": 2, "timestamp-ms": 1234567900, "representations": [{"type": "sql", "sql": "SELECT 2", "dialect": "trino"}]}
		],
		"schemas": [
			{"schema-id": 1, "type": "struct", "fields": [{"id": 1, "name": "x", "required": false, "type": "long"}]},
			{"schema-id": 2, "type": "struct", "fields": [{"id": 1, "name": "y", "required": false, "type": "string"}]}
		],
		"version-log": [
			{"timestamp-ms": 1234567890, "version-id": 1},
			{"timestamp-ms": 1234567900, "version-id": 2}
		]
	}`

	var meta metadata
	err := json.Unmarshal([]byte(validJSON), &meta)
	require.NoError(t, err)

	assert.Len(t, meta.VersionList, 2)
	assert.Len(t, meta.SchemaList, 2)
	assert.Len(t, meta.VersionLogList, 2)
}

func TestCloneSlice(t *testing.T) {
	x := []*clonable{{[]int{1, 2, 3}, 4}}
	clonedX := cloneSlice(x)
	assert.EqualValues(t, x, clonedX)
	clonedX[0].foo[0] = 5
	assert.NotEqualValues(t, x, clonedX)
}

var exampleViewJSON = `{
	"view-uuid": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
	"format-version": 1,
	"location": "s3://bucket/test/location",
	"current-version-id": 1,
	"versions": [
	  {
		"version-id": 1,
		"timestamp-ms": 1000,
		"schema-id": 0,
		"summary": {
		  "summaryProp": "summaryVal"
		},
		"representations": [
		  {
			"type": "sql",
			"sql": "select * from ns.tbl",
			"dialect": "trino"
		  }
		],
		"default-catalog": "string",
		"default-namespace": [
		  "accounting",
		  "tax"
		]
	  }
	],
	"version-log": [
	  {
		"version-id": 1,
		"timestamp-ms": 1000
	  }
	],
	"schemas": [
	  {
		"type": "struct",
		"fields": [
		  {
			"id": 1,
			"name": "x",
			"type": "long",
			"required": true,
			"doc": "",
			"initial-default": true,
			"write-default": true
		  }
		],
		"schema-id": 0,
		"identifier-field-ids": [
		  0
		]
	  }
	],
	"properties": {
		"prop": "value"
	}
}`
