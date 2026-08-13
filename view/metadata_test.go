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
	"strings"
	"testing"

	"github.com/DataDog/iceberg-go"
	"github.com/DataDog/iceberg-go/table"
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

func TestNewMetadataRejectInvalidFormatVersion(t *testing.T) {
	tests := []struct {
		name      string
		formatVer string
	}{
		{
			name:      "non-numeric format-version",
			formatVer: "banana",
		},
		{
			name:      "unsupported format-version",
			formatVer: "2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			version := newTestVersion(1, LastAddedID,
				WithVersionSummary(VersionSummary{"summary-key": "summary-val"}),
				WithTimestampMS(1000))
			schema := newTestSchema(0)
			props := iceberg.Properties{
				table.PropertyFormatVersion: tc.formatVer,
				"foo":                       "bar",
			}

			md, err := NewMetadata(version, schema, "location", props)
			require.Error(t, err)
			require.ErrorIs(t, err, iceberg.ErrInvalidFormatVersion)
			require.Nil(t, md)
			assert.Equal(t, tc.formatVer, props[table.PropertyFormatVersion])
			assert.Equal(t, "bar", props["foo"])
			assert.Len(t, props, 2)
		})
	}
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

func TestMetadataUnmarshalReplacesReceiverState(t *testing.T) {
	var metadata metadata
	require.NoError(t, json.Unmarshal([]byte(exampleViewJSON), &metadata))

	var reduced map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(exampleViewJSON), &reduced))
	delete(reduced, "properties")
	delete(reduced, "version-log")
	reducedData, err := json.Marshal(reduced)
	require.NoError(t, err)

	require.NoError(t, json.Unmarshal(reducedData, &metadata))
	assert.Empty(t, metadata.Props)
	assert.Empty(t, metadata.VersionLogList)

	beforeFailure, err := json.Marshal(&metadata)
	require.NoError(t, err)
	invalid := strings.Replace(exampleViewJSON, `"current-version-id": 1`, `"current-version-id": 99`, 1)
	require.Error(t, json.Unmarshal([]byte(invalid), &metadata))
	afterFailure, err := json.Marshal(&metadata)
	require.NoError(t, err)
	assert.Equal(t, beforeFailure, afterFailure)
}

func TestMetadataUnmarshalDoesNotMutateInputAndRoundTripsAfterReuse(t *testing.T) {
	input := []byte(exampleViewJSON)
	originalInput := append([]byte(nil), input...)

	var md metadata
	require.NoError(t, json.Unmarshal(input, &md))
	assert.Equal(t, originalInput, input)

	var replacement map[string]any
	require.NoError(t, json.Unmarshal([]byte(exampleViewJSON), &replacement))
	replacement["current-version-id"] = int64(2)
	replacement["versions"].([]any)[0].(map[string]any)["version-id"] = int64(2)
	replacement["versions"].([]any)[0].(map[string]any)["schema-id"] = 2
	replacement["schemas"].([]any)[0].(map[string]any)["schema-id"] = 2
	replacementData, err := json.Marshal(replacement)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(replacementData, &md))

	encoded, err := json.Marshal(&md)
	require.NoError(t, err)
	var roundTripped metadata
	require.NoError(t, json.Unmarshal(encoded, &roundTripped))
	reencoded, err := json.Marshal(&roundTripped)
	require.NoError(t, err)
	assert.JSONEq(t, string(encoded), string(reencoded))
}

func TestMetadataUnmarshalReplacesLookupCaches(t *testing.T) {
	var metadata metadata
	require.NoError(t, json.Unmarshal([]byte(exampleViewJSON), &metadata))

	// Prime every lookup cache before replacing the metadata.
	assert.Equal(t, int64(1), metadata.CurrentVersion().VersionID)
	assert.Equal(t, 0, metadata.CurrentSchema().ID)
	assert.Contains(t, metadata.SchemasByID(), 0)

	var replacement map[string]any
	require.NoError(t, json.Unmarshal([]byte(exampleViewJSON), &replacement))
	replacement["current-version-id"] = int64(2)
	replacement["versions"].([]any)[0].(map[string]any)["version-id"] = int64(2)
	replacement["versions"].([]any)[0].(map[string]any)["schema-id"] = 2
	replacement["schemas"].([]any)[0].(map[string]any)["schema-id"] = 2
	replacement["version-log"].([]any)[0].(map[string]any)["version-id"] = int64(2)
	replacementData, err := json.Marshal(replacement)
	require.NoError(t, err)

	require.NoError(t, json.Unmarshal(replacementData, &metadata))
	assert.Equal(t, int64(2), metadata.CurrentVersion().VersionID)
	assert.Equal(t, 2, metadata.CurrentSchema().ID)
	assert.NotContains(t, metadata.SchemasByID(), 0)
	assert.Contains(t, metadata.SchemasByID(), 2)

	_, oldVersionExists := metadata.lazyVersionsByID()[1]
	_, newVersionExists := metadata.lazyVersionsByID()[2]
	assert.False(t, oldVersionExists)
	assert.True(t, newVersionExists)

	invalid := strings.Replace(string(replacementData), `"current-version-id":2`, `"current-version-id":99`, 1)
	require.Error(t, json.Unmarshal([]byte(invalid), &metadata))
	assert.Equal(t, int64(2), metadata.CurrentVersion().VersionID)
	assert.Equal(t, 2, metadata.CurrentSchema().ID)
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

func TestParseMetadataRejectsNullAndDuplicateEntries(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(map[string]any)
		errMessage string
	}{
		{
			name: "null schema",
			mutate: func(doc map[string]any) {
				doc["schemas"] = []any{nil}
			},
			errMessage: "schema at index 0 is null",
		},
		{
			name: "duplicate schema ID",
			mutate: func(doc map[string]any) {
				schemas := doc["schemas"].([]any)
				doc["schemas"] = append(schemas, schemas[0])
			},
			errMessage: "duplicate schema-id 0",
		},
		{
			name: "null version",
			mutate: func(doc map[string]any) {
				doc["versions"] = []any{nil}
			},
			errMessage: "version at index 0 is null",
		},
		{
			name: "duplicate version ID",
			mutate: func(doc map[string]any) {
				versions := doc["versions"].([]any)
				doc["versions"] = append(versions, versions[0])
			},
			errMessage: "duplicate version-id 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var doc map[string]any
			require.NoError(t, json.Unmarshal([]byte(exampleViewJSON), &doc))
			tt.mutate(doc)
			encoded, err := json.Marshal(doc)
			require.NoError(t, err)

			var parseErr error
			require.NotPanics(t, func() {
				_, parseErr = ParseMetadataBytes(encoded)
			})
			require.ErrorIs(t, parseErr, ErrInvalidViewMetadata)
			assert.ErrorContains(t, parseErr, tt.errMessage)
		})
	}
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
				{"type": "sql", "sql": "SELECT 2", "dialect": " SPARK "}
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
	assert.Contains(t, err.Error(), "version 1")
	assert.Contains(t, err.Error(), " SPARK ")
}

func TestInvalidRepresentationInJSON(t *testing.T) {
	testCases := []struct {
		name string
		json string
	}{
		{
			name: "missing-sql",
			json: `{
				"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
				"format-version": 1,
				"location": "s3://bucket/warehouse/default.db/event_agg",
				"current-version-id": 1,
				"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "   ", "dialect": "spark"}]}],
				"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
				"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
			}`,
		},
		{
			name: "missing-dialect",
			json: `{
				"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
				"format-version": 1,
				"location": "s3://bucket/warehouse/default.db/event_agg",
				"current-version-id": 1,
				"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [{"type": "sql", "sql": "SELECT 1", "dialect": ""}]}],
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
			assert.True(t, errors.Is(err, ErrInvalidViewMetadata))
			assert.Contains(t, err.Error(), "invalid view representation")
		})
	}
}

func TestUnknownRepresentationTypeInJSON(t *testing.T) {
	validJSON := `{
		"view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
		"format-version": 1,
		"location": "s3://bucket/warehouse/default.db/event_agg",
		"current-version-id": 1,
		"versions": [{"version-id": 1, "schema-id": 1, "timestamp-ms": 1234567890, "representations": [
			{"type": "hive", "sql": "SELECT 1", "dialect": "spark"},
			{"type": "sql", "sql": "SELECT 1", "dialect": "spark"}
		]}],
		"schemas": [{"schema-id": 1, "type": "struct", "fields": []}],
		"version-log": [{"timestamp-ms": 1234567890, "version-id": 1}]
	}`

	var meta metadata
	require.NoError(t, json.Unmarshal([]byte(validJSON), &meta))
	require.Len(t, meta.VersionList, 1)
	require.Len(t, meta.VersionList[0].Representations, 2)
	assert.Equal(t, "hive", meta.VersionList[0].Representations[0].Type)
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

func TestMetadataGettersReturnDefensiveCopies(t *testing.T) {
	md, err := ParseMetadataString(exampleViewJSON)
	require.NoError(t, err)

	currentVersion := md.CurrentVersion()
	currentVersion.Summary["summaryProp"] = "changed"
	currentVersion.Representations[0].Sql = "select changed"
	currentVersion.DefaultNamespace[0] = "changed"

	versions := md.Versions()
	versions[0].VersionID = 99

	currentSchema := md.CurrentSchema()
	currentSchema.ID = 99
	currentSchema.IdentifierFieldIDs[0] = 99

	schemas := md.Schemas()
	schemas[0].ID = 98

	schemasByID := md.SchemasByID()
	schemasByID[0].ID = 97
	delete(schemasByID, 0)

	versionLog := md.VersionLog()
	versionLog[0].VersionID = 99

	props := md.Properties()
	props["prop"] = "changed"

	assert.Equal(t, int64(1), md.CurrentVersion().VersionID)
	assert.Equal(t, "summaryVal", md.CurrentVersion().Summary["summaryProp"])
	assert.Equal(t, "select * from ns.tbl", md.CurrentVersion().Representations[0].Sql)
	assert.Equal(t, "accounting", md.CurrentVersion().DefaultNamespace[0])
	assert.Equal(t, 0, md.CurrentSchema().ID)
	assert.Equal(t, 0, md.CurrentSchema().IdentifierFieldIDs[0])
	assert.Equal(t, 0, md.Schemas()[0].ID)
	assert.Equal(t, 0, md.SchemasByID()[0].ID)
	assert.Equal(t, int64(1), md.VersionLog()[0].VersionID)
	assert.Equal(t, "value", md.Properties()["prop"])
}

func TestCloneSchemaCopiesNestedValues(t *testing.T) {
	schema := iceberg.NewSchemaWithIdentifiers(1, []int{1},
		iceberg.NestedField{
			ID: 1, Name: "struct", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{{
				ID: 2, Name: "nested", Type: iceberg.PrimitiveTypes.Binary,
				InitialDefault: []byte{1}, WriteDefault: iceberg.BinaryLiteral{2},
			}}},
		},
		iceberg.NestedField{
			ID: 3, Name: "list", Type: &iceberg.ListType{
				ElementID: 4, Element: &iceberg.StructType{FieldList: []iceberg.NestedField{{
					ID: 5, Name: "element", Type: iceberg.PrimitiveTypes.String,
				}}},
			},
		},
		iceberg.NestedField{
			ID: 6, Name: "map", Type: &iceberg.MapType{
				KeyID: 7, KeyType: iceberg.PrimitiveTypes.String, ValueID: 8,
				ValueType: &iceberg.StructType{FieldList: []iceberg.NestedField{{
					ID: 9, Name: "value", Type: iceberg.PrimitiveTypes.String,
				}}},
			},
		},
	)

	cloned := cloneSchema(schema)
	cloned.IdentifierFieldIDs[0] = 99
	structField := cloned.Field(0).Type.(*iceberg.StructType)
	structField.FieldList[0].Name = "changed"
	structField.FieldList[0].InitialDefault.([]byte)[0] = 9
	structField.FieldList[0].WriteDefault.(iceberg.BinaryLiteral)[0] = 9
	listField := cloned.Field(1).Type.(*iceberg.ListType)
	listField.Element.(*iceberg.StructType).FieldList[0].Name = "changed"
	mapField := cloned.Field(2).Type.(*iceberg.MapType)
	mapField.ValueType.(*iceberg.StructType).FieldList[0].Name = "changed"

	assert.Equal(t, []int{1}, schema.IdentifierFieldIDs)
	originalStruct := schema.Field(0).Type.(*iceberg.StructType)
	assert.Equal(t, "nested", originalStruct.FieldList[0].Name)
	assert.Equal(t, []byte{1}, originalStruct.FieldList[0].InitialDefault)
	assert.Equal(t, iceberg.BinaryLiteral{2}, originalStruct.FieldList[0].WriteDefault)
	assert.Equal(t, "element", schema.Field(1).Type.(*iceberg.ListType).Element.(*iceberg.StructType).FieldList[0].Name)
	assert.Equal(t, "value", schema.Field(2).Type.(*iceberg.MapType).ValueType.(*iceberg.StructType).FieldList[0].Name)
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
