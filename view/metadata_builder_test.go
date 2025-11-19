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

package view_test

import (
	"iter"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/view"
	"github.com/stretchr/testify/require"
)

func TestBuildSuccess(t *testing.T) {
	builder := newTestBuilder(t)

	err := builder.AssignUUID("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)

	err = builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)

	schema := testSchema()
	version := testVersion(schema.ID)

	err = builder.SetCurrentVersion(version, schema)
	require.NoError(t, err)

	result := buildAndRequireNoError(t, builder)
	require.Equal(t, "550e8400-e29b-41d4-a716-446655440000", result.Metadata.ViewUUID())
	require.Equal(t, "s3://bucket/view", result.Metadata.Location())
	require.Equal(t, int64(1), result.Metadata.CurrentVersion().VersionID)
}

func TestBuildMissingLocation(t *testing.T) {
	builder := newTestBuilder(t)

	err := builder.AssignUUID("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)

	schema := testSchema()
	version := testVersion(schema.ID)

	err = builder.SetCurrentVersion(version, schema)
	require.NoError(t, err)

	_, err = builder.Build()
	require.Error(t, err)
	require.ErrorIs(t, err, view.ErrInvalidViewMetadata)
}

func TestBuildMissingVersions(t *testing.T) {
	builder := newTestBuilder(t)

	err := builder.AssignUUID("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)

	err = builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)

	_, err = builder.Build()
	requireBuilderError(t, err, "at least one version is required")
}

func TestBuildMissingUUID(t *testing.T) {
	builder := newTestBuilder(t)

	err := builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)

	schema := testSchema()
	version := testVersion(schema.ID)

	err = builder.SetCurrentVersion(version, schema)
	require.NoError(t, err)

	_, err = builder.Build()
	require.Error(t, err)
	require.ErrorIs(t, err, view.ErrInvalidViewMetadata)
}

func TestBuildMissingCurrentVersion(t *testing.T) {
	builder := newTestBuilder(t)

	err := builder.AssignUUID("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)

	err = builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)

	schema := testSchema()
	version := testVersion(schema.ID)

	err = builder.AddSchema(schema)
	require.NoError(t, err)

	err = builder.AddVersion(version)
	require.NoError(t, err)

	_, err = builder.Build()
	requireBuilderError(t, err, "current-version-id is required")
}

func TestExpiration(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	v1 := view.Version{VersionID: 1, SchemaID: 0, TimestampMs: 1000000001, Summary: map[string]string{"operation": "create-1"}, Representations: []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 1", Dialect: "spark"}}, DefaultNamespace: []string{"default"}}
	v2 := view.Version{VersionID: 2, SchemaID: 0, TimestampMs: 1000000002, Summary: map[string]string{"operation": "create-2"}, Representations: []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 2", Dialect: "spark"}}, DefaultNamespace: []string{"default"}}
	v3 := view.Version{VersionID: 3, SchemaID: 0, TimestampMs: 1000000003, Summary: map[string]string{"operation": "create-3"}, Representations: []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 3", Dialect: "spark"}}, DefaultNamespace: []string{"default"}}
	v4 := view.Version{VersionID: 4, SchemaID: 0, TimestampMs: 1000000004, Summary: map[string]string{"operation": "create-4"}, Representations: []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 4", Dialect: "spark"}}, DefaultNamespace: []string{"default"}}
	v5 := view.Version{VersionID: 5, SchemaID: 0, TimestampMs: 1000000005, Summary: map[string]string{"operation": "create-5"}, Representations: []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 5", Dialect: "spark"}}, DefaultNamespace: []string{"default"}}

	baseMetadata := &testMetadata{
		uuid:           "550e8400-e29b-41d4-a716-446655440000",
		formatVersion:  1,
		location:       "s3://bucket/view",
		schemas:        []*iceberg.Schema{schema},
		versions:       []view.Version{v1, v2, v3, v4, v5},
		currentVersion: &v5,
		properties:     map[string]string{view.PropertyVersionHistorySize: "3"},
	}

	builder := view.MetadataBuilderFrom(baseMetadata)

	result, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, result)

	versionCount := 0
	hasCurrentVersion := false
	for v := range result.Metadata.Versions() {
		versionCount++
		if v.VersionID == 5 {
			hasCurrentVersion = true
		}
	}
	require.LessOrEqual(t, versionCount, 3)
	require.True(t, hasCurrentVersion, "current version should not be expired")
}

func TestCurrentVersionIsNeverExpired(t *testing.T) {
	builder, schema := newBuilderWithSchema(t)

	err := builder.AssignUUID("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)

	err = builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)

	for i := 1; i <= 5; i++ {
		version := testVersionWithSQL(schema.ID, int64(i), "SELECT * FROM t", "spark")
		version.TimestampMs = int64(1000000000 + i*1000)
		err = builder.AddVersion(version)
		require.NoError(t, err)
	}

	err = builder.SetProperties(map[string]string{
		view.PropertyVersionHistorySize: "1",
	})
	require.NoError(t, err)

	err = builder.SetCurrentVersionID(1)
	require.NoError(t, err)

	result := buildAndRequireNoError(t, builder)

	hasCurrentVersion := false
	for v := range result.Metadata.Versions() {
		if v.VersionID == 1 {
			hasCurrentVersion = true

			break
		}
	}
	require.True(t, hasCurrentVersion, "current version must never be expired")
}

func TestUpdateHistory(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	v1 := view.Version{VersionID: 1, SchemaID: 0, TimestampMs: 1000000001, Summary: map[string]string{}, Representations: []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 1", Dialect: "spark"}}, DefaultNamespace: []string{"default"}}
	v2 := view.Version{VersionID: 2, SchemaID: 0, TimestampMs: 1000000002, Summary: map[string]string{}, Representations: []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 2", Dialect: "spark"}}, DefaultNamespace: []string{"default"}}
	v3 := view.Version{VersionID: 3, SchemaID: 0, TimestampMs: 1000000003, Summary: map[string]string{}, Representations: []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 3", Dialect: "spark"}}, DefaultNamespace: []string{"default"}}
	v5 := view.Version{VersionID: 5, SchemaID: 0, TimestampMs: 1000000005, Summary: map[string]string{}, Representations: []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 5", Dialect: "spark"}}, DefaultNamespace: []string{"default"}}

	baseMetadata := &testMetadata{
		uuid:          "550e8400-e29b-41d4-a716-446655440000",
		formatVersion: 1,
		location:      "s3://bucket/view",
		schemas:       []*iceberg.Schema{schema},
		versions:      []view.Version{v1, v2, v3, v5},
		versionLog: []view.VersionLogEntry{
			{VersionID: 1, TimestampMs: 1000000001},
			{VersionID: 2, TimestampMs: 1000000002},
			{VersionID: 3, TimestampMs: 1000000003},
			{VersionID: 4, TimestampMs: 1000000004},
			{VersionID: 5, TimestampMs: 1000000005},
		},
		currentVersion: &v5,
		properties:     map[string]string{view.PropertyVersionHistorySize: "10"},
	}

	builder := view.MetadataBuilderFrom(baseMetadata)

	result, err := builder.Build()
	require.NoError(t, err)

	for entry := range result.Metadata.VersionLog() {
		require.NotEqual(t, int64(1), entry.VersionID, "history before gap should be cleared")
		require.NotEqual(t, int64(2), entry.VersionID, "history before gap should be cleared")
		require.NotEqual(t, int64(3), entry.VersionID, "history before gap should be cleared")
	}
}

func TestNegativeHistorySize(t *testing.T) {
	builder, version := newBuilderWithVersion(t)

	err := builder.AssignUUID("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)

	err = builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)

	err = builder.SetCurrentVersionID(version.VersionID)
	require.NoError(t, err)

	err = builder.SetProperties(map[string]string{
		view.PropertyVersionHistorySize: "-1",
	})
	require.NoError(t, err)

	_, err = builder.Build()
	requireBuilderError(t, err, "must be positive")
}

func TestZeroHistorySize(t *testing.T) {
	builder, version := newBuilderWithVersion(t)

	err := builder.AssignUUID("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)

	err = builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)

	err = builder.SetCurrentVersionID(version.VersionID)
	require.NoError(t, err)

	err = builder.SetProperties(map[string]string{
		view.PropertyVersionHistorySize: "0",
	})
	require.NoError(t, err)

	_, err = builder.Build()
	requireBuilderError(t, err, "must be positive")
}

func TestDroppingDialectFailsByDefault(t *testing.T) {
	schema := testSchema()

	v1 := view.Version{
		VersionID:   1,
		SchemaID:    0,
		TimestampMs: 1000000001,
		Summary:     map[string]string{},
		Representations: []view.SQLRepresentation{
			{Type: "sql", SQL: "SELECT 1", Dialect: "spark"},
			{Type: "sql", SQL: "SELECT 1", Dialect: "trino"},
		},
		DefaultNamespace: []string{"default"},
	}

	baseMetadata := &testMetadata{
		uuid:           "550e8400-e29b-41d4-a716-446655440000",
		formatVersion:  1,
		location:       "s3://bucket/view",
		schemas:        []*iceberg.Schema{schema},
		versions:       []view.Version{v1},
		currentVersion: &v1,
		properties:     map[string]string{},
	}

	builder := view.MetadataBuilderFrom(baseMetadata)

	version := &view.Version{
		VersionID:        2,
		SchemaID:         0,
		TimestampMs:      1000000002,
		Summary:          map[string]string{"operation": "replace"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 2", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}
	err := builder.AddVersion(version)
	require.NoError(t, err)

	err = builder.SetCurrentVersionID(2)
	require.NoError(t, err)

	_, err = builder.Build()
	require.Error(t, err)
	require.Contains(t, err.Error(), "dropping dialects")
	require.Contains(t, err.Error(), "trino")
}

func TestDroppingDialectDoesNotFailWhenAllowed(t *testing.T) {
	schema := testSchema()

	v1 := view.Version{
		VersionID:   1,
		SchemaID:    0,
		TimestampMs: 1000000001,
		Summary:     map[string]string{},
		Representations: []view.SQLRepresentation{
			{Type: "sql", SQL: "SELECT 1", Dialect: "spark"},
			{Type: "sql", SQL: "SELECT 1", Dialect: "trino"},
		},
		DefaultNamespace: []string{"default"},
	}

	baseMetadata := &testMetadata{
		uuid:           "550e8400-e29b-41d4-a716-446655440000",
		formatVersion:  1,
		location:       "s3://bucket/view",
		schemas:        []*iceberg.Schema{schema},
		versions:       []view.Version{v1},
		currentVersion: &v1,
		properties:     map[string]string{view.PropertyReplaceDropDialectAllowed: "true"},
	}

	builder := view.MetadataBuilderFrom(baseMetadata)

	version := &view.Version{
		VersionID:        2,
		SchemaID:         0,
		TimestampMs:      1000000002,
		Summary:          map[string]string{"operation": "replace"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 2", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}
	err := builder.AddVersion(version)
	require.NoError(t, err)

	err = builder.SetCurrentVersionID(2)
	require.NoError(t, err)

	buildAndRequireNoError(t, builder)
}

func TestAddingDialectSucceeds(t *testing.T) {
	schema := testSchema()

	v1 := view.Version{
		VersionID:        1,
		SchemaID:         0,
		TimestampMs:      1000000001,
		Summary:          map[string]string{},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT 1", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	baseMetadata := &testMetadata{
		uuid:           "550e8400-e29b-41d4-a716-446655440000",
		formatVersion:  1,
		location:       "s3://bucket/view",
		schemas:        []*iceberg.Schema{schema},
		versions:       []view.Version{v1},
		currentVersion: &v1,
		properties:     map[string]string{},
	}

	builder := view.MetadataBuilderFrom(baseMetadata)

	version := &view.Version{
		VersionID:   2,
		SchemaID:    0,
		TimestampMs: 1000000002,
		Summary:     map[string]string{"operation": "replace"},
		Representations: []view.SQLRepresentation{
			{Type: "sql", SQL: "SELECT 2", Dialect: "spark"},
			{Type: "sql", SQL: "SELECT 2", Dialect: "trino"},
		},
		DefaultNamespace: []string{"default"},
	}
	err := builder.AddVersion(version)
	require.NoError(t, err)

	err = builder.SetCurrentVersionID(2)
	require.NoError(t, err)

	buildAndRequireNoError(t, builder)
}

func TestSetCurrentVersionUpdatesSchemaID(t *testing.T) {
	builder := newTestBuilder(t)

	err := builder.AssignUUID("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)

	err = builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)

	schema1 := testSchema()
	schema2 := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	version := testVersion(0)

	err = builder.SetCurrentVersion(version, schema1)
	require.NoError(t, err)
	require.Equal(t, 0, version.SchemaID)

	err = builder.SetCurrentVersion(version, schema2)
	require.NoError(t, err)
	require.Equal(t, 0, version.SchemaID)

	result := buildAndRequireNoError(t, builder)

	currentVersion := result.Metadata.CurrentVersion()
	require.NotNil(t, currentVersion)
	require.Equal(t, 1, currentVersion.SchemaID)
}

func TestSetCurrentVersion(t *testing.T) {
	builder := newTestBuilder(t)
	schema := testSchema()
	version := testVersion(schema.ID)

	err := builder.SetCurrentVersion(version, schema)
	require.NoError(t, err)
}

func TestSetCurrentVersionID(t *testing.T) {
	builder, version := newBuilderWithVersion(t)

	err := builder.SetCurrentVersionID(version.VersionID)
	require.NoError(t, err)

	err = builder.SetCurrentVersionID(version.VersionID)
	require.NoError(t, err)
}

func TestSetCurrentVersionIDLastAdded(t *testing.T) {
	builder, _ := newBuilderWithVersion(t)

	err := builder.SetCurrentVersionID(view.LastAdded)
	require.NoError(t, err)
}

func TestSetCurrentVersionIDInvalid(t *testing.T) {
	builder := newTestBuilder(t)
	requireBuilderError(t, builder.SetCurrentVersionID(999), "cannot set current version")
}

func TestSetCurrentVersionIDLastAddedWithoutVersion(t *testing.T) {
	builder := newTestBuilder(t)
	requireBuilderError(t, builder.SetCurrentVersionID(view.LastAdded), "cannot set last added version")
}

func TestAddVersion(t *testing.T) {
	builder, schema := newBuilderWithSchema(t)
	version := testVersion(schema.ID)

	err := builder.AddVersion(version)
	require.NoError(t, err)

	retrieved, err := builder.GetVersionByID(1)
	require.NoError(t, err)
	require.Equal(t, int64(1), retrieved.VersionID)
}

func TestAddVersionNil(t *testing.T) {
	builder := newTestBuilder(t)
	requireBuilderError(t, builder.AddVersion(nil), "cannot add nil version")
}

func TestVersionWithNonExistentSchema(t *testing.T) {
	builder := newTestBuilder(t)
	version := testVersion(999)

	requireBuilderError(t, builder.AddVersion(version), "cannot add version with unknown schema")
}

func TestMultipleSQLForSameDialect(t *testing.T) {
	builder, schema := newBuilderWithSchema(t)

	version := &view.Version{
		VersionID:   1,
		SchemaID:    schema.ID,
		TimestampMs: 1234567890,
		Summary:     map[string]string{"operation": "create"},
		Representations: []view.SQLRepresentation{
			{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"},
			{Type: "sql", SQL: "SELECT id FROM t", Dialect: "spark"},
		},
		DefaultNamespace: []string{"default"},
	}

	requireBuilderError(t, builder.AddVersion(version), "cannot add multiple queries for dialect spark")
}

func TestMultipleSQLForSameDialectCaseInsensitive(t *testing.T) {
	builder, schema := newBuilderWithSchema(t)

	version := &view.Version{
		VersionID:   1,
		SchemaID:    schema.ID,
		TimestampMs: 1234567890,
		Summary:     map[string]string{"operation": "create"},
		Representations: []view.SQLRepresentation{
			{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"},
			{Type: "sql", SQL: "SELECT id FROM t", Dialect: "SPARK"},
		},
		DefaultNamespace: []string{"default"},
	}

	requireBuilderError(t, builder.AddVersion(version), "cannot add multiple queries for dialect")
}

func TestGetVersionByID(t *testing.T) {
	builder, version := newBuilderWithVersion(t)

	retrieved, err := builder.GetVersionByID(version.VersionID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, version.VersionID, retrieved.VersionID)
}

func TestGetVersionByIDNotFound(t *testing.T) {
	builder := newTestBuilder(t)
	_, err := builder.GetVersionByID(999)
	requireBuilderError(t, err, "version with id 999 not found")
}

func TestVersionDeduplication(t *testing.T) {
	builder, schema := newBuilderWithSchema(t)

	version1 := testVersion(schema.ID)
	version2 := testVersion(schema.ID)
	version2.VersionID = 2

	err := builder.AddVersion(version1)
	require.NoError(t, err)
	require.Equal(t, int64(1), version1.VersionID)

	err = builder.AddVersion(version2)
	require.NoError(t, err)
	require.Equal(t, int64(1), version2.VersionID)
}

func TestVersionIDReassignment(t *testing.T) {
	builder, schema := newBuilderWithSchema(t)

	version1 := testVersion(schema.ID)
	version1.VersionID = 5
	err := builder.AddVersion(version1)
	require.NoError(t, err)

	version2 := testVersion(schema.ID)
	version2.VersionID = 10
	err = builder.AddVersion(version2)
	require.NoError(t, err)

	require.Equal(t, version1.VersionID, version2.VersionID)
}

func TestMultipleVersions(t *testing.T) {
	builder, schema := newBuilderWithSchema(t)

	version1 := testVersionWithSQL(schema.ID, 1, "SELECT * FROM t", "spark")
	version2 := testVersionWithSQL(schema.ID, 2, "SELECT id FROM t", "spark")
	version2.Summary = map[string]string{"operation": "replace"}

	err := builder.AddVersion(version1)
	require.NoError(t, err)

	err = builder.AddVersion(version2)
	require.NoError(t, err)

	require.NotEqual(t, version1.VersionID, version2.VersionID)
	require.Equal(t, int64(1), version1.VersionID)
	require.Equal(t, int64(2), version2.VersionID)
}

func TestVersionCopyDeepCopy(t *testing.T) {
	catalog := "test-catalog"
	original := &view.Version{
		VersionID:        1,
		SchemaID:         5,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create", "key": "value"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultCatalog:   &catalog,
		DefaultNamespace: []string{"db", "schema"},
	}

	copied := original.Copy()

	require.NotNil(t, copied)
	require.NotSame(t, original, copied)
	require.Equal(t, original.VersionID, copied.VersionID)
	require.Equal(t, original.SchemaID, copied.SchemaID)
	require.Equal(t, original.TimestampMs, copied.TimestampMs)

	require.Equal(t, original.Summary, copied.Summary)
	require.Equal(t, original.Representations, copied.Representations)
	require.Equal(t, *original.DefaultCatalog, *copied.DefaultCatalog)
	require.NotSame(t, original.DefaultCatalog, copied.DefaultCatalog)
	require.Equal(t, original.DefaultNamespace, copied.DefaultNamespace)

	original.Summary["modified"] = "changed"
	require.NotContains(t, copied.Summary, "modified")

	original.DefaultNamespace[0] = "changed"
	require.Equal(t, "db", copied.DefaultNamespace[0])
}

func TestVersionCopyNil(t *testing.T) {
	var version *view.Version
	copied := version.Copy()
	require.Nil(t, copied)
}

func TestAddSchema(t *testing.T) {
	builder := newTestBuilder(t)
	schema := testSchema()

	err := builder.AddSchema(schema)
	require.NoError(t, err)

	retrieved, err := builder.GetSchemaByID(0)
	require.NoError(t, err)
	require.Equal(t, 0, retrieved.ID)
}

func TestAddSchemaNil(t *testing.T) {
	builder := newTestBuilder(t)
	requireBuilderError(t, builder.AddSchema(nil), "cannot add nil schema")
}

func TestGetSchemaByID(t *testing.T) {
	builder, schema := newBuilderWithSchema(t)

	retrieved, err := builder.GetSchemaByID(schema.ID)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.Equal(t, schema.ID, retrieved.ID)
}

func TestGetSchemaByIDNotFound(t *testing.T) {
	builder := newTestBuilder(t)
	_, err := builder.GetSchemaByID(999)
	requireBuilderError(t, err, "schema with id 999 not found")
}

func TestSchemaDeduplication(t *testing.T) {
	builder := newTestBuilder(t)

	schema1 := testSchema()
	schema2 := testSchema()

	err := builder.AddSchema(schema1)
	require.NoError(t, err)
	require.Equal(t, 0, schema1.ID)

	err = builder.AddSchema(schema2)
	require.NoError(t, err)
	require.Equal(t, 0, schema2.ID)
}

func TestSchemaIDReassignment(t *testing.T) {
	builder := newTestBuilder(t)

	schema1 := testSchemaWithID(5)
	err := builder.AddSchema(schema1)
	require.NoError(t, err)

	schema2 := testSchemaWithID(10)
	err = builder.AddSchema(schema2)
	require.NoError(t, err)

	require.Equal(t, schema1.ID, schema2.ID)
}

func TestMultipleSchemas(t *testing.T) {
	builder := newTestBuilder(t)

	schema1 := testSchema()
	schema2 := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
	)

	err := builder.AddSchema(schema1)
	require.NoError(t, err)

	err = builder.AddSchema(schema2)
	require.NoError(t, err)

	require.NotEqual(t, schema1.ID, schema2.ID)
	require.Equal(t, 0, schema1.ID)
	require.Equal(t, 1, schema2.ID)
}

func TestUpgradeFormatVersion(t *testing.T) {
	builder := newTestBuilder(t)
	require.NoError(t, builder.UpgradeFormatVersion(1))
}

func TestDowngradeFormatVersionFails(t *testing.T) {
	builder := newTestBuilder(t)

	err := builder.UpgradeFormatVersion(0)
	require.Error(t, err)
	require.ErrorIs(t, err, iceberg.ErrInvalidFormatVersion)
}

func TestUpgradeFormatVersionUnsupported(t *testing.T) {
	builder := newTestBuilder(t)

	err := builder.UpgradeFormatVersion(2)
	require.Error(t, err)
	require.ErrorIs(t, err, iceberg.ErrInvalidFormatVersion)
}

func TestSetProperties(t *testing.T) {
	builder := newTestBuilder(t)

	err := builder.SetProperties(map[string]string{
		"key1": "value1",
		"key2": "value2",
	})
	require.NoError(t, err)

	err = builder.SetProperties(map[string]string{
		"key1": "updated",
	})
	require.NoError(t, err)

	err = builder.SetProperties(map[string]string{})
	require.NoError(t, err)
}

func TestRemoveProperties(t *testing.T) {
	builder := newTestBuilder(t)

	err := builder.SetProperties(map[string]string{
		"key1": "value1",
		"key2": "value2",
	})
	require.NoError(t, err)

	err = builder.RemoveProperties([]string{"key1"})
	require.NoError(t, err)

	err = builder.RemoveProperties([]string{})
	require.NoError(t, err)
}

func TestNewViewMetadataBuilder(t *testing.T) {
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)
	require.NotNil(t, builder)
}

func TestNewViewMetadataBuilderUnsupportedVersion(t *testing.T) {
	_, err := view.NewMetadataBuilder(0)
	require.Error(t, err)
	require.ErrorIs(t, err, iceberg.ErrInvalidFormatVersion)

	_, err = view.NewMetadataBuilder(2)
	require.Error(t, err)
	require.ErrorIs(t, err, iceberg.ErrInvalidFormatVersion)
}

func TestViewMetadataBuilderFrom(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	version := view.Version{
		VersionID:        1,
		SchemaID:         0,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}

	baseMetadata := &testMetadata{
		uuid:           "550e8400-e29b-41d4-a716-446655440000",
		formatVersion:  1,
		location:       "s3://bucket/view",
		schemas:        []*iceberg.Schema{schema},
		currentVersion: &version,
		versions:       []view.Version{version},
		versionLog:     []view.VersionLogEntry{{TimestampMs: 1234567890, VersionID: 1}},
		properties:     map[string]string{"key": "value"},
	}

	builder := view.MetadataBuilderFrom(baseMetadata)
	require.NotNil(t, builder)
}

func TestAssignUUID(t *testing.T) {
	builder := newTestBuilder(t)

	err := builder.AssignUUID("550e8400-e29b-41d4-a716-446655440001")
	require.NoError(t, err)

	err = builder.AssignUUID("550e8400-e29b-41d4-a716-446655440001")
	require.NoError(t, err)
}

func TestAssignUUIDEmpty(t *testing.T) {
	builder := newTestBuilder(t)
	requireBuilderError(t, builder.AssignUUID(""), "cannot set uuid to empty string")
}

func TestAssignUUIDInvalidFormat(t *testing.T) {
	builder := newTestBuilder(t)
	requireBuilderError(t, builder.AssignUUID("not-a-valid-uuid"), "invalid uuid format")
}

func TestAssignUUIDReassignment(t *testing.T) {
	builder := newTestBuilder(t)

	err := builder.AssignUUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	require.NoError(t, err)

	requireBuilderError(t, builder.AssignUUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8"), "cannot reassign uuid")
}

func TestSetLocation(t *testing.T) {
	builder := newTestBuilder(t)

	err := builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)

	err = builder.SetLocation("s3://bucket/view")
	require.NoError(t, err)
}

func TestSetLocationEmpty(t *testing.T) {
	builder := newTestBuilder(t)

	err := builder.SetLocation("")
	require.Error(t, err)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

// newTestBuilder creates a builder with standard test setup.
func newTestBuilder(t *testing.T) *view.MetadataBuilder {
	t.Helper()
	builder, err := view.NewMetadataBuilder(1)
	require.NoError(t, err)

	return builder
}

// newBuilderWithSchema creates a builder with a standard schema added.
func newBuilderWithSchema(t *testing.T) (*view.MetadataBuilder, *iceberg.Schema) {
	t.Helper()
	builder := newTestBuilder(t)
	schema := testSchema()
	require.NoError(t, builder.AddSchema(schema))

	return builder, schema
}

// newBuilderWithVersion creates a fully-setup builder with schema and version.
func newBuilderWithVersion(t *testing.T) (*view.MetadataBuilder, *view.Version) {
	t.Helper()
	builder, schema := newBuilderWithSchema(t)
	version := testVersion(schema.ID)
	require.NoError(t, builder.AddVersion(version))

	return builder, version
}

// testSchema returns a standard test schema.
func testSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
}

// testSchemaWithID returns a schema with a specific ID.
func testSchemaWithID(id int) *iceberg.Schema {
	return iceberg.NewSchema(id,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
}

// testVersion returns a standard test version.
func testVersion(schemaID int) *view.Version {
	return &view.Version{
		VersionID:        1,
		SchemaID:         schemaID,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: "SELECT * FROM t", Dialect: "spark"}},
		DefaultNamespace: []string{"default"},
	}
}

// testVersionWithSQL creates a version with custom SQL and dialect.
func testVersionWithSQL(schemaID int, versionID int64, sql string, dialect string) *view.Version {
	return &view.Version{
		VersionID:        versionID,
		SchemaID:         schemaID,
		TimestampMs:      1234567890,
		Summary:          map[string]string{"operation": "create"},
		Representations:  []view.SQLRepresentation{{Type: "sql", SQL: sql, Dialect: dialect}},
		DefaultNamespace: []string{"default"},
	}
}

// requireBuilderError is a helper for common error assertion pattern.
func requireBuilderError(t *testing.T, err error, contains string) {
	t.Helper()
	require.Error(t, err)
	require.Contains(t, err.Error(), contains)
}

// buildAndRequireNoError builds and asserts no error.
func buildAndRequireNoError(t *testing.T, builder *view.MetadataBuilder) *view.ViewMetadataBuildResult {
	t.Helper()
	result, err := builder.Build()
	require.NoError(t, err)
	require.NotNil(t, result)

	return result
}

// Mock types for testing

type testMetadata struct {
	uuid           string
	formatVersion  int
	location       string
	schemas        []*iceberg.Schema
	currentVersion *view.Version
	versions       []view.Version
	versionLog     []view.VersionLogEntry
	properties     map[string]string
}

func (m *testMetadata) ViewUUID() string               { return m.uuid }
func (m *testMetadata) FormatVersion() int             { return m.formatVersion }
func (m *testMetadata) Location() string               { return m.location }
func (m *testMetadata) CurrentVersion() *view.Version  { return m.currentVersion }
func (m *testMetadata) Properties() iceberg.Properties { return m.properties }

func (m *testMetadata) Schemas() iter.Seq[*iceberg.Schema] {
	return func(yield func(*iceberg.Schema) bool) {
		for _, s := range m.schemas {
			if !yield(s) {
				return
			}
		}
	}
}

func (m *testMetadata) Versions() iter.Seq[view.Version] {
	return func(yield func(view.Version) bool) {
		for _, v := range m.versions {
			if !yield(v) {
				return
			}
		}
	}
}

func (m *testMetadata) VersionLog() iter.Seq[view.VersionLogEntry] {
	return func(yield func(view.VersionLogEntry) bool) {
		for _, e := range m.versionLog {
			if !yield(e) {
				return
			}
		}
	}
}
