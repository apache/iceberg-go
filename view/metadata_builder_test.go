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
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view/internal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestBuilder() *MetadataBuilder {
	b, _ := NewMetadataBuilder()

	return b
}

func newTestVersion(versionID int64, schemaID int, opts ...VersionOpt) *Version {
	version, _ := NewVersion(
		versionID,
		schemaID,
		Representations{NewRepresentation("select * from table", "sql")},
		table.Identifier{"defaultns"},
		opts...)

	return version
}

func newTestVersionWithSQL(versionID int64, schemaID int, sql string, opts ...VersionOpt) *Version {
	version, _ := NewVersion(
		versionID,
		schemaID,
		Representations{NewRepresentation(sql, "spark")},
		table.Identifier{"defaultns"},
		opts...)

	return version
}

func newTestSchema(schemaID int, optFieldName ...string) *iceberg.Schema {
	fieldName := "x"
	if len(optFieldName) > 0 {
		fieldName = optFieldName[0]
	}

	return iceberg.NewSchema(schemaID, iceberg.NestedField{ID: 1, Name: fieldName, Type: iceberg.PrimitiveTypes.Int64})
}

func stripErr[T any](v T, _ error) T {
	return v
}

func extractVersionID(v *Version) int64 {
	return v.VersionID
}

func TestBuild_NullAndMissingFields(t *testing.T) {
	// Empty location
	md, err := newTestBuilder().Build()
	assert.Nil(t, md)
	assert.ErrorContains(t, err, "location is required")

	// Missing Versions
	md, err = newTestBuilder().SetLoc("location").Build()
	assert.Nil(t, md)
	assert.ErrorContains(t, err, "at least one version is required")

	// Attempted setting to missing version ID
	_, err = newTestBuilder().SetLoc("location").SetCurrentVersionID(1).Build()
	assert.ErrorContains(t, err, "cannot set current version to unknown version with id 1")

	// Invalid UUID
	_, err = newTestBuilder().SetUUID(uuid.Nil).Build()
	assert.ErrorContains(t, err, "cannot set uuid to null")
}

func TestSetFormatVersion_Invalid(t *testing.T) {
	// Downgrade
	_, err := newTestBuilder().SetFormatVersion(0).Build()
	assert.ErrorContains(t, err, fmt.Sprintf("downgrading format version from %d to %d is not allowed", DefaultViewFormatVersion, 0))

	// Invalid upgrade
	_, err = newTestBuilder().SetFormatVersion(SupportedViewFormatVersion + 1).Build()
	assert.ErrorContains(t, err, fmt.Sprintf("unsupported format version %d", SupportedViewFormatVersion+1))
}

func TestAddVersion_EmptySchemas(t *testing.T) {
	_, err := newTestBuilder().AddVersion(newTestVersion(1, 1)).Build()
	assert.ErrorContains(t, err, "cannot add version with unknown schema")
}

func TestSetCurrentVersionID_LastAdded(t *testing.T) {
	testSchemaID := 1
	md, err := newTestBuilder().
		SetLoc("location").
		AddSchema(newTestSchema(testSchemaID)).
		AddVersion(newTestVersion(1, LastAddedID)).
		SetCurrentVersionID(LastAddedID).
		Build()

	assert.NotNil(t, md)
	assert.NoError(t, err)
}

func TestSetCurrentVersionID_Invalid(t *testing.T) {
	_, err := newTestBuilder().
		AddSchema(newTestSchema(0)).
		AddVersion(newTestVersion(1, 0)).
		SetCurrentVersionID(23).
		Build()
	assert.ErrorContains(t, err, "cannot set current version to unknown version with id")
}

func TestAddVersion_InvalidSchemaID(t *testing.T) {
	testSchemaID := 1
	_, err := newTestBuilder().
		AddSchema(newTestSchema(testSchemaID)).
		AddVersion(newTestVersion(1, 23)).
		Build()
	assert.ErrorContains(t, err, "cannot add version with unknown schema: 23")
}

func TestViewVersionHistory_MaintainsCorrectTimeline(t *testing.T) {
	v1 := newTestVersion(1, 0, WithTimestampMS(1000))
	v2 := newTestVersion(2, 0, WithTimestampMS(2000))
	v2.Representations[0].Dialect = "differentdialect"

	// Build metadata for version V1 as current
	viewMD, err := newTestBuilder().
		SetLoc("location").
		SetProperties(iceberg.Properties{ReplaceDropDialectAllowedKey: "true"}).
		AddSchema(newTestSchema(0)).
		AddVersion(v1).
		AddVersion(v2).
		SetCurrentVersionID(1).
		Build()
	require.NoError(t, err)

	// Build updated for v2 as current
	builder, err := MetadataBuilderFromBase(viewMD)
	require.NoError(t, err)
	timeBeforeAdd := time.Now().UnixMilli()
	updatedMD, err := builder.SetCurrentVersionID(2).Build()
	require.NoError(t, err)

	require.Len(t, updatedMD.VersionLog(), 2)
	assert.Equal(t, VersionLogEntry{VersionID: 1, TimestampMS: 1000}, updatedMD.VersionLog()[0])
	// Since second build updated current version to a previously added version, it should have used
	// system time
	assert.EqualValues(t, 2, updatedMD.VersionLog()[1].VersionID)
	assert.GreaterOrEqual(t, updatedMD.VersionLog()[1].TimestampMS, timeBeforeAdd)

	// Add third version and set current version, it should use the latest timestamp
	v3 := newTestVersion(3, 0, WithTimestampMS(3000))
	v3.Representations[0].Dialect = "otherotherdialect"
	builder, err = MetadataBuilderFromBase(updatedMD)
	require.NoError(t, err)
	v3MD, err := builder.AddVersion(v3).SetCurrentVersionID(3).Build()
	require.NoError(t, err)
	// Should have final version history entry with timestamp 3000
	expectedLog := append(slices.Clone(updatedMD.VersionLog()), VersionLogEntry{VersionID: 3, TimestampMS: 3000})
	assert.Equal(t, expectedLog, v3MD.VersionLog())
}

func TestViewMetadataAndUpdates(t *testing.T) {
	// Test schemas
	s1 := newTestSchema(0, "x")
	s2 := newTestSchema(1, "y")

	// Test versions
	v1 := newTestVersionWithSQL(1, 0, "select * from ns.tbl")
	v2 := newTestVersionWithSQL(2, 0, "select count(*) from ns.tbl")
	v3 := newTestVersionWithSQL(3, 1, "select count(*) as count from ns.tbl")

	// Build metadata
	uuid_ := uuid.New()
	props := iceberg.Properties{"k1": "v1", "k2": "v2"}
	md, err := newTestBuilder().
		SetUUID(uuid_).
		SetLoc("location").
		SetProperties(props).
		AddSchema(s1).
		AddSchema(s2).
		AddVersion(v1).
		AddVersion(v2).
		AddVersion(v3).
		SetCurrentVersionID(3).
		Build()
	require.NoError(t, err)

	// Metadata properties
	assert.Len(t, md.Versions(), 3)
	assert.Equal(t, []*Version{v1, v2, v3}, md.Versions())
	assert.Len(t, md.VersionLog(), 1)
	assert.EqualValues(t, md.CurrentVersionID(), 3)
	assert.Equal(t, *md.CurrentVersion(), *v3)
	assert.Equal(t, props, md.Properties())
	assert.Equal(t, "location", md.Location())
	assert.Equal(t, []*iceberg.Schema{s1, s2}, md.Schemas())
	assert.Equal(t, s2.ID, md.CurrentSchemaID())

	// Updates
	require.Len(t, md.Changes, 9)

	assert.Equal(t, NewAssignUUIDUpdate(uuid_), md.Changes[0])
	assert.Equal(t, NewSetLocationUpdate("location"), md.Changes[1])
	assert.Equal(t, NewSetPropertiesUpdate(props), md.Changes[2])
	assert.Equal(t, NewAddSchemaUpdate(s1), md.Changes[3])
	assert.Equal(t, NewAddSchemaUpdate(s2), md.Changes[4])
	assert.Equal(t, NewAddViewVersionUpdate(v1), md.Changes[5])
	assert.Equal(t, NewAddViewVersionUpdate(v2), md.Changes[6])
	// We use last added ID (-1) where applicable
	expectedV3 := v3.Clone()
	expectedV3.SchemaID = LastAddedID
	assert.Equal(t, NewAddViewVersionUpdate(expectedV3), md.Changes[7])
	assert.Equal(t, NewSetCurrentVersionUpdate(LastAddedID), md.Changes[8])
}

func TestSetUUID(t *testing.T) {
	uuid_ := uuid.New()
	md, err := newTestBuilder().
		SetUUID(uuid_).
		SetLoc("location").
		AddSchema(newTestSchema(0)).
		AddVersion(newTestVersion(1, 0)).
		SetCurrentVersionID(1).
		Build()
	require.NoError(t, err)
	assert.Equal(t, uuid_, md.ViewUUID())

	// Should carry over to noop rebuild.
	updatedBuilder, err := MetadataBuilderFromBase(md)
	require.NoError(t, err)
	updatedMD, err := updatedBuilder.Build()
	require.NoError(t, err)
	assert.Equal(t, uuid_, updatedMD.ViewUUID())
	assert.Empty(t, updatedMD.Changes)

	// Reassignment to same UUID should be a noop with no changes
	updatedBuilder, err = MetadataBuilderFromBase(md)
	require.NoError(t, err)
	updatedMD, err = updatedBuilder.SetUUID(uuid_).Build()
	require.NoError(t, err)
	assert.Equal(t, uuid_, updatedMD.ViewUUID())
	assert.Empty(t, updatedMD.Changes)

	// Reassignment to different UUID should fail
	updatedBuilder, err = MetadataBuilderFromBase(md)
	require.NoError(t, err)
	_, err = updatedBuilder.SetUUID(uuid.New()).Build()
	require.ErrorContains(t, err, "cannot reassign uuid")
}

func TestAddVersion_IDReassignment(t *testing.T) {
	v1 := newTestVersionWithSQL(1, 0, "select * from ns.tbl")
	v2 := newTestVersionWithSQL(1, 0, "select count(*) from ns.tbl")
	v3 := newTestVersionWithSQL(1, 0, "select count(*) as count from ns.tbl")
	md, err := newTestBuilder().
		SetLoc("location").
		AddSchema(newTestSchema(0)).
		AddVersion(v1).
		AddVersion(v2).
		AddVersion(v3).
		SetCurrentVersionID(3).
		Build()
	require.NoError(t, err)

	// IDs should be mutated
	expectedV2 := v2.Clone()
	expectedV2.VersionID++
	expectedV3 := v3.Clone()
	expectedV3.VersionID += 2
	assert.Equal(t, expectedV3, md.CurrentVersion())
	assert.Equal(t, []*Version{v1, expectedV2, expectedV3}, md.Versions())
}

func TestAddVersion_Deduplication(t *testing.T) {
	v1 := newTestVersionWithSQL(1, 0, "select * from ns.tbl")
	v2 := newTestVersionWithSQL(1, 0, "select count(*) from ns.tbl")
	v3 := newTestVersionWithSQL(1, 0, "select count(*) as count from ns.tbl")
	v1Updated := newTestVersionWithSQL(1, 0, "select * from ns.tbl", WithTimestampMS(1000))
	v2Updated := newTestVersionWithSQL(1, 0, "select count(*) from ns.tbl", WithTimestampMS(100))
	v3Updated := newTestVersionWithSQL(1, 0, "select count(*) as count from ns.tbl", WithTimestampMS(10))
	md, err := newTestBuilder().
		SetLoc("location").
		AddSchema(newTestSchema(0)).
		AddVersion(v1).
		AddVersion(v2).
		AddVersion(v3).
		AddVersion(v1Updated).
		AddVersion(v2Updated).
		AddVersion(v3Updated).
		SetCurrentVersionID(3).
		Build()
	require.NoError(t, err)

	// Should retain original unique versions with updated IDs
	expectedV2 := v2.Clone()
	expectedV2.VersionID = 2
	expectedV3 := v3.Clone()
	expectedV3.VersionID = 3
	assert.Equal(t, expectedV3, md.CurrentVersion())
	assert.Equal(t, []*Version{v1, expectedV2, expectedV3}, md.Versions())
}

func TestAddVersion_DeduplicationCustomSummary(t *testing.T) {
	v1 := newTestVersionWithSQL(1, 0, "select * from ns.tbl", WithTimestampMS(0))
	v2 := newTestVersionWithSQL(1, 0, "select count(*) from ns.tbl", WithTimestampMS(0))
	v1Updated := newTestVersionWithSQL(1, 0, "select * from ns.tbl", WithTimestampMS(1000))
	v2Updated := newTestVersionWithSQL(1, 0, "select count(*) from ns.tbl",
		WithTimestampMS(0),
		WithVersionSummary(VersionSummary{"key": "val"}),
	)
	md, err := newTestBuilder().
		SetLoc("location").
		AddSchema(newTestSchema(0)).
		AddVersion(v1).
		AddVersion(v2).
		AddVersion(v1Updated).
		AddVersion(v2Updated).
		SetCurrentVersionID(3).
		Build()
	require.NoError(t, err)

	// Should retain original unique versions with updated IDs
	expectedV2 := v2.Clone()
	expectedV2.VersionID = 2
	expectedUpdatedV2 := v2Updated.Clone()
	expectedUpdatedV2.VersionID = 3
	assert.Equal(t, expectedUpdatedV2, md.CurrentVersion())
	assert.Equal(t, []*Version{v1, expectedV2, expectedUpdatedV2}, md.Versions())
}

func TestSchemaIDReassignment(t *testing.T) {
	s1 := newTestSchema(3, "x")
	s2 := newTestSchema(6, "y")
	s3 := newTestSchema(9, "z")

	version := newTestVersion(1, 0)
	md, err := newTestBuilder().
		SetLoc("location").
		AddSchema(s1).
		AddSchema(s2).
		AddSchema(s3).
		SetCurrentVersion(version, s3).
		Build()
	require.NoError(t, err)

	// Schema ID should be reassigned
	expectedVersion := version.Clone()
	expectedVersion.SchemaID = 2
	assert.Equal(t, []*Version{expectedVersion}, md.Versions())

	// All schema IDs should be re-assigned and start at 0
	assert.Equal(t,
		[]iceberg.StructType{s1.AsStruct(), s2.AsStruct(), s3.AsStruct()},
		internal.MapSlice(md.Schemas(), func(s *iceberg.Schema) iceberg.StructType { return s.AsStruct() }),
	)
	assert.Equal(t, slices.Sorted(maps.Keys(md.SchemasByID())), []int{0, 1, 2})
}

func TestSchemaDeduplication(t *testing.T) {
	// Initial unique schemas
	s1 := newTestSchema(3, "x")
	s2 := newTestSchema(6, "y")
	s3 := newTestSchema(9, "z")
	// Duplicates with different IDs
	s1Dupe := newTestSchema(4, "x")
	s2Dupe := newTestSchema(7, "y")
	s3Dupe := newTestSchema(10, "z")

	version := newTestVersion(1, 0)
	md, err := newTestBuilder().
		SetLoc("location").
		AddSchema(s1).
		AddSchema(s2).
		AddSchema(s3).
		AddSchema(s1Dupe).
		AddSchema(s2Dupe).
		AddSchema(s3Dupe).
		SetCurrentVersion(version, s3).
		Build()
	require.NoError(t, err)

	// Schema ID should be reassigned
	expectedVersion := version.Clone()
	expectedVersion.SchemaID = 2
	assert.Equal(t, []*Version{expectedVersion}, md.Versions())

	// All schema IDs should be re-assigned and start at 0
	assert.Equal(t,
		[]iceberg.StructType{s1.AsStruct(), s2.AsStruct(), s3.AsStruct()},
		internal.MapSlice(md.Schemas(), func(s *iceberg.Schema) iceberg.StructType { return s.AsStruct() }),
	)
	assert.Equal(t, slices.Sorted(maps.Keys(md.SchemasByID())), []int{0, 1, 2})
}

func TestViewVersionAndSchemaIDReassignment(t *testing.T) {
	s1 := newTestSchema(3, "x")
	s2 := newTestSchema(6, "y")
	s3 := newTestSchema(9, "z")

	v1 := newTestVersionWithSQL(1, 3, "select * from ns.tbl")
	v2 := newTestVersionWithSQL(1, 6, "select count(*) from ns.tbl")
	v3 := newTestVersionWithSQL(1, 9, "select count(*) as count from ns.tbl")
	md, err := newTestBuilder().
		SetLoc("location").
		AddSchema(s1).
		AddSchema(s2).
		AddSchema(s3).
		SetCurrentVersion(v1, s1).
		SetCurrentVersion(v2, s2).
		SetCurrentVersion(v3, s3).
		Build()
	require.NoError(t, err)

	// All versions should have reassigned IDs and use reassigned schema IDs
	expectedV1 := v1.Clone()
	expectedV1.SchemaID = 0
	expectedV2 := v2.Clone()
	expectedV2.VersionID = 2
	expectedV2.SchemaID = 1
	expectedV3 := v3.Clone()
	expectedV3.VersionID = 3
	expectedV3.SchemaID = 2

	assert.Equal(t, expectedV3, md.CurrentVersion())
	assert.Equal(t, []*Version{expectedV1, expectedV2, expectedV3}, md.Versions())

	// All schema IDs should be re-assigned and start at 0
	assert.Equal(t,
		[]iceberg.StructType{s1.AsStruct(), s2.AsStruct(), s3.AsStruct()},
		internal.MapSlice(md.Schemas(), func(s *iceberg.Schema) iceberg.StructType { return s.AsStruct() }),
	)
	assert.Equal(t, slices.Sorted(maps.Keys(md.SchemasByID())), []int{0, 1, 2})
}

func TestAddVersion_MultipleSQLForSameDialect(t *testing.T) {
	_, err := newTestBuilder().
		SetLoc("location").
		AddSchema(newTestSchema(0)).
		AddVersion(
			stripErr(NewVersion(1, 0,
				[]Representation{
					NewRepresentation("select * from ns.tbl", "spark"),
					NewRepresentation("select * from ns.tbl3", "SpArK"),
				},
				table.Identifier{"ns"},
			)),
		).
		SetCurrentVersionID(1).
		Build()
	assert.ErrorContains(t, err, "Invalid view version: Cannot add multiple queries for dialect spark")
}

func TestSetCurrentVersionID_NoLastAddedSchema(t *testing.T) {
	_, err := newTestBuilder().
		AddVersion(newTestVersion(1, LastAddedID)).
		SetCurrentVersionID(1).
		Build()
	assert.ErrorContains(t, err, "cannot set last added schema: no schema has been added")
}

func TestDeduplicationViewVersionByIDAndSchemaID(t *testing.T) {
	s1 := newTestSchema(1, "x")
	s2 := newTestSchema(2, "y")

	v1 := newTestVersionWithSQL(1, 0, "select * from ns.tbl")
	v2 := newTestVersionWithSQL(2, 1, "select x from ns.tbl")
	v3 := newTestVersionWithSQL(2, -1, "select count(*) from ns.tbl")

	md, err := newTestBuilder().
		SetLoc("location").
		AddSchema(s1).
		AddSchema(s2).
		AddVersion(v1).
		AddVersion(v2).
		AddVersion(v3).
		SetCurrentVersionID(3).
		Build()
	require.NoError(t, err)

	assert.Len(t, md.Versions(), 3)
	assert.EqualValues(t, md.CurrentVersion().VersionID, 3)
	assert.Equal(t, md.CurrentSchema().ID, 1)
}

func TestDroppingDialectFailsByDefault(t *testing.T) {
	// Build initial metadata with two dialects
	schema := newTestSchema(0, "x")
	version, _ := NewVersion(1, 0,
		Representations{
			NewRepresentation("select \"nested\".\"field\" from tbl", "trino"),
			NewRepresentation("select `nested`.`field` from tbl", "spark"),
		},
		table.Identifier{"defaultns"},
	)
	builder, err := NewMetadataBuilder()
	require.NoError(t, err)
	md, err := builder.SetLoc("location").
		AddSchema(schema).
		SetCurrentVersion(version, schema).
		Build()
	require.NoError(t, err)

	builder, err = MetadataBuilderFromBase(md)
	require.NoError(t, err)
	_, err = builder.
		SetCurrentVersion(newTestVersionWithSQL(1, 0, "select `nested`.`field` from tbl"), schema).
		Build()
	assert.ErrorContains(t, err, "dropping dialects is not enabled for this view")
}

func TestDroppingDialectDoesNotFailWhenAllowed(t *testing.T) {
	// Build initial metadata with two dialects
	schema := newTestSchema(0, "x")
	version, _ := NewVersion(1, 0,
		Representations{
			NewRepresentation("select \"nested\".\"field\" from tbl", "trino"),
			NewRepresentation("select `nested`.`field` from tbl", "spark"),
		},
		table.Identifier{"defaultns"},
	)
	builder, err := NewMetadataBuilder()
	require.NoError(t, err)
	md, err := builder.SetLoc("location").
		AddSchema(schema).
		SetCurrentVersion(version, schema).
		SetProperties(iceberg.Properties{ReplaceDropDialectAllowedKey: "true"}).
		Build()
	require.NoError(t, err)

	builder, err = MetadataBuilderFromBase(md)
	require.NoError(t, err)
	_, err = builder.
		SetCurrentVersion(newTestVersionWithSQL(1, 0, "select `nested`.`field` from tbl"), schema).
		Build()
	require.NoError(t, err)
}

func TestCurrentViewVersionIsNeverExpired(t *testing.T) {
	props := iceberg.Properties{VersionHistorySizeKey: "1"}
	schema := newTestSchema(0)
	v1 := newTestVersionWithSQL(1, 0, "select * from ns.tbl")
	v2 := newTestVersionWithSQL(2, 0, "select count(*) from ns.tbl")
	v3 := newTestVersionWithSQL(3, 0, "select count(*) as count from ns.tbl")
	md, err := stripErr(NewMetadataBuilder()).
		SetLoc("location").
		SetProperties(props).
		AddSchema(schema).
		AddVersion(v1).
		AddVersion(v2).
		AddVersion(v3).
		SetCurrentVersionID(1).
		Build()
	require.NoError(t, err)

	// the first build will not expire versions that were added in the builder
	assert.Len(t, md.Versions(), 3)
	assert.Len(t, md.VersionLog(), 1)
	assert.EqualValues(t, 1, md.VersionLog()[0].VersionID)

	// rebuild the metadata to expire older versions
	updatedMD, err := stripErr(MetadataBuilderFromBase(md)).Build()
	require.NoError(t, err)
	assert.Len(t, updatedMD.Versions(), 1)

	// make sure history and current version are retained
	assert.EqualValues(t, 1, updatedMD.CurrentVersionID())
	assert.True(t, md.CurrentVersion().Equals(updatedMD.CurrentVersion()))
	assert.Len(t, updatedMD.VersionLog(), 1)
	assert.EqualValues(t, 1, updatedMD.VersionLog()[0].VersionID)
}

func TestViewVersionHistoryIsCorrectlyRetained(t *testing.T) {
	props := iceberg.Properties{VersionHistorySizeKey: "2"}
	schema := newTestSchema(0)
	v1 := newTestVersionWithSQL(1, 0, "select * from ns.tbl")
	v2 := newTestVersionWithSQL(2, 0, "select count(*) from ns.tbl")
	v3 := newTestVersionWithSQL(3, 0, "select count(*) as count from ns.tbl")
	md, err := stripErr(NewMetadataBuilder()).
		SetLoc("location").
		SetProperties(props).
		AddSchema(schema).
		AddVersion(v1).
		AddVersion(v2).
		AddVersion(v3).
		SetCurrentVersionID(3).
		Build()
	require.NoError(t, err)

	// the first build will not expire versions that were added in the builder
	assert.Len(t, md.Versions(), 3)
	assert.Len(t, md.VersionLog(), 1)
	assert.EqualValues(t, 3, md.VersionLog()[0].VersionID)

	// rebuild the metadata to expire older versions
	updatedMD, err := stripErr(MetadataBuilderFromBase(md)).Build()
	require.NoError(t, err)
	assert.Len(t, updatedMD.Versions(), 2)
	assert.Equal(t, internal.ToSet([]int64{2, 3}), internal.ToSet(internal.MapSlice(updatedMD.Versions(), extractVersionID)))

	// make sure history and current version are retained
	assert.EqualValues(t, 3, updatedMD.CurrentVersionID())
	assert.True(t, md.CurrentVersion().Equals(updatedMD.CurrentVersion()))
	assert.Len(t, updatedMD.VersionLog(), 1)
	assert.EqualValues(t, 3, updatedMD.VersionLog()[0].VersionID)

	// Rebuild and set version ID to 2
	updatedMD, err = stripErr(MetadataBuilderFromBase(md)).SetCurrentVersionID(2).Build()
	require.NoError(t, err)
	assert.Len(t, updatedMD.Versions(), 2)
	assert.Equal(t, internal.ToSet([]int64{2, 3}), internal.ToSet(internal.MapSlice(updatedMD.Versions(), extractVersionID)))
	assert.Len(t, updatedMD.VersionLog(), 2)
	assert.EqualValues(t, 3, updatedMD.VersionLog()[0].VersionID)
	assert.EqualValues(t, 2, updatedMD.VersionLog()[1].VersionID)

	// Rebuild and set version ID back to 3
	updatedMD, err = stripErr(MetadataBuilderFromBase(updatedMD)).SetCurrentVersionID(3).Build()
	require.NoError(t, err)
	assert.Len(t, updatedMD.Versions(), 2)
	assert.Equal(t, internal.ToSet([]int64{2, 3}), internal.ToSet(internal.MapSlice(updatedMD.Versions(), extractVersionID)))
	assert.Len(t, updatedMD.VersionLog(), 3)
	assert.EqualValues(t, 3, updatedMD.VersionLog()[0].VersionID)
	assert.EqualValues(t, 2, updatedMD.VersionLog()[1].VersionID)
	assert.EqualValues(t, 3, updatedMD.VersionLog()[2].VersionID)

	// This should now fail since we've expired out version 1
	_, err = stripErr(MetadataBuilderFromBase(updatedMD)).SetCurrentVersionID(1).Build()
	assert.ErrorContains(t, err, "cannot set current version to unknown version")
}
