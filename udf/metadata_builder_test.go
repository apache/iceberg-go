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

package udf

import (
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestDefinition(t *testing.T) *Definition {
	t.Helper()
	def, err := NewDefinition(FunctionTypeUDF,
		[]Parameter{{Name: "x", Type: mustPrimitive(t, "int"), Doc: "Input integer"}},
		mustPrimitive(t, "int"),
		WithDefinitionDoc("Add one to the input integer"))
	require.NoError(t, err)

	return def
}

func newTestVersion(t *testing.T, sql string, timestampMS int64) *DefinitionVersion {
	t.Helper()
	repr, err := NewSQLRepresentation("trino", sql)
	require.NoError(t, err)
	version, err := NewDefinitionVersion([]Representation{repr},
		WithTimestampMS(timestampMS), WithDeterministic(true))
	require.NoError(t, err)

	return version
}

func TestNewDefinition(t *testing.T) {
	def := newTestDefinition(t)
	assert.Equal(t, "int", def.DefinitionID)
	assert.Equal(t, -1, def.CurrentVersionID)
	assert.Empty(t, def.Versions)

	multi, err := NewDefinition(FunctionTypeUDF,
		[]Parameter{
			{Name: "a", Type: mustPrimitive(t, "int")},
			{Name: "b", Type: ListType{Element: mustPrimitive(t, "int")}},
		},
		mustPrimitive(t, "long"),
		WithSpecificName("sum_list"), WithReturnNullable(false))
	require.NoError(t, err)
	assert.Equal(t, "int,list<int>", multi.DefinitionID)
	assert.Equal(t, "sum_list", multi.SpecificName)
	assert.False(t, multi.ReturnsNullable())

	_, err = NewDefinition(FunctionTypeUDF, []Parameter{{Name: "x", Type: nil}}, mustPrimitive(t, "int"))
	assert.ErrorIs(t, err, ErrInvalidUDFType)
}

func TestNewDefinitionVersion(t *testing.T) {
	version := newTestVersion(t, "x + 1", 1000)
	assert.Equal(t, 0, version.VersionID, "version id is assigned when added to a definition")
	assert.Equal(t, int64(1000), version.TimestampMS)
	assert.True(t, version.Deterministic)

	_, err := NewDefinitionVersion(nil)
	assert.ErrorIs(t, err, ErrInvalidUDFMetadata)

	_, err = NewDefinitionVersion([]Representation{nil})
	assert.ErrorContains(t, err, "representations must not contain nil entries")
}

// TestBuildRejectsNilRepresentation ensures a hand-built version carrying a
// nil representation fails validation at Build instead of marshaling null.
func TestBuildRejectsNilRepresentation(t *testing.T) {
	builder, err := NewMetadataBuilder()
	require.NoError(t, err)

	_, err = builder.
		AddDefinition(newTestDefinition(t)).
		AddDefinitionVersion("int", &DefinitionVersion{
			Representations: []Representation{nil},
			TimestampMS:     1000,
		}).
		Build()
	require.ErrorIs(t, err, ErrInvalidUDFMetadata)
	assert.ErrorContains(t, err, "representations must not contain null entries")
}

func TestBuildNewMetadata(t *testing.T) {
	builder, err := NewMetadataBuilder()
	require.NoError(t, err)

	meta, err := builder.
		SetLoc("s3://bucket/functions/add_one").
		SetDoc("Adds one").
		SetSecure(true).
		SetProperties(iceberg.Properties{"owner": "iceberg-go"}).
		AddDefinition(newTestDefinition(t)).
		AddDefinitionVersion("int", newTestVersion(t, "x + 1", 1000)).
		SetLogTimestampMS(2000).
		Build()
	require.NoError(t, err)

	assert.Equal(t, SupportedUDFFormatVersion, meta.FormatVersion())
	assert.NotEqual(t, uuid.Nil, meta.FunctionUUID(), "a uuid is generated when none is set")
	assert.Equal(t, "s3://bucket/functions/add_one", meta.Location())
	assert.Equal(t, "Adds one", meta.Doc())
	assert.True(t, meta.Secure())
	assert.Equal(t, iceberg.Properties{"owner": "iceberg-go"}, meta.Properties())

	def, ok := meta.DefinitionByID("int")
	require.True(t, ok)
	assert.Equal(t, initialVersionID, def.CurrentVersionID)
	require.Len(t, def.Versions, 1)
	assert.Equal(t, initialVersionID, def.Versions[0].VersionID)

	log := meta.DefinitionLog()
	require.Len(t, log, 1)
	assert.Equal(t, int64(2000), log[0].TimestampMS)
	assert.Equal(t, []DefinitionVersionRef{{DefinitionID: "int", VersionID: 1}}, log[0].DefinitionVersions)

	// built metadata round-trips through JSON
	data, err := json.Marshal(meta)
	require.NoError(t, err)
	again, err := ParseMetadataBytes(data)
	require.NoError(t, err)
	assert.True(t, meta.Equals(again))
}

func TestBuildSetUUID(t *testing.T) {
	functionID := uuid.New()

	builder, err := NewMetadataBuilder()
	require.NoError(t, err)

	meta, err := builder.
		SetUUID(functionID).
		AddDefinition(newTestDefinition(t)).
		AddDefinitionVersion("int", newTestVersion(t, "x + 1", 1000)).
		Build()
	require.NoError(t, err)
	assert.Equal(t, functionID, meta.FunctionUUID())
}

func TestBuildRequiresCompleteDefinition(t *testing.T) {
	builder, err := NewMetadataBuilder()
	require.NoError(t, err)

	// a definition without any version is incomplete
	_, err = builder.AddDefinition(newTestDefinition(t)).Build()
	assert.ErrorIs(t, err, ErrInvalidUDFMetadata)

	builder, err = NewMetadataBuilder()
	require.NoError(t, err)
	_, err = builder.Build()
	assert.ErrorContains(t, err, "at least one definition is required")
}

func TestBuilderFromBase(t *testing.T) {
	base := parseFixture(t, "udf-metadata-scalar.json")

	builder, err := MetadataBuilderFromBase(base)
	require.NoError(t, err)
	assert.False(t, builder.HasChanges())

	meta, err := builder.
		AddDefinitionVersion("int", newTestVersion(t, "x + 1 + 0", 3000)).
		SetLogTimestampMS(3000).
		Build()
	require.NoError(t, err)
	assert.True(t, builder.HasChanges())

	def, ok := meta.DefinitionByID("int")
	require.True(t, ok)
	assert.Equal(t, 3, def.CurrentVersionID, "new version gets the next id and becomes current")
	require.Len(t, def.Versions, 3)

	log := meta.DefinitionLog()
	require.Len(t, log, 4, "a definition-log entry is appended for the selection change")
	assert.Equal(t, DefinitionLogEntry{
		TimestampMS: 3000,
		DefinitionVersions: []DefinitionVersionRef{
			{DefinitionID: "float", VersionID: 1},
			{DefinitionID: "int", VersionID: 3},
		},
	}, log[3], "the appended entry snapshots every definition, ordered by definition-id")

	// the base metadata is not mutated
	baseDef, ok := base.DefinitionByID("int")
	require.True(t, ok)
	assert.Equal(t, 2, baseDef.CurrentVersionID)
	assert.Len(t, baseDef.Versions, 2)
	assert.Len(t, base.DefinitionLog(), 3)

	_, err = MetadataBuilderFromBase(nil)
	assert.ErrorIs(t, err, ErrInvalidUDFMetadata)
}

// nilPropertiesMetadata simulates a third-party Metadata implementation that
// returns nil Properties, driving the builder's defensive nil-props guard.
type nilPropertiesMetadata struct{ Metadata }

func (nilPropertiesMetadata) Properties() iceberg.Properties { return nil }

func TestBuilderFromBaseNilProperties(t *testing.T) {
	base := nilPropertiesMetadata{parseFixture(t, "udf-metadata-scalar.json")}

	builder, err := MetadataBuilderFromBase(base)
	require.NoError(t, err)

	meta, err := builder.Build()
	require.NoError(t, err)
	assert.NotNil(t, meta.Properties())
	assert.Empty(t, meta.Properties())
}

func TestBuilderNoChanges(t *testing.T) {
	base := parseFixture(t, "udf-metadata-scalar.json")

	builder, err := MetadataBuilderFromBase(base)
	require.NoError(t, err)

	meta, err := builder.Build()
	require.NoError(t, err)
	assert.False(t, builder.HasChanges())
	assert.True(t, base.Equals(meta), "building without changes appends no definition-log entry")
}

func TestBuilderRollback(t *testing.T) {
	base := parseFixture(t, "udf-metadata-scalar.json")

	builder, err := MetadataBuilderFromBase(base)
	require.NoError(t, err)

	meta, err := builder.
		SetCurrentVersion("int", 1).
		SetLogTimestampMS(4000).
		Build()
	require.NoError(t, err)

	def, ok := meta.DefinitionByID("int")
	require.True(t, ok)
	assert.Equal(t, 1, def.CurrentVersionID)
	assert.Len(t, def.Versions, 2, "rollback keeps all versions")

	log := meta.DefinitionLog()
	require.Len(t, log, 4)
	assert.Equal(t, []DefinitionVersionRef{
		{DefinitionID: "float", VersionID: 1},
		{DefinitionID: "int", VersionID: 1},
	}, log[3].DefinitionVersions)
}

func TestBuilderRemoveDefinition(t *testing.T) {
	base := parseFixture(t, "udf-metadata-scalar.json")

	builder, err := MetadataBuilderFromBase(base)
	require.NoError(t, err)

	meta, err := builder.
		RemoveDefinition("float").
		SetLogTimestampMS(5000).
		Build()
	require.NoError(t, err)

	assert.Len(t, meta.Definitions(), 1)
	_, ok := meta.DefinitionByID("float")
	assert.False(t, ok)

	log := meta.DefinitionLog()
	require.Len(t, log, 4)
	assert.Equal(t, []DefinitionVersionRef{{DefinitionID: "int", VersionID: 2}}, log[3].DefinitionVersions)
}

func TestBuilderAddDefinitionConflicts(t *testing.T) {
	builder, err := NewMetadataBuilder()
	require.NoError(t, err)

	builder = builder.
		AddDefinition(newTestDefinition(t)).
		AddDefinition(newTestDefinition(t))
	assert.ErrorContains(t, builder.Err(), "already exists")

	// a mismatched pre-set definition-id is rejected
	builder, err = NewMetadataBuilder()
	require.NoError(t, err)
	def := newTestDefinition(t)
	def.DefinitionID = "float"
	builder = builder.AddDefinition(def)
	assert.ErrorContains(t, builder.Err(), "does not match canonical form")

	// duplicate specific names are rejected across signatures
	builder, err = NewMetadataBuilder()
	require.NoError(t, err)
	first, err := NewDefinition(FunctionTypeUDF,
		[]Parameter{{Name: "x", Type: mustPrimitive(t, "int")}},
		mustPrimitive(t, "int"), WithSpecificName("add_one"))
	require.NoError(t, err)
	second, err := NewDefinition(FunctionTypeUDF,
		[]Parameter{{Name: "x", Type: mustPrimitive(t, "float")}},
		mustPrimitive(t, "float"), WithSpecificName("add_one"))
	require.NoError(t, err)
	builder = builder.AddDefinition(first).AddDefinition(second)
	assert.ErrorContains(t, builder.Err(), `specific-name "add_one" already exists`)
}

func TestBuilderVersionErrors(t *testing.T) {
	builder, err := NewMetadataBuilder()
	require.NoError(t, err)
	builder = builder.AddDefinitionVersion("int", newTestVersion(t, "x + 1", 1000))
	assert.ErrorContains(t, builder.Err(), `no definition with definition-id "int"`)

	builder, err = NewMetadataBuilder()
	require.NoError(t, err)
	explicit := newTestVersion(t, "x + 1", 1000)
	explicit.VersionID = 1
	duplicate := newTestVersion(t, "x + 2", 2000)
	duplicate.VersionID = 1
	builder = builder.
		AddDefinition(newTestDefinition(t)).
		AddDefinitionVersion("int", explicit).
		AddDefinitionVersion("int", duplicate)
	assert.ErrorContains(t, builder.Err(), `already has version-id 1`)

	builder, err = NewMetadataBuilder()
	require.NoError(t, err)
	builder = builder.
		AddDefinition(newTestDefinition(t)).
		AddDefinitionVersion("int", newTestVersion(t, "x + 1", 1000)).
		SetCurrentVersion("int", 7)
	assert.ErrorContains(t, builder.Err(), "has no version-id 7")

	builder, err = NewMetadataBuilder()
	require.NoError(t, err)
	builder = builder.RemoveDefinition("int")
	assert.ErrorContains(t, builder.Err(), `no definition with definition-id "int"`)
}

func TestBuilderErrorChaining(t *testing.T) {
	builder, err := NewMetadataBuilder()
	require.NoError(t, err)

	builder = builder.
		RemoveDefinition("missing").
		AddDefinition(newTestDefinition(t)).
		AddDefinitionVersion("int", newTestVersion(t, "x + 1", 1000))

	firstErr := builder.Err()
	require.Error(t, firstErr)

	// operations after an error are noops and Build reports the first error
	_, err = builder.Build()
	assert.Equal(t, firstErr, err)
}

func TestBuilderUUIDReassignment(t *testing.T) {
	builder, err := NewMetadataBuilder()
	require.NoError(t, err)

	builder = builder.SetUUID(uuid.New()).SetUUID(uuid.New())
	assert.ErrorContains(t, builder.Err(), "cannot reassign uuid")

	base := parseFixture(t, "udf-metadata-scalar.json")
	fromBase, err := MetadataBuilderFromBase(base)
	require.NoError(t, err)
	fromBase = fromBase.SetUUID(base.FunctionUUID())
	assert.NoError(t, fromBase.Err(), "setting the same uuid is a noop")
	fromBase = fromBase.SetUUID(uuid.New())
	assert.ErrorContains(t, fromBase.Err(), "cannot reassign uuid")
}

// TestBuilderNormalizesNilParameters ensures a hand-built zero-parameter
// definition with nil parameters serializes as the required empty list.
func TestBuilderNormalizesNilParameters(t *testing.T) {
	builder, err := NewMetadataBuilder()
	require.NoError(t, err)

	meta, err := builder.
		AddDefinition(&Definition{
			ReturnType:       mustPrimitive(t, "long"),
			CurrentVersionID: -1,
			FunctionType:     FunctionTypeUDF,
		}).
		AddDefinitionVersion("", newTestVersion(t, "42", 1000)).
		SetLogTimestampMS(1000).
		Build()
	require.NoError(t, err)

	data, err := json.Marshal(meta)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"parameters":[]`)

	again, err := ParseMetadataBytes(data)
	require.NoError(t, err)
	assert.True(t, meta.Equals(again))
}

func TestBuilderZeroParameterDefinition(t *testing.T) {
	def, err := NewDefinition(FunctionTypeUDF, nil, mustPrimitive(t, "long"))
	require.NoError(t, err)
	assert.Equal(t, "", def.DefinitionID, "a zero-parameter definition has an empty definition-id")

	builder, err := NewMetadataBuilder()
	require.NoError(t, err)
	meta, err := builder.
		AddDefinition(def).
		AddDefinitionVersion("", newTestVersion(t, "42", 1000)).
		SetLogTimestampMS(1000).
		Build()
	require.NoError(t, err)

	built, ok := meta.DefinitionByID("")
	require.True(t, ok)
	assert.Equal(t, 1, built.CurrentVersionID)

	data, err := json.Marshal(meta)
	require.NoError(t, err)
	again, err := ParseMetadataBytes(data)
	require.NoError(t, err)
	assert.True(t, meta.Equals(again))
}

// TestBuildIsolation ensures further builder operations cannot alter
// previously built metadata.
func TestBuildIsolation(t *testing.T) {
	builder, err := NewMetadataBuilder()
	require.NoError(t, err)

	builder = builder.
		AddDefinition(newTestDefinition(t)).
		AddDefinitionVersion("int", newTestVersion(t, "x + 1", 1000)).
		SetLogTimestampMS(1000)

	first, err := builder.Build()
	require.NoError(t, err)

	builder = builder.
		AddDefinitionVersion("int", newTestVersion(t, "x + 2", 2000)).
		SetProperties(iceberg.Properties{"owner": "someone"})
	_, err = builder.Build()
	require.NoError(t, err)

	def, ok := first.DefinitionByID("int")
	require.True(t, ok)
	assert.Equal(t, 1, def.CurrentVersionID)
	assert.Len(t, def.Versions, 1)
	assert.Empty(t, first.Properties())
	assert.Len(t, first.DefinitionLog(), 1)
}

func TestBuilderRejectsHandBuiltUnknownRepresentation(t *testing.T) {
	builder, err := NewMetadataBuilder()
	require.NoError(t, err)

	version, err := NewDefinitionVersion(
		[]Representation{UnknownRepresentation{TypeName: "python"}},
		WithTimestampMS(1000))
	require.NoError(t, err)

	_, err = builder.
		AddDefinition(newTestDefinition(t)).
		AddDefinitionVersion("int", version).
		Build()
	assert.ErrorContains(t, err, "unknown representation without raw JSON")
}

func TestBuilderOnNullInput(t *testing.T) {
	repr, err := NewSQLRepresentation("trino", "coalesce(x, 0)")
	require.NoError(t, err)
	version, err := NewDefinitionVersion([]Representation{repr},
		WithTimestampMS(1000), WithOnNullInput(OnNullInputReturnNull))
	require.NoError(t, err)

	builder, err := NewMetadataBuilder()
	require.NoError(t, err)
	meta, err := builder.
		AddDefinition(newTestDefinition(t)).
		AddDefinitionVersion("int", version).
		SetLogTimestampMS(1000).
		Build()
	require.NoError(t, err)

	data, err := json.Marshal(meta)
	require.NoError(t, err)
	again, err := ParseMetadataBytes(data)
	require.NoError(t, err)

	def, ok := again.DefinitionByID("int")
	require.True(t, ok)
	assert.Equal(t, OnNullInputReturnNull, def.CurrentVersion().NullInputBehavior())
}

// TestBuilderLogTimestampZero pins that epoch-0 is a settable log timestamp:
// the builder's "unset" sentinel is -1, matching DefinitionVersion.TimestampMS.
func TestBuilderLogTimestampZero(t *testing.T) {
	builder, err := NewMetadataBuilder()
	require.NoError(t, err)

	meta, err := builder.
		AddDefinition(newTestDefinition(t)).
		AddDefinitionVersion("int", newTestVersion(t, "x + 1", 1000)).
		SetLogTimestampMS(0).
		Build()
	require.NoError(t, err)

	log := meta.DefinitionLog()
	require.Len(t, log, 1)
	assert.Equal(t, int64(0), log[0].TimestampMS)

	// a negative log timestamp is rejected by validation at Build
	builder, err = NewMetadataBuilder()
	require.NoError(t, err)
	_, err = builder.
		AddDefinition(newTestDefinition(t)).
		AddDefinitionVersion("int", newTestVersion(t, "x + 1", 1000)).
		SetLogTimestampMS(-5).
		Build()
	require.ErrorIs(t, err, ErrInvalidUDFMetadata)
	assert.ErrorContains(t, err, "invalid negative timestamp-ms -5")
}

func TestBuilderRemoveProperties(t *testing.T) {
	builder, err := NewMetadataBuilder()
	require.NoError(t, err)

	meta, err := builder.
		SetProperties(iceberg.Properties{"owner": "iceberg-go", "team": "iceberg"}).
		RemoveProperties([]string{"team", "not-present"}).
		AddDefinition(newTestDefinition(t)).
		AddDefinitionVersion("int", newTestVersion(t, "x + 1", 1000)).
		Build()
	require.NoError(t, err)
	assert.Equal(t, iceberg.Properties{"owner": "iceberg-go"}, meta.Properties())
}

// TestBuilderNoopsAndErrorGuards covers the equal-value noop branches and the
// sticky-error guards of every builder operation.
func TestBuilderNoopsAndErrorGuards(t *testing.T) {
	base := parseFixture(t, "udf-metadata-scalar.json")

	builder, err := MetadataBuilderFromBase(base)
	require.NoError(t, err)
	builder = builder.
		SetLoc(base.Location()).
		SetSecure(base.Secure()).
		SetDoc(base.Doc()).
		SetProperties(nil).
		RemoveProperties(nil).
		SetCurrentVersion("int", 2)
	require.NoError(t, builder.Err())
	assert.False(t, builder.HasChanges(), "setting current values must be a noop")

	builder = builder.SetUUID(uuid.Nil)
	require.ErrorContains(t, builder.Err(), "cannot set uuid to null")

	firstErr := builder.Err()
	builder = builder.
		SetLoc("s3://other").
		SetProperties(iceberg.Properties{"k": "v"}).
		RemoveProperties([]string{"k"}).
		SetSecure(true).
		SetDoc("other").
		SetLogTimestampMS(1).
		SetUUID(uuid.New()).
		AddDefinition(newTestDefinition(t)).
		RemoveDefinition("int").
		AddDefinitionVersion("int", newTestVersion(t, "x + 1", 1)).
		SetCurrentVersion("int", 1)
	assert.Equal(t, firstErr, builder.Err(), "operations after an error must be noops")
	_, err = builder.Build()
	assert.Equal(t, firstErr, err)
}

func TestBuilderAddDefinitionWithInvalidTypes(t *testing.T) {
	builder, err := NewMetadataBuilder()
	require.NoError(t, err)

	builder = builder.AddDefinition(&Definition{
		Parameters:   []Parameter{{Name: "x", Type: nil}},
		FunctionType: FunctionTypeUDF,
	})
	assert.ErrorIs(t, builder.Err(), ErrInvalidUDFType)
}

func TestBuilderNilArguments(t *testing.T) {
	builder, err := NewMetadataBuilder()
	require.NoError(t, err)
	builder = builder.AddDefinition(nil)
	assert.ErrorContains(t, builder.Err(), "cannot add nil definition")

	builder, err = NewMetadataBuilder()
	require.NoError(t, err)
	builder = builder.AddDefinition(newTestDefinition(t)).AddDefinitionVersion("int", nil)
	assert.ErrorContains(t, builder.Err(), "cannot add nil definition version")

	builder, err = NewMetadataBuilder()
	require.NoError(t, err)
	builder = builder.SetCurrentVersion("missing", 1)
	assert.ErrorContains(t, builder.Err(), `no definition with definition-id "missing"`)

	builder, err = NewMetadataBuilder()
	require.NoError(t, err)
	negative := newTestVersion(t, "x + 1", 1000)
	negative.VersionID = -2
	builder = builder.AddDefinition(newTestDefinition(t)).AddDefinitionVersion("int", negative)
	assert.ErrorContains(t, builder.Err(), "invalid negative version-id -2")
}

// TestBuildValidatesReturnType ensures Build rejects a definition whose
// return type was never set: AddDefinition only validates the signature.
func TestBuildValidatesReturnType(t *testing.T) {
	intType := mustPrimitive(t, "int")

	builder, err := NewMetadataBuilder()
	require.NoError(t, err)
	_, err = builder.
		AddDefinition(&Definition{
			Parameters:   []Parameter{{Name: "x", Type: intType}},
			FunctionType: FunctionTypeUDF,
		}).
		AddDefinitionVersion("int", newTestVersion(t, "x + 1", 1000)).
		Build()
	require.ErrorIs(t, err, ErrInvalidUDFMetadata)
	assert.ErrorContains(t, err, "return-type")
}

func TestCloneNilAndDeepCopy(t *testing.T) {
	assert.Nil(t, (*Definition)(nil).Clone())
	assert.Nil(t, (*DefinitionVersion)(nil).Clone())

	nullable := false
	def := newTestDefinition(t)
	def.ReturnNullable = &nullable

	cloned := def.Clone()
	require.NotNil(t, cloned.ReturnNullable)
	*def.ReturnNullable = true
	assert.False(t, *cloned.ReturnNullable, "clone must not share the return-nullable pointer")
}

func TestBuilderUDTF(t *testing.T) {
	returnType := StructType{Fields: []StructField{
		{Name: "name", Type: mustPrimitive(t, "string")},
		{Name: "color", Type: mustPrimitive(t, "string")},
	}}
	def, err := NewDefinition(FunctionTypeUDTF,
		[]Parameter{{Name: "c", Type: mustPrimitive(t, "string")}}, returnType)
	require.NoError(t, err)

	builder, err := NewMetadataBuilder()
	require.NoError(t, err)
	meta, err := builder.
		AddDefinition(def).
		AddDefinitionVersion("string", newTestVersion(t, "SELECT name, color FROM fruits WHERE color = c", 1000)).
		Build()
	require.NoError(t, err)

	built, ok := meta.DefinitionByID("string")
	require.True(t, ok)
	assert.Equal(t, FunctionTypeUDTF, built.FunctionType)

	// a udtf whose return type is not a struct fails to build
	invalid, err := NewDefinition(FunctionTypeUDTF,
		[]Parameter{{Name: "c", Type: mustPrimitive(t, "int")}}, mustPrimitive(t, "int"))
	require.NoError(t, err)
	builder, err = NewMetadataBuilder()
	require.NoError(t, err)
	_, err = builder.
		AddDefinition(invalid).
		AddDefinitionVersion("int", newTestVersion(t, "SELECT 1", 1000)).
		Build()
	assert.ErrorContains(t, err, "return-type is not a struct")
}
