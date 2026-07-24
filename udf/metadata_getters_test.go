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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataGettersReturnDefensiveCopies(t *testing.T) {
	meta := parseFixture(t, "udf-metadata-scalar.json")

	properties := meta.Properties()
	properties["mutated"] = "true"
	assert.NotContains(t, meta.Properties(), "mutated")

	definitions := meta.Definitions()
	definitions[0].DefinitionID = "mutated"
	definitions[0].Parameters[0].Name = "mutated"
	definitions[0].Versions[0].VersionID = 99
	definitions[0].Versions[0].Representations[0] = SQLRepresentation{Dialect: "mutated", SQL: "mutated"}

	gotDefinitions := meta.Definitions()
	assert.Equal(t, "int", gotDefinitions[0].DefinitionID)
	assert.Equal(t, "x", gotDefinitions[0].Parameters[0].Name)
	assert.Equal(t, 1, gotDefinitions[0].Versions[0].VersionID)
	assert.Equal(t, SQLRepresentation{Dialect: "trino", SQL: "x + 2"}, gotDefinitions[0].Versions[0].Representations[0])

	definition, ok := meta.DefinitionByID("int")
	require.True(t, ok)
	definition.Doc = "mutated"
	definition, ok = meta.DefinitionByID("int")
	require.True(t, ok)
	assert.Equal(t, "Add one to the input integer", definition.Doc)

	log := meta.DefinitionLog()
	log[0].TimestampMS = 0
	log[0].DefinitionVersions[0].DefinitionID = "mutated"
	assert.Equal(t, int64(1734507000123), meta.DefinitionLog()[0].TimestampMS)
	assert.Equal(t, "int", meta.DefinitionLog()[0].DefinitionVersions[0].DefinitionID)
}

func TestDefinitionCopiesNestedTypes(t *testing.T) {
	meta := parseFixture(t, "udf-metadata-table.json")
	definition, ok := meta.DefinitionByID("string")
	require.True(t, ok)

	returnType := definition.ReturnType.(StructType)
	returnType.Fields[0].Name = "mutated"

	definition, ok = meta.DefinitionByID("string")
	require.True(t, ok)
	assert.Equal(t, "name", definition.ReturnType.(StructType).Fields[0].Name)
}

func TestUnknownRepresentationRawReturnsCopy(t *testing.T) {
	meta, err := ParseMetadataString(unknownRepresentationJSON)
	require.NoError(t, err)
	definition, ok := meta.DefinitionByID("int")
	require.True(t, ok)
	unknown := definition.CurrentVersion().Representations[0].(UnknownRepresentation)

	raw := unknown.Raw()
	raw[0] = 'x'

	definition, ok = meta.DefinitionByID("int")
	require.True(t, ok)
	unknown = definition.CurrentVersion().Representations[0].(UnknownRepresentation)
	assert.Equal(t, byte('{'), unknown.Raw()[0])
}
