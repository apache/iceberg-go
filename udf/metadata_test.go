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
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func parseFixture(t *testing.T, name string) Metadata {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	require.NoError(t, err)
	meta, err := ParseMetadataBytes(data)
	require.NoError(t, err)

	return meta
}

// TestParseMetadataScalarFixture parses the overloaded scalar function
// example from Appendix A of the UDF spec.
func TestParseMetadataScalarFixture(t *testing.T) {
	meta := parseFixture(t, "udf-metadata-scalar.json")

	assert.Equal(t, 1, meta.FormatVersion())
	assert.Equal(t, uuid.MustParse("42fd3f91-bc10-41c1-8a52-92b57dd0a9b2"), meta.FunctionUUID())
	assert.Equal(t, "Overloaded scalar UDF for integer and float inputs", meta.Doc())
	assert.False(t, meta.Secure())
	assert.Empty(t, meta.Location())
	assert.Empty(t, meta.Properties())

	require.Len(t, meta.Definitions(), 2)

	intDef, ok := meta.DefinitionByID("int")
	require.True(t, ok)
	assert.Equal(t, "int", intDef.DefinitionID)
	assert.Equal(t, FunctionTypeUDF, intDef.FunctionType)
	assert.Equal(t, "Add one to the input integer", intDef.Doc)
	require.Len(t, intDef.Parameters, 1)
	assert.Equal(t, "x", intDef.Parameters[0].Name)
	assert.Equal(t, "int", intDef.Parameters[0].Type.String())
	assert.Equal(t, "Input integer", intDef.Parameters[0].Doc)
	assert.Equal(t, "int", intDef.ReturnType.String())
	assert.True(t, intDef.ReturnsNullable(), "return-nullable defaults to true when absent")
	assert.Equal(t, 2, intDef.CurrentVersionID)
	require.Len(t, intDef.Versions, 2)

	v1 := intDef.Version(1)
	require.NotNil(t, v1)
	assert.True(t, v1.Deterministic)
	assert.Equal(t, int64(1734507000123), v1.TimestampMS)
	assert.Equal(t, OnNullInputCall, v1.NullInputBehavior(), "on-null-input defaults to call when absent")
	require.Len(t, v1.Representations, 1)
	assert.Equal(t, SQLRepresentation{Dialect: "trino", SQL: "x + 2"}, v1.Representations[0])

	current := intDef.CurrentVersion()
	require.NotNil(t, current)
	assert.Equal(t, 2, current.VersionID)
	require.Len(t, current.Representations, 2)
	assert.Equal(t, SQLRepresentation{Dialect: "trino", SQL: "x + 1"}, current.Representations[0])
	assert.Equal(t, SQLRepresentation{Dialect: "spark", SQL: "x + 1"}, current.Representations[1])

	floatDef, ok := meta.DefinitionByID("float")
	require.True(t, ok)
	assert.Equal(t, 1, floatDef.CurrentVersionID)

	_, ok = meta.DefinitionByID("string")
	assert.False(t, ok)

	log := meta.DefinitionLog()
	require.Len(t, log, 3)
	assert.Equal(t, []DefinitionVersionRef{{DefinitionID: "int", VersionID: 1}}, log[0].DefinitionVersions)
	assert.Equal(t, int64(1734507000123), log[0].TimestampMS)
	assert.Equal(t, []DefinitionVersionRef{
		{DefinitionID: "int", VersionID: 2},
		{DefinitionID: "float", VersionID: 1},
	}, log[2].DefinitionVersions)
}

// TestParseMetadataUDTFFixture parses the table function example from
// Appendix B of the UDF spec.
func TestParseMetadataUDTFFixture(t *testing.T) {
	meta := parseFixture(t, "udf-metadata-table.json")

	assert.Equal(t, uuid.MustParse("8a7fa39a-6d8f-4a2f-9d8d-3f3a8f3c2a10"), meta.FunctionUUID())
	require.Len(t, meta.Definitions(), 1)

	def, ok := meta.DefinitionByID("string")
	require.True(t, ok)
	assert.Equal(t, FunctionTypeUDTF, def.FunctionType)

	st, ok := def.ReturnType.(StructType)
	require.True(t, ok, "a udtf's return type must be a struct")
	require.Len(t, st.Fields, 2)
	assert.Equal(t, "name", st.Fields[0].Name)
	assert.Equal(t, "color", st.Fields[1].Name)
	assert.Equal(t, "struct<name:string,color:string>", def.ReturnType.String())

	require.Len(t, meta.DefinitionLog(), 1)
}

func TestMetadataRoundTrip(t *testing.T) {
	for _, fixture := range []string{"udf-metadata-scalar.json", "udf-metadata-table.json"} {
		t.Run(fixture, func(t *testing.T) {
			meta := parseFixture(t, fixture)

			data, err := json.Marshal(meta)
			require.NoError(t, err)

			again, err := ParseMetadataBytes(data)
			require.NoError(t, err)
			assert.True(t, meta.Equals(again))
		})
	}
}

func TestParseMetadataReader(t *testing.T) {
	data, err := os.ReadFile("testdata/udf-metadata-scalar.json")
	require.NoError(t, err)

	fromReader, err := ParseMetadata(strings.NewReader(string(data)))
	require.NoError(t, err)

	fromString, err := ParseMetadataString(string(data))
	require.NoError(t, err)
	assert.True(t, fromReader.Equals(fromString))
}

const unknownRepresentationJSON = `{
  "function-uuid": "42fd3f91-bc10-41c1-8a52-92b57dd0a9b2",
  "format-version": 1,
  "definitions": [
    {
      "definition-id": "int",
      "parameters": [{"name": "x", "type": "int"}],
      "return-type": "int",
      "function-type": "udf",
      "versions": [
        {
          "version-id": 1,
          "representations": [
            {"type": "python", "code": "return x + 1", "runtime": "3.12"},
            {"type": "sql", "dialect": "trino", "sql": "x + 1"}
          ],
          "timestamp-ms": 1734507000123
        }
      ],
      "current-version-id": 1
    }
  ],
  "definition-log": [
    {"timestamp-ms": 1734507000123, "definition-versions": [{"definition-id": "int", "version-id": 1}]}
  ]
}`

// TestUnknownRepresentation ensures representations of unrecognized types
// round-trip intact for forward compatibility.
func TestUnknownRepresentation(t *testing.T) {
	meta, err := ParseMetadataString(unknownRepresentationJSON)
	require.NoError(t, err)

	def, ok := meta.DefinitionByID("int")
	require.True(t, ok)
	reprs := def.CurrentVersion().Representations
	require.Len(t, reprs, 2)

	unknown, ok := reprs[0].(UnknownRepresentation)
	require.True(t, ok)
	assert.Equal(t, "python", unknown.RepresentationType())
	assert.Contains(t, string(unknown.Raw()), `"runtime"`)

	sqlRepr, ok := reprs[1].(SQLRepresentation)
	require.True(t, ok)
	assert.Equal(t, "sql", sqlRepr.RepresentationType())

	data, err := json.Marshal(meta)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"runtime"`)

	again, err := ParseMetadataBytes(data)
	require.NoError(t, err)
	assert.True(t, meta.Equals(again))
}

func TestReturnNullable(t *testing.T) {
	metaJSON := strings.Replace(unknownRepresentationJSON,
		`"return-type": "int",`, `"return-type": "int", "return-nullable": false,`, 1)
	meta, err := ParseMetadataString(metaJSON)
	require.NoError(t, err)

	def, ok := meta.DefinitionByID("int")
	require.True(t, ok)
	assert.False(t, def.ReturnsNullable())

	// an explicit false must survive serialization
	data, err := json.Marshal(meta)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"return-nullable":false`)

	// while an absent value (defaulting to true) stays absent
	meta = parseFixture(t, "udf-metadata-scalar.json")
	data, err = json.Marshal(meta)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "return-nullable")
}

func TestMetadataEquals(t *testing.T) {
	meta := parseFixture(t, "udf-metadata-scalar.json")
	other := parseFixture(t, "udf-metadata-scalar.json")

	assert.True(t, meta.Equals(meta))
	assert.True(t, meta.Equals(other))
	assert.False(t, meta.Equals(nil))
	assert.False(t, meta.Equals((*metadata)(nil)), "a typed-nil Metadata must not panic")
	assert.False(t, meta.Equals(parseFixture(t, "udf-metadata-table.json")))
}

// TestScalarMayReturnEmptyStruct pins that the udtf non-empty-struct rule is
// scoped to table functions: a scalar function may still declare an empty
// struct return type.
func TestScalarMayReturnEmptyStruct(t *testing.T) {
	m := minimalMetadata(t)
	definition(t, m, 0)["return-type"] = map[string]any{"type": "struct", "fields": []any{}}

	data, err := json.Marshal(m)
	require.NoError(t, err)

	_, err = ParseMetadataBytes(data)
	require.NoError(t, err)
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

func TestParseMetadataReaderError(t *testing.T) {
	_, err := ParseMetadata(failingReader{})
	assert.ErrorContains(t, err, "read failed")
}

func TestRepresentationsEqualCrossType(t *testing.T) {
	sqlRepr := SQLRepresentation{Dialect: "trino", SQL: "x + 1"}
	unknown := UnknownRepresentation{TypeName: "python", raw: json.RawMessage(`{"type":"python"}`)}
	other := UnknownRepresentation{TypeName: "python", raw: json.RawMessage(`{"type":"python","v":2}`)}

	assert.True(t, representationsEqual(sqlRepr, sqlRepr))
	assert.True(t, representationsEqual(unknown, unknown))
	assert.False(t, representationsEqual(sqlRepr, unknown))
	assert.False(t, representationsEqual(unknown, sqlRepr))
	assert.False(t, representationsEqual(unknown, other))
	assert.False(t, representationsEqual(nil, sqlRepr))
}

// TestMetadataInitDefaults pins init's nil-guards: a zero-value metadata
// instance gets non-nil collections and a working definition index.
func TestMetadataInitDefaults(t *testing.T) {
	m := &metadata{}
	m.init()

	assert.NotNil(t, m.DefinitionList)
	assert.NotNil(t, m.DefinitionLogList)
	assert.NotNil(t, m.Props)

	_, ok := m.DefinitionByID("missing")
	assert.False(t, ok)
}

func TestNewSQLRepresentation(t *testing.T) {
	repr, err := NewSQLRepresentation("trino", "x + 1")
	require.NoError(t, err)
	assert.Equal(t, SQLRepresentation{Dialect: "trino", SQL: "x + 1"}, repr)

	_, err = NewSQLRepresentation("", "x + 1")
	assert.ErrorIs(t, err, ErrInvalidUDFMetadata)

	_, err = NewSQLRepresentation("trino", "")
	assert.ErrorIs(t, err, ErrInvalidUDFMetadata)
}

// minimalMetadata returns a decoded minimal valid metadata document for
// mutation-based validation tests.
func minimalMetadata(t *testing.T) map[string]any {
	t.Helper()
	const minimal = `{
	  "function-uuid": "42fd3f91-bc10-41c1-8a52-92b57dd0a9b2",
	  "format-version": 1,
	  "definitions": [
	    {
	      "definition-id": "int",
	      "parameters": [{"name": "x", "type": "int"}],
	      "return-type": "int",
	      "function-type": "udf",
	      "versions": [
	        {
	          "version-id": 1,
	          "representations": [{"type": "sql", "dialect": "trino", "sql": "x + 1"}],
	          "timestamp-ms": 1734507000123
	        }
	      ],
	      "current-version-id": 1
	    }
	  ],
	  "definition-log": [
	    {"timestamp-ms": 1734507000123, "definition-versions": [{"definition-id": "int", "version-id": 1}]}
	  ]
	}`

	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(minimal), &m))

	return m
}

func definition(t *testing.T, m map[string]any, idx int) map[string]any {
	t.Helper()

	return m["definitions"].([]any)[idx].(map[string]any)
}

func version(t *testing.T, m map[string]any, defIdx, verIdx int) map[string]any {
	t.Helper()

	return definition(t, m, defIdx)["versions"].([]any)[verIdx].(map[string]any)
}

func TestMetadataValidation(t *testing.T) {
	t.Run("minimal is valid", func(t *testing.T) {
		data, err := json.Marshal(minimalMetadata(t))
		require.NoError(t, err)
		_, err = ParseMetadataBytes(data)
		require.NoError(t, err)
	})

	tests := []struct {
		name        string
		mutate      func(t *testing.T, m map[string]any)
		expectedErr error
		contains    string
	}{
		{
			"missing function-uuid",
			func(t *testing.T, m map[string]any) { delete(m, "function-uuid") },
			ErrInvalidUDFMetadata, "function-uuid is required",
		},
		{
			"missing format-version",
			func(t *testing.T, m map[string]any) { delete(m, "format-version") },
			ErrInvalidUDFMetadataFormatVersion, "format-version is required",
		},
		{
			"unsupported format-version",
			func(t *testing.T, m map[string]any) { m["format-version"] = 2 },
			ErrInvalidUDFMetadataFormatVersion, "only version 1 is supported",
		},
		{
			"empty definitions",
			func(t *testing.T, m map[string]any) { m["definitions"] = []any{} },
			ErrInvalidUDFMetadata, "at least one definition is required",
		},
		{
			"definition-id does not match parameters",
			func(t *testing.T, m map[string]any) { definition(t, m, 0)["definition-id"] = "float" },
			ErrInvalidUDFMetadata, "does not match canonical form",
		},
		{
			"duplicate signature",
			func(t *testing.T, m map[string]any) {
				m["definitions"] = append(m["definitions"].([]any), definition(t, m, 0))
			},
			ErrInvalidUDFMetadata, "duplicate definition-id",
		},
		{
			"duplicate specific-name",
			func(t *testing.T, m map[string]any) {
				definition(t, m, 0)["specific-name"] = "add_one_int"
				var second map[string]any
				data, err := json.Marshal(definition(t, m, 0))
				require.NoError(t, err)
				require.NoError(t, json.Unmarshal(data, &second))
				second["definition-id"] = "float"
				second["parameters"] = []any{map[string]any{"name": "x", "type": "float"}}
				m["definitions"] = append(m["definitions"].([]any), second)
			},
			ErrInvalidUDFMetadata, "duplicate specific-name",
		},
		{
			"missing current-version-id",
			func(t *testing.T, m map[string]any) { delete(definition(t, m, 0), "current-version-id") },
			ErrInvalidUDFMetadata, "missing current-version-id",
		},
		{
			"current-version-id not found",
			func(t *testing.T, m map[string]any) { definition(t, m, 0)["current-version-id"] = 5 },
			ErrInvalidUDFMetadata, "current-version-id 5 not found",
		},
		{
			"duplicate version-id",
			func(t *testing.T, m map[string]any) {
				def := definition(t, m, 0)
				def["versions"] = append(def["versions"].([]any), version(t, m, 0, 0))
			},
			ErrInvalidUDFMetadata, "duplicate version-id",
		},
		{
			"missing version-id",
			func(t *testing.T, m map[string]any) { delete(version(t, m, 0, 0), "version-id") },
			ErrInvalidUDFMetadata, "missing version-id",
		},
		{
			"missing timestamp-ms",
			func(t *testing.T, m map[string]any) { delete(version(t, m, 0, 0), "timestamp-ms") },
			ErrInvalidUDFMetadata, "missing timestamp-ms",
		},
		{
			"empty representations",
			func(t *testing.T, m map[string]any) { version(t, m, 0, 0)["representations"] = []any{} },
			ErrInvalidUDFMetadata, "at least one representation",
		},
		{
			"representation without type",
			func(t *testing.T, m map[string]any) {
				version(t, m, 0, 0)["representations"] = []any{map[string]any{"dialect": "trino", "sql": "x + 1"}}
			},
			ErrInvalidUDFMetadata, "representation requires a type",
		},
		{
			"sql representation without dialect",
			func(t *testing.T, m map[string]any) {
				version(t, m, 0, 0)["representations"] = []any{map[string]any{"type": "sql", "sql": "x + 1"}}
			},
			ErrInvalidUDFMetadata, "without a dialect",
		},
		{
			"sql representation without sql",
			func(t *testing.T, m map[string]any) {
				version(t, m, 0, 0)["representations"] = []any{map[string]any{"type": "sql", "dialect": "trino"}}
			},
			ErrInvalidUDFMetadata, "without a sql expression",
		},
		{
			"duplicate sql dialect",
			func(t *testing.T, m map[string]any) {
				version(t, m, 0, 0)["representations"] = []any{
					map[string]any{"type": "sql", "dialect": "trino", "sql": "x + 1"},
					map[string]any{"type": "sql", "dialect": "Trino", "sql": "x + 2"},
				}
			},
			ErrInvalidUDFMetadata, "duplicate sql dialect",
		},
		{
			"invalid on-null-input",
			func(t *testing.T, m map[string]any) { version(t, m, 0, 0)["on-null-input"] = "always" },
			ErrInvalidUDFMetadata, "invalid on-null-input",
		},
		{
			"udtf with non-struct return type",
			func(t *testing.T, m map[string]any) { definition(t, m, 0)["function-type"] = "udtf" },
			ErrInvalidUDFMetadata, "return-type is not a struct",
		},
		{
			"udtf with empty struct return type",
			func(t *testing.T, m map[string]any) {
				definition(t, m, 0)["function-type"] = "udtf"
				definition(t, m, 0)["return-type"] = map[string]any{"type": "struct", "fields": []any{}}
			},
			ErrInvalidUDFMetadata, "return-type struct has no fields",
		},
		{
			"parameter struct with duplicate field names",
			func(t *testing.T, m map[string]any) {
				definition(t, m, 0)["parameters"] = []any{map[string]any{
					"name": "x",
					"type": map[string]any{"type": "struct", "fields": []any{
						map[string]any{"name": "a", "type": "int"},
						map[string]any{"name": "a", "type": "string"},
					}},
				}}
			},
			ErrInvalidUDFType, `duplicate field name "a"`,
		},
		{
			"missing function-type",
			func(t *testing.T, m map[string]any) { delete(definition(t, m, 0), "function-type") },
			ErrInvalidUDFMetadata, "missing function-type",
		},
		{
			"invalid function-type",
			func(t *testing.T, m map[string]any) { definition(t, m, 0)["function-type"] = "aggregate" },
			ErrInvalidUDFMetadata, "invalid function-type",
		},
		{
			"parameter without name",
			func(t *testing.T, m map[string]any) {
				definition(t, m, 0)["parameters"] = []any{map[string]any{"name": "", "type": "int"}}
			},
			ErrInvalidUDFMetadata, "parameter without a name",
		},
		{
			"duplicate parameter names",
			func(t *testing.T, m map[string]any) {
				def := definition(t, m, 0)
				def["parameters"] = []any{
					map[string]any{"name": "x", "type": "int"},
					map[string]any{"name": "x", "type": "string"},
				}
				def["definition-id"] = "int,string"
			},
			ErrInvalidUDFMetadata, "duplicate parameter name",
		},
		{
			"definition-log entry without definition-versions",
			func(t *testing.T, m map[string]any) {
				m["definition-log"] = []any{map[string]any{"timestamp-ms": 1, "definition-versions": []any{}}}
			},
			ErrInvalidUDFMetadata, "must have definition-versions",
		},
		{
			"missing return-type",
			func(t *testing.T, m map[string]any) { delete(definition(t, m, 0), "return-type") },
			ErrInvalidUDFMetadata, "requires a return-type",
		},
		{
			"absent definitions field",
			func(t *testing.T, m map[string]any) { delete(m, "definitions") },
			ErrInvalidUDFMetadata, "at least one definition is required",
		},
		{
			"malformed parameter",
			func(t *testing.T, m map[string]any) { definition(t, m, 0)["parameters"] = []any{42} },
			nil, "cannot unmarshal number",
		},
		{
			"malformed representation",
			func(t *testing.T, m map[string]any) { version(t, m, 0, 0)["representations"] = []any{42} },
			nil, "cannot unmarshal number",
		},
		{
			"sql representation with non-string dialect",
			func(t *testing.T, m map[string]any) {
				version(t, m, 0, 0)["representations"] = []any{
					map[string]any{"type": "sql", "dialect": 5, "sql": "x + 1"},
				}
			},
			nil, "cannot unmarshal number",
		},
		{
			"malformed version",
			func(t *testing.T, m map[string]any) { definition(t, m, 0)["versions"] = []any{42} },
			nil, "cannot unmarshal number",
		},
		{
			"parameter with invalid type",
			func(t *testing.T, m map[string]any) {
				definition(t, m, 0)["parameters"] = []any{map[string]any{"name": "x", "type": "foo"}}
			},
			ErrInvalidUDFType, "not a valid Iceberg type string",
		},
		{
			"null definition entry",
			func(t *testing.T, m map[string]any) { m["definitions"] = []any{nil} },
			ErrInvalidUDFMetadata, "definitions must not contain null entries",
		},
		{
			// definition-id is required even though a zero-parameter
			// definition legitimately has the empty string as its id
			"definition missing definition-id",
			func(t *testing.T, m map[string]any) {
				delete(definition(t, m, 0), "definition-id")
				definition(t, m, 0)["parameters"] = []any{}
			},
			ErrInvalidUDFMetadata, "definition is missing definition-id",
		},
		{
			// parameters is required even though a zero-parameter
			// definition legitimately has an empty list
			"definition missing parameters",
			func(t *testing.T, m map[string]any) {
				definition(t, m, 0)["definition-id"] = ""
				delete(definition(t, m, 0), "parameters")
			},
			ErrInvalidUDFMetadata, `definition "" is missing parameters`,
		},
		{
			"definition with null parameters",
			func(t *testing.T, m map[string]any) {
				definition(t, m, 0)["definition-id"] = ""
				definition(t, m, 0)["parameters"] = nil
			},
			ErrInvalidUDFMetadata, `definition "" is missing parameters`,
		},
		{
			"definition-log reference missing definition-id",
			func(t *testing.T, m map[string]any) {
				m["definition-log"].([]any)[0].(map[string]any)["definition-versions"] = []any{
					map[string]any{"version-id": 1},
				}
			},
			ErrInvalidUDFMetadata, "definition-log reference is missing definition-id",
		},
		{
			"malformed definition-log reference",
			func(t *testing.T, m map[string]any) {
				m["definition-log"].([]any)[0].(map[string]any)["definition-versions"] = []any{42}
			},
			nil, "cannot unmarshal number",
		},
		{
			"missing definition-log",
			func(t *testing.T, m map[string]any) { delete(m, "definition-log") },
			ErrInvalidUDFMetadata, "definition-log is required",
		},
		{
			"null definition-log",
			func(t *testing.T, m map[string]any) { m["definition-log"] = nil },
			ErrInvalidUDFMetadata, "definition-log is required",
		},
		{
			"null version entry",
			func(t *testing.T, m map[string]any) { definition(t, m, 0)["versions"] = []any{nil} },
			ErrInvalidUDFMetadata, "versions must not contain null entries",
		},
		{
			"negative version-id",
			func(t *testing.T, m map[string]any) {
				version(t, m, 0, 0)["version-id"] = -2
				definition(t, m, 0)["current-version-id"] = -2
			},
			ErrInvalidUDFMetadata, "invalid negative version-id -2",
		},
		{
			"negative current-version-id",
			func(t *testing.T, m map[string]any) { definition(t, m, 0)["current-version-id"] = -2 },
			ErrInvalidUDFMetadata, "invalid negative current-version-id -2",
		},
		{
			"negative version timestamp-ms",
			func(t *testing.T, m map[string]any) { version(t, m, 0, 0)["timestamp-ms"] = -5 },
			ErrInvalidUDFMetadata, "invalid negative timestamp-ms -5",
		},
		{
			"definition-log entry missing timestamp-ms",
			func(t *testing.T, m map[string]any) {
				delete(m["definition-log"].([]any)[0].(map[string]any), "timestamp-ms")
			},
			ErrInvalidUDFMetadata, "definition-log entry is missing timestamp-ms",
		},
		{
			"definition-log entry with negative timestamp-ms",
			func(t *testing.T, m map[string]any) {
				m["definition-log"].([]any)[0].(map[string]any)["timestamp-ms"] = -7
			},
			ErrInvalidUDFMetadata, "invalid negative timestamp-ms -7",
		},
		{
			"definition-log reference missing version-id",
			func(t *testing.T, m map[string]any) {
				m["definition-log"].([]any)[0].(map[string]any)["definition-versions"] = []any{
					map[string]any{"definition-id": "int"},
				}
			},
			ErrInvalidUDFMetadata, `reference to definition "int" missing version-id`,
		},
		{
			"definition-log reference with negative version-id",
			func(t *testing.T, m map[string]any) {
				m["definition-log"].([]any)[0].(map[string]any)["definition-versions"] = []any{
					map[string]any{"definition-id": "int", "version-id": -3},
				}
			},
			ErrInvalidUDFMetadata, `invalid negative version-id -3 for definition "int"`,
		},
		{
			"definition-log entry with duplicate definition references",
			func(t *testing.T, m map[string]any) {
				m["definition-log"].([]any)[0].(map[string]any)["definition-versions"] = []any{
					map[string]any{"definition-id": "int", "version-id": 1},
					map[string]any{"definition-id": "int", "version-id": 2},
				}
			},
			ErrInvalidUDFMetadata, `references definition "int" more than once`,
		},
		{
			"invalid return-type",
			func(t *testing.T, m map[string]any) { definition(t, m, 0)["return-type"] = "foo" },
			ErrInvalidUDFType, "not a valid Iceberg type string",
		},
		{
			"parameter without type",
			func(t *testing.T, m map[string]any) {
				definition(t, m, 0)["parameters"] = []any{map[string]any{"name": "x"}}
			},
			ErrInvalidUDFMetadata, "requires a type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := minimalMetadata(t)
			tt.mutate(t, m)

			data, err := json.Marshal(m)
			require.NoError(t, err)

			_, err = ParseMetadataBytes(data)
			if tt.expectedErr != nil {
				require.ErrorIs(t, err, tt.expectedErr)
			}
			assert.ErrorContains(t, err, tt.contains)
		})
	}
}

// TestParseMetadataWithEmptyDefinitionLog pins the required-field boundary:
// an absent or null definition-log is rejected, but a present, empty list is
// structurally accepted — the spec requires the field without stating a
// non-empty cardinality, and the builder never emits an empty log for built
// metadata with definitions.
func TestParseMetadataWithEmptyDefinitionLog(t *testing.T) {
	m := minimalMetadata(t)
	m["definition-log"] = []any{}

	data, err := json.Marshal(m)
	require.NoError(t, err)

	meta, err := ParseMetadataBytes(data)
	require.NoError(t, err)
	assert.Empty(t, meta.DefinitionLog())
}

func TestOnNullInputValues(t *testing.T) {
	for _, behavior := range []OnNullInput{OnNullInputCall, OnNullInputReturnNull} {
		m := minimalMetadata(t)
		version(t, m, 0, 0)["on-null-input"] = string(behavior)

		data, err := json.Marshal(m)
		require.NoError(t, err)

		meta, err := ParseMetadataBytes(data)
		require.NoError(t, err)
		def, _ := meta.DefinitionByID("int")
		assert.Equal(t, behavior, def.CurrentVersion().NullInputBehavior())
	}
}
