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

package main

import (
	"bytes"
	"os"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/pterm/pterm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildSchemaFieldsWithDefaults(t *testing.T) {
	schema, err := iceberg.NewSchemaFromJsonFields(1, `[
		{"id": 1, "name": "id", "required": true, "type": "long"},
		{"id": 2, "name": "name", "required": false, "type": "string", "initial-default": "unknown", "write-default": "N/A"},
		{"id": 3, "name": "count", "required": false, "type": "int", "initial-default": 0, "write-default": 42}
	]`)
	require.NoError(t, err)

	fields := buildSchemaFieldsWithDefaults(schema)
	require.Len(t, fields, 3)

	assert.Equal(t, 1, fields[0].FieldID)
	assert.Equal(t, "id", fields[0].Name)
	assert.Equal(t, "long", fields[0].Type)
	assert.True(t, fields[0].Required)
	assert.Nil(t, fields[0].InitialDefault)
	assert.Nil(t, fields[0].WriteDefault)

	assert.Equal(t, 2, fields[1].FieldID)
	assert.Equal(t, "name", fields[1].Name)
	assert.False(t, fields[1].Required)
	assert.Equal(t, "unknown", fields[1].InitialDefault)
	assert.Equal(t, "N/A", fields[1].WriteDefault)

	assert.Equal(t, 3, fields[2].FieldID)
	assert.Equal(t, "count", fields[2].Name)
}

func TestFormatDefault(t *testing.T) {
	assert.Equal(t, "-", formatDefault(nil))
	assert.Equal(t, `"hello"`, formatDefault("hello"))
	assert.Equal(t, "42", formatDefault(42))
	assert.Equal(t, "3.14", formatDefault(3.14))
	assert.Equal(t, "true", formatDefault(true))
}

func TestTextOutputSchemaWithDefaults(t *testing.T) {
	var buf bytes.Buffer
	pterm.SetDefaultOutput(&buf)
	pterm.DisableColor()

	schema, err := iceberg.NewSchemaFromJsonFields(1, `[
		{"id": 1, "name": "id", "required": true, "type": "long"},
		{"id": 2, "name": "name", "required": false, "type": "string", "initial-default": "unknown", "write-default": "N/A"}
	]`)
	require.NoError(t, err)

	buf.Reset()

	textOutput{}.SchemaWithDefaults(schema)

	output := buf.String()
	assert.Contains(t, output, "FIELD ID")
	assert.Contains(t, output, "NAME")
	assert.Contains(t, output, "TYPE")
	assert.Contains(t, output, "REQUIRED")
	assert.Contains(t, output, "INIT DEFAULT")
	assert.Contains(t, output, "WRITE DEFAULT")
	assert.Contains(t, output, "id")
	assert.Contains(t, output, "name")
	assert.Contains(t, output, `"unknown"`)
}

func TestJSONOutputSchemaWithDefaults(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

	schema, err := iceberg.NewSchemaFromJsonFields(5, `[
		{"id": 1, "name": "x", "required": true, "type": "long"}
	]`)
	require.NoError(t, err)

	jsonOutput{}.SchemaWithDefaults(schema)

	w.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(r)

	output := buf.String()
	assert.Contains(t, output, `"schema_id":5`)
	assert.Contains(t, output, `"fields":[`)
	assert.Contains(t, output, `"field_id":1`)
	assert.Contains(t, output, `"name":"x"`)
}
