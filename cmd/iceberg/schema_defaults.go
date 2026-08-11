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
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/apache/iceberg-go"
	"github.com/pterm/pterm"
)

func runSchemaWithDefaults(output Output, schema *iceberg.Schema) {
	output.SchemaWithDefaults(schema)
}

func buildSchemaFieldsWithDefaults(schema *iceberg.Schema) []SchemaFieldWithDefaults {
	fields := schema.Fields()
	entries := make([]SchemaFieldWithDefaults, 0, len(fields))

	for _, f := range fields {
		entries = append(entries, SchemaFieldWithDefaults{
			FieldID:        f.ID,
			Name:           f.Name,
			Type:           f.Type.String(),
			Required:       f.Required,
			InitialDefault: f.InitialDefault,
			WriteDefault:   f.WriteDefault,
		})
	}

	return entries
}

func formatDefault(v any) string {
	if v == nil {
		return "-"
	}

	if s, ok := v.(string); ok {
		return strconv.Quote(s)
	}

	return fmt.Sprintf("%v", v)
}

func (t textOutput) SchemaWithDefaults(schema *iceberg.Schema) {
	entries := buildSchemaFieldsWithDefaults(schema)

	if len(entries) == 0 {
		pterm.Println("No fields in schema.")

		return
	}

	data := pterm.TableData{{"FIELD ID", "NAME", "TYPE", "REQUIRED", "INIT DEFAULT", "WRITE DEFAULT"}}

	for _, e := range entries {
		data = append(data, []string{
			strconv.Itoa(e.FieldID),
			e.Name,
			e.Type,
			strconv.FormatBool(e.Required),
			formatDefault(e.InitialDefault),
			formatDefault(e.WriteDefault),
		})
	}

	pterm.DefaultTable.
		WithHasHeader(true).
		WithHeaderRowSeparator("-").
		WithData(data).Render()
}

func (j jsonOutput) SchemaWithDefaults(schema *iceberg.Schema) {
	entries := buildSchemaFieldsWithDefaults(schema)

	result := struct {
		SchemaID int                       `json:"schema_id"`
		Fields   []SchemaFieldWithDefaults `json:"fields"`
	}{
		SchemaID: schema.ID,
		Fields:   entries,
	}

	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		j.Error(err)
	}
}
