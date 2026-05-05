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
	"testing"

	"github.com/alexflint/go-arg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArgsParsing(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		check   func(t *testing.T, a Args)
		wantErr bool
	}{
		{
			name: "list without parent",
			args: []string{"list"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.List)
				assert.Equal(t, "", a.List.Parent)
			},
		},
		{
			name: "list with parent namespace",
			args: []string{"list", "prod.db"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.List)
				assert.Equal(t, "prod.db", a.List.Parent)
			},
		},
		{
			name: "describe direct identifier",
			args: []string{"describe", "prod.db.events"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.Describe)
				assert.Equal(t, "prod.db.events", a.Describe.Identifier)
				assert.Equal(t, "", a.Describe.Target)
			},
		},
		{
			name: "describe namespace with identifier",
			args: []string{"describe", "namespace", "prod.db"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.Describe)
				assert.Equal(t, "namespace", a.Describe.Identifier)
				assert.Equal(t, "prod.db", a.Describe.Target)
			},
		},
		{
			name: "describe table with identifier",
			args: []string{"describe", "table", "prod.db.events"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.Describe)
				assert.Equal(t, "table", a.Describe.Identifier)
				assert.Equal(t, "prod.db.events", a.Describe.Target)
			},
		},
		{
			name: "schema",
			args: []string{"schema", "prod.db.events"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.Schema)
				assert.Equal(t, "prod.db.events", a.Schema.TableID)
			},
		},
		{
			name: "uuid",
			args: []string{"uuid", "prod.db.events"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.Uuid)
				assert.Equal(t, "prod.db.events", a.Uuid.TableID)
			},
		},
		{
			name: "location",
			args: []string{"location", "prod.db.events"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.Location)
				assert.Equal(t, "prod.db.events", a.Location.TableID)
			},
		},
		{
			name: "files with history",
			args: []string{"files", "--history", "prod.db.events"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.Files)
				assert.Equal(t, "prod.db.events", a.Files.TableID)
				assert.True(t, a.Files.History)
			},
		},
		{
			name: "rename",
			args: []string{"rename", "ns.old_table", "ns.new_table"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.Rename)
				assert.Equal(t, "ns.old_table", a.Rename.From)
				assert.Equal(t, "ns.new_table", a.Rename.To)
			},
		},
		{
			name: "create namespace",
			args: []string{"create", "namespace", "prod.db"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.Create)
				require.NotNil(t, a.Create.Namespace)
				assert.Equal(t, "prod.db", a.Create.Namespace.Identifier)
			},
		},
		{
			name: "create table with schema",
			args: []string{"create", "table", "prod.db.t", "--schema", `[{"name":"id","type":"int"}]`},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.Create)
				require.NotNil(t, a.Create.Table)
				assert.Equal(t, "prod.db.t", a.Create.Table.Identifier)
				assert.Equal(t, `[{"name":"id","type":"int"}]`, a.Create.Table.Schema)
			},
		},
		{
			name: "drop table",
			args: []string{"drop", "table", "prod.db.events"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.Drop)
				require.NotNil(t, a.Drop.Table)
				assert.Equal(t, "prod.db.events", a.Drop.Table.Identifier)
			},
		},
		{
			name: "properties get",
			args: []string{"properties", "get", "table", "prod.db.t", "write.format.default"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.Properties)
				require.NotNil(t, a.Properties.Get)
				assert.Equal(t, "table", a.Properties.Get.Type)
				assert.Equal(t, "prod.db.t", a.Properties.Get.Identifier)
				assert.Equal(t, "write.format.default", a.Properties.Get.PropName)
			},
		},
		{
			name: "properties set",
			args: []string{"properties", "set", "namespace", "prod.db", "location", "s3://bucket/path"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.Properties)
				require.NotNil(t, a.Properties.Set)
				assert.Equal(t, "namespace", a.Properties.Set.Type)
				assert.Equal(t, "prod.db", a.Properties.Set.Identifier)
				assert.Equal(t, "location", a.Properties.Set.PropName)
				assert.Equal(t, "s3://bucket/path", a.Properties.Set.Value)
			},
		},
		{
			name: "compact analyze",
			args: []string{"compact", "analyze", "prod.db.events", "--target-file-size", "134217728"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.Compact)
				require.NotNil(t, a.Compact.Analyze)
				assert.Equal(t, "prod.db.events", a.Compact.Analyze.TableID)
				assert.Equal(t, int64(134217728), a.Compact.Analyze.TargetFileSize)
			},
		},
		{
			name: "compact run with all flags",
			args: []string{"compact", "run", "prod.db.events", "--partial-progress", "--preserve-dead-equality-deletes"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.Compact)
				require.NotNil(t, a.Compact.Run)
				assert.Equal(t, "prod.db.events", a.Compact.Run.TableID)
				assert.True(t, a.Compact.Run.PartialProgress)
				assert.True(t, a.Compact.Run.PreserveDeadEqualityDeletes)
			},
		},
		{
			name: "global flags",
			args: []string{"--catalog", "glue", "--output", "json", "--warehouse", "s3://wh", "list"},
			check: func(t *testing.T, a Args) {
				assert.Equal(t, "glue", a.Catalog)
				assert.Equal(t, "json", a.Output)
				assert.Equal(t, "s3://wh", a.Warehouse)
				require.NotNil(t, a.List)
			},
		},
		{
			name:    "describe without identifier errors",
			args:    []string{"describe"},
			wantErr: true,
		},
		{
			name:    "schema without table errors",
			args:    []string{"schema"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var a Args
			p, err := arg.NewParser(arg.Config{}, &a)
			require.NoError(t, err)

			err = p.Parse(tt.args)
			if tt.wantErr {
				assert.Error(t, err)

				return
			}

			require.NoError(t, err)
			tt.check(t, a)
		})
	}
}
