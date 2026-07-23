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
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/alexflint/go-arg"
	"github.com/apache/iceberg-go/config"
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
			name: "list with page-size",
			args: []string{"list", "prod.db", "--page-size", "100"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.List)
				assert.Equal(t, "prod.db", a.List.Parent)
				assert.Equal(t, 100, a.List.PageSize)
			},
		},
		{
			name: "list defaults page-size to zero",
			args: []string{"list"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.List)
				assert.Equal(t, 0, a.List.PageSize)
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
				assert.False(t, a.Drop.Table.Purge)
			},
		},
		{
			name: "drop table with --purge",
			args: []string{"drop", "table", "prod.db.events", "--purge"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.Drop)
				require.NotNil(t, a.Drop.Table)
				assert.Equal(t, "prod.db.events", a.Drop.Table.Identifier)
				assert.True(t, a.Drop.Table.Purge)
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
			name: "rewrite-manifests with flags",
			args: []string{"rewrite-manifests", "prod.db.events", "--target-manifest-size", "8388608", "--spec-id", "2"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.RewriteManifests)
				assert.Equal(t, "prod.db.events", a.RewriteManifests.TableID)
				assert.Equal(t, int64(8388608), a.RewriteManifests.TargetManifestSize)
				assert.Equal(t, 2, a.RewriteManifests.SpecID)
			},
		},
		{
			name: "rewrite-manifests defaults",
			args: []string{"rewrite-manifests", "prod.db.events"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.RewriteManifests)
				assert.Equal(t, int64(0), a.RewriteManifests.TargetManifestSize)
				assert.Equal(t, -1, a.RewriteManifests.SpecID)
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
			name: "aws-profile flag",
			args: []string{"--catalog", "glue", "--aws-profile", "my-profile", "list"},
			check: func(t *testing.T, a Args) {
				assert.Equal(t, "my-profile", a.AwsProfile)
			},
		},
		{
			name: "rollback with snapshot id",
			args: []string{"rollback", "prod.db.events", "--snapshot-id", "123", "--yes"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.Rollback)
				assert.Equal(t, "prod.db.events", a.Rollback.TableID)
				require.NotNil(t, a.Rollback.SnapshotID)
				assert.Equal(t, int64(123), *a.Rollback.SnapshotID)
				assert.Equal(t, "", a.Rollback.Timestamp)
				assert.True(t, a.Rollback.Yes)
			},
		},
		{
			name: "rollback with timestamp",
			args: []string{"rollback", "prod.db.events", "--timestamp", "2026-01-15T03:00:00Z"},
			check: func(t *testing.T, a Args) {
				require.NotNil(t, a.Rollback)
				assert.Equal(t, "prod.db.events", a.Rollback.TableID)
				assert.Nil(t, a.Rollback.SnapshotID)
				assert.Equal(t, "2026-01-15T03:00:00Z", a.Rollback.Timestamp)
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

func TestResolveCatalogName(t *testing.T) {
	tests := []struct {
		name          string
		explicitFlags map[string]bool
		flagValue     string
		want          string
	}{
		// explicit --catalog-name wins and is passed through
		{"explicit flag wins", map[string]bool{"catalog-name": true}, "prod", "prod"},
		// no flag: return "" so ParseConfig falls back to default-catalog / "default"
		{"no flag returns empty string", map[string]bool{}, "default", ""},
		// other flags set but not catalog-name: still falls back
		{"other flags set only", map[string]bool{"uri": true}, "default", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, resolveCatalogName(tt.explicitFlags, tt.flagValue))
		})
	}
}

func TestApplyConfigFileFailsClosed(t *testing.T) {
	t.Run("explicit missing file", func(t *testing.T) {
		args := Args{Config: filepath.Join(t.TempDir(), "missing.yaml")}
		err := applyConfigFile(&args, map[string]bool{"config": true})
		require.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("malformed file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(":\t[broken yaml"), 0o600))
		args := Args{Config: path}
		err := applyConfigFile(&args, map[string]bool{"config": true})
		require.ErrorContains(t, err, "parse config file")
		require.ErrorContains(t, err, path)
	})

	t.Run("missing selected catalog", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte("catalog:\n  available:\n    type: rest\n"), 0o600))
		args := Args{Config: path, CatalogName: "missing"}
		err := applyConfigFile(&args, map[string]bool{"config": true, "catalog-name": true})
		require.EqualError(t, err, `catalog "missing" not found in config file `+path)
	})

	t.Run("valid selected catalog", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte("catalog:\n  selected:\n    type: rest\n    uri: http://catalog.example\n"), 0o600))
		args := Args{Config: path, CatalogName: "selected"}
		err := applyConfigFile(&args, map[string]bool{"config": true, "catalog-name": true})
		require.NoError(t, err)
		require.Equal(t, "http://catalog.example", args.URI)
	})
}

func TestCLIExplicitMissingConfigFailsBeforeCatalogInit(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a subprocess")
	}

	path := filepath.Join(t.TempDir(), "missing.yaml")
	cmd := exec.Command(os.Args[0], "list", "--config", path)
	cmd.Env = append(os.Environ(), icebergCLISubprocessEnv+"=1")
	out, err := cmd.CombinedOutput()

	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "expected non-zero exit; output: %s", out)
	assert.Equal(t, 1, exitErr.ExitCode())
	assert.Contains(t, string(out), "configuration error")
	assert.Contains(t, string(out), path)
}

func TestMergeConfAwsProfile(t *testing.T) {
	fileCfg := &config.CatalogConfig{AwsProfile: "file-profile"}

	t.Run("file value applied when flag absent", func(t *testing.T) {
		var a Args
		mergeConf(fileCfg, &a, map[string]bool{})
		assert.Equal(t, "file-profile", a.AwsProfile)
	})

	t.Run("cli value preserved when flag explicit", func(t *testing.T) {
		a := Args{AwsProfile: "cli-profile"}
		mergeConf(fileCfg, &a, map[string]bool{"aws-profile": true})
		assert.Equal(t, "cli-profile", a.AwsProfile)
	})
}
