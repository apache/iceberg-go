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

package recipe

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/mod/modfile"
)

func TestGoModRequiresNoContainerSDKs(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	require.NoError(t, err)
	f, err := modfile.Parse("go.mod", data, nil)
	require.NoError(t, err)
	require.Equal(t, "github.com/apache/iceberg-go", f.Module.Mod.Path)
	require.NotEmpty(t, f.Require)

	denied := []string{
		"github.com/testcontainers/testcontainers-go",
		"github.com/docker/docker",
		"github.com/docker/compose",
		"github.com/moby/moby",
	}
	for _, r := range f.Require {
		for _, d := range denied {
			assert.False(t, r.Mod.Path == d || strings.HasPrefix(r.Mod.Path, d+"/"),
				"go.mod requires %s: container SDKs raise dependency versions for every "+
					"iceberg-go consumer, use the docker CLI as internal/recipe does", r.Mod.Path)
		}
	}
}

func fakeDocker(t *testing.T) func() [][]string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("the fake docker binary is a shell script")
	}

	dir := t.TempDir()
	calls := filepath.Join(dir, "calls")
	script := `#!/bin/sh
for a in "$@"; do printf '%s\037' "$a"; done >> "$FAKE_DOCKER_CALLS"
echo >> "$FAKE_DOCKER_CALLS"
printf '%s' "$FAKE_DOCKER_OUTPUT"
if [ -n "$FAKE_DOCKER_FAIL_ON" ]; then
	for a in "$@"; do [ "$a" = "$FAKE_DOCKER_FAIL_ON" ] && exit 3; done
fi
exit 0
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "docker"), []byte(script), 0o755))
	t.Setenv("PATH", dir)
	t.Setenv("FAKE_DOCKER_CALLS", calls)
	t.Setenv("FAKE_DOCKER_OUTPUT", "")
	t.Setenv("FAKE_DOCKER_FAIL_ON", "")

	return func() [][]string {
		data, err := os.ReadFile(calls)
		if os.IsNotExist(err) {
			return nil
		}
		require.NoError(t, err)

		var got [][]string
		for line := range strings.Lines(string(data)) {
			got = append(got, strings.Split(strings.TrimSuffix(line, "\037\n"), "\037"))
		}

		return got
	}
}

func unsetenv(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "")
	require.NoError(t, os.Unsetenv(key))
}

func resetStartStack(t *testing.T) {
	t.Helper()
	prev := startStack
	startStack = newStartStack()
	t.Cleanup(func() { startStack = prev })
}

func TestExecuteSpark(t *testing.T) {
	tests := []struct {
		name        string
		containerID string
		wantArgs    []string
	}{
		{
			name:     "named spark container by default",
			wantArgs: []string{"exec", "spark-iceberg", "python", "./validation.py", "--sql", "SELECT 1"},
		},
		{
			name:        "SPARK_CONTAINER_ID overrides the container",
			containerID: "abc123",
			wantArgs:    []string{"exec", "abc123", "python", "./validation.py", "--sql", "SELECT 1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := fakeDocker(t)
			t.Setenv("FAKE_DOCKER_OUTPUT", "1")
			unsetenv(t, "SPARK_CONTAINER_ID")
			if tt.containerID != "" {
				t.Setenv("SPARK_CONTAINER_ID", tt.containerID)
			}

			out, err := ExecuteSpark(t, "./validation.py", "--sql", "SELECT 1")
			require.NoError(t, err)
			assert.Equal(t, "1", out)
			assert.Equal(t, [][]string{tt.wantArgs}, calls())
		})
	}
}

func TestExecuteSparkFailedScriptReturnsError(t *testing.T) {
	fakeDocker(t)
	unsetenv(t, "SPARK_CONTAINER_ID")
	t.Setenv("FAKE_DOCKER_OUTPUT", "Traceback")
	t.Setenv("FAKE_DOCKER_FAIL_ON", "./validation.py")

	out, err := ExecuteSpark(t, "./validation.py", "--sql", "SELECT 1")
	require.ErrorContains(t, err, "failed to execute script ./validation.py")
	assert.ErrorContains(t, err, "exit status 3")
	assert.Empty(t, out)
}

func TestStartSkipsComposeWhenStackIsRunning(t *testing.T) {
	calls := fakeDocker(t)
	resetStartStack(t)
	t.Setenv("AWS_S3_ENDPOINT", "http://object-store:9000")

	require.NoError(t, Start(t))
	assert.Empty(t, calls())
	assert.Equal(t, "http://object-store:9000", os.Getenv("AWS_S3_ENDPOINT"))
}

func TestStartBringsUpAndProvisionsStackOnce(t *testing.T) {
	calls := fakeDocker(t)
	resetStartStack(t)
	unsetenv(t, "AWS_S3_ENDPOINT")
	unsetenv(t, "AWS_REGION")
	composeFile, err := filepath.Abs("docker-compose.yml")
	require.NoError(t, err)
	require.FileExists(t, composeFile)

	// Each subtest restores the env on exit, so the second Start reaches the
	// compose path again and only the once keeps it from re-provisioning.
	for _, name := range []string{"first", "second"} {
		t.Run(name, func(t *testing.T) {
			_, running := os.LookupEnv("AWS_S3_ENDPOINT")
			require.False(t, running)

			require.NoError(t, Start(t))
			assert.Equal(t, "http://localhost:9000", os.Getenv("AWS_S3_ENDPOINT"))
			assert.Equal(t, "us-east-1", os.Getenv("AWS_REGION"))
		})
	}

	assert.Equal(t, [][]string{
		{"compose", "-f", composeFile, "up", "-d", "--wait"},
		{"compose", "-f", composeFile, "exec", "-T", "spark-iceberg", "ipython", "./provision.py"},
	}, calls())
}

func TestStartFailureIsReportedAndNotRetried(t *testing.T) {
	tests := []struct {
		name      string
		failOn    string
		wantErr   string
		wantCalls int
	}{
		{name: "compose up fails", failOn: "up", wantErr: "fail to up compose", wantCalls: 1},
		{name: "provision fails", failOn: "./provision.py", wantErr: "fail to seed provision.py", wantCalls: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := fakeDocker(t)
			resetStartStack(t)
			unsetenv(t, "AWS_S3_ENDPOINT")
			t.Setenv("FAKE_DOCKER_OUTPUT", "boom")
			t.Setenv("FAKE_DOCKER_FAIL_ON", tt.failOn)

			err := Start(t)
			require.ErrorContains(t, err, tt.wantErr)
			assert.ErrorContains(t, err, "boom")
			_, running := os.LookupEnv("AWS_S3_ENDPOINT")
			assert.False(t, running)

			assert.ErrorContains(t, Start(t), tt.wantErr)
			assert.Len(t, calls(), tt.wantCalls)
		})
	}
}
