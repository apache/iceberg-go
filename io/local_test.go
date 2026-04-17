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

package io

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalFSWalkDir(t *testing.T) {
	dir := t.TempDir()

	// Create a directory structure.
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "data"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "metadata"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data", "file1.parquet"), []byte("a"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data", "file2.parquet"), []byte("b"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "metadata", "v1.json"), []byte("c"), 0o644))

	lfs := LocalFS{}

	var files []string
	err := lfs.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			files = append(files, path)
		}

		return nil
	})
	require.NoError(t, err)

	sort.Strings(files)
	expected := []string{
		filepath.Join(dir, "data", "file1.parquet"),
		filepath.Join(dir, "data", "file2.parquet"),
		filepath.Join(dir, "metadata", "v1.json"),
	}
	sort.Strings(expected)
	assert.Equal(t, expected, files)
}

func TestLocalFSWalkDirWithFileScheme(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(dir, "test.txt"), []byte("hello"), 0o644))

	lfs := LocalFS{}

	var files []string
	err := lfs.WalkDir("file://"+dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			files = append(files, path)
		}

		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{filepath.Join(dir, "test.txt")}, files)
}

func TestLocalFSImplementsListableIO(t *testing.T) {
	var _ ListableIO = LocalFS{}
}

func TestLocalFSDoesNotImplementBulkRemovableIO(t *testing.T) {
	var fs IO = LocalFS{}
	_, ok := fs.(BulkRemovableIO)
	assert.False(t, ok, "LocalFS should not implement BulkRemovableIO")
}
