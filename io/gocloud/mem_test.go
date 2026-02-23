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

package gocloud_test

import (
	"context"
	"io"
	"testing"

	icebergio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemIO_BasicOperations(t *testing.T) {
	ctx := context.Background()

	memIO, err := icebergio.LoadFS(ctx, map[string]string{}, "mem://bucket/")
	require.NoError(t, err)
	require.NotNil(t, memIO)

	writeIO, ok := memIO.(icebergio.WriteFileIO)
	require.True(t, ok, "mem IO should implement WriteFileIO")

	testData := []byte("Hello, Iceberg!")
	err = writeIO.WriteFile("test-file.txt", testData)
	require.NoError(t, err)

	file, err := memIO.Open("test-file.txt")
	require.NoError(t, err)
	defer file.Close()

	content, err := io.ReadAll(file)
	require.NoError(t, err)
	assert.Equal(t, testData, content)

	err = memIO.Remove("test-file.txt")
	require.NoError(t, err)

	_, err = memIO.Open("test-file.txt")
	assert.Error(t, err)
}

func TestMemIO_Create(t *testing.T) {
	ctx := context.Background()

	memIO, err := icebergio.LoadFS(ctx, map[string]string{}, "mem://bucket/")
	require.NoError(t, err)

	writeIO := memIO.(icebergio.WriteFileIO)

	writer, err := writeIO.Create("created-file.txt")
	require.NoError(t, err)
	require.NotNil(t, writer)

	testData := []byte("Data written via Create")
	n, err := writer.Write(testData)
	require.NoError(t, err)
	assert.Equal(t, len(testData), n)

	err = writer.Close()
	require.NoError(t, err)

	file, err := memIO.Open("created-file.txt")
	require.NoError(t, err)
	defer file.Close()

	content, err := io.ReadAll(file)
	require.NoError(t, err)
	assert.Equal(t, testData, content)
}

func TestMemIO_MultipleFiles(t *testing.T) {
	ctx := context.Background()

	memIO, err := icebergio.LoadFS(ctx, map[string]string{}, "mem://bucket/")
	require.NoError(t, err)

	writeIO := memIO.(icebergio.WriteFileIO)

	files := map[string][]byte{
		"file1.txt": []byte("Content of file 1"),
		"file2.txt": []byte("Content of file 2"),
		"file3.txt": []byte("Content of file 3"),
	}

	for name, content := range files {
		err := writeIO.WriteFile(name, content)
		require.NoError(t, err)
	}

	for name, expectedContent := range files {
		file, err := memIO.Open(name)
		require.NoError(t, err)

		content, err := io.ReadAll(file)
		require.NoError(t, err)
		assert.Equal(t, expectedContent, content)

		err = file.Close()
		require.NoError(t, err)
	}

	err = memIO.Remove("file2.txt")
	require.NoError(t, err)

	_, err = memIO.Open("file2.txt")
	assert.Error(t, err)

	file1, err := memIO.Open("file1.txt")
	require.NoError(t, err)
	file1.Close()

	file3, err := memIO.Open("file3.txt")
	require.NoError(t, err)
	file3.Close()
}
