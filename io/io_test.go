// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package io_test

import (
	"io/fs"
	"testing"
	"testing/fstest"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

type recordingFS struct {
	files   fstest.MapFS
	opened  string
	read    string
	removed string
}

func (f *recordingFS) Open(name string) (fs.File, error) {
	f.opened = name

	return f.files.Open(name)
}

func (f *recordingFS) ReadFile(name string) ([]byte, error) {
	f.read = name

	return f.files.ReadFile(name)
}

func (f *recordingFS) Remove(name string) error {
	f.removed = name

	return nil
}

func TestFSPreProcNameTransformsAllOperations(t *testing.T) {
	underlying := &recordingFS{files: fstest.MapFS{
		"rewritten/file.txt": &fstest.MapFile{Data: []byte("data")},
	}}
	fsys := iceio.FSPreProcName(underlying, func(string) string {
		return "/rewritten/file.txt"
	})

	file, err := fsys.Open("file://bucket/original.txt")
	require.NoError(t, err)
	require.NoError(t, file.Close())
	_, err = fsys.(iceio.ReadFileIO).ReadFile("file://bucket/original.txt")
	require.NoError(t, err)
	require.NoError(t, fsys.Remove("file://bucket/original.txt"))

	require.Equal(t, "rewritten/file.txt", underlying.opened)
	require.Equal(t, underlying.opened, underlying.read)
	require.Equal(t, underlying.opened, underlying.removed)
}
