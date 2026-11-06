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
	"context"
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

type contextReadRecordingFS struct {
	files   fstest.MapFS
	readCtx context.Context
	read    string
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

func (f *contextReadRecordingFS) Open(name string) (fs.File, error) {
	return f.files.Open(name)
}

func (f *contextReadRecordingFS) ReadFile(ctx context.Context, name string) ([]byte, error) {
	f.readCtx = ctx
	f.read = name
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return f.files.ReadFile(name)
}

func TestFSWithNamePreprocessorTransformsAllOperations(t *testing.T) {
	underlying := &recordingFS{files: fstest.MapFS{
		"rewritten/file.txt": &fstest.MapFile{Data: []byte("data")},
	}}
	fsys := iceio.FS(underlying, iceio.WithNamePreprocessor(func(string) string {
		return "/rewritten/file.txt"
	}))

	file, err := fsys.Open(context.Background(), "file://bucket/original.txt")
	require.NoError(t, err)
	require.NoError(t, file.Close())
	_, err = fsys.(iceio.ReadFileIO).ReadFile(context.Background(), "file://bucket/original.txt")
	require.NoError(t, err)
	require.NoError(t, fsys.Remove(context.Background(), "file://bucket/original.txt"))

	require.Equal(t, "rewritten/file.txt", underlying.opened)
	require.Equal(t, underlying.opened, underlying.read)
	require.Equal(t, underlying.opened, underlying.removed)
}

func TestFSDefaultAndNilOptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []iceio.FSOption
	}{
		{name: "default"},
		{name: "nil option", opts: []iceio.FSOption{nil}},
		{name: "nil preprocessor", opts: []iceio.FSOption{iceio.WithNamePreprocessor(nil)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			underlying := &recordingFS{files: fstest.MapFS{
				"plain.txt": &fstest.MapFile{Data: []byte("data")},
			}}
			fsys := iceio.FS(underlying, tc.opts...)

			file, err := fsys.Open(context.Background(), "/plain.txt")
			require.NoError(t, err)
			require.NoError(t, file.Close())

			reader, ok := fsys.(iceio.ReadFileIO)
			require.True(t, ok)
			data, err := reader.ReadFile(context.Background(), "/plain.txt")
			require.NoError(t, err)
			require.Equal(t, []byte("data"), data)

			require.NoError(t, fsys.Remove(context.Background(), "/plain.txt"))
			require.Equal(t, "plain.txt", underlying.opened)
			require.Equal(t, underlying.opened, underlying.read)
			require.Equal(t, underlying.opened, underlying.removed)
		})
	}
}

type openOnlyFS struct{ fs.FS }

func TestFSDoesNotAdvertiseUnsupportedReadFile(t *testing.T) {
	fsys := iceio.FS(openOnlyFS{fstest.MapFS{
		"plain.txt": &fstest.MapFile{Data: []byte("data")},
	}}, iceio.WithNamePreprocessor(func(string) string {
		return "/plain.txt"
	}))

	_, ok := fsys.(iceio.ReadFileIO)
	require.False(t, ok)

	file, err := fsys.Open(context.Background(), "unused")
	require.NoError(t, err)
	require.NoError(t, file.Close())
}

func TestFSContextReadFilePassesOperationContext(t *testing.T) {
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "caller")
	underlying := &contextReadRecordingFS{files: fstest.MapFS{
		"rewritten/file.txt": &fstest.MapFile{Data: []byte("data")},
	}}
	fsys := iceio.FS(underlying, iceio.WithNamePreprocessor(func(string) string {
		return "/rewritten/file.txt"
	}))

	reader, ok := fsys.(iceio.ReadFileIO)
	require.True(t, ok)
	data, err := reader.ReadFile(ctx, "file://bucket/original.txt")
	require.NoError(t, err)
	require.Equal(t, []byte("data"), data)
	require.NotNil(t, underlying.readCtx)
	require.Equal(t, "caller", underlying.readCtx.Value(key{}))
	require.Equal(t, "rewritten/file.txt", underlying.read)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = reader.ReadFile(canceledCtx, "file://bucket/original.txt")
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, underlying.readCtx.Err(), context.Canceled)
}

func TestFSPlainReadFileIgnoresCancellation(t *testing.T) {
	underlying := &recordingFS{files: fstest.MapFS{
		"plain.txt": &fstest.MapFile{Data: []byte("data")},
	}}
	reader := iceio.FS(underlying).(iceio.ReadFileIO)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	data, err := reader.ReadFile(ctx, "plain.txt")
	require.NoError(t, err)
	require.Equal(t, []byte("data"), data)
}
