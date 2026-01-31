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
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"

	"gocloud.dev/blob"
)

// blobOpenFile describes a single open blob as a File.
// It implements the iceberg-go/io.File interface.
// It is based on gocloud.dev/blob.iofsOpenFile which:
// - Doesn't support the `io.ReaderAt` interface
// - Is not externally accessible, so copied here
type blobOpenFile struct {
	*blob.Reader

	name, key string
	b         *blobFileIO
	ctx       context.Context
}

func (f *blobOpenFile) ReadAt(p []byte, off int64) (n int, err error) {
	var rdr io.ReadCloser
	if f.b.newRangeReader != nil {
		rdr, err = f.b.newRangeReader(f.ctx, f.key, off, int64(len(p)))
	} else {
		rdr, err = f.b.NewRangeReader(f.ctx, f.key, off, int64(len(p)), nil)
	}
	if err != nil {
		return 0, err
	}
	// not using internal.CheckedClose due to import cycle
	defer func() { err = errors.Join(err, rdr.Close()) }()

	return io.ReadFull(rdr, p)
}

// Functions to implement the `Stat()` function in the `io/fs.File` interface

func (f *blobOpenFile) Name() string               { return f.name }
func (f *blobOpenFile) Mode() fs.FileMode          { return fs.ModeIrregular }
func (f *blobOpenFile) Sys() any                   { return f.b }
func (f *blobOpenFile) IsDir() bool                { return false }
func (f *blobOpenFile) Stat() (fs.FileInfo, error) { return f, nil }

// KeyExtractor extracts the object key from an input path
type KeyExtractor func(path string) (string, error)

// defaultKeyExtractor extracts the object key by removing the scheme and bucket name from the URI
// e.g., s3://bucket/path/file -> path/file
func defaultKeyExtractor(bucketName string) KeyExtractor {
	return func(location string) (string, error) {
		_, after, found := strings.Cut(location, "://")
		if found {
			location = after
		}

		key := strings.TrimPrefix(location, bucketName+"/")

		if key == "" {
			return "", fmt.Errorf("URI path is empty: %s", location)
		}

		return key, nil
	}
}

type blobFileIO struct {
	*blob.Bucket

	keyExtractor KeyExtractor
	ctx          context.Context

	// newRangeReader is an optional hook for testing.
	// It allows injecting a mock reader to verify Close calls.
	newRangeReader func(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
}

func (bfs *blobFileIO) preprocess(path string) (string, error) {
	return bfs.keyExtractor(path)
}

func (bfs *blobFileIO) Open(path string) (File, error) {
	var err error
	path, err = bfs.preprocess(path)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: path, Err: err}
	}
	if !fs.ValidPath(path) {
		return nil, &fs.PathError{Op: "open", Path: path, Err: fs.ErrInvalid}
	}

	key, name := path, filepath.Base(path)

	r, err := bfs.NewReader(bfs.ctx, key, nil)
	if err != nil {
		return nil, err
	}

	return &blobOpenFile{Reader: r, name: name, key: key, b: bfs, ctx: bfs.ctx}, nil
}

func (bfs *blobFileIO) Remove(name string) error {
	var err error
	name, err = bfs.preprocess(name)
	if err != nil {
		return &fs.PathError{Op: "remove", Path: name, Err: err}
	}

	return bfs.Delete(bfs.ctx, name)
}

func (bfs *blobFileIO) Create(name string) (FileWriter, error) {
	return bfs.NewWriter(bfs.ctx, name, true, nil)
}

func (bfs *blobFileIO) WriteFile(name string, content []byte) error {
	var err error
	name, err = bfs.preprocess(name)
	if err != nil {
		return &fs.PathError{Op: "write file", Path: name, Err: err}
	}

	return bfs.WriteAll(bfs.ctx, name, content, nil)
}

// NewWriter returns a Writer that writes to the blob stored at path.
// A nil WriterOptions is treated the same as the zero value.
//
// If overwrite is disabled and a blob with this path already exists,
// an error will be returned.
//
// The caller must call Close on the returned Writer, even if the write is
// aborted.
func (bfs *blobFileIO) NewWriter(ctx context.Context, path string, overwrite bool, opts *blob.WriterOptions) (w *blobWriteFile, err error) {
	path, err = bfs.preprocess(path)
	if err != nil {
		return nil, &fs.PathError{Op: "new writer", Path: path, Err: err}
	}
	if !fs.ValidPath(path) {
		return nil, &fs.PathError{Op: "new writer", Path: path, Err: fs.ErrInvalid}
	}

	if !overwrite {
		if exists, err := bfs.Exists(ctx, path); exists {
			if err != nil {
				return nil, &fs.PathError{Op: "new writer", Path: path, Err: err}
			}

			return nil, &fs.PathError{Op: "new writer", Path: path, Err: fs.ErrInvalid}
		}
	}
	bw, err := bfs.Bucket.NewWriter(ctx, path, opts)
	if err != nil {
		return nil, err
	}

	return &blobWriteFile{
			Writer: bw,
			name:   path,
		},
		nil
}

func createBlobFS(ctx context.Context, bucket *blob.Bucket, keyExtractor KeyExtractor) IO {
	return &blobFileIO{Bucket: bucket, keyExtractor: keyExtractor, ctx: ctx}
}

type blobWriteFile struct {
	*blob.Writer
	name string
	b    *blobFileIO
}

func (f *blobWriteFile) Name() string                { return f.name }
func (f *blobWriteFile) Sys() any                    { return f.b }
func (f *blobWriteFile) Close() error                { return f.Writer.Close() }
func (f *blobWriteFile) Write(p []byte) (int, error) { return f.Writer.Write(p) }
