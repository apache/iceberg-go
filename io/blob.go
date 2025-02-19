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

func (f *blobOpenFile) ReadAt(p []byte, off int64) (int, error) {
	rdr, err := f.b.Bucket.NewRangeReader(f.ctx, f.key, off, int64(len(p)), nil)
	if err != nil {
		return 0, err
	}

	// ensure the buffer is read, or EOF is reached for this read of this "chunk"
	// given we are using offsets to read this block, it is constrained by size of 'p'
	size, err := io.ReadFull(rdr, p)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return size, err
		}
		// check we are at the end of the underlying file
		if off+int64(size) > f.Size() {
			return size, err
		}
	}

	return size, rdr.Close()
}

// Functions to implement the `Stat()` function in the `io/fs.File` interface

func (f *blobOpenFile) Name() string               { return f.name }
func (f *blobOpenFile) Mode() fs.FileMode          { return fs.ModeIrregular }
func (f *blobOpenFile) Sys() interface{}           { return f.b }
func (f *blobOpenFile) IsDir() bool                { return false }
func (f *blobOpenFile) Stat() (fs.FileInfo, error) { return f, nil }

type blobFileIO struct {
	*blob.Bucket

	bucketName string
	ctx        context.Context
}

func (bfs *blobFileIO) preprocess(key string) string {
	_, after, found := strings.Cut(key, "://")
	if found {
		key = after
	}

	return strings.TrimPrefix(key, bfs.bucketName+"/")
}

func (bfs *blobFileIO) Open(path string) (File, error) {
	path = bfs.preprocess(path)
	if !fs.ValidPath(path) {
		return nil, &fs.PathError{Op: "open", Path: path, Err: fs.ErrInvalid}
	}

	var (
		key, name = path, filepath.Base(path)
	)

	r, err := bfs.NewReader(bfs.ctx, key, nil)
	if err != nil {
		return nil, err
	}

	return &blobOpenFile{Reader: r, name: name, key: key, b: bfs, ctx: bfs.ctx}, nil
}

func (bfs *blobFileIO) Remove(name string) error {
	name = bfs.preprocess(name)
	return bfs.Bucket.Delete(bfs.ctx, name)
}

func (bfs *blobFileIO) Create(name string) (FileWriter, error) {
	return bfs.NewWriter(bfs.ctx, name, true, nil)
}

func (bfs *blobFileIO) WriteFile(name string, content []byte) error {
	name = bfs.preprocess(name)
	return bfs.Bucket.WriteAll(bfs.ctx, name, content, nil)
}

// NewWriter returns a Writer that writes to the blob stored at path.
// A nil WriterOptions is treated the same as the zero value.
//
// If overwrite is disabled and a blob with this path already exists,
// an error will be returned.
//
// The caller must call Close on the returned Writer, even if the write is
// aborted.
func (io *blobFileIO) NewWriter(ctx context.Context, path string, overwrite bool, opts *blob.WriterOptions) (w *blobWriteFile, err error) {
	path = io.preprocess(path)
	if !fs.ValidPath(path) {
		return nil, &fs.PathError{Op: "new writer", Path: path, Err: fs.ErrInvalid}
	}

	if !overwrite {
		if exists, err := io.Bucket.Exists(ctx, path); exists {
			if err != nil {
				return nil, &fs.PathError{Op: "new writer", Path: path, Err: err}
			}
			return nil, &fs.PathError{Op: "new writer", Path: path, Err: fs.ErrInvalid}
		}
	}
	bw, err := io.Bucket.NewWriter(ctx, path, opts)
	if err != nil {
		return nil, err
	}
	return &blobWriteFile{
			Writer: bw,
			name:   path},
		nil
}

func createBlobFS(ctx context.Context, bucket *blob.Bucket, bucketName string) IO {
	return &blobFileIO{Bucket: bucket, bucketName: bucketName, ctx: ctx}
}

type blobWriteFile struct {
	*blob.Writer
	name string
	b    *blobFileIO
}

func (f *blobWriteFile) Name() string                { return f.name }
func (f *blobWriteFile) Sys() interface{}            { return f.b }
func (f *blobWriteFile) Close() error                { return f.Writer.Close() }
func (f *blobWriteFile) Write(p []byte) (int, error) { return f.Writer.Write(p) }
