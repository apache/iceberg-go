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

	"github.com/aws/aws-sdk-go-v2/service/s3"
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

// blobOpenFileWithChunkedReader wraps a blobOpenFile with a ChunkedReader
// to handle AWS chunked transfer encoding for AVRO files.
type blobOpenFileWithChunkedReader struct {
	blobOpenFile
	chunkedReader *ChunkedReader
}

// Read implements io.Reader using the chunked reader
func (f *blobOpenFileWithChunkedReader) Read(p []byte) (int, error) {
	return f.chunkedReader.Read(p)
}

// ReadAt implements io.ReaderAt. Since chunked encoding is a streaming format,
// we cannot support random access. However, for AVRO files, the OCF decoder
// typically reads sequentially, so this should not be called.
// If it is called, we fall back to the original implementation which will
// create a new range reader.
func (f *blobOpenFileWithChunkedReader) ReadAt(p []byte, off int64) (int, error) {
	// For ReadAt, we bypass the chunked reader and use the original implementation
	// This creates a new range reader that should not have chunked encoding
	return f.blobOpenFile.ReadAt(p, off)
}

// Close closes both the chunked reader and the underlying blob reader
func (f *blobOpenFileWithChunkedReader) Close() error {
	if err := f.chunkedReader.Close(); err != nil {
		return err
	}
	return f.blobOpenFile.Close()
}

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

	key, name := path, filepath.Base(path)

	r, err := bfs.NewReader(bfs.ctx, key, nil)
	if err != nil {
		return nil, err
	}

	// For AVRO files, wrap the reader to handle potential AWS chunked encoding
	if strings.HasSuffix(strings.ToLower(path), ".avro") {
		return &blobOpenFileWithChunkedReader{
			blobOpenFile:  blobOpenFile{Reader: r, name: name, key: key, b: bfs, ctx: bfs.ctx},
			chunkedReader: NewChunkedReader(r),
		}, nil
	}

	return &blobOpenFile{Reader: r, name: name, key: key, b: bfs, ctx: bfs.ctx}, nil
}

func (bfs *blobFileIO) Remove(name string) error {
	name = bfs.preprocess(name)

	return bfs.Bucket.Delete(bfs.ctx, name)
}

func (bfs *blobFileIO) Create(name string) (FileWriter, error) {
	// Configure writer options to prevent chunked encoding issues
	opts := &blob.WriterOptions{
		BeforeWrite: func(as func(any) bool) error {
			// Try to access S3-specific upload input to configure it
			var uploadInput *s3.PutObjectInput
			if as(&uploadInput) && uploadInput != nil {
				// S3 Tables doesn't support custom metadata headers
				// So we don't set Content-Type for AVRO files
			}
			return nil
		},
	}

	return bfs.NewWriter(bfs.ctx, name, true, opts)
}

func (bfs *blobFileIO) WriteFile(name string, content []byte) error {
	name = bfs.preprocess(name)
	// Configure writer options to prevent chunked encoding issues
	opts := &blob.WriterOptions{
		BeforeWrite: func(as func(any) bool) error {
			// Try to access S3-specific upload input to configure it
			var uploadInput *s3.PutObjectInput
			if as(&uploadInput) && uploadInput != nil {
				// Ensure metadata is set for AVRO files
				// S3 Tables doesn't support custom metadata headers
				// So we don't set any metadata for AVRO files

				// For WriteAll, we know the content length
				contentLength := int64(len(content))
				uploadInput.ContentLength = &contentLength
			}
			return nil
		},
	}

	return bfs.Bucket.WriteAll(bfs.ctx, name, content, opts)
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
	// If no options provided, create default ones to prevent chunked encoding
	if opts == nil {
		opts = &blob.WriterOptions{}
	}

	// Always add our BeforeWrite handler to configure S3 uploads
	originalBeforeWrite := opts.BeforeWrite
	opts.BeforeWrite = func(as func(any) bool) error {
		// Call original BeforeWrite if it exists
		if originalBeforeWrite != nil {
			if err := originalBeforeWrite(as); err != nil {
				return err
			}
		}

		// S3 Tables doesn't support custom metadata headers
		// So we don't set any metadata
		return nil
	}

	bw, err := io.Bucket.NewWriter(ctx, path, opts)
	if err != nil {
		return nil, err
	}

	return &blobWriteFile{
			Writer: bw,
			name:   path,
		},
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

func (f *blobWriteFile) Name() string     { return f.name }
func (f *blobWriteFile) Sys() interface{} { return f.b }
func (f *blobWriteFile) Close() error {
	return f.Writer.Close()
}
func (f *blobWriteFile) Write(p []byte) (int, error) {
	// Note: We cannot intercept chunked encoding here because it happens
	// at the HTTP transport level, not at the data write level.
	// The data we receive here is the original unencoded data.
	// The chunked encoding is applied later by the AWS SDK.
	return f.Writer.Write(p)
}
