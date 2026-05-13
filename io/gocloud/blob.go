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

package gocloud

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"path/filepath"
	"strings"
	"sync"

	icebergio "github.com/apache/iceberg-go/io"
	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
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

// BucketOpener creates a new blob.Bucket for the given bucket name.
// Used to lazily open secondary buckets when a URI references a bucket
// different from the primary one (e.g. write.metadata.path pointing to
// a separate metadata bucket).
type BucketOpener func(ctx context.Context, bucketName string) (*blob.Bucket, error)

type blobFileIO struct {
	*blob.Bucket

	primaryBucket string
	bucketOpener  BucketOpener

	mu      sync.RWMutex
	buckets map[string]*blob.Bucket

	keyExtractor KeyExtractor
	ctx          context.Context

	// newRangeReader is an optional hook for testing.
	// It allows injecting a mock reader to verify Close calls.
	newRangeReader func(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
}

// resolveBucket parses path into a bucket handle and object key. If the URI
// references the primary bucket (or has no scheme), the primary bucket is
// returned without any additional allocation. URIs that reference a different
// bucket are resolved via BucketOpener and cached for reuse.
func (bfs *blobFileIO) resolveBucket(path string) (*blob.Bucket, string, error) {
	_, after, hasScheme := strings.Cut(path, "://")
	if !hasScheme {
		// No scheme: treat as a key in the primary bucket (legacy behavior).
		key, err := bfs.keyExtractor(path)
		return bfs.Bucket, key, err
	}

	bucketName, key, _ := strings.Cut(after, "/")
	if key == "" {
		return nil, "", fmt.Errorf("URI path is empty: %s", path)
	}

	// Fast path: primary bucket.
	if bucketName == bfs.primaryBucket {
		return bfs.Bucket, key, nil
	}

	// Secondary bucket: check cache first.
	if bfs.bucketOpener == nil {
		// No opener configured: fall back to primary bucket with legacy key extraction.
		// This preserves backward compatibility for callers that don't set a BucketOpener.
		key, err := bfs.keyExtractor(path)
		return bfs.Bucket, key, err
	}

	bfs.mu.RLock()
	b, ok := bfs.buckets[bucketName]
	bfs.mu.RUnlock()
	if ok {
		return b, key, nil
	}

	// Open a new bucket handle.
	b, err := bfs.bucketOpener(bfs.ctx, bucketName)
	if err != nil {
		return nil, "", fmt.Errorf("failed to open bucket %q: %w", bucketName, err)
	}

	bfs.mu.Lock()
	// Double-check: another goroutine may have opened it concurrently.
	if existing, ok := bfs.buckets[bucketName]; ok {
		bfs.mu.Unlock()
		_ = b.Close()
		return existing, key, nil
	}
	bfs.buckets[bucketName] = b
	bfs.mu.Unlock()

	return b, key, nil
}

func (bfs *blobFileIO) preprocess(path string) (string, error) {
	return bfs.keyExtractor(path)
}

func (bfs *blobFileIO) Open(path string) (icebergio.File, error) {
	bucket, key, err := bfs.resolveBucket(path)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: path, Err: err}
	}
	if !fs.ValidPath(key) {
		return nil, &fs.PathError{Op: "open", Path: key, Err: fs.ErrInvalid}
	}

	name := filepath.Base(key)

	r, err := bucket.NewReader(bfs.ctx, key, nil)
	if err != nil {
		return nil, err
	}

	return &blobOpenFile{Reader: r, name: name, key: key, b: bfs, ctx: bfs.ctx}, nil
}

func (bfs *blobFileIO) Remove(name string) error {
	bucket, key, err := bfs.resolveBucket(name)
	if err != nil {
		return &fs.PathError{Op: "remove", Path: name, Err: err}
	}

	return bucket.Delete(bfs.ctx, key)
}

func (bfs *blobFileIO) Create(name string) (icebergio.FileWriter, error) {
	return bfs.NewWriter(bfs.ctx, name, true, nil)
}

func (bfs *blobFileIO) WriteFile(name string, content []byte) error {
	bucket, key, err := bfs.resolveBucket(name)
	if err != nil {
		return &fs.PathError{Op: "write file", Path: name, Err: err}
	}

	return bucket.WriteAll(bfs.ctx, key, content, nil)
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
	bucket, key, err := bfs.resolveBucket(path)
	if err != nil {
		return nil, &fs.PathError{Op: "new writer", Path: path, Err: err}
	}
	if !fs.ValidPath(key) {
		return nil, &fs.PathError{Op: "new writer", Path: key, Err: fs.ErrInvalid}
	}

	if !overwrite {
		if exists, err := bucket.Exists(ctx, key); err != nil || exists {
			if err != nil {
				return nil, &fs.PathError{Op: "new writer", Path: key, Err: err}
			}

			return nil, &fs.PathError{Op: "new writer", Path: key, Err: fs.ErrInvalid}
		}
	}
	bw, err := bucket.NewWriter(ctx, key, opts)
	if err != nil {
		return nil, err
	}

	return &blobWriteFile{
			Writer: bw,
			name:   key,
		},
		nil
}

func createBlobFS(ctx context.Context, bucket *blob.Bucket, keyExtractor KeyExtractor) icebergio.IO {
	return &blobFileIO{
		Bucket:       bucket,
		keyExtractor: keyExtractor,
		ctx:          ctx,
		buckets:      make(map[string]*blob.Bucket),
	}
}

func createMultiBucketBlobFS(ctx context.Context, bucket *blob.Bucket, primaryBucket string, opener BucketOpener) icebergio.IO {
	return &blobFileIO{
		Bucket:        bucket,
		primaryBucket: primaryBucket,
		bucketOpener:  opener,
		buckets:       make(map[string]*blob.Bucket),
		keyExtractor:  defaultKeyExtractor(primaryBucket),
		ctx:           ctx,
	}
}

func (bfs *blobFileIO) WalkDir(root string, fn fs.WalkDirFunc) error {
	parsed, err := url.Parse(root)
	if err != nil {
		return fmt.Errorf("invalid URL %s: %w", root, err)
	}

	walkPath := strings.TrimPrefix(parsed.Path, "/")
	if walkPath == "" {
		walkPath = "."
	}

	parsed.Path = ""

	return fs.WalkDir(bfs.Bucket, walkPath, func(path string, d fs.DirEntry, err error) error {
		return fn(parsed.JoinPath(path).String(), d, err)
	})
}

func (bfs *blobFileIO) DeleteFiles(ctx context.Context, paths []string) ([]string, error) {
	if len(paths) == 0 {
		return nil, nil
	}

	deleted := make([]string, 0, len(paths))

	var errs error

	for _, p := range paths {
		bucket, key, err := bfs.resolveBucket(p)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("failed to delete %s: %w", p, err))

			continue
		}

		if err := bucket.Delete(ctx, key); err != nil {
			// Missing files are not errors per the interface contract.
			if gcerrors.Code(err) == gcerrors.NotFound {
				deleted = append(deleted, p)

				continue
			}

			errs = errors.Join(errs, fmt.Errorf("failed to delete %s: %w", p, err))

			continue
		}

		deleted = append(deleted, p)
	}

	return deleted, errs
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
