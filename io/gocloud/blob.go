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
	"path"
	"path/filepath"
	"strings"
	"time"

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
	b         *BlobFileIO
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

type BlobFileIO struct {
	*blob.Bucket

	keyExtractor KeyExtractor
	ctx          context.Context

	// newRangeReader is an optional hook for testing.
	// It allows injecting a mock reader to verify Close calls.
	newRangeReader func(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
}

var _ icebergio.ListableIO = (*BlobFileIO)(nil)

type blobFileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
	sys     any
}

// blobFileInfo implements the fs.FileInfo interface for a blob object
var _ fs.FileInfo = (*blobFileInfo)(nil)

func (f blobFileInfo) Name() string       { return f.name }
func (f blobFileInfo) Size() int64        { return f.size }
func (f blobFileInfo) Mode() fs.FileMode  { return f.mode }
func (f blobFileInfo) ModTime() time.Time { return f.modTime }
func (f blobFileInfo) IsDir() bool        { return f.mode.IsDir() }
func (f blobFileInfo) Sys() any           { return f.sys }

func (bfs *BlobFileIO) preprocess(path string) (string, error) {
	return bfs.keyExtractor(path)
}

func blobPathError(op, name string, err error) error {
	if gcerrors.Code(err) == gcerrors.NotFound {
		err = fs.ErrNotExist
	}

	return &fs.PathError{Op: op, Path: name, Err: err}
}

func directoryMarker(key string) string {
	key = strings.TrimRight(key, "/")
	if key == "" {
		return ""
	}

	return key + "/"
}

func directoryName(key string) string {
	key = strings.TrimRight(key, "/")
	if key == "" {
		return "."
	}

	return path.Base(key)
}

func (bfs *BlobFileIO) Open(path string) (icebergio.File, error) {
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

func (bfs *BlobFileIO) Remove(name string) error {
	var err error
	name, err = bfs.preprocess(name)
	if err != nil {
		return &fs.PathError{Op: "remove", Path: name, Err: err}
	}

	if err := bfs.Delete(bfs.ctx, name); err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			marker := directoryMarker(name)
			if marker != "" {
				if markerErr := bfs.Delete(bfs.ctx, marker); markerErr == nil {
					return nil
				} else if gcerrors.Code(markerErr) != gcerrors.NotFound {
					return &fs.PathError{Op: "remove", Path: name, Err: markerErr}
				}
			}

			err = fs.ErrNotExist
		}

		return &fs.PathError{Op: "remove", Path: name, Err: err}
	}

	return nil
}

func (bfs *BlobFileIO) Create(name string) (icebergio.FileWriter, error) {
	return bfs.NewWriter(bfs.ctx, name, true, nil)
}

func (bfs *BlobFileIO) WriteFile(name string, content []byte) error {
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
func (bfs *BlobFileIO) NewWriter(ctx context.Context, path string, overwrite bool, opts *blob.WriterOptions) (w *blobWriteFile, err error) {
	path, err = bfs.preprocess(path)
	if err != nil {
		return nil, &fs.PathError{Op: "new writer", Path: path, Err: err}
	}
	if !fs.ValidPath(path) {
		return nil, &fs.PathError{Op: "new writer", Path: path, Err: fs.ErrInvalid}
	}

	if !overwrite {
		if exists, err := bfs.Exists(ctx, path); err != nil || exists {
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

func createBlobFS(ctx context.Context, bucket *blob.Bucket, keyExtractor KeyExtractor) icebergio.IO {
	return &BlobFileIO{Bucket: bucket, keyExtractor: keyExtractor, ctx: ctx}
}

func (bfs *BlobFileIO) WalkDir(root string, fn fs.WalkDirFunc) error {
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

func (bfs *BlobFileIO) DeleteFiles(ctx context.Context, paths []string) ([]string, error) {
	if len(paths) == 0 {
		return nil, nil
	}

	deleted := make([]string, 0, len(paths))

	var errs error

	for _, p := range paths {
		key, err := bfs.preprocess(p)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("failed to delete %s: %w", p, err))

			continue
		}

		if err := bfs.Delete(ctx, key); err != nil {
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

func (bfs *BlobFileIO) MkdirAll(path string) error {
	key, err := bfs.preprocess(path)
	if err != nil {
		return &fs.PathError{Op: "mkdir", Path: path, Err: err}
	}

	key = strings.Trim(key, "/")
	if key == "" || key == "." {
		return nil
	}

	parts := strings.Split(key, "/")
	for idx := range parts {
		marker := strings.Join(parts[:idx+1], "/") + "/"
		if err := bfs.WriteAll(bfs.ctx, marker, nil, nil); err != nil {
			return &fs.PathError{Op: "mkdir", Path: path, Err: err}
		}
	}

	return nil
}

func (bfs *BlobFileIO) ReadFile(path string) ([]byte, error) {
	key, err := bfs.preprocess(path)
	if err != nil {
		return nil, &fs.PathError{Op: "read file", Path: path, Err: err}
	}

	data, err := bfs.ReadAll(bfs.ctx, key)
	if err != nil {
		return nil, blobPathError("read file", path, err)
	}

	return data, nil
}

func (bfs *BlobFileIO) Stat(path string) (fs.FileInfo, error) {
	key, err := bfs.preprocess(path)
	if err != nil {
		return nil, &fs.PathError{Op: "stat", Path: path, Err: err}
	}

	attrs, err := bfs.Attributes(bfs.ctx, key)
	if err == nil {
		return blobFileInfo{
			name:    filepath.Base(key),
			size:    attrs.Size,
			mode:    fs.ModeIrregular,
			modTime: attrs.ModTime,
			sys:     attrs,
		}, nil
	}

	if gcerrors.Code(err) != gcerrors.NotFound {
		return nil, blobPathError("stat", path, err)
	}

	marker := directoryMarker(key)
	if marker != "" {
		if attrs, markerErr := bfs.Attributes(bfs.ctx, marker); markerErr == nil {
			return blobFileInfo{
				name:    directoryName(key),
				mode:    fs.ModeDir,
				modTime: attrs.ModTime,
				sys:     attrs,
			}, nil
		} else if gcerrors.Code(markerErr) != gcerrors.NotFound {
			return nil, blobPathError("stat", path, markerErr)
		}
	}

	prefix := marker
	if prefix == "" {
		prefix = key
	}

	iter := bfs.List(&blob.ListOptions{Prefix: prefix})
	if obj, listErr := iter.Next(bfs.ctx); listErr == nil {
		return blobFileInfo{
			name: directoryName(key),
			mode: fs.ModeDir,
			sys:  obj,
		}, nil
	} else if listErr != io.EOF {
		return nil, blobPathError("stat", path, listErr)
	}

	return nil, &fs.PathError{Op: "stat", Path: path, Err: fs.ErrNotExist}
}

func (bfs *BlobFileIO) Rename(oldpath, newpath string) error {
	oldKey, err := bfs.preprocess(oldpath)
	if err != nil {
		return &fs.PathError{Op: "rename", Path: oldpath, Err: err}
	}

	newKey, err := bfs.preprocess(newpath)
	if err != nil {
		return &fs.PathError{Op: "rename", Path: newpath, Err: err}
	}

	if err := bfs.Copy(bfs.ctx, newKey, oldKey, nil); err != nil {
		return blobPathError("rename", oldpath, err)
	}

	if err := bfs.Delete(bfs.ctx, oldKey); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
		return blobPathError("rename", oldpath, err)
	}

	return nil
}

func (bfs *BlobFileIO) RenameNoReplace(oldpath, newpath string) error {
	if _, err := bfs.Stat(newpath); err == nil {
		return &fs.PathError{Op: "rename", Path: newpath, Err: fs.ErrExist}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	return bfs.Rename(oldpath, newpath)
}

func (bfs *BlobFileIO) RemoveAll(name string) error {
	key, err := bfs.preprocess(name)
	if err != nil {
		return &fs.PathError{Op: "remove all", Path: name, Err: err}
	}

	var errs error
	if err := bfs.Delete(bfs.ctx, key); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
		errs = errors.Join(errs, err)
	}

	prefix := directoryMarker(key)
	if prefix == "" {
		prefix = key
	}

	iter := bfs.List(&blob.ListOptions{Prefix: prefix})
	for {
		obj, err := iter.Next(bfs.ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			errs = errors.Join(errs, err)

			break
		}
		if err := bfs.Delete(bfs.ctx, obj.Key); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
			errs = errors.Join(errs, err)
		}
	}

	if errs != nil {
		return &fs.PathError{Op: "remove all", Path: name, Err: errs}
	}

	return nil
}

type blobWriteFile struct {
	*blob.Writer
	name string
	b    *BlobFileIO
}

func (f *blobWriteFile) Name() string                { return f.name }
func (f *blobWriteFile) Sys() any                    { return f.b }
func (f *blobWriteFile) Close() error                { return f.Writer.Close() }
func (f *blobWriteFile) Write(p []byte) (int, error) { return f.Writer.Write(p) }
