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
	"io"
	"io/fs"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"gocloud.dev/blob"
)

// iofsFileInfo describes a single file in an io/fs.FS.
// It implements fs.FileInfo and fs.DirEntry.
// Copied from `gocloud.dev/blob.iofsDir` because it is private
type iofsFileInfo struct {
	lo   *blob.ListObject
	name string
}

func (f *iofsFileInfo) Name() string               { return f.name }
func (f *iofsFileInfo) Size() int64                { return f.lo.Size }
func (f *iofsFileInfo) Mode() fs.FileMode          { return fs.ModeIrregular }
func (f *iofsFileInfo) ModTime() time.Time         { return f.lo.ModTime }
func (f *iofsFileInfo) IsDir() bool                { return false }
func (f *iofsFileInfo) Sys() interface{}           { return f.lo }
func (f *iofsFileInfo) Info() (fs.FileInfo, error) { return f, nil }
func (f *iofsFileInfo) Type() fs.FileMode          { return fs.ModeIrregular }

// iofsDir describes a single directory in an `iceberg-go/io.FS`.
// It implements `io/fs.FileInfo`, `io/fs.DirEntry`, and `io/fs.File`.
// Copied from `gocloud.dev/blob.iofsDir`, but modified to use `iceberg-go/io.File` instead of `io/fs.File`
type iofsDir struct {
	b    *blobFileIO
	key  string
	name string
	// If opened is true, we've read entries via openOnce().
	opened  bool
	entries []fs.DirEntry
	offset  int
}

func newDir(b *blobFileIO, key, name string) *iofsDir {
	return &iofsDir{b: b, key: key, name: name}
}

func (d *iofsDir) Name() string               { return d.name }
func (d *iofsDir) Size() int64                { return 0 }
func (d *iofsDir) Mode() fs.FileMode          { return fs.ModeDir }
func (d *iofsDir) Type() fs.FileMode          { return fs.ModeDir }
func (d *iofsDir) ModTime() time.Time         { return time.Time{} }
func (d *iofsDir) IsDir() bool                { return true }
func (d *iofsDir) Sys() interface{}           { return d }
func (d *iofsDir) Info() (fs.FileInfo, error) { return d, nil }
func (d *iofsDir) Stat() (fs.FileInfo, error) { return d, nil }
func (d *iofsDir) Read([]byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: d.key, Err: fs.ErrInvalid}
}
func (d *iofsDir) ReadAt(p []byte, off int64) (int, error) {
	return 0, &fs.PathError{Op: "readAt", Path: d.key, Err: fs.ErrInvalid}
}
func (d *iofsDir) Seek(offset int64, whence int) (int64, error) {
	return 0, &fs.PathError{Op: "seek", Path: d.key, Err: fs.ErrInvalid}
}
func (d *iofsDir) Close() error { return nil }
func (d *iofsDir) ReadDir(count int) ([]fs.DirEntry, error) {
	if err := d.openOnce(); err != nil {
		return nil, err
	}
	n := len(d.entries) - d.offset
	if n == 0 && count > 0 {
		return nil, io.EOF
	}
	if count > 0 && n > count {
		n = count
	}
	list := make([]fs.DirEntry, n)
	for i := range list {
		list[i] = d.entries[d.offset+i]
	}
	d.offset += n
	return list, nil
}

func (d *iofsDir) openOnce() error {
	if d.opened {
		return nil
	}
	d.opened = true

	// blob expects directories to end in the delimiter, except at the top level.
	prefix := d.key
	if prefix != "" {
		prefix += "/"
	}
	listOpts := blob.ListOptions{
		Prefix:    prefix,
		Delimiter: "/",
	}
	ctx := d.b.ctx

	// Fetch all the directory entries.
	// Conceivably we could only fetch a few here, and fetch the rest lazily
	// on demand, but that would add significant complexity.
	iter := d.b.List(&listOpts)
	for {
		item, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		name := filepath.Base(item.Key)
		if item.IsDir {
			d.entries = append(d.entries, newDir(d.b, item.Key, name))
		} else {
			d.entries = append(d.entries, &iofsFileInfo{item, name})
		}
	}
	// There is no such thing as an empty directory in Bucket, so if
	// we didn't find anything, it doesn't exist.
	if len(d.entries) == 0 {
		return fs.ErrNotExist
	}
	return nil
}

// blobOpenFile describes a single open blob as a File.
// It implements the iceberg-go/io.File interface.
// It is based on gocloud.dev/blob.iofsOpenFile which:
// - Doesn't support the `io.ReaderAt` interface
// - Is not externally accessible, so copied here
type blobOpenFile struct {
	*blob.Reader
	name string
}

func (f *blobOpenFile) ReadAt(p []byte, off int64) (int, error) {
	finalOff, err := f.Reader.Seek(off, io.SeekStart)
	if err != nil {
		return -1, err
	} else if finalOff != off {
		return -1, io.ErrUnexpectedEOF
	}

	return f.Read(p)
}

// Functions to implement the `Stat()` function in the `io/fs.File` interface

func (f *blobOpenFile) Name() string               { return f.name }
func (f *blobOpenFile) Mode() fs.FileMode          { return fs.ModeIrregular }
func (f *blobOpenFile) Sys() interface{}           { return f.Reader }
func (f *blobOpenFile) IsDir() bool                { return false }
func (f *blobOpenFile) Stat() (fs.FileInfo, error) { return f, nil }

// blobFileIO represents a file system backed by a bucket in object store. It implements the `iceberg-go/io.FileIO` interface.
type blobFileIO struct {
	*blob.Bucket
	ctx    context.Context
	opts   *blob.ReaderOptions
	prefix string
}

func (io *blobFileIO) preprocess(n string) string {
	_, after, found := strings.Cut(n, "://")
	if found {
		n = after
	}

	out := strings.TrimPrefix(n, io.prefix)
	if out == "/" {
		out = "."
	} else {
		out = strings.TrimPrefix(out, "/")
	}

	return out
}

// Open a Blob from a Bucket using the blobFileIO. Note this
// function is copied from blob.Bucket.Open, but extended to
// return a iceberg-go/io.File instance instead of io/fs.File
func (io *blobFileIO) Open(path string) (File, error) {
	if _, err := url.Parse(path); err != nil {
		return nil, &fs.PathError{Op: "open", Path: path, Err: fs.ErrInvalid}
	}
	path = io.preprocess(path)

	var isDir bool
	var key, name string // name is the last part of the path
	if path == "." {
		// Root is always a directory, but blob doesn't want the "." in the key.
		isDir = true
		key, name = "", "."
	} else {
		exists, _ := io.Bucket.Exists(io.ctx, path)
		isDir = !exists
		key, name = path, filepath.Base(path)
	}

	// If it's a directory, list the directory contents. We can't do this lazily
	// because we need to error out here if it doesn't exist.
	if isDir {
		dir := newDir(io, key, name)
		err := dir.openOnce()
		if err != nil {
			if err == fs.ErrNotExist && path == "." {
				// The root directory must exist.
				return dir, nil
			}
			return nil, &fs.PathError{Op: "open", Path: path, Err: err}
		}
		return dir, nil
	}

	// It's a file; open it and return a wrapper.
	r, err := io.Bucket.NewReader(io.ctx, path, io.opts)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: path, Err: err}
	}

	return &blobOpenFile{Reader: r, name: name}, nil
}

// Remove a Blob from a Bucket using the blobFileIO
func (io *blobFileIO) Remove(path string) error {
	if !fs.ValidPath(path) {
		return &fs.PathError{Op: "remove", Path: path, Err: fs.ErrInvalid}
	}
	path = io.preprocess(path)

	return io.Bucket.Delete(io.ctx, path)
}

// NewWriter returns a Writer that writes to the blob stored at path.
// A nil WriterOptions is treated the same as the zero value.
//
// If overwrite is disabled and a blob with this path already exists,
// an error will be returned.
//
// The returned Writer will store ctx for later use in Write and/or Close.
// To abort a write, cancel ctx; otherwise, it must remain open until
// Close is called.
//
// The caller must call Close on the returned Writer, even if the write is
// aborted.
func (io *blobFileIO) NewWriter(path string, overwrite bool, opts *blob.WriterOptions) (w *BlobWriteFile, err error) {
	if !fs.ValidPath(path) {
		return nil, &fs.PathError{Op: "new writer", Path: path, Err: fs.ErrInvalid}
	}
	path = io.preprocess(path)
	if !overwrite {
		if exists, err := io.Bucket.Exists(io.ctx, path); exists {
			if err != nil {
				return nil, &fs.PathError{Op: "new writer", Path: path, Err: err}
			}
			return nil, &fs.PathError{Op: "new writer", Path: path, Err: fs.ErrInvalid}
		}
	}
	bw, err := io.Bucket.NewWriter(io.ctx, path, opts)
	if err != nil {
		return nil, err
	}
	return &BlobWriteFile{
			Writer: bw,
			name:   path,
			opts:   opts},
		nil
}

// createblobFileIO creates a new blobFileIO instance
func createBlobFileIO(parsed *url.URL, bucket *blob.Bucket) *blobFileIO {
	ctx := context.Background()
	return &blobFileIO{Bucket: bucket, ctx: ctx, opts: &blob.ReaderOptions{}, prefix: parsed.Host + parsed.Path}
}

type BlobWriteFile struct {
	*blob.Writer
	name string
	opts *blob.WriterOptions
}

func (f *BlobWriteFile) Name() string                { return f.name }
func (f *BlobWriteFile) Sys() interface{}            { return f.Writer }
func (f *BlobWriteFile) Close() error                { return f.Writer.Close() }
func (f *BlobWriteFile) Write(p []byte) (int, error) { return f.Writer.Write(p) }
