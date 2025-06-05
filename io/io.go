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
	"net/url"
	"strings"

	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"
)

// IO is an interface to a hierarchical file system.
//
// The IO interface is the minimum implementation required for a file
// system to utilize an iceberg table. A file system may implement
// additional interfaces, such as ReadFileIO, to provide additional or
// optimized functionality.
type IO interface {
	// Open opens the named file.
	//
	// When Open returns an error, it should be of type *PathError
	// with the Op field set to "open", the Path field set to name,
	// and the Err field describing the problem.
	//
	// Open should reject attempts to open names that do not satisfy
	// fs.ValidPath(name), returning a *PathError with Err set to
	// ErrInvalid or ErrNotExist.
	Open(name string) (File, error)

	// Remove removes the named file or (empty) directory.
	//
	// If there is an error, it will be of type *PathError.
	Remove(name string) error
}

// ReadFileIO is the interface implemented by a file system that
// provides an optimized implementation of ReadFile.
type ReadFileIO interface {
	IO

	// ReadFile reads the named file and returns its contents.
	// A successful call returns a nil error, not io.EOF.
	// (Because ReadFile reads the whole file, the expected EOF
	// from the final Read is not treated as an error to be reported.)
	//
	// The caller is permitted to modify the returned byte slice.
	// This method should return a copy of the underlying data.
	ReadFile(name string) ([]byte, error)
}

// WriteFileIO is the interface implemented by a file system that
// provides an optimized implementation of WriteFile
type WriteFileIO interface {
	IO

	// Create attempts to create the named file and return a writer
	// for it.
	Create(name string) (FileWriter, error)

	// WriteFile writes p to the named file.
	WriteFile(name string, p []byte) error
}

// A File provides access to a single file. The File interface is the
// minimum implementation required for Iceberg to interact with a file.
// Directory files should also implement
type File interface {
	fs.File
	io.ReadSeekCloser
	io.ReaderAt
}

// A FileWriter represents an open writable file.
type FileWriter interface {
	io.WriteCloser
	io.ReaderFrom
}

// A ReadDirFile is a directory file whose entries can be read with the
// ReadDir method. Every directory file should implement this interface.
// (It is permissible for any file to implement this interface, but
// if so ReadDir should return an error for non-directories.)
type ReadDirFile interface {
	File

	// ReadDir read the contents of the directory and returns a slice
	// of up to n DirEntry values in directory order. Subsequent calls
	// on the same file will yield further DirEntry values.
	//
	// If n > 0, ReadDir returns at most n DirEntry structures. In this
	// case, if ReadDir returns an empty slice, it will return a non-nil
	// error explaining why.
	//
	// At the end of a directory, the error is io.EOF. (ReadDir must return
	// io.EOF itself, not an error wrapping io.EOF.)
	//
	// If n <= 0, ReadDir returns all the DirEntry values from the directory
	// in a single slice. In this case, if ReadDir succeeds (reads all the way
	// to the end of the directory), it returns the slice and a nil error.
	// If it encounters an error before the end of the directory, ReadDir
	// returns the DirEntry list read until that point and a non-nil error.
	ReadDir(n int) ([]fs.DirEntry, error)
}

// FS wraps an io/fs.FS as an IO interface.
func FS(fsys fs.FS) IO {
	if _, ok := fsys.(fs.ReadFileFS); ok {
		return readFileFS{ioFS{fsys, nil}}
	}

	return ioFS{fsys, nil}
}

// FSPreProcName wraps an io/fs.FS like FS, only if fn is non-nil then
// it is called to preprocess any filenames before they are passed to
// the underlying fsys.
func FSPreProcName(fsys fs.FS, fn func(string) string) IO {
	if _, ok := fsys.(fs.ReadFileFS); ok {
		return readFileFS{ioFS{fsys, fn}}
	}

	return ioFS{fsys, fn}
}

type readFileFS struct {
	ioFS
}

func (r readFileFS) ReadFile(name string) ([]byte, error) {
	if r.preProcessName != nil {
		name = r.preProcessName(name)
	}

	rfs, ok := r.fsys.(fs.ReadFileFS)
	if !ok {
		return nil, errMissingReadFile
	}

	return rfs.ReadFile(name)
}

type ioFS struct {
	fsys fs.FS

	preProcessName func(string) string
}

func (f ioFS) Open(name string) (File, error) {
	if f.preProcessName != nil {
		name = f.preProcessName(name)
	}

	if name == "/" {
		name = "."
	} else {
		name = strings.TrimPrefix(name, "/")
	}
	file, err := f.fsys.Open(name)
	if err != nil {
		return nil, err
	}

	return ioFile{file}, nil
}

func (f ioFS) Remove(name string) error {
	r, ok := f.fsys.(interface{ Remove(name string) error })
	if !ok {
		return errMissingRemove
	}

	return r.Remove(name)
}

var (
	errMissingReadDir  = errors.New("fs.File directory missing ReadDir method")
	errMissingSeek     = errors.New("fs.File missing Seek method")
	errMissingReadAt   = errors.New("fs.File missing ReadAt")
	errMissingRemove   = errors.New("fs.FS missing Remove method")
	errMissingReadFile = errors.New("fs.FS missing ReadFile method")
)

type ioFile struct {
	file fs.File
}

func (f ioFile) Close() error               { return f.file.Close() }
func (f ioFile) Read(b []byte) (int, error) { return f.file.Read(b) }
func (f ioFile) Stat() (fs.FileInfo, error) { return f.file.Stat() }
func (f ioFile) Seek(offset int64, whence int) (int64, error) {
	s, ok := f.file.(io.Seeker)
	if !ok {
		return 0, errMissingSeek
	}

	return s.Seek(offset, whence)
}

func (f ioFile) ReadAt(p []byte, off int64) (n int, err error) {
	r, ok := f.file.(io.ReaderAt)
	if !ok {
		return 0, errMissingReadAt
	}

	return r.ReadAt(p, off)
}

func (f ioFile) ReadDir(count int) ([]fs.DirEntry, error) {
	d, ok := f.file.(fs.ReadDirFile)
	if !ok {
		return nil, errMissingReadDir
	}

	return d.ReadDir(count)
}

func inferFileIOFromSchema(ctx context.Context, path string, props map[string]string) (IO, error) {
	parsed, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	var bucket *blob.Bucket

	switch parsed.Scheme {
	case "s3", "s3a", "s3n":
		bucket, err = createS3Bucket(ctx, parsed, props)
		if err != nil {
			return nil, err
		}
	case "gs":
		bucket, err = createGCSBucket(ctx, parsed, props)
		if err != nil {
			return nil, err
		}
	case "mem":
		// memblob doesn't use the URL host or path
		bucket = memblob.OpenBucket(nil)
	case "file", "":
		return LocalFS{}, nil
	case "abfs", "abfss", "wasb", "wasbs":
		bucket, err = createAzureBucket(ctx, parsed, props)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("IO for file '%s' not implemented", path)
	}

	return createBlobFS(ctx, bucket, parsed.Host), nil
}

// LoadFS takes a map of properties and an optional URI location
// and attempts to infer an IO object from it.
//
// A schema of "file://" or an empty string will result in a LocalFS
// implementation. Otherwise this will return an error if the schema
// does not yet have an implementation here.
//
// Currently local, S3, GCS, and In-Memory FSs are implemented.
func LoadFS(ctx context.Context, props map[string]string, location string) (IO, error) {
	if location == "" {
		location = props["warehouse"]
	}

	iofs, err := inferFileIOFromSchema(ctx, location, props)
	if err != nil {
		return nil, err
	}

	if iofs == nil {
		iofs = LocalFS{}
	}

	return iofs, nil
}

// LoadFSFunc is a helper function to create IO Func factory for later usage
func LoadFSFunc(props map[string]string, location string) func(ctx context.Context) (IO, error) {
	return func(ctx context.Context) (IO, error) {
		iofs, err := LoadFS(ctx, props, location)
		if err != nil {
			return nil, fmt.Errorf("failed to load metadata file at %s: %w", location, err)
		}

		return iofs, nil
	}
}
