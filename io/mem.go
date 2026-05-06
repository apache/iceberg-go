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
	"bytes"
	"context"
	"io"
	"io/fs"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	memBuckets   = map[string]*MemFS{}
	memBucketsMu sync.Mutex
)

func openOrCreateMemBucket(bucket string) *MemFS {
	memBucketsMu.Lock()
	defer memBucketsMu.Unlock()
	if b, ok := memBuckets[bucket]; ok {
		return b
	}
	b := &MemFS{files: map[string][]byte{}}
	memBuckets[bucket] = b
	return b
}

func init() {
	Register("mem", func(_ context.Context, parsed *url.URL, _ map[string]string) (IO, error) {
		return openOrCreateMemBucket(parsed.Host), nil
	})
}

// MemFS is a simple in-memory IO implementation used for testing and lightweight workloads.
// Files are stored per named bucket and persist for the lifetime of the process.
// Use the "mem://bucket/path" URI scheme to access files.
type MemFS struct {
	mu    sync.RWMutex
	files map[string][]byte // keyed by full URI
}

func (m *MemFS) Open(name string) (File, error) {
	m.mu.RLock()
	data, ok := m.files[name]
	m.mu.RUnlock()
	if !ok {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	return &memFile{data: cp, name: filepath.Base(name)}, nil
}

func (m *MemFS) Remove(name string) error {
	m.mu.Lock()
	delete(m.files, name)
	m.mu.Unlock()
	return nil
}

func (m *MemFS) Create(name string) (FileWriter, error) {
	return &memWriter{fs: m, name: name}, nil
}

func (m *MemFS) WriteFile(name string, content []byte) error {
	cp := make([]byte, len(content))
	copy(cp, content)
	m.mu.Lock()
	m.files[name] = cp
	m.mu.Unlock()
	return nil
}

func (m *MemFS) WalkDir(root string, fn fs.WalkDirFunc) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for key, data := range m.files {
		if !strings.HasPrefix(key, root) {
			continue
		}
		info := &memFileInfo{name: filepath.Base(key), size: int64(len(data))}
		if err := fn(key, fs.FileInfoToDirEntry(info), nil); err != nil {
			return err
		}
	}
	return nil
}

// memFile implements File for an in-memory byte slice.
type memFile struct {
	data []byte
	name string
	pos  int64
}

func (f *memFile) Read(p []byte) (int, error) {
	if f.pos >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.pos:])
	f.pos += int64(n)
	return n, nil
}

func (f *memFile) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = f.pos + offset
	case io.SeekEnd:
		abs = int64(len(f.data)) + offset
	}
	if abs < 0 {
		return 0, &fs.PathError{Op: "seek", Path: f.name, Err: fs.ErrInvalid}
	}
	f.pos = abs
	return f.pos, nil
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) Close() error { return nil }

func (f *memFile) Stat() (fs.FileInfo, error) {
	return &memFileInfo{name: f.name, size: int64(len(f.data))}, nil
}

type memFileInfo struct {
	name string
	size int64
}

func (fi *memFileInfo) Name() string        { return fi.name }
func (fi *memFileInfo) Size() int64         { return fi.size }
func (fi *memFileInfo) Mode() fs.FileMode   { return 0 }
func (fi *memFileInfo) ModTime() time.Time  { return time.Time{} }
func (fi *memFileInfo) IsDir() bool         { return false }
func (fi *memFileInfo) Sys() any            { return nil }

// memWriter collects written bytes and stores them in MemFS on Close.
type memWriter struct {
	fs   *MemFS
	name string
	buf  bytes.Buffer
}

func (w *memWriter) Write(p []byte) (int, error)       { return w.buf.Write(p) }
func (w *memWriter) ReadFrom(r io.Reader) (int64, error) { return w.buf.ReadFrom(r) }
func (w *memWriter) Close() error {
	return w.fs.WriteFile(w.name, w.buf.Bytes())
}
