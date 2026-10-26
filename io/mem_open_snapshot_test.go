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

package io_test

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"sync"
	"testing"

	icebergio "github.com/apache/iceberg-go/io"
)

func TestMemFSOpenPreservesSnapshots(t *testing.T) {
	memFS := icebergio.NewMemFS()
	name := "mem://bucket/data.parquet"
	input := []byte("original")
	if err := memFS.WriteFile(name, input); err != nil {
		t.Fatal(err)
	}

	oldFile, err := memFS.Open(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	defer oldFile.Close()

	// The caller's input must not alias a published file.
	input[0] = 'X'
	if err := memFS.WriteFile(name, []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	newFile, err := memFS.Open(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	defer newFile.Close()

	if err := memFS.Remove(t.Context(), name); err != nil {
		t.Fatal(err)
	}
	if _, err := memFS.Open(t.Context(), name); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("open after remove: got %v, want fs.ErrNotExist", err)
	}

	for _, test := range []struct {
		file icebergio.File
		want string
	}{
		{oldFile, "original"},
		{newFile, "replacement"},
	} {
		got, err := io.ReadAll(test.file)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != test.want {
			t.Fatalf("read after replacement/removal: got %q, want %q", got, test.want)
		}
		if _, err := test.file.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		at := make([]byte, len(test.want))
		if _, err := test.file.ReadAt(at, 0); err != nil {
			t.Fatal(err)
		}
		if string(at) != test.want {
			t.Fatalf("readat after replacement/removal: got %q, want %q", at, test.want)
		}
		info, err := test.file.Stat()
		if err != nil || info.Size() != int64(len(test.want)) {
			t.Fatalf("stat after replacement/removal: info=%v err=%v", info, err)
		}
	}
}

func TestMemFSOpenPreservesSnapshotAcrossCreate(t *testing.T) {
	memFS := icebergio.NewMemFS()
	name := "mem://bucket/created.parquet"

	firstWriter, err := memFS.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := firstWriter.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := firstWriter.Close(); err != nil {
		t.Fatal(err)
	}

	oldFile, err := memFS.Open(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	defer oldFile.Close()

	secondWriter, err := memFS.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := secondWriter.Write([]byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := secondWriter.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := io.ReadAll(oldFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "first" {
		t.Fatalf("open reader changed after Create replacement: got %q", got)
	}

	current, err := memFS.Open(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	got, err = io.ReadAll(current)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "second" {
		t.Fatalf("new reader did not see Create replacement: got %q", got)
	}
}

func TestMemFSOpenConcurrentReplacement(t *testing.T) {
	memFS := icebergio.NewMemFS()
	name := "mem://bucket/data.parquet"
	a := bytes.Repeat([]byte{'a'}, 4096)
	b := bytes.Repeat([]byte{'b'}, 4096)
	if err := memFS.WriteFile(name, a); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 300 {
			value := a
			if i%2 != 0 {
				value = b
			}
			if err := memFS.WriteFile(name, value); err != nil {
				t.Errorf("write: %v", err)

				return
			}
		}
	})

	for range 300 {
		file, err := memFS.Open(t.Context(), name)
		if err != nil {
			t.Errorf("open: %v", err)

			break
		}
		data, readErr := io.ReadAll(file)
		closeErr := file.Close()
		if readErr != nil || closeErr != nil {
			t.Errorf("read: %v, close: %v", readErr, closeErr)

			break
		}
		if !bytes.Equal(data, a) && !bytes.Equal(data, b) {
			t.Errorf("file changed while open")

			break
		}
	}
	wg.Wait()
}
