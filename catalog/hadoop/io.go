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

package hadoop

import (
	"io/fs"

	icebergio "github.com/apache/iceberg-go/io"
)

// HadoopCatalogFS represents all the interfaces that a filesystem implementation
// must satisfy to be used for a Hadoop catalog implementation.
type HadoopCatalogFS interface {
	icebergio.ListableIO
	icebergio.ReadFileIO
	icebergio.WriteFileIO
	StatIO
	RenameIO
	RemoveAllIO
	MkdirAllIO
	MkdirIO
	ReadDirIO
}

// Ensure that the LocalFS implements the extensions
// thus ensuring that the hadoop catalog can use the LocalFS for its
// IO operations
var (
	_ icebergio.IO = (*icebergio.LocalFS)(nil)
	_ StatIO       = (*icebergio.LocalFS)(nil)
	_ RenameIO     = (*icebergio.LocalFS)(nil)
	_ RemoveAllIO  = (*icebergio.LocalFS)(nil)
	_ MkdirIO      = (*icebergio.LocalFS)(nil)
	_ MkdirAllIO   = (*icebergio.LocalFS)(nil)
	_ ReadDirIO    = (*icebergio.LocalFS)(nil)
)

// StatIO is an extension of IO interface that includes the Stat
// method for retrieving file information without reading the file
type StatIO interface {
	icebergio.IO

	Stat(name string) (fs.FileInfo, error)
}

// RenameIO is an extension of IO interface that includes the Rename
// method for renaming (moving) files or directories; this should be atomic
type RenameIO interface {
	icebergio.IO

	Rename(oldpath, newpath string) error
}

// RemoveAllIO is an extension of IO interface that includes the RemoveAll
// method for removing a file path and any children recursively
type RemoveAllIO interface {
	icebergio.IO

	RemoveAll(name string) error
}

// MkdirIO is an extension of IO interface that includes the Mkdir
// method for creating a directory
type MkdirIO interface {
	icebergio.IO

	Mkdir(path string) error
}

// MkdirAllIO is an extension of IO interface that includes the MkdirAll
// method for creating a directory path recursively
type MkdirAllIO interface {
	icebergio.IO

	MkdirAll(path string) error
}

// ReadDirIO is an extension of IO interface that includes the ReadDir
// method for reading the contents of a directory and returning a slice of
// DirEntry values
type ReadDirIO interface {
	icebergio.IO

	ReadDir(name string) ([]fs.DirEntry, error)
}
