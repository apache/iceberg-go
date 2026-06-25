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
}

var _ HadoopCatalogFS = (*icebergio.LocalFS)(nil)

// StatIO is an extension of IO interface that includes the Stat
// method for retrieving file or directory information
type StatIO interface {
	icebergio.IO

	// The Stat method returns a FileInfo describing the named file or directory,
	// or an error satisfying errors.Is(err, fs.ErrNotExist) if the file does not exist
	Stat(path string) (fs.FileInfo, error)
}

// RenameIO is an extension of IO interface that includes the Rename
// method for renaming (moving) files or directories; this must be
// atomic and can be used for committing metadata updates
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

// MkdirAllIO is an extension of IO interface that includes the MkdirAll
// method for creating a directory path recursively
type MkdirAllIO interface {
	icebergio.IO

	// MkdirAll should not raise an error if the directory already exists,
	// and should create any parent directories in the path if they
	// do not already exist.
	MkdirAll(path string) error
}
