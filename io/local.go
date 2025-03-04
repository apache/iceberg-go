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
	"os"
	"path/filepath"
	"strings"
)

// LocalFS is an implementation of IO that implements interaction with
// the local file system.
type LocalFS struct{}

func (LocalFS) Open(name string) (File, error) {
	return os.Open(strings.TrimPrefix(name, "file://"))
}

func (LocalFS) Create(name string) (FileWriter, error) {
	filename := strings.TrimPrefix(name, "file://")
	if err := os.MkdirAll(filepath.Dir(filename), 0o777); err != nil {
		return nil, err
	}

	return os.Create(filename)
}

func (LocalFS) WriteFile(name string, content []byte) error {
	return os.WriteFile(strings.TrimPrefix(name, "file://"), content, 0o777)
}

func (LocalFS) Remove(name string) error {
	return os.Remove(strings.TrimPrefix(name, "file://"))
}
