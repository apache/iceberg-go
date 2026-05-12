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

package main

import (
	"context"
	"errors"
	"os"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

func runSnapshots(_ context.Context, output Output, _ catalog.Catalog, _ *SnapshotsCmd) {
	output.Error(errors.New("snapshots: not yet implemented"))
	os.Exit(1)
}

func runRefs(_ context.Context, output Output, _ catalog.Catalog, _ *RefsCmd) {
	output.Error(errors.New("refs: not yet implemented"))
	os.Exit(1)
}

func (textOutput) Snapshots(_ *table.Table)      {}
func (jsonOutput) Snapshots(_ *table.Table)      {}
func (textOutput) Refs(_ *table.Table, _ string) {}
func (jsonOutput) Refs(_ *table.Table, _ string) {}
