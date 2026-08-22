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

package rest_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// These deliberately independent types are shared by the fake-server contract
// tests and the Java fixture integration tests. They must not use production
// REST response structs: their purpose is to catch both sides agreeing on an
// incorrect wire representation.
type rawPlanningFixture struct {
	Status             string                        `json:"status"`
	PlanID             string                        `json:"plan-id"`
	PlanTasks          []string                      `json:"plan-tasks"`
	FileScanTasks      []rawFileScanTaskFixture      `json:"file-scan-tasks"`
	DeleteFiles        []rawDeleteFileFixture        `json:"delete-files"`
	StorageCredentials []rawStorageCredentialFixture `json:"storage-credentials"`
}

type rawFileScanTaskFixture struct {
	DataFile             rawDataFileFixture `json:"data-file"`
	DeleteFileReferences []int              `json:"delete-file-references"`
	ResidualFilter       json.RawMessage    `json:"residual-filter"`
}

type rawDataFileFixture struct {
	SpecID      int                `json:"spec-id"`
	Partition   []json.RawMessage  `json:"partition"`
	Content     string             `json:"content"`
	FilePath    string             `json:"file-path"`
	LowerBounds rawValueMapFixture `json:"lower-bounds"`
	UpperBounds rawValueMapFixture `json:"upper-bounds"`
	ColumnSizes rawCountMapFixture `json:"column-sizes"`
}

type rawDeleteFileFixture struct {
	SpecID    int               `json:"spec-id"`
	Partition []json.RawMessage `json:"partition"`
	Content   string            `json:"content"`
	FilePath  string            `json:"file-path"`
}

type rawValueMapFixture struct {
	Keys   []int    `json:"keys"`
	Values []string `json:"values"`
}

type rawCountMapFixture struct {
	Keys   []int   `json:"keys"`
	Values []int64 `json:"values"`
}

type rawStorageCredentialFixture struct {
	Prefix string            `json:"prefix"`
	Config map[string]string `json:"config"`
}

func parseRawPlanningFixture(t *testing.T, body string) rawPlanningFixture {
	t.Helper()

	var fixture rawPlanningFixture
	require.NoError(t, json.Unmarshal([]byte(body), &fixture))

	return fixture
}
