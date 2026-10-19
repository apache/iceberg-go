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

package rest

import (
	"encoding/json"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeScanTasksReusesAdjacentResidualFilters(t *testing.T) {
	t.Parallel()

	metadata := newScanTaskDecoderMetadata()
	base := validScanTasksWire().FileScanTasks[0].DataFile
	fallback := iceberg.AlwaysTrue{}
	filters := []string{
		`{"type":"eq","term":"id","value":34}`,
		`{"type":"eq","term":"id","value":34}`,
		`{"type":"eq","term":"id","value":35}`,
		`{"type":"eq","term":"id","value":34}`,
		"",
		`{"type":"eq","term":"id","value":34}`,
	}
	wire := ScanTasks{FileScanTasks: make([]RESTFileScanTask, len(filters))}
	for i, filter := range filters {
		data := *base
		wire.FileScanTasks[i] = RESTFileScanTask{
			DataFile:       &data,
			ResidualFilter: json.RawMessage(filter),
		}
	}

	tasks, err := DecodeScanTasks(wire, metadata, metadata.schema, fallback)
	require.NoError(t, err)
	require.Len(t, tasks, len(filters))

	assert.True(t, tasks[0].Residual.Equals(iceberg.EqualTo(iceberg.Reference("id"), int64(34))))
	assert.Same(t, tasks[0].Residual, tasks[1].Residual, "consecutive equal filters should reuse the parsed expression")
	assert.True(t, tasks[2].Residual.Equals(iceberg.EqualTo(iceberg.Reference("id"), int64(35))))
	assert.True(t, tasks[3].Residual.Equals(iceberg.EqualTo(iceberg.Reference("id"), int64(34))))
	assert.NotSame(t, tasks[0].Residual, tasks[3].Residual, "different filters should invalidate the consecutive cache")
	assert.Equal(t, fallback, tasks[4].Residual)
	assert.True(t, tasks[5].Residual.Equals(iceberg.EqualTo(iceberg.Reference("id"), int64(34))))
	assert.NotSame(t, tasks[3].Residual, tasks[5].Residual, "missing filters should reset the consecutive cache")
}

func TestDecodeScanTasksResidualReuseDoesNotHideErrors(t *testing.T) {
	t.Parallel()

	metadata := newScanTaskDecoderMetadata()
	base := validScanTasksWire().FileScanTasks[0].DataFile
	wire := ScanTasks{FileScanTasks: make([]RESTFileScanTask, 3)}
	for i := range wire.FileScanTasks {
		data := *base
		wire.FileScanTasks[i] = RESTFileScanTask{
			DataFile:       &data,
			ResidualFilter: json.RawMessage(`{"type":"eq","term":"id","value":34}`),
		}
	}
	wire.FileScanTasks[2].ResidualFilter = json.RawMessage(`null`)

	_, err := DecodeScanTasks(wire, metadata, metadata.schema, nil)
	require.ErrorContains(t, err, "file-scan-tasks[2].residual-filter")
	require.ErrorContains(t, err, "explicit null residual-filter is invalid")
}
