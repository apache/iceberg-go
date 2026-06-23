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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanTableScanResponseRequiresPlanIDForTrackedStatuses(t *testing.T) {
	t.Parallel()

	for _, status := range []PlanStatus{PlanStatusCompleted, PlanStatusSubmitted} {
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()

			var resp PlanTableScanResponse
			err := json.Unmarshal([]byte(`{"status":"`+string(status)+`"}`), &resp)
			require.ErrorIs(t, err, ErrRESTError)
		})
	}
}

func TestPlanTableScanResponseRejectsCancelled(t *testing.T) {
	t.Parallel()

	var resp PlanTableScanResponse
	err := json.Unmarshal([]byte(`{"status":"cancelled","plan-id":"abc"}`), &resp)
	require.ErrorIs(t, err, ErrRESTError)
}

func TestPlanTableScanResponseAcceptsCompletedWithPlanID(t *testing.T) {
	t.Parallel()

	var resp PlanTableScanResponse
	err := json.Unmarshal([]byte(`{
		"status":"completed",
		"plan-id":"abc",
		"plan-tasks":["next"],
		"file-scan-tasks":[{"data-file":{}}],
		"delete-files":[{"content":"position-deletes"}]
	}`), &resp)
	require.NoError(t, err)

	require.NotNil(t, resp.PlanID)
	assert.Equal(t, "abc", *resp.PlanID)
	assert.Len(t, resp.PlanTasks, 1)
	// TODO(Phase 2): assert decoded task/delete-file content once the
	// scan-task decoder fills RESTFileScanTask and RESTDeleteFile.
	assert.Len(t, resp.FileScanTasks, 1)
	assert.Len(t, resp.DeleteFiles, 1)
}

func TestPlanTableScanResponseRejectsFailedWithoutError(t *testing.T) {
	t.Parallel()

	var resp PlanTableScanResponse
	err := json.Unmarshal([]byte(`{"status":"failed"}`), &resp)
	require.ErrorIs(t, err, ErrRESTError)
}

func TestPlanTableScanResponseAcceptsFailedWithError(t *testing.T) {
	t.Parallel()

	var resp PlanTableScanResponse
	err := json.Unmarshal([]byte(`{"status":"failed","error":{"message":"boom","type":"ServerError","code":500}}`), &resp)
	require.NoError(t, err)
	require.NotNil(t, resp.Error)
	assert.Equal(t, "boom", resp.Error.Message)
}

func TestPlanTableScanResponseRejectsUnknownStatus(t *testing.T) {
	t.Parallel()

	var resp PlanTableScanResponse
	err := json.Unmarshal([]byte(`{"status":"bogus"}`), &resp)
	require.ErrorIs(t, err, ErrRESTError)
}
