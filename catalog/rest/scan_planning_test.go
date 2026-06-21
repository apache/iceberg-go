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
	"errors"
	"testing"
)

func TestPlanTableScanResponseRequiresPlanIDForTrackedStatuses(t *testing.T) {
	for _, status := range []PlanStatus{PlanStatusCompleted, PlanStatusSubmitted} {
		t.Run(string(status), func(t *testing.T) {
			var resp PlanTableScanResponse
			err := json.Unmarshal([]byte(`{"status":"`+string(status)+`"}`), &resp)
			if !errors.Is(err, ErrRESTError) {
				t.Fatalf("expected ErrRESTError, got %v", err)
			}
		})
	}
}

func TestPlanTableScanResponseRejectsCancelled(t *testing.T) {
	var resp PlanTableScanResponse
	err := json.Unmarshal([]byte(`{"status":"cancelled","plan-id":"abc"}`), &resp)
	if !errors.Is(err, ErrRESTError) {
		t.Fatalf("expected ErrRESTError, got %v", err)
	}
}

func TestPlanTableScanResponseAcceptsCompletedWithPlanID(t *testing.T) {
	var resp PlanTableScanResponse
	err := json.Unmarshal([]byte(`{
		"status":"completed",
		"plan-id":"abc",
		"plan-tasks":["next"],
		"file-scan-tasks":[{"data-file":{}}],
		"delete-files":[{"content":"position-deletes"}]
	}`), &resp)
	if err != nil {
		t.Fatal(err)
	}

	if resp.PlanID == nil || *resp.PlanID != "abc" {
		t.Fatalf("expected plan id abc, got %v", resp.PlanID)
	}
	if len(resp.PlanTasks) != 1 || len(resp.FileScanTasks) != 1 || len(resp.DeleteFiles) != 1 {
		t.Fatalf("expected scan task envelope to decode, got %+v", resp.ScanTasks)
	}
}
