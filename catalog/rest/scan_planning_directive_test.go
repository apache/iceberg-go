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
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
)

// captureSlog replaces the default slog logger for the rest of the test. Tests
// that use it must not be parallel.
func captureSlog(t *testing.T) *bytes.Buffer {
	t.Helper()

	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	return &logs
}

func TestScanPlanningDirective(t *testing.T) {
	logs := captureSlog(t)

	key := table.ScanPlanningModeKey
	for _, tc := range []struct {
		name        string
		catalog     iceberg.Properties
		server      iceberg.Properties
		want        string
		wantPresent bool
		wantWarn    bool
	}{
		{name: "neither"},
		{name: "server only", server: iceberg.Properties{key: "server"}, want: "server", wantPresent: true},
		{name: "catalog only", catalog: iceberg.Properties{key: "client"}, want: "client", wantPresent: true},
		{name: "empty server value", server: iceberg.Properties{key: ""}, want: "", wantPresent: true},
		{name: "agree", catalog: iceberg.Properties{key: "SERVER"}, server: iceberg.Properties{key: "server"}, want: "server", wantPresent: true},
		{name: "mismatch", catalog: iceberg.Properties{key: "client"}, server: iceberg.Properties{key: "server"}, want: "server", wantPresent: true, wantWarn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs.Reset()
			r := &Catalog{props: tc.catalog}

			got, present := r.scanPlanningDirective(context.Background(), []string{"db", "tbl"}, tc.server)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.wantPresent, present)
			if tc.wantWarn {
				assert.Contains(t, logs.String(), "level=WARN")
				assert.Contains(t, logs.String(), "scan-planning-mode mismatch")
				assert.Contains(t, logs.String(), "table=db.tbl")
			} else {
				assert.Empty(t, logs.String())
			}
		})
	}
}

// Refresh reloads the table, so a stable mismatch must not warn on every load.
func TestScanPlanningDirectiveMismatchWarnsOncePerCatalog(t *testing.T) {
	logs := captureSlog(t)

	key := table.ScanPlanningModeKey
	r := &Catalog{props: iceberg.Properties{key: "client"}}
	for range 3 {
		r.scanPlanningDirective(context.Background(), []string{"db", "tbl"}, iceberg.Properties{key: "server"})
	}

	out := logs.String()
	assert.Equal(t, 1, strings.Count(out, "level=WARN"), out)
	assert.Equal(t, 2, strings.Count(out, "level=DEBUG"), out)

	// A separate catalog warns again.
	logs.Reset()
	other := &Catalog{props: iceberg.Properties{key: "client"}}
	other.scanPlanningDirective(context.Background(), []string{"db", "tbl"}, iceberg.Properties{key: "server"})
	assert.Contains(t, logs.String(), "level=WARN")
}
