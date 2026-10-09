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
	"log/slog"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
)

// Not parallel: it replaces the default slog logger to capture the warning.
func TestScanPlanningDirectiveConfig(t *testing.T) {
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	key := table.ScanPlanningModeKey
	for _, tc := range []struct {
		name     string
		catalog  iceberg.Properties
		server   iceberg.Properties
		want     iceberg.Properties
		wantWarn bool
	}{
		{name: "neither", want: nil},
		{name: "server only", server: iceberg.Properties{key: "server"}, want: iceberg.Properties{key: "server"}},
		{name: "catalog only", catalog: iceberg.Properties{key: "client"}, want: iceberg.Properties{key: "client"}},
		{name: "agree", catalog: iceberg.Properties{key: "SERVER"}, server: iceberg.Properties{key: "server"}, want: iceberg.Properties{key: "server"}},
		{name: "mismatch", catalog: iceberg.Properties{key: "client"}, server: iceberg.Properties{key: "server"}, want: iceberg.Properties{key: "server"}, wantWarn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs.Reset()
			r := &Catalog{props: tc.catalog}

			got := r.scanPlanningDirectiveConfig([]string{"db", "tbl"}, tc.server)
			assert.Equal(t, tc.want, got)
			if tc.wantWarn {
				assert.Contains(t, logs.String(), "scan-planning-mode mismatch")
				assert.Contains(t, logs.String(), "table=db.tbl")
			} else {
				assert.Empty(t, logs.String())
			}
		})
	}
}
