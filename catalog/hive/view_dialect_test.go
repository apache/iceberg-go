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

package hive

import (
	"testing"

	"github.com/apache/iceberg-go/view"
	"github.com/stretchr/testify/require"
)

func TestSQLFromVersionPreservesDialect(t *testing.T) {
	tests := []struct {
		name        string
		version     *view.Version
		wantSQL     string
		wantDialect string
		wantErr     string
	}{
		{
			name: "prefer Hive representation",
			version: &view.Version{Representations: []view.Representation{
				view.NewRepresentation("SELECT trino", "trino"),
				view.NewRepresentation("SELECT hive", "HIVE"),
			}},
			wantSQL:     "SELECT hive",
			wantDialect: "HIVE",
		},
		{
			name: "keep fallback dialect",
			version: &view.Version{Representations: []view.Representation{
				view.NewRepresentation("SELECT spark", "spark"),
				view.NewRepresentation("SELECT trino", "trino"),
			}},
			wantSQL:     "SELECT spark",
			wantDialect: "spark",
		},
		{name: "nil version", wantErr: "no representations"},
		{
			name:    "no SQL representation",
			version: &view.Version{Representations: []view.Representation{{Type: "other"}}},
			wantErr: "no SQL representation",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, dialect, err := sqlFromVersion(tt.version)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantSQL, sql)
			require.Equal(t, tt.wantDialect, dialect)
		})
	}
}
