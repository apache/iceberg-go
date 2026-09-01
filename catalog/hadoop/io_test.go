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

// The integration build blank-imports io/gocloud, which registers the schemes
// this file asserts are absent.
//go:build !integration

package hadoop

import (
	"context"
	"testing"

	"github.com/apache/iceberg-go/internal/schemes"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImportRegistersNoCloudSchemes(t *testing.T) {
	registered := icebergio.GetRegisteredSchemes()
	for _, list := range [][]string{schemes.S3, schemes.GCS, schemes.Azure} {
		for _, scheme := range list {
			assert.NotContains(t, registered, scheme)
		}
	}
}

func TestLoadFSCloudPathReportsMissingBackend(t *testing.T) {
	for _, tt := range []struct {
		location string
		wantHint string
	}{
		{"s3://bucket/key", "io/gocloud/s3"},
		{"gs://bucket/key", "io/gocloud/gcs"},
		{"abfs://container@account.dfs.core.windows.net/key", "io/gocloud/azure"},
	} {
		t.Run(tt.location, func(t *testing.T) {
			_, err := icebergio.LoadFS(context.Background(), nil, tt.location)
			require.ErrorIs(t, err, icebergio.ErrIOSchemeNotFound)
			assert.ErrorContains(t, err, tt.wantHint)
		})
	}
}
