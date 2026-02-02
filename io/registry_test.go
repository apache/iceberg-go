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

package io_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIORegistry(t *testing.T) {
	ctx := context.Background()

	// Check that default schemes are registered
	schemes := io.GetRegisteredSchemes()
	assert.Contains(t, schemes, "file")
	assert.Contains(t, schemes, "")
	assert.Contains(t, schemes, "s3")
	assert.Contains(t, schemes, "gs")
	assert.Contains(t, schemes, "mem")
	assert.Contains(t, schemes, "abfs")

	// Register a custom scheme
	customFactoryCalled := false
	io.Register("custom", func(ctx context.Context, parsed *url.URL, props map[string]string) (io.IO, error) {
		customFactoryCalled = true
		assert.Equal(t, "custom", parsed.Scheme)
		assert.Equal(t, "bucket", parsed.Host)
		return io.LocalFS{}, nil
	})

	schemes = io.GetRegisteredSchemes()
	assert.Contains(t, schemes, "custom")

	// Test loading with custom scheme
	customIO, err := io.LoadFS(ctx, map[string]string{}, "custom://bucket/path")
	assert.NoError(t, err)
	assert.NotNil(t, customIO)
	assert.True(t, customFactoryCalled)

	// Unregister custom scheme
	io.Unregister("custom")
	schemes = io.GetRegisteredSchemes()
	assert.NotContains(t, schemes, "custom")

	// Verify custom scheme no longer works
	_, err = io.LoadFS(ctx, map[string]string{}, "custom://bucket/path")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "IO for scheme 'custom' not implemented")
}

func TestRegistryPanic(t *testing.T) {
	assert.PanicsWithValue(t, "io: Register factory is nil", func() {
		io.Register("invalid", nil)
	})
}

func TestLoadFS(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		location    string
		expectError bool
		ioType      string
	}{
		{
			name:        "file scheme",
			location:    "file:///tmp/test",
			expectError: false,
			ioType:      "io.LocalFS",
		},
		{
			name:        "empty scheme",
			location:    "/tmp/test",
			expectError: false,
			ioType:      "io.LocalFS",
		},
		{
			name:        "mem scheme",
			location:    "mem://bucket/path",
			expectError: false,
			ioType:      "*gocloud.blobFileIO",
		},
		{
			name:        "s3 scheme",
			location:    "s3://bucket/path",
			expectError: false,
			ioType:      "*gocloud.blobFileIO",
		},
		{
			name:        "unsupported scheme",
			location:    "unsupported://bucket/path",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iofs, err := io.LoadFS(ctx, map[string]string{}, tt.location)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, iofs)
			}
		})
	}
}

func TestLoadFSWithWarehouse(t *testing.T) {
	ctx := context.Background()

	// Test with warehouse property
	iofs, err := io.LoadFS(ctx, map[string]string{
		"warehouse": "file:///tmp/warehouse",
	}, "")
	require.NoError(t, err)
	assert.NotNil(t, iofs)
	assert.IsType(t, io.LocalFS{}, iofs)
}
