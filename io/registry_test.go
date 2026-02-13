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

	assert.ElementsMatch(t, []string{
		"file",
		"",
		"s3",
		"s3a",
		"s3n",
		"gs",
		"mem",
		"abfs",
		"abfss",
		"wasb",
		"wasbs",
	}, io.GetRegisteredSchemes())

	customFactoryCalled := false
	io.Register("custom", func(ctx context.Context, parsed *url.URL, props map[string]string) (io.IO, error) {
		customFactoryCalled = true
		assert.Equal(t, "custom", parsed.Scheme)
		assert.Equal(t, "bucket", parsed.Host)

		return io.LocalFS{}, nil
	})

	assert.ElementsMatch(t, []string{
		"file",
		"",
		"s3",
		"s3a",
		"s3n",
		"gs",
		"mem",
		"abfs",
		"abfss",
		"wasb",
		"wasbs",
		"custom",
	}, io.GetRegisteredSchemes())

	customIO, err := io.LoadFS(ctx, map[string]string{}, "custom://bucket/path")
	assert.NoError(t, err)
	assert.NotNil(t, customIO)
	assert.True(t, customFactoryCalled)

	io.Unregister("custom")
	assert.ElementsMatch(t, []string{
		"file",
		"",
		"s3",
		"s3a",
		"s3n",
		"gs",
		"mem",
		"abfs",
		"abfss",
		"wasb",
		"wasbs",
	}, io.GetRegisteredSchemes())

	_, err = io.LoadFS(ctx, map[string]string{}, "custom://bucket/path")
	assert.Error(t, err)
	assert.ErrorIs(t, err, io.ErrIOSchemeNotFound)
}

func TestRegistryPanic(t *testing.T) {
	assert.PanicsWithValue(t, "io: Register factory is nil", func() {
		io.Register("invalid", nil)
	})
}

func TestRegisterDuplicatePanic(t *testing.T) {
	dummyFactory := func(ctx context.Context, parsed *url.URL, props map[string]string) (io.IO, error) {
		return io.LocalFS{}, nil
	}

	io.Register("test-duplicate", dummyFactory)
	defer io.Unregister("test-duplicate")

	assert.PanicsWithValue(t, "io: Register called twice for scheme test-duplicate", func() {
		io.Register("test-duplicate", dummyFactory)
	}, "Attempting to register the same scheme twice should panic")
}

func TestLoadFS(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		location    string
		expectError bool
	}{
		{
			name:        "file scheme",
			location:    "file:///tmp/test",
			expectError: false,
		},
		{
			name:        "empty scheme",
			location:    "/tmp/test",
			expectError: false,
		},
		{
			name:        "mem scheme",
			location:    "mem://bucket/path",
			expectError: false,
		},
		{
			name:        "s3 scheme",
			location:    "s3://bucket/path",
			expectError: false,
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
				assert.ErrorIs(t, err, io.ErrIOSchemeNotFound)
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
