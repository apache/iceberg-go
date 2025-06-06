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

package io

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegister(t *testing.T) {
	// Save original registry state
	original := make(registry)
	for k, v := range defaultRegistry {
		original[k] = v
	}
	defer func() {
		// Restore original registry
		defaultRegistry = original
	}()

	// Clear registry for clean test
	defaultRegistry = make(registry)

	testRegistrar := RegistrarFunc(func(ctx context.Context, props map[string]string) (IO, error) {
		return LocalFS{}, nil
	})

	Register("test", testRegistrar)

	registeredIOs := GetRegisteredIOs()
	assert.Contains(t, registeredIOs, "test")
}

func TestUnregister(t *testing.T) {
	// Save original registry state
	original := make(registry)
	for k, v := range defaultRegistry {
		original[k] = v
	}
	defer func() {
		// Restore original registry
		defaultRegistry = original
	}()

	testRegistrar := RegistrarFunc(func(ctx context.Context, props map[string]string) (IO, error) {
		return LocalFS{}, nil
	})

	Register("test", testRegistrar)
	assert.Contains(t, GetRegisteredIOs(), "test")

	Unregister("test")
	assert.NotContains(t, GetRegisteredIOs(), "test")
}

func TestDefaultRegisteredIOs(t *testing.T) {
	registeredIOs := GetRegisteredIOs()

	// Check that default IO implementations are registered
	expectedIOs := []string{"file", "", "s3", "s3a", "s3n", "gs", "abfs", "abfss", "wasb", "wasbs", "mem"}
	for _, expected := range expectedIOs {
		assert.Contains(t, registeredIOs, expected, "IO type %s should be registered", expected)
	}
}

func TestLoadWithRegisteredIO(t *testing.T) {
	ctx := context.Background()

	// Test loading local filesystem
	io, err := Load(ctx, map[string]string{}, "file:///tmp/test")
	require.NoError(t, err)
	assert.IsType(t, LocalFS{}, io)

	// Test loading with empty scheme (defaults to local)
	io, err = Load(ctx, map[string]string{}, "/tmp/test")
	require.NoError(t, err)
	assert.IsType(t, LocalFS{}, io)

	// Test loading memory filesystem
	io, err = Load(ctx, map[string]string{}, "mem://bucket/path")
	require.NoError(t, err)
	// Should return a blobFileIO (which implements IO)
	assert.NotNil(t, io)
}

func TestLoadWithWarehouseFromProps(t *testing.T) {
	ctx := context.Background()

	// Test loading from warehouse property
	io, err := Load(ctx, map[string]string{"warehouse": "file:///tmp/warehouse"}, "")
	require.NoError(t, err)
	assert.IsType(t, LocalFS{}, io)
}

func TestLoadWhenUnknownScheme(t *testing.T) {
	ctx := context.Background()

	_, err := Load(ctx, map[string]string{}, "unknown://bucket/path")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrIONotFound)
}

func TestRegisterPanic(t *testing.T) {
	assert.Panics(t, func() {
		Register("test", nil)
	})
}

func TestConcurrentAccess(t *testing.T) {
	// Test that concurrent access to registry doesn't cause data races
	ctx := context.Background()

	done := make(chan bool, 100)

	// Start multiple goroutines doing registry operations
	for i := 0; i < 10; i++ {
		go func(i int) {
			defer func() { done <- true }()

			// Get registered IOs
			GetRegisteredIOs()

			// Try to load IO
			_, err := Load(ctx, map[string]string{}, "file:///tmp")
			require.NoError(t, err)
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}
