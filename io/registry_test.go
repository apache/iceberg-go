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
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sync"
	"testing"

	"github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIORegistry(t *testing.T) {
	ctx := context.Background()

	assert.Equal(t, []string{
		"",
		"abfs",
		"abfss",
		"file",
		"gs",
		"mem",
		"oss",
		"s3",
		"s3a",
		"s3n",
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

	assert.Equal(t, []string{
		"",
		"abfs",
		"abfss",
		"custom",
		"file",
		"gs",
		"mem",
		"oss",
		"s3",
		"s3a",
		"s3n",
		"wasb",
		"wasbs",
	}, io.GetRegisteredSchemes())

	customIO, err := io.LoadFS(ctx, map[string]string{}, "custom://bucket/path")
	assert.NoError(t, err)
	assert.NotNil(t, customIO)
	assert.True(t, customFactoryCalled)

	io.Unregister("custom")
	assert.Equal(t, []string{
		"",
		"abfs",
		"abfss",
		"file",
		"gs",
		"mem",
		"oss",
		"s3",
		"s3a",
		"s3n",
		"wasb",
		"wasbs",
	}, io.GetRegisteredSchemes())

	_, err = io.LoadFS(ctx, map[string]string{}, "custom://bucket/path")
	assert.Error(t, err)
	assert.ErrorIs(t, err, io.ErrIOSchemeNotFound)
}

func TestRegisterRejectsNilFactory(t *testing.T) {
	assert.PanicsWithValue(t, "io: Register factory is nil", func() { io.Register("invalid", nil) })
	assert.NotContains(t, io.GetRegisteredSchemes(), "invalid")
}

func TestRegisterRejectsDuplicate(t *testing.T) {
	dummyFactory := func(context.Context, *url.URL, map[string]string) (io.IO, error) {
		return io.LocalFS{}, nil
	}

	io.Register("test-duplicate", dummyFactory)
	defer io.Unregister("test-duplicate")

	assert.PanicsWithValue(t, "io: Register called twice for scheme test-duplicate", func() {
		io.Register("test-duplicate", func(context.Context, *url.URL, map[string]string) (io.IO, error) {
			return nil, assert.AnError
		})
	})
	fs, err := io.LoadFS(t.Context(), nil, "test-duplicate://bucket/path")
	require.NoError(t, err)
	assert.IsType(t, io.LocalFS{}, fs)
}

func TestMustRegisterPanicsOnInvalidRegistration(t *testing.T) {
	assert.Panics(t, func() { io.MustRegister("nil-factory-test", nil) })
	assert.NotContains(t, io.GetRegisteredSchemes(), "nil-factory-test")
}

func TestScopedRegistryDuplicateDoesNotReplaceFactory(t *testing.T) {
	t.Parallel()

	reg := io.NewRegistry()
	const scheme = "scoped-duplicate-regression"
	require.NoError(t, reg.Register(scheme, func(context.Context, *url.URL, map[string]string) (io.IO, error) {
		return io.LocalFS{}, nil
	}))

	err := reg.Register(scheme, func(context.Context, *url.URL, map[string]string) (io.IO, error) {
		return nil, assert.AnError
	})
	require.ErrorIs(t, err, io.ErrSchemeAlreadyRegistered)

	err = reg.Register("invalid-nil-regression", nil)
	require.ErrorIs(t, err, io.ErrInvalidSchemeFactory)
	assert.NotContains(t, reg.Names(), "invalid-nil-regression")

	fs, err := reg.LoadFS(t.Context(), nil, scheme+"://bucket/key")
	require.NoError(t, err)
	assert.IsType(t, io.LocalFS{}, fs)
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

func TestIndependentRegistryIsolation(t *testing.T) {
	t.Parallel()

	first := io.NewRegistry()
	second := io.NewRegistry()
	require.Equal(t, io.GetRegisteredSchemes(), first.Names())
	require.Equal(t, first.Names(), second.Names())
	assert.True(t, slices.IsSorted(first.Names()))

	const scheme = "scoped-registry-test"
	factory := func(context.Context, *url.URL, map[string]string) (io.IO, error) {
		return io.LocalFS{}, nil
	}
	require.NoError(t, first.Register(scheme, factory))

	fs, err := first.LoadFS(t.Context(), nil, scheme+"://bucket/path")
	require.NoError(t, err)
	assert.IsType(t, io.LocalFS{}, fs)

	_, err = second.LoadFS(t.Context(), nil, scheme+"://bucket/path")
	require.ErrorIs(t, err, io.ErrIOSchemeNotFound)
	_, err = io.LoadFS(t.Context(), nil, scheme+"://bucket/path")
	require.ErrorIs(t, err, io.ErrIOSchemeNotFound)

	clone := first.Clone()
	first.Unregister(scheme)
	_, err = first.LoadFS(t.Context(), nil, scheme+"://bucket/path")
	require.ErrorIs(t, err, io.ErrIOSchemeNotFound)
	_, err = clone.LoadFS(t.Context(), nil, scheme+"://bucket/path")
	require.NoError(t, err)

	_, err = first.LoadFS(t.Context(), nil, "file:///tmp/sample")
	require.NoError(t, err)
}

func TestNewRegistrySnapshotsGlobalRegistrations(t *testing.T) {
	const scheme = "late-global-registration-test"
	before := io.NewRegistry()

	io.Register(scheme, func(context.Context, *url.URL, map[string]string) (io.IO, error) {
		return io.LocalFS{}, nil
	})
	t.Cleanup(func() { io.Unregister(scheme) })

	_, err := before.LoadFS(t.Context(), nil, scheme+"://bucket/key")
	require.ErrorIs(t, err, io.ErrIOSchemeNotFound)

	after := io.NewRegistry()
	_, err = after.LoadFS(t.Context(), nil, scheme+"://bucket/key")
	require.NoError(t, err)
	assert.Contains(t, after.Names(), scheme)
	assert.NotContains(t, before.Names(), scheme)
}

func TestRegistryZeroValueIsUsable(t *testing.T) {
	var reg io.Registry
	require.Empty(t, reg.Names())

	factory := func(context.Context, *url.URL, map[string]string) (io.IO, error) {
		return io.LocalFS{}, nil
	}
	require.NoError(t, reg.Register("private", factory))

	fs, err := reg.LoadFS(t.Context(), nil, "private://bucket/key")
	require.NoError(t, err)
	assert.IsType(t, io.LocalFS{}, fs)

	cloned := reg.Clone()
	reg.Unregister("private")
	require.Empty(t, reg.Names())
	assert.Equal(t, []string{"private"}, cloned.Names())
}

func TestRegistryAllowsFactoriesToRegisterSchemes(t *testing.T) {
	t.Parallel()

	reg := io.NewRegistry()
	factory := func(context.Context, *url.URL, map[string]string) (io.IO, error) {
		err := reg.Register("second-scheme", func(context.Context, *url.URL, map[string]string) (io.IO, error) {
			return io.LocalFS{}, nil
		})
		if err != nil {
			return nil, err
		}

		return io.LocalFS{}, nil
	}
	require.NoError(t, reg.Register("first-scheme", factory))
	_, err := reg.LoadFS(t.Context(), nil, "first-scheme://bucket/key")
	require.NoError(t, err)
	_, err = reg.LoadFS(t.Context(), nil, "second-scheme://bucket/key")
	require.NoError(t, err)
}

func TestRegistryPreservesFactoryErrors(t *testing.T) {
	t.Parallel()

	reg := io.NewRegistry()
	want := errors.New("factory failed")
	require.NoError(t, reg.Register("fails", func(context.Context, *url.URL, map[string]string) (io.IO, error) {
		return nil, want
	}))
	_, err := reg.LoadFS(t.Context(), nil, "fails://bucket/key")
	require.ErrorIs(t, err, want)
}

func TestRegistryLoadFSFuncUsesScopedFactories(t *testing.T) {
	t.Parallel()

	reg := io.NewRegistry()
	require.NoError(t, reg.Register("lazy-scheme", func(context.Context, *url.URL, map[string]string) (io.IO, error) {
		return io.LocalFS{}, nil
	}))
	load := reg.LoadFSFunc(map[string]string{"warehouse": "lazy-scheme://bucket/key"}, "")
	_, err := load(t.Context())
	require.NoError(t, err)
	_, err = io.LoadFS(t.Context(), nil, "lazy-scheme://bucket/key")
	require.ErrorIs(t, err, io.ErrIOSchemeNotFound)
}

func TestRegistryConcurrentRegistrationsAndLoads(t *testing.T) {
	t.Parallel()

	reg := io.NewRegistry()
	const workers = 32
	factory := func(context.Context, *url.URL, map[string]string) (io.IO, error) {
		return io.LocalFS{}, nil
	}

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := range workers {
		wg.Go(func() {
			scheme := fmt.Sprintf("worker-%d", i)
			if err := reg.Register(scheme, factory); err != nil {
				errs <- err

				return
			}

			if _, err := reg.LoadFS(t.Context(), nil, scheme+"://bucket/key"); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, reg.Names(), workers+len(io.NewRegistry().Names()))
}
