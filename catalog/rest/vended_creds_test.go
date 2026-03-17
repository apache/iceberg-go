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
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

func newTestRefresher(fetchConfig func(ctx context.Context, ident []string) (iceberg.Properties, error)) *vendedCredentialRefresher {
	return &vendedCredentialRefresher{
		mu:          semaphore.NewWeighted(1),
		identifier:  []string{"db", "tbl"},
		location:    "file:///tmp/test",
		props:       iceberg.Properties{},
		fetchConfig: fetchConfig,
	}
}

func TestVendedCredsCachedIOReturnedWhenNotExpired(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int32

	r := newTestRefresher(func(ctx context.Context, ident []string) (iceberg.Properties, error) {
		callCount.Add(1)

		return iceberg.Properties{}, nil
	})

	// Seed cached IO and set expiry in the future.
	r.cachedIO = iceio.LocalFS{}
	r.expiresAt = time.Now().Add(time.Hour)

	io1, err := r.loadFS(context.Background())
	require.NoError(t, err)
	assert.Equal(t, r.cachedIO, io1)

	io2, err := r.loadFS(context.Background())
	require.NoError(t, err)
	assert.Equal(t, io1, io2)

	assert.Equal(t, int32(0), callCount.Load(),
		"fetchConfig should not be called when credentials have not expired")
}

func TestVendedCredsInitialLoadUsesProps(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int32

	r := newTestRefresher(func(ctx context.Context, ident []string) (iceberg.Properties, error) {
		callCount.Add(1)

		return iceberg.Properties{}, nil
	})

	// First call with no cached IO should create IO from props,
	// not call fetchConfig.
	io1, err := r.loadFS(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, io1)
	assert.Equal(t, int32(0), callCount.Load(),
		"fetchConfig should not be called on initial load")

	// Second call should return cached IO.
	io2, err := r.loadFS(context.Background())
	require.NoError(t, err)
	assert.Equal(t, io1, io2)
}

func TestVendedCredsRefreshTriggeredOnExpiry(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int32
	now := time.Now()

	r := newTestRefresher(func(ctx context.Context, ident []string) (iceberg.Properties, error) {
		callCount.Add(1)

		return iceberg.Properties{}, nil
	})
	r.nowFunc = func() time.Time { return now }

	// Seed with expired IO.
	r.cachedIO = iceio.LocalFS{}
	r.expiresAt = now.Add(-time.Second)

	_, err := r.loadFS(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int32(1), callCount.Load(),
		"fetchConfig should be called once on expired credentials")

	// Second call should use cached IO (expiry was reset).
	_, err = r.loadFS(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int32(1), callCount.Load(),
		"fetchConfig should not be called again within expiry window")
}

func TestVendedCredsConcurrentAccess(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int32

	r := newTestRefresher(func(ctx context.Context, ident []string) (iceberg.Properties, error) {
		callCount.Add(1)

		return iceberg.Properties{}, nil
	})

	// No cached IO — concurrent initial loads should only create IO once.
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := r.loadFS(context.Background())
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	// Initial load uses props directly, not fetchConfig.
	assert.Equal(t, int32(0), callCount.Load(),
		"fetchConfig should not be called during initial load")
	assert.NotNil(t, r.cachedIO, "cachedIO should be set after initial load")
}

func TestVendedCredsGracefulDegradation(t *testing.T) {
	t.Parallel()

	fetchErr := errors.New("network error")
	now := time.Now()

	r := newTestRefresher(func(ctx context.Context, ident []string) (iceberg.Properties, error) {
		return nil, fetchErr
	})
	r.nowFunc = func() time.Time { return now }

	// Seed with valid cached IO but expired.
	existingIO := iceio.LocalFS{}
	r.cachedIO = existingIO
	r.expiresAt = now.Add(-time.Second)

	got, err := r.loadFS(context.Background())
	require.NoError(t, err, "should not return error when cached IO exists and refresh fails")
	assert.Equal(t, existingIO, got, "should return cached IO on refresh failure")
}

func TestVendedCredsErrorWhenInitialLoadFails(t *testing.T) {
	t.Parallel()

	r := newTestRefresher(func(ctx context.Context, ident []string) (iceberg.Properties, error) {
		return iceberg.Properties{}, nil
	})
	// Use an unregistered scheme so LoadFS fails on the initial load.
	r.location = "notascheme://bucket/path"

	got, err := r.loadFS(context.Background())
	require.Error(t, err)
	assert.Nil(t, got)
}

func TestParseCredentialExpiry(t *testing.T) {
	t.Parallel()

	epoch := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	epochMs := strconv.FormatInt(epoch.UnixMilli(), 10)

	tests := []struct {
		name   string
		config iceberg.Properties
		want   time.Time
		found  bool
	}{
		{
			name:   "s3 token expiry",
			config: iceberg.Properties{keyS3TokenExpiresAtMs: epochMs},
			want:   epoch,
			found:  true,
		},
		{
			name:   "adls sas expiry",
			config: iceberg.Properties{keyAdlsSasExpiresAtMs: epochMs},
			want:   epoch,
			found:  true,
		},
		{
			name:   "gcs oauth expiry",
			config: iceberg.Properties{keyGcsOAuthExpiresAt: epochMs},
			want:   epoch,
			found:  true,
		},
		{
			name:   "generic expiration-time",
			config: iceberg.Properties{keyExpirationTime: epochMs},
			want:   epoch,
			found:  true,
		},
		{
			name:   "s3 takes precedence over generic",
			config: iceberg.Properties{keyS3TokenExpiresAtMs: epochMs, keyExpirationTime: "9999999999999"},
			want:   epoch,
			found:  true,
		},
		{
			name:   "no expiry keys",
			config: iceberg.Properties{"some-other-key": "value"},
			found:  false,
		},
		{
			name:   "invalid value ignored",
			config: iceberg.Properties{keyS3TokenExpiresAtMs: "not-a-number"},
			found:  false,
		},
		{
			name:   "zero value ignored",
			config: iceberg.Properties{keyS3TokenExpiresAtMs: "0"},
			found:  false,
		},
		{
			name:   "empty config",
			config: iceberg.Properties{},
			found:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, found := parseCredentialExpiry(tt.config)
			assert.Equal(t, tt.found, found)
			if tt.found {
				assert.Equal(t, tt.want.UnixMilli(), got.UnixMilli())
			}
		})
	}
}

func TestVendedCredsExpiresAtFromConfig(t *testing.T) {
	t.Parallel()

	now := time.Now()

	t.Run("uses server-provided expiry", func(t *testing.T) {
		t.Parallel()
		serverExpiry := now.Add(30 * time.Minute)
		config := iceberg.Properties{
			keyS3TokenExpiresAtMs: strconv.FormatInt(serverExpiry.UnixMilli(), 10),
		}
		r := &vendedCredentialRefresher{
			mu:      semaphore.NewWeighted(1),
			nowFunc: func() time.Time { return now },
		}
		got := r.expiresAtFromConfig(config)
		assert.Equal(t, serverExpiry.UnixMilli(), got.UnixMilli())
	})

	t.Run("falls back to default 60m when no expiry key", func(t *testing.T) {
		t.Parallel()
		r := &vendedCredentialRefresher{
			mu:      semaphore.NewWeighted(1),
			nowFunc: func() time.Time { return now },
		}
		got := r.expiresAtFromConfig(iceberg.Properties{})
		assert.Equal(t, now.Add(60*time.Minute).UnixMilli(), got.UnixMilli())
	})
}

func TestVendedCredsServerExpiryUsedOnRefresh(t *testing.T) {
	t.Parallel()

	now := time.Now()
	serverExpiry := now.Add(20 * time.Minute)

	var callCount atomic.Int32
	r := newTestRefresher(func(ctx context.Context, ident []string) (iceberg.Properties, error) {
		callCount.Add(1)

		return iceberg.Properties{
			keyS3TokenExpiresAtMs: strconv.FormatInt(serverExpiry.UnixMilli(), 10),
		}, nil
	})
	r.nowFunc = func() time.Time { return now }

	// Seed with expired IO to trigger a refresh (not initial load).
	r.cachedIO = iceio.LocalFS{}
	r.expiresAt = now.Add(-time.Second)

	_, err := r.loadFS(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int32(1), callCount.Load())

	// expiresAt should be the server-provided value, not now+default.
	assert.Equal(t, serverExpiry.UnixMilli(), r.expiresAt.UnixMilli())
}
