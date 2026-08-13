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

	"github.com/DataDog/iceberg-go"
	iceio "github.com/DataDog/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

func newTestRefresher(fetchCreds func(ctx context.Context, ident []string) (iceberg.Properties, error)) *vendedCredentialRefresher {
	return &vendedCredentialRefresher{
		mu:         semaphore.NewWeighted(1),
		identifier: []string{"db", "tbl"},
		location:   "file:///tmp/test",
		props:      iceberg.Properties{},
		fetchCreds: fetchCreds,
	}
}

func TestVendedCredsCachedIOReturnedWhenNotExpired(t *testing.T) {
	t.Parallel()

	refreshErr := errors.New("refresh unavailable")
	var callCount atomic.Int32

	r := newTestRefresher(func(ctx context.Context, ident []string) (iceberg.Properties, error) {
		callCount.Add(1)

		return nil, refreshErr
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

func TestVendedCredsReturnsRefreshFailureForExpiredCredentials(t *testing.T) {
	t.Parallel()

	fetchErr := errors.New("network error")
	now := time.Now()

	var callCount atomic.Int32
	r := newTestRefresher(func(ctx context.Context, ident []string) (iceberg.Properties, error) {
		callCount.Add(1)

		return nil, fetchErr
	})
	r.nowFunc = func() time.Time { return now }

	// Seed with valid cached IO but expired.
	existingIO := iceio.LocalFS{}
	r.cachedIO = existingIO
	r.expiresAt = now.Add(-time.Second)

	got, err := r.loadFS(context.Background())
	require.ErrorIs(t, err, fetchErr)
	require.ErrorContains(t, err, "refresh vended credentials for file:///tmp/test")
	assert.Nil(t, got)

	assert.Equal(t, int32(1), callCount.Load(), "a refresh should have been attempted")
}

func TestVendedCredsReturnsLoadFailureForExpiredCredentials(t *testing.T) {
	t.Parallel()

	now := time.Now()
	r := newTestRefresher(func(ctx context.Context, ident []string) (iceberg.Properties, error) {
		return iceberg.Properties{}, nil
	})
	r.nowFunc = func() time.Time { return now }
	r.location = "notascheme://bucket/path"
	r.cachedIO = iceio.LocalFS{}
	r.expiresAt = now.Add(-time.Second)

	got, err := r.loadFS(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "load filesystem with refreshed credentials for notascheme://bucket/path")
	assert.Nil(t, got)
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
	assert.NotContains(t, err.Error(), "refreshed credentials")
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

	t.Run("refreshable creds fall back to default 60m when no expiry key", func(t *testing.T) {
		t.Parallel()
		r := &vendedCredentialRefresher{
			mu:      semaphore.NewWeighted(1),
			nowFunc: func() time.Time { return now },
			fetchCreds: func(context.Context, []string) (iceberg.Properties, error) {
				return nil, nil
			},
		}
		got := r.expiresAtFromConfig(iceberg.Properties{})
		assert.Equal(t, now.Add(60*time.Minute).UnixMilli(), got.UnixMilli())
	})

	t.Run("non-refreshable creds without expiry never expire", func(t *testing.T) {
		t.Parallel()
		// fetchCreds nil: no re-fetch to trigger, so no fallback TTL — zero time.
		r := &vendedCredentialRefresher{
			mu:      semaphore.NewWeighted(1),
			nowFunc: func() time.Time { return now },
		}
		got := r.expiresAtFromConfig(iceberg.Properties{})
		assert.True(t, got.IsZero())
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

func TestVendedCredsRefreshTriggeredWithinExpiryBuffer(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		expiresIn     time.Duration
		wantRefreshed bool
	}{
		{
			name:          "within buffer refreshes",
			expiresIn:     defaultVendedCredentialsExpiryBuffer - time.Minute,
			wantRefreshed: true,
		},
		{
			name:          "at buffer boundary : reuse the cache",
			expiresIn:     defaultVendedCredentialsExpiryBuffer,
			wantRefreshed: false,
		},
		{
			name:          "past buffer reuses cache",
			expiresIn:     defaultVendedCredentialsExpiryBuffer + time.Minute,
			wantRefreshed: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var callCount atomic.Int32
			now := time.Now()

			r := newTestRefresher(func(ctx context.Context, ident []string) (iceberg.Properties, error) {
				callCount.Add(1)

				return iceberg.Properties{}, nil
			})
			r.nowFunc = func() time.Time { return now }

			r.cachedIO = iceio.LocalFS{}
			r.expiresAt = now.Add(tc.expiresIn)

			_, err := r.loadFS(context.Background())
			require.NoError(t, err)

			if tc.wantRefreshed {
				assert.Equal(t, int32(1), callCount.Load(), "credentials within the expiry buffer must be proactively refreshed")
			} else {
				assert.Equal(t, int32(0), callCount.Load(), "credentials outside the expiry buffer must reuse the cache")
			}
		})
	}
}

func TestVendedCredsPlanScopedNotRefusedWithinBuffer(t *testing.T) {
	t.Parallel()

	now := time.Now()

	// Plan-scoped refresher: no fetchCreds, so creds can't be renewed.
	// A cred still valid but inside the prefetch buffer must be served, not refused.
	r := &vendedCredentialRefresher{
		mu:       semaphore.NewWeighted(1),
		location: "file:///tmp/test",
		props:    iceberg.Properties{},
		nowFunc:  func() time.Time { return now },
	}
	r.cachedIO = iceio.LocalFS{}
	r.expiresAt = now.Add(defaultVendedCredentialsExpiryBuffer - time.Minute)

	got, err := r.loadFS(context.Background())
	require.NoError(t, err,
		"plan-scoped creds still within their hard expiry must not be refused early")
	assert.Equal(t, r.cachedIO, got)

	// Past the hard expiry, with no way to renew, the load fails.
	r.nowFunc = func() time.Time { return now.Add(time.Hour) }
	_, err = r.loadFS(context.Background())
	require.ErrorIs(t, err, ErrVendedCredentialsExpired)
}

func TestVendedCredsRefreshBufferClampedToLifetime(t *testing.T) {
	t.Parallel()

	now := time.Now()
	// Short-lived (2m) renewable token.
	// Buffer : lifetime/2 (1m), so a just-issued token is served from cache instead of re-fetched at once.
	shortTTL := 2 * time.Minute

	var callCount atomic.Int32
	r := newTestRefresher(func(ctx context.Context, ident []string) (iceberg.Properties, error) {
		callCount.Add(1)

		return iceberg.Properties{
			keyS3TokenExpiresAtMs: strconv.FormatInt(now.Add(shortTTL).UnixMilli(), 10),
		}, nil
	})
	r.nowFunc = func() time.Time { return now }

	// Seed an already-issued short-lived credential.
	r.cachedIO = iceio.LocalFS{}
	r.issuedAt = now
	r.expiresAt = now.Add(shortTTL)

	// buffer = min(5m, 2m/2) = 1m, so at issue time the cache is still served.
	_, err := r.loadFS(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int32(0), callCount.Load(),
		"a freshly issued short-lived token must be served from cache, not re-fetched")

	// Once inside the clamped 1m window, it refreshes proactively.
	r.nowFunc = func() time.Time { return now.Add(90 * time.Second) }
	_, err = r.loadFS(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int32(1), callCount.Load(),
		"token within the clamped buffer window must be refreshed")
}

func TestVendedCredsShouldRefreshNeverExpires(t *testing.T) {
	t.Parallel()

	r := &vendedCredentialRefresher{
		mu:        semaphore.NewWeighted(1),
		nowFunc:   func() time.Time { return time.Now() },
		expiresAt: time.Time{},
	}
	assert.False(t, r.shouldRefresh(),
		"credentials with no expiry must never be proactively refreshed")
}

func TestVendedCredsRefreshBuffer(t *testing.T) {
	t.Parallel()

	now := time.Now()

	cases := []struct {
		name      string
		issuedAt  time.Time
		expiresAt time.Time
		want      time.Duration
	}{
		{
			name:      "no issuedAt uses default buffer",
			expiresAt: now.Add(time.Hour),
			want:      defaultVendedCredentialsExpiryBuffer,
		},
		{
			name:      "long lifetime uses default buffer",
			issuedAt:  now,
			expiresAt: now.Add(time.Hour),
			want:      defaultVendedCredentialsExpiryBuffer,
		},
		{
			name:      "short lifetime clamps to half",
			issuedAt:  now,
			expiresAt: now.Add(2 * time.Minute),
			want:      time.Minute,
		},
		{
			name:      "negative lifetime clamps to zero",
			issuedAt:  now,
			expiresAt: now.Add(-time.Minute),
			want:      0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := &vendedCredentialRefresher{
				mu:        semaphore.NewWeighted(1),
				issuedAt:  tc.issuedAt,
				expiresAt: tc.expiresAt,
			}
			assert.Equal(t, tc.want, r.refreshBuffer())
		})
	}
}

func TestVendedCredsIssuedAtUpdatedOnRefreshPreventsSelfRetrigger(t *testing.T) {
	t.Parallel()

	now := time.Now()
	clock := func() time.Time { return now }

	var callCount atomic.Int32
	r := newTestRefresher(func(ctx context.Context, ident []string) (iceberg.Properties, error) {
		callCount.Add(1)

		// Server vends a fresh 60m token relative to the current clock.
		return iceberg.Properties{
			keyS3TokenExpiresAtMs: strconv.FormatInt(clock().Add(time.Hour).UnixMilli(), 10),
		}, nil
	})
	r.nowFunc = clock

	// Seed a hard-expired cred to force a refresh.
	r.cachedIO = iceio.LocalFS{}
	r.expiresAt = now.Add(-time.Second)

	_, err := r.loadFS(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int32(1), callCount.Load(), "expired cred must be refreshed")
	assert.Equal(t, now, r.issuedAt, "issuedAt must be updated to the refresh time")

	// Immediately after refresh, the fresh token must be served from cache
	// rather than re-fetched (no self-retrigger).
	_, err = r.loadFS(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int32(1), callCount.Load(),
		"a freshly refreshed token must not immediately retrigger another fetch")
}

func TestResolveStorageCredentials(t *testing.T) {
	t.Parallel()

	s3Creds := iceberg.Properties{"s3.access-key-id": "AKID", "s3.secret-access-key": "secret"}
	specificCreds := iceberg.Properties{"s3.access-key-id": "SPECIFIC"}

	tests := []struct {
		name     string
		creds    []StorageCredential
		location string
		want     iceberg.Properties
	}{
		{
			name:     "empty credentials",
			creds:    nil,
			location: "s3://bucket/path",
			want:     nil,
		},
		{
			name: "matching prefix",
			creds: []StorageCredential{
				{Prefix: "s3://bucket/", Config: s3Creds},
			},
			location: "s3://bucket/path/to/file",
			want:     s3Creds,
		},
		{
			name: "no matching prefix",
			creds: []StorageCredential{
				{Prefix: "s3://other-bucket/", Config: s3Creds},
			},
			location: "s3://bucket/path",
			want:     nil,
		},
		{
			name: "longest prefix wins",
			creds: []StorageCredential{
				{Prefix: "s3://bucket/", Config: s3Creds},
				{Prefix: "s3://bucket/specific/", Config: specificCreds},
			},
			location: "s3://bucket/specific/path",
			want:     specificCreds,
		},
		{
			name: "longest prefix wins regardless of order",
			creds: []StorageCredential{
				{Prefix: "s3://bucket/specific/", Config: specificCreds},
				{Prefix: "s3://bucket/", Config: s3Creds},
			},
			location: "s3://bucket/specific/path",
			want:     specificCreds,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := resolveStorageCredentials(tt.creds, tt.location)
			assert.Equal(t, tt.want, got)
		})
	}
}
