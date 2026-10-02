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
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/io/gocloud/blobfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
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
		wg.Go(func() {
			_, err := r.loadFS(context.Background())
			assert.NoError(t, err)
		})
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

func TestParseCredentialExpirySupportsAccountScopedADLSKey(t *testing.T) {
	t.Parallel()

	want := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	got, ok := parseCredentialExpiry(iceberg.Properties{
		keyAdlsSasExpiresAtMs + ".account.dfs.core.windows.net": strconv.FormatInt(want.UnixMilli(), 10),
	})

	require.True(t, ok)
	assert.Equal(t, want, got)
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

func TestPrefixScopedIOUsesLongestCredentialPerLocation(t *testing.T) {
	t.Parallel()

	p := newPrefixScopedIO(context.Background(), iceberg.Properties{"base": "yes"}, []StorageCredential{
		{Prefix: "s3://metadata/table/", Config: iceberg.Properties{"credential": "metadata"}},
		{Prefix: "s3://data/table/", Config: iceberg.Properties{"credential": "data"}},
		{Prefix: "s3://data/table/private/", Config: iceberg.Properties{"credential": "private"}},
	})

	assert.Equal(t, "metadata", p.propertiesForLocation("s3://metadata/table/metadata.json")["credential"])
	assert.Equal(t, "data", p.propertiesForLocation("s3://data/table/file.parquet")["credential"])
	assert.Equal(t, "private", p.propertiesForLocation("s3://data/table/private/file.parquet")["credential"])
	assert.Equal(t, "yes", p.propertiesForLocation("s3://other/table/file.parquet")["base"])
}

func TestPrefixScopedIOReplacesS3CredentialAtomically(t *testing.T) {
	t.Parallel()

	p := newPrefixScopedIO(context.Background(), iceberg.Properties{
		iceio.S3EndpointURL:     "https://s3.local",
		iceio.S3AccessKeyID:     "load-access",
		iceio.S3SecretAccessKey: "load-secret",
		iceio.S3SessionToken:    "load-token",
		keyS3TokenExpiresAtMs:   "1000",
	}, []StorageCredential{{
		Prefix: "s3://bucket/data/",
		Config: iceberg.Properties{
			iceio.S3AccessKeyID:     "plan-access",
			iceio.S3SecretAccessKey: "plan-secret",
		},
	}})

	props := p.propertiesForLocation("s3://bucket/data/file.parquet")
	assert.Equal(t, "https://s3.local", props[iceio.S3EndpointURL])
	assert.Equal(t, "plan-access", props[iceio.S3AccessKeyID])
	assert.Equal(t, "plan-secret", props[iceio.S3SecretAccessKey])
	assert.NotContains(t, props, iceio.S3SessionToken)
	assert.NotContains(t, props, keyS3TokenExpiresAtMs)
}

func TestPrefixScopedIODoesNotHoldLockDuringFilesystemLoad(t *testing.T) {
	loadStarted := make(chan struct{})
	releaseLoad := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseLoad) }) }

	iceio.Register("prefix-scoped-lock-test", func(context.Context, *url.URL, map[string]string) (iceio.IO, error) {
		close(loadStarted)
		<-releaseLoad

		return iceio.LocalFS{}, nil
	})
	defer iceio.Unregister("prefix-scoped-lock-test")
	defer release()

	p := newPrefixScopedIO(context.Background(), nil, nil)
	loaded := make(chan error, 1)
	go func() {
		_, err := p.filesystemFor("prefix-scoped-lock-test://bucket/file.parquet")
		loaded <- err
	}()

	<-loadStarted
	lockAcquired := make(chan struct{})
	go func() {
		p.mu.Lock()
		close(lockAcquired)
		p.mu.Unlock()
	}()

	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		t.Fatal("filesystem cache lock was held during filesystem loading")
	}

	release()
	require.NoError(t, <-loaded)
}

type closeTrackingIO struct {
	iceio.IO
	closeCount *atomic.Int32
}

func (f *closeTrackingIO) Close() error {
	f.closeCount.Add(1)

	return nil
}

type blockingCloseIO struct {
	iceio.IO
	started chan struct{}
	release <-chan struct{}
}

func (f *blockingCloseIO) Close() error {
	close(f.started)
	<-f.release

	return nil
}

func TestPrefixScopedIOClosesFilesystemLostToCacheRace(t *testing.T) {
	const scheme = "prefix-scoped-cache-race-test"

	loadStarted := make(chan struct{}, 2)
	releaseLoad := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseLoad) }) }
	defer release()

	var closeCount atomic.Int32
	iceio.Register(scheme, func(context.Context, *url.URL, map[string]string) (iceio.IO, error) {
		loadStarted <- struct{}{}
		<-releaseLoad

		return &closeTrackingIO{IO: iceio.LocalFS{}, closeCount: &closeCount}, nil
	})
	defer iceio.Unregister(scheme)

	p := newPrefixScopedIO(context.Background(), nil, nil)
	type result struct {
		fs  iceio.IO
		err error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			fs, err := p.filesystemFor(scheme + "://bucket/file.parquet")
			results <- result{fs: fs, err: err}
		}()
	}

	for range 2 {
		select {
		case <-loadStarted:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for concurrent filesystem loads")
		}
	}
	release()

	var cached iceio.IO
	for range 2 {
		select {
		case result := <-results:
			require.NoError(t, result.err)
			if cached == nil {
				cached = result.fs
			} else {
				assert.Same(t, cached, result.fs)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for filesystem loads")
		}
	}

	assert.Equal(t, int32(1), closeCount.Load(),
		"the filesystem that lost the cache race must be closed")
	require.NoError(t, p.Close())
	assert.Equal(t, int32(2), closeCount.Load(),
		"the cached filesystem must be closed when the scoped IO closes")
}

func TestPrefixScopedIOClosesFilesystemLoadedAfterClose(t *testing.T) {
	const scheme = "prefix-scoped-close-during-load-test"

	loadStarted := make(chan struct{})
	releaseLoad := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseLoad) }) }
	defer release()

	var closeCount atomic.Int32
	iceio.Register(scheme, func(context.Context, *url.URL, map[string]string) (iceio.IO, error) {
		close(loadStarted)
		<-releaseLoad

		return &closeTrackingIO{IO: iceio.LocalFS{}, closeCount: &closeCount}, nil
	})
	defer iceio.Unregister(scheme)

	p := newPrefixScopedIO(context.Background(), nil, nil)
	loaded := make(chan error, 1)
	go func() {
		_, err := p.filesystemFor(scheme + "://bucket/file.parquet")
		loaded <- err
	}()

	select {
	case <-loadStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for filesystem load")
	}
	require.NoError(t, p.Close())
	release()

	select {
	case err := <-loaded:
		require.Error(t, err)
		assert.ErrorContains(t, err, "prefix-scoped IO is closed")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for filesystem load to finish")
	}
	assert.Equal(t, int32(1), closeCount.Load(),
		"a filesystem loaded after Close must be closed before returning")
}

func TestPrefixScopedIODoesNotHoldLockDuringFilesystemClose(t *testing.T) {
	t.Parallel()

	closeStarted := make(chan struct{})
	releaseClose := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseClose) }) }
	defer release()

	p := newPrefixScopedIO(context.Background(), nil, nil)
	p.filesystems["cached"] = &blockingCloseIO{
		IO:      iceio.LocalFS{},
		started: closeStarted,
		release: releaseClose,
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- p.Close() }()
	select {
	case <-closeStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for filesystem close")
	}

	lockAcquired := make(chan struct{})
	go func() {
		p.mu.Lock()
		close(lockAcquired)
		p.mu.Unlock()
	}()
	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		t.Fatal("filesystem cache lock was held during filesystem close")
	}

	release()
	require.NoError(t, <-closeDone)
}

func TestPrefixScopedIODoesNotApplyCredentialOutsidePrefix(t *testing.T) {
	t.Parallel()

	p := newPrefixScopedIO(context.Background(), iceberg.Properties{
		iceio.S3EndpointURL: "https://s3.local",
	}, []StorageCredential{{
		Prefix: "s3://bucket/data/",
		Config: iceberg.Properties{
			iceio.S3AccessKeyID:     "plan-access",
			iceio.S3SecretAccessKey: "plan-secret",
		},
	}})

	props := p.propertiesForLocation("s3://other-bucket/data/file.parquet")
	assert.Equal(t, "https://s3.local", props[iceio.S3EndpointURL])
	assert.NotContains(t, props, iceio.S3AccessKeyID)
	assert.NotContains(t, props, iceio.S3SecretAccessKey)
}

// newPrefixScopedIO does no detaching of its own: it keeps the context it is
// given, which is how the refresher's close reaches the filesystems it opens.
// loadFS hands it the refresher-owned context, never a caller's.
func TestPrefixScopedIOPreservesReadContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	p := newPrefixScopedIO(ctx, nil, nil)

	cancel()
	require.ErrorIs(t, p.ctx.Err(), context.Canceled)
}

// contextCapturingScheme is a filesystem that behaves like the gocloud
// backends: every load returns a blobfs.FileIO over its own in-memory bucket,
// opened on the context it is given, which blobfs keeps and uses for every
// operation. It records each context and counts closes; closing an IO closes
// its bucket.
type contextCapturingScheme struct {
	scheme string
	mu     sync.Mutex
	opened []context.Context
	closes []*atomic.Int32
}

func registerContextCapturingScheme(t *testing.T, scheme string) *contextCapturingScheme {
	t.Helper()

	c := &contextCapturingScheme{scheme: scheme}

	iceio.Register(scheme, func(ctx context.Context, u *url.URL, _ map[string]string) (iceio.IO, error) {
		closes := &atomic.Int32{}

		c.mu.Lock()
		c.opened = append(c.opened, ctx)
		c.closes = append(c.closes, closes)
		c.mu.Unlock()

		bfs := blobfs.New(ctx, memblob.OpenBucket(nil), blobfs.DefaultObjectLocationExtractor(u.Host, scheme))

		return &closeCountingIO{FileIO: bfs, closeCount: closes}, nil
	})
	t.Cleanup(func() { iceio.Unregister(scheme) })

	return c
}

// closeCountingIO counts closes and forwards them, so a closed IO fails the
// way a closed gocloud bucket does.
type closeCountingIO struct {
	*blobfs.FileIO
	closeCount *atomic.Int32
}

func (f *closeCountingIO) Close() error {
	f.closeCount.Add(1)

	return f.FileIO.Close()
}

func (c *contextCapturingScheme) loads() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.opened)
}

func (c *contextCapturingScheme) ctx(i int) context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.opened[i]
}

func (c *contextCapturingScheme) closeCount(i int) int32 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.closes[i].Load()
}

// writeAndRead writes a file through fs and reads it back, failing the test if
// either operation fails.
func writeAndRead(t *testing.T, fs iceio.IO, location string) {
	t.Helper()

	wfs, ok := fs.(iceio.WriteFileIO)
	require.True(t, ok, "%T is not writable", fs)
	require.NoError(t, wfs.WriteFile(location, []byte("data")))

	f, err := fs.Open(location)
	require.NoError(t, err)
	defer f.Close()

	buf := make([]byte, 4)
	_, err = f.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, "data", string(buf))
}

type vendedCtxKey struct{}

// The cached IO outlives the call that built it, so a caller that scopes one
// operation to its own context (cancelled when the operation returns) must not
// leave every later operation with a filesystem opened on a dead context.
// Renewal must not tear down the superseded IO either: callers that loaded it
// may still be using it.
func TestVendedCredsCachedIODoesNotInheritCallerCancellation(t *testing.T) {
	const scheme = "vended-ctx-detach-test"

	fs := registerContextCapturingScheme(t, scheme)

	r := newTestRefresher(func(context.Context, []string) (iceberg.Properties, error) {
		return iceberg.Properties{}, nil
	})
	r.location = scheme + "://bucket/tbl"

	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), vendedCtxKey{}, "first"))
	first, err := r.loadFS(ctx)
	require.NoError(t, err)
	cancel()

	// A later caller gets the cached IO; it works although the first caller's
	// context is done.
	cached, err := r.loadFS(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, fs.loads())
	writeAndRead(t, cached, scheme+"://bucket/tbl/a")

	require.NoError(t, fs.ctx(0).Err(), "the cached IO must not be bound to the caller's cancellation")
	// WithoutCancel keeps values, so the first caller's values reach the shared
	// IO. This documents the current behaviour; it is a trade-off, not a promise.
	assert.Equal(t, "first", fs.ctx(0).Value(vendedCtxKey{}))

	// Renewal rebuilds the IO on the renewing caller's values.
	r.expiresAt = time.Now().Add(-time.Minute)
	ctx, cancel = context.WithCancel(context.WithValue(context.Background(), vendedCtxKey{}, "second"))
	renewed, err := r.loadFS(ctx)
	require.NoError(t, err)
	cancel()

	require.Equal(t, 2, fs.loads(), "renewal must rebuild the IO")
	require.NoError(t, fs.ctx(1).Err(), "the renewed IO must not be bound to the caller's cancellation")
	assert.Equal(t, "second", fs.ctx(1).Value(vendedCtxKey{}))
	writeAndRead(t, renewed, scheme+"://bucket/tbl/b")

	// The superseded IO stays usable for callers that still hold it.
	writeAndRead(t, first, scheme+"://bucket/tbl/c")
	require.NoError(t, fs.ctx(0).Err(), "renewal must not cancel the superseded IO's context")
	assert.Equal(t, int32(0), fs.closeCount(0), "renewal must not close the superseded IO")
}

func TestVendedCredsCloseEndsTheIOContext(t *testing.T) {
	const scheme = "vended-ctx-close-test"

	fs := registerContextCapturingScheme(t, scheme)

	r := newTestRefresher(func(context.Context, []string) (iceberg.Properties, error) {
		return iceberg.Properties{}, nil
	})
	r.location = scheme + "://bucket/tbl"

	_, err := r.loadFS(context.Background())
	require.NoError(t, err)
	require.NoError(t, r.close())

	require.ErrorIs(t, fs.ctx(0).Err(), context.Canceled)
	assert.Equal(t, int32(1), fs.closeCount(0))
}

func TestVendedCredsFailedRenewalKeepsTheCachedIO(t *testing.T) {
	const scheme = "vended-ctx-failed-renewal-test"

	fs := registerContextCapturingScheme(t, scheme)

	r := newTestRefresher(func(context.Context, []string) (iceberg.Properties, error) {
		return iceberg.Properties{}, nil
	})
	r.location = scheme + "://bucket/tbl"

	cached, err := r.loadFS(context.Background())
	require.NoError(t, err)

	// The renewed load resolves to a scheme with no registered filesystem.
	r.location = "vended-ctx-unregistered://bucket/tbl"
	r.expiresAt = time.Now().Add(-time.Minute)
	_, err = r.loadFS(context.Background())
	require.Error(t, err)

	assert.Same(t, cached, r.cachedIO, "a failed renewal leaves the cached IO in place")
	require.NoError(t, fs.ctx(0).Err(), "and its context alive")
	assert.Equal(t, int32(0), fs.closeCount(0))
}

func TestVendedCredsPrefixScopedIODoesNotInheritCallerCancellation(t *testing.T) {
	const scheme = "vended-prefix-ctx-detach-test"

	fs := registerContextCapturingScheme(t, scheme)

	r := newTestRefresher(nil)
	r.location = scheme + "://bucket/tbl"
	r.credentials = []StorageCredential{{Prefix: scheme + "://bucket/", Config: iceberg.Properties{}}}

	ctx, cancel := context.WithCancel(context.Background())
	loaded, err := r.loadFS(ctx)
	require.NoError(t, err)
	cancel()

	p, ok := loaded.(*prefixScopedIO)
	require.True(t, ok, "scan-plan credentials load a prefixScopedIO, got %T", loaded)

	// prefixScopedIO opens its filesystems lazily, after the call that built it returned.
	location := scheme + "://bucket/tbl/data/file.parquet"
	opened, err := p.filesystemFor(location)
	require.NoError(t, err)
	writeAndRead(t, opened, location)

	require.Equal(t, 1, fs.loads())
	require.NoError(t, fs.ctx(0).Err(), "filesystems opened later must not inherit the first caller's cancellation")

	require.NoError(t, r.close())
	require.ErrorIs(t, fs.ctx(0).Err(), context.Canceled, "close ends the context its filesystems were opened with")
}
