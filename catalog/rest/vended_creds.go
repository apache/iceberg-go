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
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/DataDog/iceberg-go"
	iceio "github.com/DataDog/iceberg-go/io"
	"golang.org/x/sync/semaphore"
)

// ErrVendedCredentialsExpired is returned when a cached FileIO's vended creds
// expired with no endpoint to renew them (as a scan plan's own creds), so the
// caller sees this instead of undiagnosable storage 403s.
var ErrVendedCredentialsExpired = fmt.Errorf("%w: vended storage credentials expired", ErrRESTError)

const (
	keyS3TokenExpiresAtMs = "s3.session-token-expires-at-ms"
	keyAdlsSasExpiresAtMs = "adls.sas-token-expires-at-ms"
	keyGcsOAuthExpiresAt  = "gcs.oauth2.token-expires-at"
	keyExpirationTime     = "expiration-time"

	defaultVendedCredentialsTTL          = 60 * time.Minute
	defaultVendedCredentialsExpiryBuffer = 5 * time.Minute
)

// resolveStorageCredentials finds the best-matching credential for the given
// location using longest-prefix match, mirroring the Java and Python implementations.
func resolveStorageCredentials(creds []StorageCredential, location string) iceberg.Properties {
	var best *StorageCredential
	for i := range creds {
		c := &creds[i]
		if strings.HasPrefix(location, c.Prefix) {
			if best == nil || len(c.Prefix) > len(best.Prefix) {
				best = c
			}
		}
	}
	if best == nil {
		return nil
	}

	return best.Config
}

var credentialExpiryKeys = []string{
	keyS3TokenExpiresAtMs,
	keyAdlsSasExpiresAtMs,
	keyGcsOAuthExpiresAt,
	keyExpirationTime,
}

func parseCredentialExpiry(config iceberg.Properties) (time.Time, bool) {
	for _, key := range credentialExpiryKeys {
		if v, ok := config[key]; ok {
			ms, err := strconv.ParseInt(v, 10, 64)
			if err == nil && ms > 0 {
				return time.UnixMilli(ms), true
			}
		}
	}

	return time.Time{}, false
}

type vendedCredentialRefresher struct {
	// Use a weighted semaphore with a single unit to use as an exclusive lock
	// but cancellation (via context) is supported. This is important as we do IO
	// while holding this lock and we want to allow others to cancel during acquisition.
	mu        *semaphore.Weighted
	cachedIO  iceio.IO
	expiresAt time.Time
	issuedAt  time.Time

	identifier []string
	location   string
	props      iceberg.Properties

	fetchCreds func(ctx context.Context, ident []string) (iceberg.Properties, error)

	nowFunc func() time.Time // for testing
}

func (v *vendedCredentialRefresher) now() time.Time {
	if v.nowFunc != nil {
		return v.nowFunc()
	}

	return time.Now()
}

func (v *vendedCredentialRefresher) loadFS(ctx context.Context) (iceio.IO, error) {
	if err := v.mu.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer v.mu.Release(1)

	if v.cachedIO != nil && !v.needsRenewal() {
		return v.cachedIO, nil
	}

	var config iceberg.Properties
	switch {
	case v.cachedIO == nil:
		config = v.props
		// Plan creds can already be past their expiry by the time we first use
		// them, so check before building an IO we'd only hand back 403s from.
		if v.fetchCreds == nil {
			if exp, ok := parseCredentialExpiry(config); ok && v.now().After(exp) {
				return nil, v.expiredError(exp)
			}
		}
	case v.fetchCreds == nil:
		// Expired with no endpoint to renew from (plan-scoped creds). Fail loudly
		// rather than hand back an IO whose reads 403.
		return nil, v.expiredError(v.expiresAt)
	default:
		freshCreds, err := v.fetchCreds(ctx, v.identifier)
		if err != nil {
			return nil, fmt.Errorf("refresh vended credentials for %s: %w", v.location, err)
		}

		config = maps.Clone(v.props)
		maps.Copy(config, freshCreds)
	}

	newIO, err := iceio.LoadFS(ctx, config, v.location)
	if err != nil {
		if v.cachedIO == nil {
			return nil, err
		}

		return nil, fmt.Errorf("load filesystem with refreshed credentials for %s: %w", v.location, err)
	}

	v.cachedIO = newIO
	v.expiresAt = v.expiresAtFromConfig(config)
	v.issuedAt = v.now()

	return v.cachedIO, nil
}

func (v *vendedCredentialRefresher) expiredError(at time.Time) error {
	return fmt.Errorf("%w: %s expired at %s",
		ErrVendedCredentialsExpired, v.location, at.Format(time.RFC3339))
}

func (v *vendedCredentialRefresher) needsRenewal() bool {
	if v.fetchCreds != nil {
		return v.shouldRefresh()
	}

	return v.expired()
}

// expired reports whether the cached IO's credentials are past their expiry, with no safety buffer. A
// zero expiresAt means "never expires" — see expiresAtFromConfig.
func (v *vendedCredentialRefresher) expired() bool {
	return !v.expiresAt.IsZero() && v.now().After(v.expiresAt)
}

func (v *vendedCredentialRefresher) shouldRefresh() bool {
	if v.expiresAt.IsZero() {
		return false
	}

	return v.now().After(v.expiresAt.Add(-v.refreshBuffer()))
}

func (v *vendedCredentialRefresher) refreshBuffer() time.Duration {
	buffer := defaultVendedCredentialsExpiryBuffer
	if v.issuedAt.IsZero() {
		return buffer
	}

	half := v.expiresAt.Sub(v.issuedAt) / 2
	if half < 0 {
		half = 0
	}
	if half < buffer {
		buffer = half
	}

	return buffer
}

func (v *vendedCredentialRefresher) expiresAtFromConfig(config iceberg.Properties) time.Time {
	if exp, ok := parseCredentialExpiry(config); ok {
		return exp
	}

	// No re-fetch to trigger, so the fallback TTL doesn't apply: never expires.
	if v.fetchCreds == nil {
		return time.Time{}
	}

	return v.now().Add(defaultVendedCredentialsTTL)
}

// close releases the cached IO. The refresher is unusable afterwards.
func (v *vendedCredentialRefresher) close() error {
	if err := v.mu.Acquire(context.Background(), 1); err != nil {
		return err
	}
	defer v.mu.Release(1)

	closer, ok := v.cachedIO.(interface{ Close() error })
	v.cachedIO = nil
	if ok {
		return closer.Close()
	}

	return nil
}
