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
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
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
	index := matchingStorageCredentialIndex(creds, location)
	if index < 0 {
		return nil
	}

	return creds[index].Config
}

func matchingStorageCredentialIndex(creds []StorageCredential, location string) int {
	best := -1
	for i := range creds {
		if !strings.HasPrefix(location, creds[i].Prefix) {
			continue
		}
		if best == -1 || len(creds[i].Prefix) > len(creds[best].Prefix) {
			best = i
		}
	}

	return best
}

var credentialExpiryKeys = []string{
	keyS3TokenExpiresAtMs,
	keyGcsOAuthExpiresAt,
	keyExpirationTime,
}

func parseCredentialExpiry(config iceberg.Properties) (time.Time, bool) {
	var earliest time.Time
	found := false
	for key, value := range config {
		if !slices.Contains(credentialExpiryKeys, key) &&
			key != keyAdlsSasExpiresAtMs &&
			!strings.HasPrefix(key, keyAdlsSasExpiresAtMs+".") {
			continue
		}

		ms, err := strconv.ParseInt(value, 10, 64)
		if err == nil && ms > 0 {
			expiresAt := time.UnixMilli(ms)
			if !found || expiresAt.Before(earliest) {
				earliest = expiresAt
				found = true
			}
		}
	}

	return earliest, found
}

type vendedCredentialRefresher struct {
	// Use a weighted semaphore with a single unit to use as an exclusive lock
	// but cancellation (via context) is supported. This is important as we do IO
	// while holding this lock and we want to allow others to cancel during acquisition.
	mu       *semaphore.Weighted
	cachedIO iceio.IO
	// ioCancel ends the context cachedIO was opened with. That context is the
	// refresher's own, detached from the caller that triggered the load, and
	// close is what cancels it. An IO superseded on renewal is left open with
	// its context alive, since callers that loaded it may still be using it.
	ioCancel  context.CancelFunc
	expiresAt time.Time
	issuedAt  time.Time

	identifier []string
	location   string
	props      iceberg.Properties
	// credentials is populated only for scan-plan IO. Unlike table-load
	// credentials, which are already resolved against the metadata location,
	// plan credentials must be selected against every file path opened later.
	credentials []StorageCredential

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
		// A single resolved credential can already be past its expiry by the
		// time we first use it, so check before building an IO we'd only hand
		// back 403s from. Plan credentials are resolved later, per location, by
		// prefixScopedIO and must not be rejected as one global credential set.
		if v.fetchCreds == nil && len(v.credentials) == 0 {
			expiresAt, ok := parseCredentialExpiry(config)
			if ok && v.now().After(expiresAt) {
				return nil, v.expiredError(expiresAt)
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

	// The IO is cached and shared by every later caller, so it must not
	// inherit this caller's cancellation: a filesystem may keep the context it
	// is opened with (blobfs does, so every gocloud backend), and a cached IO
	// built on a per-operation context would fail every later operation with
	// context.Canceled once that context is done. The refresher owns the IO's
	// lifetime instead, through ioCancel. fetchCreds above still honours ctx.
	//
	// WithoutCancel keeps ctx's values, and the IO depends on them: the S3
	// factory takes its base AWS config from utils.GetAwsConfig(ctx), so do
	// not replace this with context.Background(). The flip side is that the
	// first caller's request-scoped values stay attached to the shared IO.
	ioCtx, ioCancel := context.WithCancel(context.WithoutCancel(ctx))

	var (
		newIO     iceio.IO
		expiresAt time.Time
		issuedAt  = v.issuedAt
	)

	if len(v.credentials) > 0 {
		prefixIO := newPrefixScopedIO(ioCtx, v.props, v.credentials)
		prefixIO.nowFunc = v.now
		newIO = prefixIO
		// Expiry is enforced by prefixScopedIO against only the credential
		// selected for each object location.
	} else {
		loaded, err := iceio.LoadFS(ioCtx, config, v.location)
		if err != nil {
			ioCancel()

			if v.cachedIO == nil {
				return nil, err
			}

			return nil, fmt.Errorf("load filesystem with refreshed credentials for %s: %w", v.location, err)
		}

		newIO = loaded
		expiresAt = v.expiresAtFromConfig(config)
		issuedAt = v.now()
	}

	v.replaceIO(newIO, ioCancel)
	v.expiresAt = expiresAt
	v.issuedAt = issuedAt

	return v.cachedIO, nil
}

// replaceIO installs io as the cached IO. The superseded IO is left open:
// callers that loaded it earlier may still be using it, and its credentials
// remain valid until their own expiry. Its cancel is dropped without being
// called on purpose: cancelling it would end the context those callers'
// operations still run on. The detached context has no parent to deregister
// from, so it is collected with the IO.
func (v *vendedCredentialRefresher) replaceIO(io iceio.IO, cancel context.CancelFunc) {
	v.cachedIO, v.ioCancel = io, cancel
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

	half := max(v.expiresAt.Sub(v.issuedAt)/2, 0)
	if half < buffer {
		buffer = half
	}

	return buffer
}

func (v *vendedCredentialRefresher) expiresAtFromConfig(config iceberg.Properties) time.Time {
	if len(v.credentials) > 0 {
		return time.Time{}
	}

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

	cachedIO, cancel := v.cachedIO, v.ioCancel
	v.cachedIO, v.ioCancel = nil, nil

	var err error
	if cachedIO != nil {
		err = closeOptionalIO(cachedIO)
	}

	if cancel != nil {
		cancel()
	}

	return err
}

// prefixScopedIO selects a plan credential using the actual object location
// passed to Open/Remove. A single plan may cover metadata, data, and delete
// files in different storage prefixes, so resolving credentials once at plan
// creation would be incorrect.
type prefixScopedIO struct {
	ctx         context.Context
	baseProps   iceberg.Properties
	credentials []StorageCredential

	mu          sync.Mutex
	filesystems map[string]iceio.IO
	closed      bool
	nowFunc     func() time.Time
}

// newPrefixScopedIO keeps ctx as given and opens every filesystem with it, so
// ctx bounds their lifetime. It does no detaching of its own: loadFS hands it
// the refresher-owned context, which close cancels.
func newPrefixScopedIO(ctx context.Context, baseProps iceberg.Properties, credentials []StorageCredential) *prefixScopedIO {
	return &prefixScopedIO{
		ctx:         ctx,
		baseProps:   maps.Clone(baseProps),
		credentials: slices.Clone(credentials),
		filesystems: make(map[string]iceio.IO),
	}
}

func (p *prefixScopedIO) Open(name string) (iceio.File, error) {
	fs, err := p.filesystemFor(name)
	if err != nil {
		return nil, err
	}

	return fs.Open(name)
}

func (p *prefixScopedIO) Remove(name string) error {
	fs, err := p.filesystemFor(name)
	if err != nil {
		return err
	}

	return fs.Remove(name)
}

func (p *prefixScopedIO) filesystemFor(name string) (iceio.IO, error) {
	credentialIndex := matchingStorageCredentialIndex(p.credentials, name)
	key := scopedFilesystemKey(credentialIndex, name)

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()

		return nil, errors.New("prefix-scoped IO is closed")
	}
	if credentialIndex >= 0 {
		if expiresAt, ok := parseCredentialExpiry(p.credentials[credentialIndex].Config); ok &&
			p.now().After(expiresAt) {
			p.mu.Unlock()

			return nil, fmt.Errorf("%w: %s expired at %s",
				ErrVendedCredentialsExpired, name, expiresAt.Format(time.RFC3339))
		}
	}
	if fs, ok := p.filesystems[key]; ok {
		p.mu.Unlock()

		return fs, nil
	}
	p.mu.Unlock()

	props := p.propertiesForLocation(name)

	fs, err := iceio.LoadFS(p.ctx, props, name)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()

		closeErr := closeOptionalIO(fs)
		if closeErr != nil {
			return nil, errors.Join(errors.New("prefix-scoped IO is closed"), closeErr)
		}

		return nil, errors.New("prefix-scoped IO is closed")
	}
	if existing, ok := p.filesystems[key]; ok {
		p.mu.Unlock()

		if err := closeOptionalIO(fs); err != nil {
			return nil, err
		}

		return existing, nil
	}
	p.filesystems[key] = fs
	p.mu.Unlock()

	return fs, nil
}

func (p *prefixScopedIO) now() time.Time {
	if p.nowFunc != nil {
		return p.nowFunc()
	}

	return time.Now()
}

func (p *prefixScopedIO) propertiesForLocation(name string) iceberg.Properties {
	credentialIndex := matchingStorageCredentialIndex(p.credentials, name)
	props := make(iceberg.Properties, len(p.baseProps))
	maps.Copy(props, p.baseProps)
	if credentialIndex >= 0 {
		credentialConfig := p.credentials[credentialIndex].Config
		clearOverriddenCredentialProperties(props, credentialConfig)
		maps.Copy(props, credentialConfig)
	}

	return props
}

// clearOverriddenCredentialProperties prevents a matched credential from being
// combined with optional fields from another credential. In particular, an S3
// access/secret pair without a session token must not inherit a stale token
// from the table or catalog configuration.
func clearOverriddenCredentialProperties(props, credentialConfig iceberg.Properties) {
	_, hasS3AccessKey := credentialConfig[iceio.S3AccessKeyID]
	_, hasS3SecretKey := credentialConfig[iceio.S3SecretAccessKey]
	_, hasS3SessionToken := credentialConfig[iceio.S3SessionToken]
	if hasS3AccessKey || hasS3SecretKey || hasS3SessionToken {
		delete(props, iceio.S3AccessKeyID)
		delete(props, iceio.S3SecretAccessKey)
		delete(props, iceio.S3SessionToken)
		delete(props, keyS3TokenExpiresAtMs)
	}

	_, hasGCSOAuthToken := credentialConfig[iceio.GCSOAuthToken]
	_, hasGCSOAuthExpiry := credentialConfig[iceio.GCSOAuthExpiresAt]
	if hasGCSOAuthToken || hasGCSOAuthExpiry {
		delete(props, iceio.GCSOAuthToken)
		delete(props, iceio.GCSOAuthExpiresAt)
	}

	for key := range credentialConfig {
		switch {
		case strings.HasPrefix(key, iceio.ADLSSasTokenPrefix):
			suffix := strings.TrimPrefix(key, iceio.ADLSSasTokenPrefix)
			delete(props, iceio.ADLSSasTokenPrefix+suffix)
			delete(props, keyAdlsSasExpiresAtMs+"."+suffix)
		case strings.HasPrefix(key, keyAdlsSasExpiresAtMs+"."):
			suffix := strings.TrimPrefix(key, keyAdlsSasExpiresAtMs+".")
			delete(props, iceio.ADLSSasTokenPrefix+suffix)
			delete(props, keyAdlsSasExpiresAtMs+"."+suffix)
		}
	}
}

func scopedFilesystemKey(credentialIndex int, location string) string {
	parsed, err := url.Parse(location)
	if err != nil {
		return fmt.Sprintf("%d:%s", credentialIndex, location)
	}

	return fmt.Sprintf("%d:%s://%s", credentialIndex, parsed.Scheme, parsed.Host)
}

func closeOptionalIO(fs iceio.IO) error {
	if closer, ok := fs.(interface{ Close() error }); ok {
		return closer.Close()
	}

	return nil
}

func (p *prefixScopedIO) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()

		return nil
	}
	p.closed = true
	filesystems := p.filesystems
	p.filesystems = nil
	p.mu.Unlock()

	var closeErr error
	for _, fs := range filesystems {
		closeErr = errors.Join(closeErr, closeOptionalIO(fs))
	}

	return closeErr
}
