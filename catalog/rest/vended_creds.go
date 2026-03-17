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
	"maps"
	"strconv"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"golang.org/x/sync/semaphore"
)

const (
	keyS3TokenExpiresAtMs = "s3.session-token-expires-at-ms"
	keyAdlsSasExpiresAtMs = "adls.sas-token-expires-at-ms"
	keyGcsOAuthExpiresAt  = "gcs.oauth2.token-expires-at"
	keyExpirationTime     = "expiration-time"

	defaultVendedCredentialsTTL = 60 * time.Minute
)

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
	mu        *semaphore.Weighted
	cachedIO  iceio.IO
	expiresAt time.Time

	identifier []string
	location   string
	props      iceberg.Properties

	fetchConfig func(ctx context.Context, ident []string) (iceberg.Properties, error)

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

	if v.cachedIO != nil && !v.now().After(v.expiresAt) {
		return v.cachedIO, nil
	}

	var config iceberg.Properties
	if v.cachedIO == nil {
		config = v.props
	} else {
		freshConfig, err := v.fetchConfig(ctx, v.identifier)
		if err != nil {
			return v.cachedIO, nil
		}

		config = make(iceberg.Properties, len(v.props)+len(freshConfig))
		maps.Copy(config, v.props)
		maps.Copy(config, freshConfig)
	}

	newIO, err := iceio.LoadFS(ctx, config, v.location)
	if err != nil {
		if v.cachedIO != nil {
			return v.cachedIO, nil
		}

		return nil, err
	}

	v.cachedIO = newIO
	v.expiresAt = v.expiresAtFromConfig(config)

	return v.cachedIO, nil
}

func (v *vendedCredentialRefresher) expiresAtFromConfig(config iceberg.Properties) time.Time {
	if exp, ok := parseCredentialExpiry(config); ok {
		return exp
	}

	return v.now().Add(defaultVendedCredentialsTTL)
}
