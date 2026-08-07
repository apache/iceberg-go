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

package gocloud

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/DataDog/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2/google"
)

// Both parse without a real RSA key (the private key is only read lazily when a
// token is fetched), keeping tests hermetic.
const (
	authorizedUserJSON = `{"type":"authorized_user","client_id":"id","client_secret":"secret","refresh_token":"token"}`
	serviceAccountJSON = `{"type":"service_account","project_id":"p","private_key":"fake","client_email":"sa@p.iam.gserviceaccount.com","token_uri":"https://oauth2.googleapis.com/token"}`
)

func TestParseGCSConfigUseJSONAPI(t *testing.T) {
	t.Run("defaults to disabled", func(t *testing.T) {
		cfg := ParseGCSConfig(map[string]string{})
		assert.Len(t, cfg.ClientOptions, 0)
	})

	t.Run("enables reads on true", func(t *testing.T) {
		cfg := ParseGCSConfig(map[string]string{io.GCSUseJSONAPI: "true"})
		assert.Len(t, cfg.ClientOptions, 1)
	})

	t.Run("does not enable on false", func(t *testing.T) {
		cfg := ParseGCSConfig(map[string]string{io.GCSUseJSONAPI: "false"})
		assert.Len(t, cfg.ClientOptions, 0)
	})

	t.Run("does not enable on invalid value", func(t *testing.T) {
		cfg := ParseGCSConfig(map[string]string{io.GCSUseJSONAPI: "not-a-bool"})
		assert.Len(t, cfg.ClientOptions, 0)
	})
}

// Inline JSON key (gcs.jsonkey) yields explicit credentials, not ADC.
func TestGCSCredentialsFromInlineJSONKey(t *testing.T) {
	creds, err := gcsCredentials(context.Background(), map[string]string{
		io.GCSJSONKey:  authorizedUserJSON,
		io.GCSCredType: "authorized_user",
	})
	require.NoError(t, err)
	require.NotNil(t, creds, "GCSJSONKey should yield explicit credentials")
	assert.JSONEq(t, authorizedUserJSON, string(creds.JSON),
		"credentials must originate from the supplied key, not ADC")
}

// gcsCredentials must build credentials from a key file (GCSKeyPath).
func TestGCSCredentialsFromKeyPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sa.json")
	require.NoError(t, os.WriteFile(path, []byte(authorizedUserJSON), 0o600))

	creds, err := gcsCredentials(context.Background(), map[string]string{
		io.GCSKeyPath:  path,
		io.GCSCredType: "authorized_user",
	})
	require.NoError(t, err)
	require.NotNil(t, creds, "GCSKeyPath should yield explicit credentials")
	assert.JSONEq(t, authorizedUserJSON, string(creds.JSON))
}

// A missing key file is a hard error, not a silent fallback to ADC.
func TestGCSCredentialsMissingKeyPath(t *testing.T) {
	_, err := gcsCredentials(context.Background(), map[string]string{
		io.GCSKeyPath: filepath.Join(t.TempDir(), "does-not-exist.json"),
	})
	require.Error(t, err)
}

func TestResolveGCSCredType(t *testing.T) {
	for _, ct := range []string{"service_account", "authorized_user", "impersonated_service_account", "external_account"} {
		got, ok := resolveGCSCredType(map[string]string{io.GCSCredType: ct})
		require.True(t, ok, ct)
		assert.Equal(t, ct, got)
	}

	for name, props := range map[string]map[string]string{
		"unset":   {},
		"unknown": {io.GCSCredType: "not-a-real-type"},
	} {
		_, ok := resolveGCSCredType(props)
		assert.False(t, ok, name)
	}
}

func TestGCSCredentialsDefaultsToServiceAccount(t *testing.T) {
	creds, err := gcsCredentials(context.Background(), map[string]string{
		io.GCSJSONKey: serviceAccountJSON,
	})
	require.NoError(t, err)
	require.NotNil(t, creds)
	assert.JSONEq(t, serviceAccountJSON, string(creds.JSON))
}

func TestGCSCredentialsNonServiceAccountNeedsCredType(t *testing.T) {
	_, err := gcsCredentials(context.Background(), map[string]string{
		io.GCSJSONKey: authorizedUserJSON,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), io.GCSCredType)
}

func TestGCSCredentialsIgnoresUnknownCredType(t *testing.T) {
	creds, err := gcsCredentials(context.Background(), map[string]string{
		io.GCSJSONKey:  serviceAccountJSON,
		io.GCSCredType: "not-a-real-type",
	})
	require.NoError(t, err)
	require.NotNil(t, creds)
}

// gcs.no-auth yields nil credentials so the client is built anonymously.
func TestGCSCredentialsNoAuth(t *testing.T) {
	creds, err := gcsCredentials(context.Background(), map[string]string{io.GCSNoAuth: "true"})
	require.NoError(t, err)
	assert.Nil(t, creds)
}

func TestGCSCredentialsPropagatesADCError(t *testing.T) {
	t.Run("missing configured file", func(t *testing.T) {
		t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "missing.json"))

		creds, err := gcsCredentials(context.Background(), nil)
		require.ErrorContains(t, err, "gcs: no credentials found")
		require.ErrorContains(t, err, io.GCSNoAuth)
		assert.Nil(t, creds)
	})

	t.Run("no configured credentials", func(t *testing.T) {
		t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")

		creds, err := gcsCredentialsWithDefault(context.Background(), nil,
			func(context.Context) (*google.Credentials, error) {
				return nil, errors.New("application default credentials unavailable")
			})
		require.ErrorContains(t, err, "gcs: no credentials found")
		require.ErrorContains(t, err, io.GCSNoAuth)
		assert.Nil(t, creds)
	})

	t.Run("nil credentials", func(t *testing.T) {
		creds, err := gcsCredentialsWithDefault(context.Background(), nil,
			func(context.Context) (*google.Credentials, error) { return nil, nil })
		require.ErrorContains(t, err, "gcs: no credentials found")
		assert.Nil(t, creds)
	})

	t.Run("no-auth skips default credentials", func(t *testing.T) {
		called := false
		creds, err := gcsCredentialsWithDefault(context.Background(), map[string]string{io.GCSNoAuth: "true"},
			func(context.Context) (*google.Credentials, error) {
				called = true

				return nil, errors.New("default credentials should not be requested")
			})
		require.NoError(t, err)
		assert.Nil(t, creds)
		assert.False(t, called)
	})

	creds, err := gcsCredentials(context.Background(), map[string]string{io.GCSNoAuth: "true"})
	require.NoError(t, err)
	assert.Nil(t, creds)
}

// A vended gcs.oauth2.token is turned into a static token source, not dropped.
func TestGCSCredentialsFromOAuthToken(t *testing.T) {
	exp := time.Now().Add(time.Hour).UnixMilli()
	creds, err := gcsCredentials(context.Background(), map[string]string{
		io.GCSOAuthToken:     "vended-token",
		io.GCSOAuthExpiresAt: strconv.FormatInt(exp, 10),
	})
	require.NoError(t, err)
	require.NotNil(t, creds)
	require.NotNil(t, creds.TokenSource)

	tok, err := creds.TokenSource.Token()
	require.NoError(t, err)
	assert.Equal(t, "vended-token", tok.AccessToken)
	assert.Equal(t, time.UnixMilli(exp), tok.Expiry)
}

// gcs.service.host is the standard endpoint key; gcs.endpoint is a fallback alias.
func TestGCSEndpointPrefersServiceHost(t *testing.T) {
	assert.Equal(t, "svc", gcsEndpoint(map[string]string{io.GCSServiceHost: "svc"}))
	assert.Equal(t, "ep", gcsEndpoint(map[string]string{io.GCSEndpoint: "ep"}))
	assert.Equal(t, "svc", gcsEndpoint(map[string]string{io.GCSServiceHost: "svc", io.GCSEndpoint: "ep"}))
	assert.Empty(t, gcsEndpoint(map[string]string{}))
}
