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
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
