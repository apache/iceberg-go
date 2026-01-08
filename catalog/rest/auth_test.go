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
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOauth2AuthManagerAuthHeader(t *testing.T) {
	t.Run("returns static token when set", func(t *testing.T) {
		manager := Oauth2AuthManager{Token: "static-token"}
		key, val, err := manager.AuthHeader()
		require.NoError(t, err)
		assert.Equal(t, "Authorization", key)
		assert.Equal(t, "Bearer static-token", val)
	})

	t.Run("fetches token when only credential is set", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "client_credentials", r.PostFormValue("grant_type"))
			assert.Equal(t, "test-client-id", r.PostFormValue("client_id"))
			assert.Equal(t, "test-client-secret", r.PostFormValue("client_secret"))
			assert.Equal(t, "catalog", r.PostFormValue("scope"))

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(oauthTokenResponse{
				AccessToken: "fetched-token",
				TokenType:   "Bearer",
			})
		}))
		defer server.Close()

		serverURL, err := url.Parse(server.URL)
		require.NoError(t, err)

		manager := Oauth2AuthManager{
			Credential: "test-client-id:test-client-secret",
			AuthURI:    serverURL,
			Client:     server.Client(),
		}

		key, val, err := manager.AuthHeader()
		require.NoError(t, err)
		assert.Equal(t, "Authorization", key)
		assert.Equal(t, "Bearer fetched-token", val)
		assert.Equal(t, "fetched-token", manager.Token, "manager token should be updated")
	})

	t.Run("fetches token with custom scope", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "client_credentials", r.PostFormValue("grant_type"))
			assert.Equal(t, "test-client-id", r.PostFormValue("client_id"))
			assert.Equal(t, "test-client-secret", r.PostFormValue("client_secret"))
			assert.Equal(t, "custom-scope", r.PostFormValue("scope")) // Assert custom scope

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(oauthTokenResponse{
				AccessToken: "fetched-token-custom-scope",
				TokenType:   "Bearer",
			})
		}))
		defer server.Close()

		serverURL, err := url.Parse(server.URL)
		require.NoError(t, err)

		manager := Oauth2AuthManager{
			Credential: "test-client-id:test-client-secret",
			AuthURI:    serverURL,
			Scope:      "custom-scope", // Set custom scope here
			Client:     server.Client(),
		}

		key, val, err := manager.AuthHeader()
		require.NoError(t, err)
		assert.Equal(t, "Authorization", key)
		assert.Equal(t, "Bearer fetched-token-custom-scope", val)
		assert.Equal(t, "fetched-token-custom-scope", manager.Token, "manager token should be updated")
	})

	t.Run("fetches token with only client secret", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "client_credentials", r.PostFormValue("grant_type"))
			assert.Equal(t, "", r.PostFormValue("client_id"))
			assert.Equal(t, "test-client-secret", r.PostFormValue("client_secret"))
			assert.Equal(t, "catalog", r.PostFormValue("scope"))

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(oauthTokenResponse{
				AccessToken: "fetched-token-secret",
				TokenType:   "Bearer",
			})
		}))
		defer server.Close()

		serverURL, err := url.Parse(server.URL)
		require.NoError(t, err)

		manager := Oauth2AuthManager{
			Credential: "test-client-secret",
			AuthURI:    serverURL,
			Client:     server.Client(),
		}

		key, val, err := manager.AuthHeader()
		require.NoError(t, err)
		assert.Equal(t, "Authorization", key)
		assert.Equal(t, "Bearer fetched-token-secret", val)
		assert.Equal(t, "fetched-token-secret", manager.Token, "manager token should be updated")
	})

	t.Run("returns error if fetch fails", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(oauthErrorResponse{
				Err:     "invalid_client",
				ErrDesc: "client secret is invalid",
			})
		}))
		defer server.Close()

		serverURL, err := url.Parse(server.URL)
		require.NoError(t, err)

		manager := Oauth2AuthManager{
			Credential: "test-client-id:invalid-secret",
			AuthURI:    serverURL,
			Client:     server.Client(),
		}

		_, _, err = manager.AuthHeader()
		require.Error(t, err)
		var oauthErr oauthErrorResponse
		assert.ErrorAs(t, err, &oauthErr)
		assert.Equal(t, "invalid_client: client secret is invalid", err.Error())
	})

	t.Run("returns error if http client is missing", func(t *testing.T) {
		manager := Oauth2AuthManager{Credential: "some-credential"}
		_, _, err := manager.AuthHeader()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot fetch token without http client")
	})
}

func TestOauth2AuthManagerFetchAccessToken(t *testing.T) {
	t.Run("returns error if auth uri is missing", func(t *testing.T) {
		manager := Oauth2AuthManager{
			Credential: "cred",
			Client:     http.DefaultClient,
		}
		_, err := manager.fetchAccessToken()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing auth uri for fetching token")
	})

	t.Run("returns error if response is not ok and not a known error status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		serverURL, err := url.Parse(server.URL)
		require.NoError(t, err)

		manager := Oauth2AuthManager{
			Credential: "cred",
			AuthURI:    serverURL,
			Client:     server.Client(),
		}

		_, err = manager.fetchAccessToken()
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrServerError))
	})

	t.Run("returns error on bad json for success response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("not-json"))
		}))
		defer server.Close()

		serverURL, err := url.Parse(server.URL)
		require.NoError(t, err)

		manager := Oauth2AuthManager{
			Credential: "cred",
			AuthURI:    serverURL,
			Client:     server.Client(),
		}

		_, err = manager.fetchAccessToken()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to decode oauth token response")
	})

	t.Run("returns error on bad json for error response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("not-json"))
		}))
		defer server.Close()

		serverURL, err := url.Parse(server.URL)
		require.NoError(t, err)

		manager := Oauth2AuthManager{
			Credential: "cred",
			AuthURI:    serverURL,
			Client:     server.Client(),
		}

		_, err = manager.fetchAccessToken()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to decode oauth error")
	})

	t.Run("successfully fetches token", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(oauthTokenResponse{
				AccessToken: "successfully-fetched-token",
				TokenType:   "Bearer",
			})
		}))
		defer server.Close()

		serverURL, err := url.Parse(server.URL)
		require.NoError(t, err)

		manager := Oauth2AuthManager{
			Credential: "test-client-id:test-client-secret",
			AuthURI:    serverURL,
			Client:     server.Client(),
		}

		token, err := manager.fetchAccessToken()
		require.NoError(t, err)
		assert.Equal(t, "successfully-fetched-token", token)
	})
}
