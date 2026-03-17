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
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

func TestOauth2AuthManager_AuthHeader_StaticToken(t *testing.T) {
	manager := &Oauth2AuthManager{
		tokenSource: oauth2.StaticTokenSource(&oauth2.Token{
			AccessToken: "static_token",
			TokenType:   "Bearer",
		}),
	}

	key, value, err := manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, "Authorization", key)
	assert.Equal(t, "Bearer static_token", value)
}

func TestOauth2AuthManager_AuthHeader_FetchToken_Success(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "client_credentials", r.FormValue("grant_type"))
		assert.Equal(t, "client", r.FormValue("client_id"))
		assert.Equal(t, "secret", r.FormValue("client_secret"))
		assert.Equal(t, "catalog", r.FormValue("scope"))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": "fetched_token",
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	})

	cfg := &clientcredentials.Config{
		ClientID:     "client",
		ClientSecret: "secret",
		TokenURL:     server.URL + "/oauth/token",
		Scopes:       []string{"catalog"},
		AuthStyle:    oauth2.AuthStyleInParams,
	}

	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, server.Client())
	manager := &Oauth2AuthManager{
		tokenSource: cfg.TokenSource(ctx),
	}

	key, value, err := manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, "Authorization", key)
	assert.Equal(t, "Bearer fetched_token", value)
}

func TestOauth2AuthManager_AuthHeader_FetchToken_ErrorResponse(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{
			"error":             "invalid_client",
			"error_description": "Invalid client credentials",
		})
	})

	cfg := &clientcredentials.Config{
		ClientID:     "client",
		ClientSecret: "secret",
		TokenURL:     server.URL + "/oauth/token",
		AuthStyle:    oauth2.AuthStyleInParams,
	}

	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, server.Client())
	manager := &Oauth2AuthManager{
		tokenSource: cfg.TokenSource(ctx),
	}

	_, _, err := manager.AuthHeader()
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrOAuthError), "error should wrap ErrOAuthError")
	assert.Contains(t, err.Error(), "invalid_client")
	assert.Contains(t, err.Error(), "Invalid client credentials")
}
