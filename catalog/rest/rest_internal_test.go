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
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthHeader(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)

	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]any{}, "overrides": map[string]any{},
		})
	})

	mux.HandleFunc("/v1/oauth/tokens", func(w http.ResponseWriter, req *http.Request) {
		assert.Equal(t, http.MethodPost, req.Method)

		assert.Equal(t, req.Header.Get("Content-Type"), "application/x-www-form-urlencoded")

		require.NoError(t, req.ParseForm())
		values := req.PostForm
		assert.Equal(t, values.Get("grant_type"), "client_credentials")
		assert.Equal(t, values.Get("client_id"), "client")
		assert.Equal(t, values.Get("client_secret"), "secret")
		assert.Equal(t, values.Get("scope"), "catalog")

		w.WriteHeader(http.StatusOK)

		json.NewEncoder(w).Encode(map[string]any{
			"access_token":      "some_jwt_token",
			"token_type":        "Bearer",
			"expires_in":        86400,
			"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
		})
	})

	cat, err := NewCatalog(context.Background(), "rest", srv.URL,
		WithCredential("client:secret"))
	require.NoError(t, err)
	assert.NotNil(t, cat)

	require.IsType(t, (*sessionTransport)(nil), cat.cl.Transport)
	assert.Equal(t, http.Header{
		"Authorization":               {"Bearer some_jwt_token"},
		"Content-Type":                {"application/json"},
		"User-Agent":                  {"GoIceberg/(unknown version)"},
		"X-Client-Version":            {icebergRestSpecVersion},
		"X-Iceberg-Access-Delegation": {"vended-credentials"},
	}, cat.cl.Transport.(*sessionTransport).defaultHeaders)
}

func TestAuthUriHeader(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)

	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]any{}, "overrides": map[string]any{},
		})
	})

	mux.HandleFunc("/auth-token-url", func(w http.ResponseWriter, req *http.Request) {
		assert.Equal(t, http.MethodPost, req.Method)

		assert.Equal(t, req.Header.Get("Content-Type"), "application/x-www-form-urlencoded")

		require.NoError(t, req.ParseForm())
		values := req.PostForm
		assert.Equal(t, values.Get("grant_type"), "client_credentials")
		assert.Equal(t, values.Get("client_id"), "client")
		assert.Equal(t, values.Get("client_secret"), "secret")
		assert.Equal(t, values.Get("scope"), "catalog")

		w.WriteHeader(http.StatusOK)

		json.NewEncoder(w).Encode(map[string]any{
			"access_token":      "some_jwt_token",
			"token_type":        "Bearer",
			"expires_in":        86400,
			"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
		})
	})

	authUri, err := url.Parse(srv.URL)
	require.NoError(t, err)
	cat, err := NewCatalog(context.Background(), "rest", srv.URL,
		WithCredential("client:secret"), WithAuthURI(authUri.JoinPath("auth-token-url")))
	require.NoError(t, err)
	assert.NotNil(t, cat)

	require.IsType(t, (*sessionTransport)(nil), cat.cl.Transport)
	assert.Equal(t, http.Header{
		"Authorization":               {"Bearer some_jwt_token"},
		"Content-Type":                {"application/json"},
		"User-Agent":                  {"GoIceberg/(unknown version)"},
		"X-Client-Version":            {icebergRestSpecVersion},
		"X-Iceberg-Access-Delegation": {"vended-credentials"},
	}, cat.cl.Transport.(*sessionTransport).defaultHeaders)
}
