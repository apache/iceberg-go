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
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOauth2AuthManager_AuthHeader_StaticToken(t *testing.T) {
	manager := &Oauth2AuthManager{
		Token: "static_token",
	}

	key, value, err := manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, "Authorization", key)
	assert.Equal(t, "Bearer static_token", value)
}

func TestOauth2AuthManager_AuthHeader_MissingClient(t *testing.T) {
	manager := &Oauth2AuthManager{
		Credential: "client:secret",
	}

	_, _, err := manager.AuthHeader()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot fetch token without http client")
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

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(oauthTokenResponse{
			AccessToken: "fetched_token",
			TokenType:   "Bearer",
			ExpiresIn:   3600,
		})
	})

	authURL, err := url.Parse(server.URL + "/oauth/token")
	require.NoError(t, err)

	manager := &Oauth2AuthManager{
		Credential: "client:secret",
		AuthURI:    authURL,
		Client:     server.Client(),
	}

	key, value, err := manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, "Authorization", key)
	assert.Equal(t, "Bearer fetched_token", value)
	assert.Equal(t, "fetched_token", manager.Token)
}

func TestOauth2AuthManager_AuthHeader_FetchToken_ErrorResponse(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(oauthErrorResponse{
			Err:     "invalid_client",
			ErrDesc: "Invalid client credentials",
		})
	})

	authURL, err := url.Parse(server.URL + "/oauth/token")
	require.NoError(t, err)

	manager := &Oauth2AuthManager{
		Credential: "client:secret",
		AuthURI:    authURL,
		Client:     server.Client(),
	}

	_, _, err = manager.AuthHeader()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid_client: Invalid client credentials")
}
