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
	"sync/atomic"
	"testing"
	"time"

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

func TestOauth2AuthManager_TokenCaching(t *testing.T) {
	var fetchCount atomic.Int32
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		fetchCount.Add(1)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(oauthTokenResponse{
			AccessToken: "cached_token",
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

	// First call should fetch
	_, v1, err := manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, "Bearer cached_token", v1)
	assert.Equal(t, int32(1), fetchCount.Load())

	// Second call should use cache, not fetch again
	_, v2, err := manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, "Bearer cached_token", v2)
	assert.Equal(t, int32(1), fetchCount.Load(), "should not fetch again when token is cached and valid")
}

func TestOauth2AuthManager_TokenExpiry(t *testing.T) {
	var fetchCount atomic.Int32
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		n := fetchCount.Add(1)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(oauthTokenResponse{
			AccessToken: "token_" + string(rune('0'+n)),
			TokenType:   "Bearer",
			ExpiresIn:   1, // 1 second -- will expire quickly with 5s threshold
		})
	})

	authURL, err := url.Parse(server.URL + "/oauth/token")
	require.NoError(t, err)

	manager := &Oauth2AuthManager{
		Credential: "client:secret",
		AuthURI:    authURL,
		Client:     server.Client(),
	}

	// First call fetches
	_, _, err = manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, int32(1), fetchCount.Load())

	// With ExpiresIn=1 and expiryThreshold=5s, the token is already "expired"
	// so next call should fetch again
	_, _, err = manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, int32(2), fetchCount.Load(), "should fetch again after token expiry")
}

func TestOauth2AuthManager_TokenExchange(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	var lastGrantType string
	var lastSubjectToken string
	var lastSubjectTokenType string
	var lastAudience string
	var lastResource string

	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		lastGrantType = r.FormValue("grant_type")
		lastSubjectToken = r.FormValue("subject_token")
		lastSubjectTokenType = r.FormValue("subject_token_type")
		lastAudience = r.FormValue("audience")
		lastResource = r.FormValue("resource")

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(oauthTokenResponse{
			AccessToken:     "access_tok",
			IssuedTokenType: tokenTypeAccess,
			TokenType:       "Bearer",
			ExpiresIn:       1, // short-lived so it expires
		})
	})

	authURL, err := url.Parse(server.URL + "/oauth/token")
	require.NoError(t, err)

	manager := &Oauth2AuthManager{
		Credential: "client:secret",
		AuthURI:    authURL,
		Audience:   "urn:example:audience",
		Resource:   "urn:example:resource",
		Client:     server.Client(),
	}

	// First call uses client_credentials; server returns issued_token_type.
	// Audience and resource are not sent on client_credentials.
	_, _, err = manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, grantClientCredentials, lastGrantType)
	assert.Empty(t, lastAudience)
	assert.Empty(t, lastResource)

	// Second call should use token exchange since server advertised
	// issued_token_type and the token is expired. Audience and resource
	// should be included in the exchange request.
	_, _, err = manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, grantTokenExchange, lastGrantType)
	assert.Equal(t, "access_tok", lastSubjectToken)
	assert.Equal(t, tokenTypeAccess, lastSubjectTokenType)
	assert.Equal(t, "urn:example:audience", lastAudience)
	assert.Equal(t, "urn:example:resource", lastResource)
}

func TestOauth2AuthManager_RefreshAuth(t *testing.T) {
	var fetchCount atomic.Int32
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		n := fetchCount.Add(1)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(oauthTokenResponse{
			AccessToken: "refreshed_" + string(rune('0'+n)),
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

	// Initial fetch
	_, v1, err := manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, int32(1), fetchCount.Load())

	// RefreshAuth should force a new fetch even though token is valid
	err = manager.RefreshAuth(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int32(2), fetchCount.Load())

	// AuthHeader should now return the refreshed token
	_, v2, err := manager.AuthHeader()
	require.NoError(t, err)
	assert.NotEqual(t, v1, v2, "AuthHeader should return new token after refresh")
}

func TestOauth2AuthManager_RefreshAuth_StaticToken(t *testing.T) {
	manager := &Oauth2AuthManager{
		Token: "my_static_token",
	}

	err := manager.RefreshAuth(context.Background())
	require.NoError(t, err)
}

func TestOauth2AuthManager_TokenPrecedence(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	var oauthCalled bool
	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		oauthCalled = true
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(oauthTokenResponse{
			AccessToken: "oauth_token",
			TokenType:   "Bearer",
			ExpiresIn:   3600,
		})
	})

	authURL, err := url.Parse(server.URL + "/oauth/token")
	require.NoError(t, err)

	manager := &Oauth2AuthManager{
		Token:      "initial_token",
		Credential: "client:secret",
		AuthURI:    authURL,
		Client:     server.Client(),
	}

	// With both Token and Credential set, Token should take precedence
	_, v, err := manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, "Bearer initial_token", v)
	assert.False(t, oauthCalled, "OAuth should not be called when Token takes precedence")
}

func TestIsAuthFailure(t *testing.T) {
	assert.True(t, isAuthFailure(http.StatusUnauthorized))
	assert.True(t, isAuthFailure(http.StatusForbidden))
	assert.True(t, isAuthFailure(statusAuthorizationExpired))
	assert.False(t, isAuthFailure(http.StatusOK))
	assert.False(t, isAuthFailure(http.StatusNotFound))
	assert.False(t, isAuthFailure(http.StatusInternalServerError))
}

func TestAuthRetryOn401(t *testing.T) {
	var requestCount atomic.Int32
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]any{}, "overrides": map[string]any{},
		})
	})

	mux.HandleFunc("/v1/oauth/tokens", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(oauthTokenResponse{
			AccessToken: "new_token",
			TokenType:   "Bearer",
			ExpiresIn:   3600,
		})
	})

	mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		n := requestCount.Add(1)
		if n == 1 {
			// First request returns 401
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"message": "Token expired",
					"type":    "UnauthorizedException",
					"code":    401,
				},
			})

			return
		}
		// Retry should succeed
		json.NewEncoder(w).Encode(map[string]any{
			"namespaces": []any{},
		})
	})

	cat, err := NewCatalog(context.Background(), "rest", server.URL,
		WithCredential("client:secret"))
	require.NoError(t, err)

	// This should trigger a 401, refresh, and retry
	ns, err := cat.ListNamespaces(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, ns)
	assert.Equal(t, int32(2), requestCount.Load(), "should have retried after 401")
}

func TestOauth2AuthManager_ExchangeFallback(t *testing.T) {
	var callCount atomic.Int32
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		callCount.Add(1)

		if r.FormValue("grant_type") == grantTokenExchange {
			// Token exchange fails
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(oauthErrorResponse{
				Err:     "invalid_grant",
				ErrDesc: "Token exchange not supported",
			})

			return
		}

		// Fallback to client_credentials succeeds
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(oauthTokenResponse{
			AccessToken:     "fallback_token",
			IssuedTokenType: tokenTypeAccess,
			TokenType:       "Bearer",
			ExpiresIn:       3600,
		})
	})

	authURL, err := url.Parse(server.URL + "/oauth/token")
	require.NoError(t, err)

	manager := &Oauth2AuthManager{
		Credential:  "client:secret",
		AuthURI:     authURL,
		Client:      server.Client(),
		accessToken: "stale_token",
		tokenType:   tokenTypeAccess,
		expiry:      time.Now().Add(-time.Hour), // force expiry
	}

	_, v, err := manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, "Bearer fallback_token", v)
	// exchange (with subject token) fails, exchange (with Basic auth) fails,
	// then client_credentials succeeds = 3 calls
	assert.Equal(t, int32(3), callCount.Load(),
		"should try exchange, exchange with basic, then fall back to credentials")
}

func TestOauth2AuthManager_NoExchangeWithoutIssuedTokenType(t *testing.T) {
	var callCount atomic.Int32
	var lastGrantType string
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	// Server does NOT return issued_token_type -- exchange should
	// never be attempted.
	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		callCount.Add(1)
		lastGrantType = r.FormValue("grant_type")

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(oauthTokenResponse{
			AccessToken: "cred_token",
			TokenType:   "Bearer",
			ExpiresIn:   1, // short-lived so it expires
		})
	})

	authURL, err := url.Parse(server.URL + "/oauth/token")
	require.NoError(t, err)

	manager := &Oauth2AuthManager{
		Credential: "client:secret",
		AuthURI:    authURL,
		Client:     server.Client(),
	}

	// First call
	_, _, err = manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, grantClientCredentials, lastGrantType)
	assert.Equal(t, int32(1), callCount.Load())

	// Second call -- no issued_token_type was returned, so should
	// use client_credentials again, never exchange
	_, _, err = manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, grantClientCredentials, lastGrantType)
	assert.Equal(t, int32(2), callCount.Load())
}

func TestOauth2AuthManager_ExchangeWithBasicAuth(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	var lastGrantType string
	var lastAuthHeader string

	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		lastGrantType = r.FormValue("grant_type")
		lastAuthHeader = r.Header.Get("Authorization")

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(oauthTokenResponse{
			AccessToken:     "exchanged_token",
			IssuedTokenType: tokenTypeAccess,
			TokenType:       "Bearer",
			ExpiresIn:       3600,
		})
	})

	authURL, err := url.Parse(server.URL + "/oauth/token")
	require.NoError(t, err)

	manager := &Oauth2AuthManager{
		Credential: "client:secret",
		AuthURI:    authURL,
		Client:     server.Client(),
	}

	// First we need an initial token via client_credentials;
	// server returns issued_token_type, enabling exchange.
	_, _, err = manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, grantClientCredentials, lastGrantType)

	// Now RefreshAuth clears the cached token and calls
	// refreshExpiredTokenLocked which uses Basic auth for exchange
	err = manager.RefreshAuth(context.Background())
	require.NoError(t, err)
	// The expired path first tries client_credentials (no access
	// token after clear), so this should be client_credentials.
	// But wait -- RefreshAuth clears accessToken, so
	// refreshExpiredTokenLocked's exchange guard (accessToken != "")
	// won't trigger, and it goes straight to client_credentials.
	assert.Equal(t, grantClientCredentials, lastGrantType)
	assert.Empty(t, lastAuthHeader)

	// To test exchange with Basic auth, we need an expired but present
	// access token. Simulate by setting expiry in the past.
	manager.mu.Lock()
	manager.expiry = time.Now().Add(-time.Hour)
	manager.mu.Unlock()

	// AuthHeader sees expired token and calls refreshCurrentTokenLocked
	// which tries exchange (no Basic auth). If that fails, it calls
	// refreshExpiredTokenLocked which tries exchange with Basic auth.
	// In our test server, exchange always succeeds, so the first
	// exchange (without Basic) will succeed.
	lastGrantType = ""
	lastAuthHeader = ""
	_, _, err = manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, grantTokenExchange, lastGrantType)

	// For explicit Basic auth testing: make exchange-without-Basic fail
	// but exchange-with-Basic succeed.
	manager.mu.Lock()
	manager.expiry = time.Now().Add(-time.Hour)
	manager.mu.Unlock()

	var callIdx atomic.Int32
	mux.HandleFunc("/oauth/token-basic", func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		lastGrantType = r.FormValue("grant_type")
		lastAuthHeader = r.Header.Get("Authorization")
		n := callIdx.Add(1)

		if n == 1 && r.Header.Get("Authorization") == "" {
			// First exchange without Basic auth fails
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(oauthErrorResponse{
				Err:     "invalid_token",
				ErrDesc: "Token expired",
			})

			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(oauthTokenResponse{
			AccessToken:     "basic_auth_token",
			IssuedTokenType: tokenTypeAccess,
			TokenType:       "Bearer",
			ExpiresIn:       3600,
		})
	})

	basicAuthURL, err := url.Parse(server.URL + "/oauth/token-basic")
	require.NoError(t, err)
	manager.AuthURI = basicAuthURL

	_, v, err := manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, "Bearer basic_auth_token", v)
	assert.Equal(t, grantTokenExchange, lastGrantType)
	assert.Contains(t, lastAuthHeader, "Basic ")
}

func TestOauth2AuthManager_RefreshToken(t *testing.T) {
	var grantTypes []string
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		gt := r.FormValue("grant_type")
		grantTypes = append(grantTypes, gt)

		switch gt {
		case grantClientCredentials:
			// Initial fetch returns a refresh token.
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(oauthTokenResponse{
				AccessToken:     "access_1",
				IssuedTokenType: tokenTypeAccess,
				TokenType:       "Bearer",
				ExpiresIn:       1, // short-lived
				RefreshToken:    "refresh_1",
			})
		case grantTokenExchange:
			// Exchange fails, forcing fallback to refresh_token.
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(oauthErrorResponse{
				Err:     "invalid_grant",
				ErrDesc: "exchange not supported",
			})
		case grantRefreshToken:
			assert.Equal(t, "refresh_1", r.FormValue("refresh_token"))
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(oauthTokenResponse{
				AccessToken:     "access_2",
				IssuedTokenType: tokenTypeAccess,
				TokenType:       "Bearer",
				ExpiresIn:       3600,
			})
		default:
			t.Fatalf("unexpected grant_type: %s", gt)
		}
	})

	authURL, err := url.Parse(server.URL + "/oauth/token")
	require.NoError(t, err)

	manager := &Oauth2AuthManager{
		Credential: "client:secret",
		AuthURI:    authURL,
		Client:     server.Client(),
	}

	// First call: client_credentials, gets refresh_token.
	_, v, err := manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, "Bearer access_1", v)

	// Second call: token expired, exchange fails, exchange with
	// Basic auth fails, refresh_token succeeds.
	_, v, err = manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, "Bearer access_2", v)
	assert.Equal(t, []string{
		grantClientCredentials,
		grantTokenExchange, // exchange (no Basic) — fails
		grantTokenExchange, // exchange (Basic auth) — fails
		grantRefreshToken,  // refresh token — succeeds
	}, grantTypes)
}

func TestOauth2AuthManager_RefreshToken_NewTokenReplaces(t *testing.T) {
	var refreshTokensSeen []string
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())

		if r.FormValue("grant_type") == grantRefreshToken {
			refreshTokensSeen = append(refreshTokensSeen, r.FormValue("refresh_token"))
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(oauthTokenResponse{
			AccessToken:  "access",
			TokenType:    "Bearer",
			ExpiresIn:    1,
			RefreshToken: "refresh_" + string(rune('A'+len(refreshTokensSeen))),
		})
	})

	authURL, err := url.Parse(server.URL + "/oauth/token")
	require.NoError(t, err)

	manager := &Oauth2AuthManager{
		Credential: "client:secret",
		AuthURI:    authURL,
		Client:     server.Client(),
	}

	// First call: client_credentials, gets refresh_A.
	_, _, err = manager.AuthHeader()
	require.NoError(t, err)

	// Second call: expired, falls through to refresh_token grant
	// with refresh_A. Server returns refresh_B.
	_, _, err = manager.AuthHeader()
	require.NoError(t, err)

	// Third call: expired, should use refresh_B now.
	_, _, err = manager.AuthHeader()
	require.NoError(t, err)

	assert.Equal(t, []string{"refresh_A", "refresh_B"}, refreshTokensSeen)
}

func TestOauth2AuthManager_RefreshToken_PreservedWhenOmitted(t *testing.T) {
	var refreshTokensSeen []string
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())

		if r.FormValue("grant_type") == grantRefreshToken {
			refreshTokensSeen = append(refreshTokensSeen, r.FormValue("refresh_token"))
		}

		resp := oauthTokenResponse{
			AccessToken: "access",
			TokenType:   "Bearer",
			ExpiresIn:   1,
		}
		// Only return refresh_token on the first call.
		if len(refreshTokensSeen) == 0 {
			resp.RefreshToken = "the_refresh_token"
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	})

	authURL, err := url.Parse(server.URL + "/oauth/token")
	require.NoError(t, err)

	manager := &Oauth2AuthManager{
		Credential: "client:secret",
		AuthURI:    authURL,
		Client:     server.Client(),
	}

	// First: client_credentials, gets refresh token.
	_, _, err = manager.AuthHeader()
	require.NoError(t, err)

	// Second: expired, uses refresh_token grant. Server omits
	// refresh_token in response — should be preserved.
	_, _, err = manager.AuthHeader()
	require.NoError(t, err)

	// Third: expired again, should still use the original refresh token.
	_, _, err = manager.AuthHeader()
	require.NoError(t, err)

	assert.Equal(t, []string{"the_refresh_token", "the_refresh_token"}, refreshTokensSeen)
}

func TestOauth2AuthManager_RefreshToken_FallbackToCredentials(t *testing.T) {
	var grantTypes []string
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		gt := r.FormValue("grant_type")
		grantTypes = append(grantTypes, gt)

		switch gt {
		case grantRefreshToken:
			// Refresh token grant fails.
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(oauthErrorResponse{
				Err:     "invalid_grant",
				ErrDesc: "refresh token expired",
			})
		default:
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(oauthTokenResponse{
				AccessToken:  "access",
				TokenType:    "Bearer",
				ExpiresIn:    3600,
				RefreshToken: "stale_refresh",
			})
		}
	})

	authURL, err := url.Parse(server.URL + "/oauth/token")
	require.NoError(t, err)

	// Start with a stale refresh token and no exchange support
	// (tokenType is empty), so it goes straight to refresh_token.
	manager := &Oauth2AuthManager{
		Credential:   "client:secret",
		AuthURI:      authURL,
		Client:       server.Client(),
		accessToken:  "old",
		refreshToken: "stale_refresh",
		expiry:       time.Now().Add(-time.Hour),
	}

	_, v, err := manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, "Bearer access", v)
	// No tokenType → skip exchange → refresh_token fails →
	// client_credentials succeeds.
	assert.Equal(t, []string{
		grantRefreshToken,
		grantClientCredentials,
	}, grantTypes)
}

func TestOauth2AuthManager_RefreshAuth_ClearsRefreshToken(t *testing.T) {
	var grantTypes []string
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		grantTypes = append(grantTypes, r.FormValue("grant_type"))

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(oauthTokenResponse{
			AccessToken:  "access",
			TokenType:    "Bearer",
			ExpiresIn:    3600,
			RefreshToken: "some_refresh",
		})
	})

	authURL, err := url.Parse(server.URL + "/oauth/token")
	require.NoError(t, err)

	manager := &Oauth2AuthManager{
		Credential:   "client:secret",
		AuthURI:      authURL,
		Client:       server.Client(),
		accessToken:  "old",
		refreshToken: "old_refresh",
		tokenType:    tokenTypeAccess,
		expiry:       time.Now().Add(time.Hour),
	}

	// RefreshAuth clears both accessToken and refreshToken, so
	// it should go straight to client_credentials.
	grantTypes = nil
	err = manager.RefreshAuth(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{grantClientCredentials}, grantTypes)
}

func TestExpiryThreshold(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Duration
		expected time.Duration
	}{
		{"short token", 10 * time.Second, 5 * time.Second},
		{"1 minute token", time.Minute, 6 * time.Second},
		{"1 hour token", time.Hour, 5 * time.Minute},
		{"very long token", 24 * time.Hour, 5 * time.Minute},
		{"zero", 0, 5 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := expiryThreshold(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}
