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
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func TestOauth2AuthManager_AuthHeaderWithContext_CallerDeadline(t *testing.T) {
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	// The token endpoint stalls until the test ends. Nothing cancels the refresh
	// request itself, so the release must come before server.Close waits on it.
	release := make(chan struct{})
	defer close(release)
	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		<-release
	})

	cfg := &clientcredentials.Config{
		ClientID:     "client",
		ClientSecret: "secret",
		TokenURL:     server.URL + "/oauth/token",
		AuthStyle:    oauth2.AuthStyleInParams,
	}

	tokenCtx := context.WithValue(context.Background(), oauth2.HTTPClient, server.Client())
	manager := &Oauth2AuthManager{
		tokenSource: cfg.TokenSource(tokenCtx),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _, err := manager.AuthHeaderWithContext(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// blockingTokenSource counts Token calls and holds each until released.
type blockingTokenSource struct {
	calls   atomic.Int32
	release chan struct{}
}

func (s *blockingTokenSource) Token() (*oauth2.Token, error) {
	s.calls.Add(1)
	<-s.release

	return &oauth2.Token{AccessToken: "shared_token", TokenType: "Bearer"}, nil
}

func TestOauth2AuthManager_AuthHeaderWithContext_SharesRefresh(t *testing.T) {
	source := &blockingTokenSource{release: make(chan struct{})}
	manager := &Oauth2AuthManager{tokenSource: source}

	// Concurrent callers give up before the refresh completes.
	var callers sync.WaitGroup
	for range 5 {
		callers.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			_, _, err := manager.AuthHeaderWithContext(ctx)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
		})
	}
	callers.Wait()
	assert.EqualValues(t, 1, source.calls.Load(), "waiting callers must share one refresh")

	// The shared refresh completes in the background and serves later callers.
	close(source.release)
	key, value, err := manager.AuthHeader()
	require.NoError(t, err)
	assert.Equal(t, "Authorization", key)
	assert.Equal(t, "Bearer shared_token", value)
}

// sequenceTokenSource returns its tokens in order, repeating the last one, and
// counts Token calls.
type sequenceTokenSource struct {
	tokens []*oauth2.Token
	err    error
	calls  atomic.Int32
}

func (s *sequenceTokenSource) Token() (*oauth2.Token, error) {
	n := int(s.calls.Add(1))
	if s.err != nil {
		return nil, s.err
	}

	return s.tokens[min(n, len(s.tokens))-1], nil
}

func TestNewOauth2AuthManager_ReusesValidToken(t *testing.T) {
	// An empty TokenType defaults to Bearer, as oauth2.Token.Type does.
	source := &sequenceTokenSource{tokens: []*oauth2.Token{
		{AccessToken: "a", Expiry: time.Now().Add(time.Hour)},
	}}
	manager := NewOauth2AuthManager(source)

	for range 3 {
		key, value, err := manager.AuthHeader()
		require.NoError(t, err)
		assert.Equal(t, "Authorization", key)
		assert.Equal(t, "Bearer a", value)
	}
	assert.EqualValues(t, 1, source.calls.Load(), "a valid token must be reused, not refetched per request")
}

func TestNewOauth2AuthManager_RefreshesExpiredToken(t *testing.T) {
	source := &sequenceTokenSource{tokens: []*oauth2.Token{
		{AccessToken: "a", TokenType: "Bearer", Expiry: time.Now().Add(-time.Minute)},
		{AccessToken: "b", TokenType: "Bearer", Expiry: time.Now().Add(time.Hour)},
	}}
	manager := NewOauth2AuthManager(source)

	for _, want := range []string{"Bearer a", "Bearer b", "Bearer b"} {
		_, value, err := manager.AuthHeaderWithContext(context.Background())
		require.NoError(t, err)
		assert.Equal(t, want, value)
	}
	assert.EqualValues(t, 2, source.calls.Load())
}

func TestNewOauth2AuthManager_RetrieveErrorIsOAuthError(t *testing.T) {
	manager := NewOauth2AuthManager(&sequenceTokenSource{err: &oauth2.RetrieveError{
		ErrorCode:        "invalid_grant",
		ErrorDescription: "Token has been expired or revoked.",
	}})

	_, _, err := manager.AuthHeader()
	require.ErrorIs(t, err, ErrOAuthError)
	assert.Contains(t, err.Error(), "invalid_grant")
	assert.Contains(t, err.Error(), "Token has been expired or revoked.")
}

func TestNewOauth2AuthManager_CanceledContextSkipsSource(t *testing.T) {
	source := &sequenceTokenSource{tokens: []*oauth2.Token{{AccessToken: "a"}}}
	manager := NewOauth2AuthManager(source)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := manager.AuthHeaderWithContext(ctx)

	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, source.calls.Load(), "a canceled caller must not trigger a refresh")
}

func TestNewOauth2AuthManager_NilSourcePanics(t *testing.T) {
	assert.PanicsWithValue(t, "rest: NewOauth2AuthManager called with a nil oauth2.TokenSource", func() {
		NewOauth2AuthManager(nil)
	})
}
