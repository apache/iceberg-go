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

package rest_test

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type staticAuthManager struct{ value string }

func (s staticAuthManager) AuthHeader() (string, string, error) {
	return "Authorization", s.value, nil
}

// authRecorder is a REST catalog that records the Authorization header of
// every request and returns the given /v1/config overrides.
type authRecorder struct {
	*httptest.Server

	mu      sync.Mutex
	headers map[string][]string
}

func newAuthRecorder(t *testing.T, overrides map[string]string) *authRecorder {
	t.Helper()

	rec := &authRecorder{headers: map[string][]string{}}
	rec.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		rec.headers[r.URL.Path] = append(rec.headers[r.URL.Path], r.Header.Get("Authorization"))
		rec.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/config":
			_ = json.NewEncoder(w).Encode(map[string]any{"defaults": map[string]string{}, "overrides": overrides})
		case "/v1/namespaces":
			_, _ = w.Write([]byte(`{"namespaces": []}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(rec.Close)

	return rec
}

// authHeaders loads a catalog with props, lists namespaces, and returns the
// Authorization header sent to /v1/config and to /v1/namespaces. Without opts
// it goes through catalog.Load, which parses token before /v1/config;
// WithAdditionalProps applies it only after.
func (rec *authRecorder) authHeaders(t *testing.T, props iceberg.Properties, opts ...rest.Option) (configAuth, catalogAuth string) {
	t.Helper()

	var (
		cat catalog.Catalog
		err error
	)
	if len(opts) == 0 {
		loadProps := iceberg.Properties{"type": "rest", "uri": rec.URL}
		maps.Copy(loadProps, props)
		cat, err = catalog.Load(context.Background(), "test", loadProps)
	} else {
		cat, err = rest.NewCatalog(context.Background(), "test", rec.URL, append(opts, rest.WithAdditionalProps(props))...)
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })

	_, err = cat.ListNamespaces(context.Background(), nil)
	require.NoError(t, err)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Len(t, rec.headers["/v1/config"], 1)
	require.Len(t, rec.headers["/v1/namespaces"], 1)

	return rec.headers["/v1/config"][0], rec.headers["/v1/namespaces"][0]
}

// registerCounting registers a static auth manager under name and returns the
// number of times its factory ran and the properties it last received.
func registerCounting(t *testing.T, name, header string) (*atomic.Int32, *iceberg.Properties) {
	t.Helper()

	var (
		calls atomic.Int32
		got   iceberg.Properties
	)
	rest.RegisterAuthManager(name, func(_ context.Context, cfg rest.AuthManagerConfig) (rest.AuthManager, error) {
		calls.Add(1)
		got = cfg.Props

		return staticAuthManager{value: header}, nil
	})

	return &calls, &got
}

func TestAuthTypeSelectsAuthentication(t *testing.T) {
	calls, gotProps := registerCounting(t, "test-registry-custom", "Custom abc")

	cases := []struct {
		name  string
		props iceberg.Properties
		want  string
	}{
		{"unset uses token", iceberg.Properties{"token": "tok"}, "Bearer tok"},
		{"oauth2 uses token", iceberg.Properties{"token": "tok", "rest.auth.type": "oauth2"}, "Bearer tok"},
		{"none ignores token", iceberg.Properties{"token": "tok", "rest.auth.type": "none"}, ""},
		{"none matches case-insensitively", iceberg.Properties{"token": "tok", "rest.auth.type": " NONE "}, ""},
		{"registered type", iceberg.Properties{"token": "tok", "rest.auth.type": "Test-Registry-Custom", "x": "y"}, "Custom abc"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config, cat := newAuthRecorder(t, nil).authHeaders(t, tc.props)
			assert.Equal(t, tc.want, config, "the /v1/config request")
			assert.Equal(t, tc.want, cat, "the catalog request")
		})
	}

	assert.Equal(t, int32(1), calls.Load(), "the factory runs once and serves both sessions")
	assert.Equal(t, "y", (*gotProps)["x"], "the factory receives the client properties")
}

func TestAuthTypeIgnoresServerOverrides(t *testing.T) {
	calls, _ := registerCounting(t, "test-registry-server", "Server-chosen")

	for _, override := range []string{"none", "test-registry-server", "unknown-type"} {
		t.Run(override, func(t *testing.T) {
			rec := newAuthRecorder(t, map[string]string{"rest.auth.type": override})
			config, cat := rec.authHeaders(t, iceberg.Properties{"token": "tok"})
			assert.Equal(t, "Bearer tok", config)
			assert.Equal(t, "Bearer tok", cat, "a server override must not change the client's authentication")
		})
	}
	assert.Zero(t, calls.Load(), "a server override must not build an auth manager")
}

func TestWithAuthManagerTakesPrecedenceOverAuthType(t *testing.T) {
	calls, _ := registerCounting(t, "test-registry-precedence", "Registered")

	config, cat := newAuthRecorder(t, nil).authHeaders(t,
		iceberg.Properties{"rest.auth.type": "test-registry-precedence"},
		rest.WithAuthManager(staticAuthManager{value: "Explicit"}))
	assert.Equal(t, "Explicit", config)
	assert.Equal(t, "Explicit", cat)
	assert.Zero(t, calls.Load())
}

func TestAuthTypeErrors(t *testing.T) {
	rest.RegisterAuthManager("test-registry-failing", func(context.Context, rest.AuthManagerConfig) (rest.AuthManager, error) {
		return nil, errors.New("no credentials")
	})
	rest.RegisterAuthManager("test-registry-nil", func(context.Context, rest.AuthManagerConfig) (rest.AuthManager, error) {
		return nil, nil
	})

	cases := []struct {
		authType string
		wantErr  string
	}{
		{"made-up", `unknown rest.auth.type "made-up"`},
		{"google", `import _ "github.com/apache/iceberg-go/catalog/rest/googleauth"`},
		{"sigv4", "set rest.sigv4-enabled=true"},
		{"test-registry-failing", "rest.auth.type=test-registry-failing: no credentials"},
		{"test-registry-nil", "factory returned nil"},
	}
	for _, tc := range cases {
		t.Run(tc.authType, func(t *testing.T) {
			rec := newAuthRecorder(t, nil)
			_, err := catalog.Load(context.Background(), "test", iceberg.Properties{
				"type": "rest", "uri": rec.URL, "rest.auth.type": tc.authType,
			})
			require.ErrorContains(t, err, tc.wantErr)

			rec.mu.Lock()
			defer rec.mu.Unlock()
			assert.Empty(t, rec.headers, "an auth error stops the catalog before any request")
		})
	}
}

func TestRegisterAuthManagerRejectsReservedNames(t *testing.T) {
	factory := func(context.Context, rest.AuthManagerConfig) (rest.AuthManager, error) {
		return staticAuthManager{}, nil
	}
	for _, name := range []string{"", "none", "OAuth2"} {
		assert.Panics(t, func() { rest.RegisterAuthManager(name, factory) }, name)
	}
	assert.Panics(t, func() { rest.RegisterAuthManager("test-registry-nil-factory", nil) })
}
