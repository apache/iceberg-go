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
	"encoding/base64"
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
		{"basic ignores token", iceberg.Properties{
			"token": "tok", "rest.auth.type": "Basic",
			"rest.auth.basic.username": "user", "rest.auth.basic.password": "p@ss:word",
		}, "Basic " + base64.StdEncoding.EncodeToString([]byte("user:p@ss:word"))},
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

func TestBasicAuthIgnoresServerOverrides(t *testing.T) {
	rec := newAuthRecorder(t, map[string]string{
		"rest.auth.basic.username": "server-user",
		"rest.auth.basic.password": "server-password",
	})
	config, cat := rec.authHeaders(t, iceberg.Properties{
		"rest.auth.type":           "basic",
		"rest.auth.basic.username": "user",
		"rest.auth.basic.password": "password",
	})

	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:password"))
	assert.Equal(t, want, config)
	assert.Equal(t, want, cat, "server overrides must not replace the client's basic credentials")
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
		name     string
		authType string
		props    iceberg.Properties
		wantErr  string
	}{
		{"unknown", "made-up", nil, `unknown rest.auth.type "made-up"`},
		{"google without import", "google", nil, `import _ "github.com/apache/iceberg-go/catalog/rest/googleauth"`},
		{
			"sigv4 without backend", "sigv4", nil,
			`rest.auth.type=sigv4) but no signer backend is registered; add a blank import: import _ "github.com/apache/iceberg-go/catalog/rest/sigv4"`,
		},
		{
			"sigv4 delegating to sigv4", "sigv4",
			iceberg.Properties{"rest.auth.sigv4.delegate-auth-type": "SigV4"},
			"cannot delegate a SigV4 auth manager to another SigV4 auth manager",
		},
		{"factory error", "test-registry-failing", nil, "rest.auth.type=test-registry-failing: no credentials"},
		{"nil manager", "test-registry-nil", nil, "factory returned nil"},
		{
			"basic without username", "basic",
			iceberg.Properties{"rest.auth.basic.password": "hunter2"},
			"rest.auth.type=basic: missing required property rest.auth.basic.username",
		},
		{
			"basic without password", "basic",
			iceberg.Properties{"rest.auth.basic.username": "user"},
			"rest.auth.type=basic: missing required property rest.auth.basic.password",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := newAuthRecorder(t, nil)
			props := iceberg.Properties{"type": "rest", "uri": rec.URL, "rest.auth.type": tc.authType}
			maps.Copy(props, tc.props)
			_, err := catalog.Load(context.Background(), "test", props)
			require.ErrorContains(t, err, tc.wantErr)
			assert.NotContains(t, err.Error(), "hunter2", "errors must not include the password")

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
	for _, name := range []string{"", "none", "Basic", "OAuth2", "SigV4"} {
		assert.Panics(t, func() { rest.RegisterAuthManager(name, factory) }, name)
	}
	assert.Panics(t, func() { rest.RegisterAuthManager("test-registry-nil-factory", nil) })
}

// testSigner marks each request it signs, standing in for the AWS backend,
// which the catalog/rest tests can't import.
type testSigner struct{}

func (testSigner) SignRequest(r *http.Request) error {
	r.Header.Set("X-Test-Signature", "signed")

	return nil
}

func withTestSigner() rest.Option {
	return rest.WithSignerFactory(func(context.Context, rest.SignerConfig) (rest.RequestSigner, error) {
		return testSigner{}, nil
	})
}

// signedRequest is what the server saw on one request.
type signedRequest struct{ auth, signature string }

// newSigV4Recorder is a REST catalog that records the Authorization and test
// signature headers of /v1/config and /v1/namespaces, and returns overrides
// from /v1/config.
func newSigV4Recorder(t *testing.T, overrides map[string]string) (*httptest.Server, func() map[string]signedRequest) {
	t.Helper()

	var (
		mu   sync.Mutex
		seen = map[string]signedRequest{}
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.URL.Path] = signedRequest{r.Header.Get("Authorization"), r.Header.Get("X-Test-Signature")}
		mu.Unlock()

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
	t.Cleanup(srv.Close)

	return srv, func() map[string]signedRequest {
		mu.Lock()
		defer mu.Unlock()

		return maps.Clone(seen)
	}
}

func TestSigV4AuthTypeSignsAndDelegates(t *testing.T) {
	basic := "Basic " + base64.StdEncoding.EncodeToString([]byte("user:password"))
	cases := []struct {
		name     string
		props    iceberg.Properties
		opts     []rest.Option
		wantAuth string
	}{
		{
			name:     "default delegate is oauth2",
			props:    iceberg.Properties{"rest.auth.type": "SigV4"},
			opts:     []rest.Option{rest.WithOAuthToken("tok")},
			wantAuth: "Bearer tok",
		},
		{
			name: "basic delegate",
			props: iceberg.Properties{
				"rest.auth.type":                     "sigv4",
				"rest.auth.sigv4.delegate-auth-type": "Basic",
				"rest.auth.basic.username":           "user",
				"rest.auth.basic.password":           "password",
			},
			wantAuth: basic,
		},
		{
			name:     "none delegate",
			props:    iceberg.Properties{"rest.auth.type": "sigv4", "rest.auth.sigv4.delegate-auth-type": "none"},
			opts:     []rest.Option{rest.WithOAuthToken("tok")},
			wantAuth: "",
		},
		{
			name:     "WithAuthManager replaces the delegate but not signing",
			props:    iceberg.Properties{"rest.auth.type": "sigv4", "rest.auth.sigv4.delegate-auth-type": "basic"},
			opts:     []rest.Option{rest.WithAuthManager(staticAuthManager{value: "Explicit"})},
			wantAuth: "Explicit",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, seen := newSigV4Recorder(t, nil)
			opts := append([]rest.Option{withTestSigner(), rest.WithAdditionalProps(tc.props)}, tc.opts...)
			cat, err := rest.NewCatalog(context.Background(), "test", srv.URL, opts...)
			require.NoError(t, err)
			t.Cleanup(func() { _ = cat.Close() })
			_, err = cat.ListNamespaces(context.Background(), nil)
			require.NoError(t, err)

			want := signedRequest{tc.wantAuth, "signed"}
			got := seen()
			assert.Equal(t, want, got["/v1/config"], "the /v1/config request")
			assert.Equal(t, want, got["/v1/namespaces"], "the catalog request")
		})
	}
}

func TestSigV4AuthTypeIgnoresServerOverrides(t *testing.T) {
	srv, seen := newSigV4Recorder(t, map[string]string{
		"rest.sigv4-enabled": "false",
		"rest.auth.type":     "none",
	})
	cat, err := rest.NewCatalog(context.Background(), "test", srv.URL, withTestSigner(),
		rest.WithOAuthToken("tok"), rest.WithAdditionalProps(iceberg.Properties{"rest.auth.type": "sigv4"}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })
	_, err = cat.ListNamespaces(context.Background(), nil)
	require.NoError(t, err)

	assert.Equal(t, signedRequest{"Bearer tok", "signed"}, seen()["/v1/namespaces"],
		"a server override must not turn off signing or the delegate the client chose")
}
