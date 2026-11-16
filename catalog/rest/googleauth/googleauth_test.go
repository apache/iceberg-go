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

package googleauth_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest/googleauth"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

// tokenServer is a fake Google OAuth token endpoint for service-account JWT
// assertions. It records the scope claim of each assertion it receives.
type tokenServer struct {
	*httptest.Server

	mu     sync.Mutex
	scopes []string
}

func newTokenServer(t *testing.T, accessToken string) *tokenServer {
	t.Helper()

	ts := &tokenServer{}
	ts.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !assert.NoError(t, r.ParseForm()) {
			http.Error(w, "bad form", http.StatusBadRequest)

			return
		}
		var scope string
		switch grant := r.PostForm.Get("grant_type"); grant {
		case "urn:ietf:params:oauth:grant-type:jwt-bearer":
			scope = jwtClaims(t, r.PostForm.Get("assertion")).Scope
		case "refresh_token":
			scope = r.PostForm.Get("scope")
		default:
			t.Errorf("unexpected grant_type %q", grant)
		}
		ts.mu.Lock()
		ts.scopes = append(ts.scopes, scope)
		ts.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": accessToken,
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	}))
	t.Cleanup(ts.Close)

	return ts
}

func (ts *tokenServer) requestedScopes() []string {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	return append([]string(nil), ts.scopes...)
}

type claimSet struct {
	Iss   string `json:"iss"`
	Scope string `json:"scope"`
}

func jwtClaims(t *testing.T, assertion string) claimSet {
	t.Helper()

	parts := strings.Split(assertion, ".")
	require.Len(t, parts, 3, "assertion is not a JWT")
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)

	var claims claimSet
	require.NoError(t, json.Unmarshal(payload, &claims))

	return claims
}

// serviceAccountJSON returns a service account key whose token_uri points at
// tokenURL, signed with a freshly generated RSA key.
func serviceAccountJSON(t *testing.T, tokenURL string) string {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	out, err := json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     "test-project",
		"private_key_id": "key-1",
		"private_key":    string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"client_email":   "catalog@test-project.iam.gserviceaccount.com",
		"client_id":      "1",
		"token_uri":      tokenURL,
	})
	require.NoError(t, err)

	return string(out)
}

// newRESTServer is a REST catalog that requires the bearer token on every
// request and records the paths it served.
func newRESTServer(t *testing.T, wantToken string) (*httptest.Server, func() []string) {
	t.Helper()

	var (
		mu    sync.Mutex
		paths []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()

		if got := r.Header.Get("Authorization"); got != "Bearer "+wantToken {
			http.Error(w, "unexpected Authorization "+got, http.StatusUnauthorized)

			return
		}

		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/config":
			_, _ = w.Write([]byte(`{"defaults": {}, "overrides": {}}`))
		case "/v1/namespaces":
			_, _ = w.Write([]byte(`{"namespaces": [["db"]]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()

		return append([]string(nil), paths...)
	}
}

func TestLoadCatalogWithGoogleAuthType(t *testing.T) {
	tokens := newTokenServer(t, "google-token")
	srv, served := newRESTServer(t, "google-token")

	cat, err := catalog.Load(context.Background(), "biglake", iceberg.Properties{
		"type":                        "rest",
		"uri":                         srv.URL,
		"rest.auth.type":              "google",
		googleauth.CredentialsJSONKey: serviceAccountJSON(t, tokens.URL),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.Close() })

	namespaces, err := cat.ListNamespaces(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, []table.Identifier{{"db"}}, namespaces)

	assert.Equal(t, []string{"/v1/config", "/v1/namespaces"}, served())
	assert.Equal(t, []string{googleauth.DefaultScopes}, tokens.requestedScopes(),
		"one token, with the default scope, serves both the config and the catalog requests")
}

func TestTokenSourceCredentialsPathAndScopes(t *testing.T) {
	tokens := newTokenServer(t, "path-token")
	path := filepath.Join(t.TempDir(), "sa.json")
	require.NoError(t, os.WriteFile(path, []byte(serviceAccountJSON(t, tokens.URL)), 0o600))

	ts, err := googleauth.TokenSource(context.Background(), iceberg.Properties{
		googleauth.CredentialsPathKey: path,
		googleauth.ScopesKey:          " scope-a , ,scope-b ",
	})
	require.NoError(t, err)

	tok, err := ts.Token()
	require.NoError(t, err)
	assert.Equal(t, "path-token", tok.AccessToken)
	assert.Equal(t, []string{"scope-a scope-b"}, tokens.requestedScopes())
}

// TestTokenSourceOutlivesContext uses authorized_user credentials because their
// refresh sends the token source's context with the request; a service
// account's JWT exchange doesn't, so it would pass either way.
func TestTokenSourceOutlivesContext(t *testing.T) {
	tokens := newTokenServer(t, "late-token")
	ctx, cancel := context.WithCancel(context.Background())

	creds, err := json.Marshal(map[string]string{
		"type":          "authorized_user",
		"client_id":     "client",
		"client_secret": "client-secret",
		"refresh_token": "refresh",
		"token_uri":     tokens.URL,
	})
	require.NoError(t, err)

	ts, err := googleauth.TokenSource(ctx, iceberg.Properties{
		googleauth.CredentialsJSONKey: string(creds),
	})
	require.NoError(t, err)
	cancel()

	tok, err := ts.Token()
	require.NoError(t, err, "a refresh after the constructor's context is canceled must still work")
	assert.Equal(t, "late-token", tok.AccessToken)
}

type countingTransport struct{ calls atomic.Int32 }

func (c *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.calls.Add(1)

	return http.DefaultTransport.RoundTrip(r)
}

func TestTokenSourceUsesContextHTTPClient(t *testing.T) {
	tokens := newTokenServer(t, "ctx-client-token")
	transport := &countingTransport{}
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, &http.Client{Transport: transport})

	ts, err := googleauth.TokenSource(ctx, iceberg.Properties{
		googleauth.CredentialsJSONKey: serviceAccountJSON(t, tokens.URL),
	})
	require.NoError(t, err)

	tok, err := ts.Token()
	require.NoError(t, err)
	assert.Equal(t, "ctx-client-token", tok.AccessToken)
	assert.Equal(t, int32(1), transport.calls.Load(), "the token request goes through the context's client")
}

func TestTokenSourceRejectsBadCredentials(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name    string
		props   iceberg.Properties
		wantErr string
	}{
		{
			name: "both path and json",
			props: iceberg.Properties{
				googleauth.CredentialsPathKey: filepath.Join(dir, "sa.json"),
				googleauth.CredentialsJSONKey: `{"type":"service_account"}`,
			},
			wantErr: "cannot set both",
		},
		{
			name:    "missing file",
			props:   iceberg.Properties{googleauth.CredentialsPathKey: filepath.Join(dir, "missing.json")},
			wantErr: googleauth.CredentialsPathKey,
		},
		{
			name:    "not json",
			props:   iceberg.Properties{googleauth.CredentialsJSONKey: "private-key-material"},
			wantErr: "not valid JSON",
		},
		{
			name:    "no type",
			props:   iceberg.Properties{googleauth.CredentialsJSONKey: `{"private_key":"secret"}`},
			wantErr: `no "type" field`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := googleauth.TokenSource(context.Background(), tc.props)
			require.ErrorContains(t, err, tc.wantErr)
			assert.NotContains(t, err.Error(), "private-key-material")
			assert.NotContains(t, err.Error(), "secret")
		})
	}
}
