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
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitIdentForPathRequiresNamespaceAndName(t *testing.T) {
	cat := &Catalog{}

	for _, ident := range []table.Identifier{
		nil,
		{"table"},
		{"namespace", ""},
		{"namespace", "."},
		{"namespace", ".."},
		{"namespace", "table/name"},
		{"namespace", "table\nname"},
	} {
		_, _, err := cat.splitIdentForPath(ident)
		require.ErrorIs(t, err, catalog.ErrNoSuchTable)
	}

	_, _, err := cat.splitViewIdentForPath(table.Identifier{"view"})
	require.ErrorIs(t, err, catalog.ErrNoSuchView)
	require.NotErrorIs(t, err, catalog.ErrNoSuchTable)

	ns, tbl, err := cat.splitIdentForPath(table.Identifier{"namespace", "table"})
	require.NoError(t, err)
	assert.Equal(t, "namespace", ns)
	assert.Equal(t, "table", tbl)

	ns, tbl, err = cat.splitIdentForPath(table.Identifier{"parent", "namespace", "table"})
	require.NoError(t, err)
	assert.Equal(t, "parent%1Fnamespace", ns)
	assert.Equal(t, "table", tbl)
}

func TestLoadRegisteredCatalogRejectsInvalidAuthURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		authURL string
		wantErr string
	}{
		{
			name:    "malformed URL",
			authURL: "http://[::1",
			wantErr: "invalid rest.authorization-url",
		},
		{
			name:    "missing scheme",
			authURL: "example.com/auth",
			wantErr: "missing scheme",
		},
		{
			name:    "scheme and opaque value with no host",
			authURL: "localhost:8080",
			wantErr: "missing host",
		},
		{
			name:    "scheme with empty host",
			authURL: "http:///path",
			wantErr: "missing host",
		},
		{
			name:    "empty URL",
			authURL: "",
			wantErr: "missing scheme",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cat, err := catalog.Load(context.Background(), "rest", iceberg.Properties{
				"uri":                    "http://example.com",
				"rest.authorization-url": tt.authURL,
			})
			require.Error(t, err)
			assert.Nil(t, cat)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestLoadRegisteredCatalogAcceptsValidAuthURL(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	var oauthCalled atomic.Bool
	mux.HandleFunc("/auth-token-url", func(w http.ResponseWriter, req *http.Request) {
		oauthCalled.Store(true)
		assert.Equal(t, http.MethodPost, req.Method)

		require.NoError(t, req.ParseForm())
		assert.Equal(t, "client", req.PostForm.Get("client_id"))
		assert.Equal(t, "secret", req.PostForm.Get("client_secret"))

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": "some_jwt_token",
			"token_type":   "Bearer",
			"expires_in":   86400,
		})
	})
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]any{}, "overrides": map[string]any{},
		})
	})

	authURL := srv.URL + "/auth-token-url"
	cat, err := catalog.Load(context.Background(), "rest", iceberg.Properties{
		"uri":              srv.URL,
		keyAuthUrl:         authURL,
		keyOauthCredential: "client:secret",
	})
	require.NoError(t, err)
	require.True(t, oauthCalled.Load())

	restCat, ok := cat.(*Catalog)
	require.True(t, ok)
	assert.Equal(t, authURL, restCat.props[keyOAuth2ServerURI])
}

func TestNewCatalogRejectsInvalidAuthURLFromConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		section string
		authURL string
		wantErr string
	}{
		{
			name:    "malformed URL in defaults",
			section: "defaults",
			authURL: "http://[::1",
			wantErr: "invalid oauth2-server-uri",
		},
		{
			name:    "scheme-less URL in overrides",
			section: "overrides",
			authURL: "example.com/auth",
			wantErr: "missing scheme",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mux := http.NewServeMux()
			srv := httptest.NewServer(mux)
			defer srv.Close()

			defaults := map[string]any{}
			overrides := map[string]any{}
			switch tt.section {
			case "defaults":
				defaults[keyAuthUrl] = tt.authURL
			case "overrides":
				overrides[keyAuthUrl] = tt.authURL
			}

			mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{
					"defaults":  defaults,
					"overrides": overrides,
				})
			})

			cat, err := NewCatalog(context.Background(), "rest", srv.URL)
			require.Error(t, err)
			assert.Nil(t, cat)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestNewCatalogAcceptsValidAuthURLFromConfig(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	authURL := "https://auth.example.com/oauth/token"
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"defaults":  map[string]any{},
			"overrides": map[string]any{keyAuthUrl: authURL},
		})
	})

	cat, err := NewCatalog(context.Background(), "rest", srv.URL)
	require.NoError(t, err)
	assert.Equal(t, authURL, cat.props[keyOAuth2ServerURI])
}

func TestOAuthServerURIProps(t *testing.T) {
	t.Parallel()

	const (
		serverURI = "https://auth.example.com/oauth/tokens"
		authURL   = "https://legacy.example.com/v1/oauth/tokens"
	)

	t.Run("oauth2-server-uri configures the token endpoint", func(t *testing.T) {
		t.Parallel()

		var opts options
		require.NoError(t, fromProps(iceberg.Properties{keyOAuth2ServerURI: serverURI}, &opts))
		require.NotNil(t, opts.authUri)
		assert.Equal(t, serverURI, opts.authUri.String())
	})

	t.Run("rest.authorization-url still works as an alias", func(t *testing.T) {
		t.Parallel()

		var opts options
		require.NoError(t, fromProps(iceberg.Properties{keyAuthUrl: authURL}, &opts))
		require.NotNil(t, opts.authUri)
		assert.Equal(t, authURL, opts.authUri.String())
	})

	t.Run("oauth2-server-uri takes precedence when both are set", func(t *testing.T) {
		t.Parallel()

		var opts options
		require.NoError(t, fromProps(iceberg.Properties{
			keyAuthUrl:         authURL,
			keyOAuth2ServerURI: serverURI,
		}, &opts))
		require.NotNil(t, opts.authUri)
		assert.Equal(t, serverURI, opts.authUri.String())
	})

	t.Run("neither key leaves the endpoint unset for fallback", func(t *testing.T) {
		t.Parallel()

		var opts options
		require.NoError(t, fromProps(iceberg.Properties{}, &opts))
		assert.Nil(t, opts.authUri)
	})

	t.Run("neither key is retained as an additional prop", func(t *testing.T) {
		t.Parallel()

		var opts options
		require.NoError(t, fromProps(iceberg.Properties{
			keyAuthUrl:         authURL,
			keyOAuth2ServerURI: serverURI,
		}, &opts))
		_, hasAuthURL := opts.additionalProps[keyAuthUrl]
		_, hasServerURI := opts.additionalProps[keyOAuth2ServerURI]
		assert.False(t, hasAuthURL)
		assert.False(t, hasServerURI)
	})

	t.Run("invalid oauth2-server-uri is rejected", func(t *testing.T) {
		t.Parallel()

		for _, tt := range []struct {
			name    string
			uri     string
			wantErr string
		}{
			{"malformed URL", "http://[::1", "invalid oauth2-server-uri"},
			{"missing scheme", "auth.example.com/token", "missing scheme"},
			{"missing host", "https://", "missing host"},
		} {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				var opts options
				err := fromProps(iceberg.Properties{keyOAuth2ServerURI: tt.uri}, &opts)
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.wantErr)
			})
		}
	})

	t.Run("toProps serializes under the portable oauth2-server-uri key", func(t *testing.T) {
		t.Parallel()

		u, err := url.Parse(serverURI)
		require.NoError(t, err)

		props := toProps(&options{authUri: u})
		assert.Equal(t, serverURI, props[keyOAuth2ServerURI])
		// Only the portable key is emitted; the legacy rest.authorization-url
		// alias is not, to avoid carrying the endpoint under two names.
		_, hasLegacy := props[keyAuthUrl]
		assert.False(t, hasLegacy)
	})
}

func TestTokenAuthenticationPriority(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Track which authentication method was used
	var authHeader string
	var oauthCalled bool

	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]any{}, "overrides": map[string]any{},
		})
	})

	mux.HandleFunc("/v1/oauth/tokens", func(w http.ResponseWriter, r *http.Request) {
		oauthCalled = true
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": "oauth_token_response",
			"token_type":   "Bearer",
		})
	})

	t.Run("token takes precedence over credential", func(t *testing.T) {
		authHeader = ""
		oauthCalled = false

		// When both token and credential are provided, token should be used directly
		cat, err := NewCatalog(context.Background(), "rest", srv.URL,
			WithOAuthToken("direct_token"),
			WithCredential("client:secret"))

		require.NoError(t, err)
		assert.NotNil(t, cat)

		// Should use the direct token, not call OAuth endpoint
		assert.Equal(t, "Bearer direct_token", authHeader)
		assert.False(t, oauthCalled, "OAuth endpoint should not be called when token is provided")
	})

	t.Run("credential used when no token provided", func(t *testing.T) {
		authHeader = ""
		oauthCalled = false

		// When only credential is provided, should use OAuth flow
		cat, err := NewCatalog(context.Background(), "rest", srv.URL,
			WithCredential("client:secret"))

		require.NoError(t, err)
		assert.NotNil(t, cat)

		// Should call OAuth endpoint and use returned token
		assert.Equal(t, "Bearer oauth_token_response", authHeader)
		assert.True(t, oauthCalled, "OAuth endpoint should be called when only credential is provided")
	})

	t.Run("direct token only", func(t *testing.T) {
		authHeader = ""
		oauthCalled = false

		// When only token is provided, should use it directly
		cat, err := NewCatalog(context.Background(), "rest", srv.URL,
			WithOAuthToken("only_token"))

		require.NoError(t, err)
		assert.NotNil(t, cat)

		// Should use the direct token, not call OAuth endpoint
		assert.Equal(t, "Bearer only_token", authHeader)
		assert.False(t, oauthCalled, "OAuth endpoint should not be called when only token is provided")
	})
}

func TestOAuthTokenRequestParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		opts   []Option
		expect map[string]string
		absent []string
	}{
		{
			name: "default scope",
			expect: map[string]string{
				"scope": "catalog",
			},
			absent: []string{"audience", "resource"},
		},
		{
			name: "custom scope",
			opts: []Option{WithScope("my_scope")},
			expect: map[string]string{
				"scope": "my_scope",
			},
			absent: []string{"audience", "resource"},
		},
		{
			name: "audience only",
			opts: []Option{WithAudience("my-aud")},
			expect: map[string]string{
				"scope":    "catalog",
				"audience": "my-aud",
			},
			absent: []string{"resource"},
		},
		{
			name: "resource only",
			opts: []Option{WithResource("my-res")},
			expect: map[string]string{
				"scope":    "catalog",
				"resource": "my-res",
			},
			absent: []string{"audience"},
		},
		{
			name: "audience and resource",
			opts: []Option{WithAudience("my-aud"), WithResource("my-res")},
			expect: map[string]string{
				"scope":    "catalog",
				"audience": "my-aud",
				"resource": "my-res",
			},
		},
		{
			name: "scope, audience and resource",
			opts: []Option{WithScope("s"), WithAudience("a"), WithResource("r")},
			expect: map[string]string{
				"scope":    "s",
				"audience": "a",
				"resource": "r",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mux := http.NewServeMux()
			srv := httptest.NewServer(mux)
			defer srv.Close()

			mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
				err := json.NewEncoder(w).Encode(map[string]any{
					"defaults": map[string]any{}, "overrides": map[string]any{},
				})
				assert.NoError(t, err)
			})

			mux.HandleFunc("/v1/oauth/tokens", func(w http.ResponseWriter, req *http.Request) {
				assert.Equal(t, http.MethodPost, req.Method)
				assert.Equal(t, "application/x-www-form-urlencoded", req.Header.Get("Content-Type"))

				require.NoError(t, req.ParseForm())
				values := req.PostForm
				assert.Equal(t, "client_credentials", values.Get("grant_type"))
				assert.Equal(t, "secret", values.Get("client_secret"))

				for k, v := range tt.expect {
					assert.Equal(t, v, values.Get(k), "form param %s", k)
				}
				for _, k := range tt.absent {
					assert.Empty(t, values.Get(k), "form param %s should be absent", k)
				}

				w.Header().Set("Content-Type", "application/json")
				err := json.NewEncoder(w).Encode(map[string]any{
					"access_token": "tok",
					"token_type":   "Bearer",
					"expires_in":   86400,
				})
				assert.NoError(t, err)
			})

			opts := append([]Option{WithCredential("secret")}, tt.opts...)
			cat, err := NewCatalog(context.Background(), "rest", srv.URL, opts...)
			require.NoError(t, err)
			assert.NotNil(t, cat)
		})
	}
}

func TestOAuthTLSConfig(t *testing.T) {
	t.Parallel()

	// Helper that returns a config endpoint handler.
	configHandler := func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]any{}, "overrides": map[string]any{},
		})
	}

	// Helper that returns an oauth token endpoint handler.
	tokenHandler := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": "tok",
			"token_type":   "Bearer",
			"expires_in":   86400,
		})
	}

	// generateTLSCert creates a self-signed CA and leaf certificate for
	// use with an httptest server. The returned tls.Certificate can be
	// assigned to the server's TLS config, and the CA cert can be added
	// to a client's root CA pool.
	generateTLSCert := func(t *testing.T) (tls.Certificate, *x509.Certificate) {
		t.Helper()

		// Generate a CA key pair.
		caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)

		caTemplate := &x509.Certificate{
			SerialNumber:          big.NewInt(1),
			NotBefore:             time.Now(),
			NotAfter:              time.Now().Add(time.Hour),
			KeyUsage:              x509.KeyUsageCertSign,
			BasicConstraintsValid: true,
			IsCA:                  true,
		}
		caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
		require.NoError(t, err)
		caCert, err := x509.ParseCertificate(caDER)
		require.NoError(t, err)

		// Generate a leaf certificate signed by this CA.
		leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)

		leafTemplate := &x509.Certificate{
			SerialNumber: big.NewInt(2),
			NotBefore:    time.Now(),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		}
		leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
		require.NoError(t, err)

		tlsCert := tls.Certificate{
			Certificate: [][]byte{leafDER},
			PrivateKey:  leafKey,
		}

		return tlsCert, caCert
	}

	// startTLSServer creates an httptest server using the provided
	// certificate instead of the default shared one.
	startTLSServer := func(handler http.Handler, cert tls.Certificate) *httptest.Server {
		srv := httptest.NewUnstartedServer(handler)
		srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
		srv.StartTLS()

		return srv
	}

	catalogCert, catalogCA := generateTLSCert(t)
	oauthCert, oauthCA := generateTLSCert(t)

	catalogMux := http.NewServeMux()
	catalogMux.HandleFunc("/v1/config", configHandler)
	catalogSrv := startTLSServer(catalogMux, catalogCert)
	t.Cleanup(catalogSrv.Close)

	oauthMux := http.NewServeMux()
	oauthMux.HandleFunc("/oauth/tokens", tokenHandler)
	oauthSrv := startTLSServer(oauthMux, oauthCert)
	t.Cleanup(oauthSrv.Close)

	authURI, err := url.Parse(oauthSrv.URL + "/oauth/tokens")
	require.NoError(t, err)

	catalogPool := x509.NewCertPool()
	catalogPool.AddCert(catalogCA)
	oauthPool := x509.NewCertPool()
	oauthPool.AddCert(oauthCA)

	t.Run("separate oauth tls config", func(t *testing.T) {
		t.Parallel()

		// Each TLS config trusts only its respective server.
		cat, err := NewCatalog(context.Background(), "rest", catalogSrv.URL,
			WithCredential("secret"),
			WithAuthURI(authURI),
			WithTLSConfig(&tls.Config{RootCAs: catalogPool}),
			WithOAuthTLSConfig(&tls.Config{RootCAs: oauthPool}),
		)
		require.NoError(t, err)
		assert.NotNil(t, cat)
	})

	t.Run("fails without separate oauth tls config", func(t *testing.T) {
		t.Parallel()

		// Only the catalog TLS config is set — the OAuth token request
		// should fail because the catalog's CA doesn't trust the OAuth
		// server's certificate.
		_, err := NewCatalog(context.Background(), "rest", catalogSrv.URL,
			WithCredential("secret"),
			WithAuthURI(authURI),
			WithTLSConfig(&tls.Config{RootCAs: catalogPool}),
		)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrOAuthError)
		assert.Contains(t, err.Error(), "ECDSA verification failure")
	})
}

func TestAuthHeader(t *testing.T) {
	t.Parallel()
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
		assert.Equal(t, "client_credentials", values.Get("grant_type"))
		assert.Equal(t, "client", values.Get("client_id"))
		assert.Equal(t, "secret", values.Get("client_secret"))
		assert.Equal(t, "catalog", values.Get("scope"))

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token":      "some_jwt_token",
			"token_type":        "Bearer",
			"expires_in":        86400,
			"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
		})
	})

	var capturedAuthHeader string
	mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		capturedAuthHeader = r.Header.Get("Authorization")
		json.NewEncoder(w).Encode(map[string]any{"namespaces": [][]string{}})
	})

	cat, err := NewCatalog(context.Background(), "rest", srv.URL,
		WithCredential("client:secret"))
	require.NoError(t, err)
	assert.NotNil(t, cat)

	// Verify default headers (excluding Authorization, which is now set per-request).
	require.IsType(t, (*sessionTransport)(nil), cat.cl.Transport)
	assert.Equal(t, http.Header{
		"Content-Type":                {"application/json"},
		"User-Agent":                  {"GoIceberg/(unknown version)"},
		"X-Client-Version":            {icebergRestSpecVersion},
		"X-Iceberg-Access-Delegation": {"vended-credentials"},
	}, cat.cl.Transport.(*sessionTransport).defaultHeaders)

	// Verify Authorization is set on actual requests.
	_, err = cat.ListNamespaces(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, "Bearer some_jwt_token", capturedAuthHeader)
}

func TestAuthUriHeader(t *testing.T) {
	t.Parallel()
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
		assert.Equal(t, "client_credentials", values.Get("grant_type"))
		assert.Equal(t, "client", values.Get("client_id"))
		assert.Equal(t, "secret", values.Get("client_secret"))
		assert.Equal(t, "catalog", values.Get("scope"))

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token":      "some_jwt_token",
			"token_type":        "Bearer",
			"expires_in":        86400,
			"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
		})
	})

	var capturedAuthHeader string
	mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		capturedAuthHeader = r.Header.Get("Authorization")
		json.NewEncoder(w).Encode(map[string]any{"namespaces": [][]string{}})
	})

	authUri, err := url.Parse(srv.URL)
	require.NoError(t, err)
	cat, err := NewCatalog(context.Background(), "rest", srv.URL,
		WithCredential("client:secret"), WithAuthURI(authUri.JoinPath("auth-token-url")))
	require.NoError(t, err)
	assert.NotNil(t, cat)

	require.IsType(t, (*sessionTransport)(nil), cat.cl.Transport)
	assert.Equal(t, http.Header{
		"Content-Type":                {"application/json"},
		"User-Agent":                  {"GoIceberg/(unknown version)"},
		"X-Client-Version":            {icebergRestSpecVersion},
		"X-Iceberg-Access-Delegation": {"vended-credentials"},
	}, cat.cl.Transport.(*sessionTransport).defaultHeaders)

	_, err = cat.ListNamespaces(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, "Bearer some_jwt_token", capturedAuthHeader)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestRoundTripDefaultHeaderHandling(t *testing.T) {
	t.Parallel()

	newSession := func(captured *http.Header) *sessionTransport {
		s := &sessionTransport{
			RoundTripper: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				*captured = r.Header.Clone()

				return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
			}),
			defaultHeaders: http.Header{},
		}
		s.defaultHeaders.Set(headerIcebergAccessDelegation, defaultAccessDelegation)

		return s
	}

	t.Run("applies default when absent", func(t *testing.T) {
		t.Parallel()

		var got http.Header
		req, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
		require.NoError(t, err)
		_, err = newSession(&got).RoundTrip(req)
		require.NoError(t, err)
		assert.Equal(t, []string{defaultAccessDelegation}, got.Values(headerIcebergAccessDelegation))
	})

	t.Run("per-request value overrides default without duplicating", func(t *testing.T) {
		t.Parallel()

		var got http.Header
		req, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
		require.NoError(t, err)
		req.Header.Set(headerIcebergAccessDelegation, "remote-signing")
		_, err = newSession(&got).RoundTrip(req)
		require.NoError(t, err)
		assert.Equal(t, []string{"remote-signing"}, got.Values(headerIcebergAccessDelegation))
	})

	t.Run("context suppression drops the default", func(t *testing.T) {
		t.Parallel()

		var got http.Header
		ctx := withSuppressedHeadersCtx(context.Background(), []string{headerIcebergAccessDelegation})
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://example.com", nil)
		require.NoError(t, err)
		_, err = newSession(&got).RoundTrip(req)
		require.NoError(t, err)
		assert.Empty(t, got.Values(headerIcebergAccessDelegation))
	})
}

// staticAuthManager is a test AuthManager returning a fixed authorization header.
type staticAuthManager struct {
	key, value string
}

func (s staticAuthManager) AuthHeader() (string, string, error) { return s.key, s.value, nil }

func TestRoundTripAuthManagerWinsOverPerRequestHeader(t *testing.T) {
	t.Parallel()

	// The generalized per-request override lets withHeaders replace a session
	// default of the same key, but the managed Authorization header must still
	// win: authManager runs after the default-header loop and Set-overwrites, so
	// a caller cannot suppress or spoof it via withHeaders. This pins that
	// ordering, which is load-bearing but otherwise implicit.
	var got http.Header
	s := &sessionTransport{
		RoundTripper: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			got = r.Header.Clone()

			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		}),
		defaultHeaders: http.Header{},
		authManager:    staticAuthManager{key: "Authorization", value: "Bearer managed-token"},
	}

	req, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer caller-supplied")

	_, err = s.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, []string{"Bearer managed-token"}, got.Values("Authorization"))
}

func TestReqOptionsCompose(t *testing.T) {
	t.Parallel()

	cfg := newReqConfig([]reqOption{
		withHeaders(map[string]string{"X-First": "one"}),
		withHeaders(map[string]string{"X-Second": "two"}),
		withSuppressedHeaders("X-First"),
		withSuppressedHeaders("X-Second", "X-Third"),
	})

	assert.Equal(t, map[string]string{
		"X-First":  "one",
		"X-Second": "two",
	}, cfg.headers)
	assert.Equal(t, []string{"X-First", "X-Second", "X-Third"}, cfg.suppressHeaders)
}

func TestCredentialRefreshOnExpiry(t *testing.T) {
	t.Parallel()

	var tokenVersion atomic.Int64
	var oauthCallCount atomic.Int64

	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]any{}, "overrides": map[string]any{},
		})
	})

	mux.HandleFunc("/v1/oauth/tokens", func(w http.ResponseWriter, r *http.Request) {
		n := oauthCallCount.Add(1)
		tokenVersion.Store(n)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": fmt.Sprintf("token_v%d", n),
			"token_type":   "Bearer",
			"expires_in":   1, // expires in 1 second
		})
	})

	mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		currentVersion := tokenVersion.Load()
		expectedToken := fmt.Sprintf("Bearer token_v%d", currentVersion)

		if auth != expectedToken {
			// Simulate server rejecting an expired/stale token.
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"message": "Token expired",
					"type":    "NotAuthorizedException",
					"code":    401,
				},
			})

			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"namespaces": [][]string{{"ns1"}},
		})
	})

	cat, err := NewCatalog(context.Background(), "rest", srv.URL,
		WithCredential("client:secret"))
	require.NoError(t, err)

	// First call should succeed - the token was just fetched during session creation.
	namespaces, err := cat.ListNamespaces(context.Background(), nil)
	require.NoError(t, err)
	assert.Len(t, namespaces, 1)

	// Wait for the token to "expire" and bump the server's expected version
	// so the old token is rejected.
	time.Sleep(2 * time.Second)
	tokenVersion.Add(1)

	// The catalog should automatically refresh its credential and retry,
	// so this call should succeed transparently.
	namespaces, err = cat.ListNamespaces(context.Background(), nil)
	require.NoError(t, err, "catalog should refresh expired credentials automatically")
	assert.Len(t, namespaces, 1)

	// The OAuth endpoint should have been called a second time to get a fresh token.
	assert.GreaterOrEqual(t, oauthCallCount.Load(), int64(2),
		"OAuth endpoint should be called again to refresh the expired token")
}

// trackingReadCloser wraps an io.ReadCloser to track if Close() was called
type trackingReadCloser struct {
	io.ReadCloser
	closed bool
}

func (t *trackingReadCloser) Close() error {
	t.closed = true

	return t.ReadCloser.Close()
}

// trackingTransport wraps http.RoundTripper to track response bodies
type trackingTransport struct {
	transport http.RoundTripper
	body      *trackingReadCloser
}

func (t *trackingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	t.body = &trackingReadCloser{ReadCloser: resp.Body}
	resp.Body = t.body

	return resp, nil
}

// TestResponseBodyLeak checks if response body is closed properly.
func TestResponseBodyLeak(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{
			"error": errorResponse{
				Message: "not found",
				Type:    "NoSuchTableException",
				Code:    404,
			},
		})
	})

	t.Run("do", func(t *testing.T) {
		tracker := &trackingTransport{transport: http.DefaultTransport}
		client := &http.Client{Transport: tracker}

		baseURI, err := url.Parse(srv.URL)
		require.NoError(t, err)

		_, err = do[struct{}](context.Background(), http.MethodGet, baseURI, []string{"test"}, client, nil)
		require.Error(t, err)

		assert.True(t, tracker.body.closed,
			"response body should be closed on non-200 status")
	})

	t.Run("doPost", func(t *testing.T) {
		tracker := &trackingTransport{transport: http.DefaultTransport}
		client := &http.Client{Transport: tracker}

		baseURI, err := url.Parse(srv.URL)
		require.NoError(t, err)

		_, err = doPost[map[string]string, struct{}](
			context.Background(), baseURI, []string{"test"}, map[string]string{"key": "value"}, client, nil, allowNoContent())
		require.Error(t, err)

		assert.True(t, tracker.body.closed,
			"response body should be closed on non-200 status")
	})
}

func TestHandleNon200_DecodeFlatMessagePreservesSentinel(t *testing.T) {
	rsp := &http.Response{
		StatusCode:    http.StatusBadRequest,
		ContentLength: int64(len(`{"message":"flat error"}`)),
		Body:          io.NopCloser(bytes.NewBufferString(`{"message":"flat error"}`)),
	}

	err := handleNon200(rsp, nil, nil)
	require.Error(t, err)
	require.Equal(t, "flat error", err.Error())
	require.True(t, errors.Is(err, ErrBadRequest))
}

func TestHandleNon200_DecodeCanonicalErrorRendersExpectedMessage(t *testing.T) {
	rsp := &http.Response{
		StatusCode:    http.StatusForbidden,
		ContentLength: int64(len(`{"error":{"message":"nested error","type":"ValidationException"}}`)),
		Body:          io.NopCloser(bytes.NewBufferString(`{"error":{"message":"nested error","type":"ValidationException"}}`)),
	}

	err := handleNon200(rsp, nil, nil)
	require.Error(t, err)
	require.Equal(t, "ValidationException: nested error", err.Error())
	require.True(t, errors.Is(err, ErrForbidden))
}

func TestHandleNon200_RejectsTrailingJSON(t *testing.T) {
	body := `{"message":"bad request"} {}`
	rsp := &http.Response{
		StatusCode:    http.StatusBadRequest,
		ContentLength: int64(len(body)),
		Body:          io.NopCloser(bytes.NewBufferString(body)),
	}

	err := handleNon200(rsp, nil, nil)
	require.ErrorIs(t, err, ErrRESTError)
	require.ErrorContains(t, err, "multiple JSON values")
}

func TestHandleNon200_EmptyBodyFallback(t *testing.T) {
	rsp := &http.Response{
		StatusCode:    http.StatusBadRequest,
		ContentLength: 0,
		Body:          http.NoBody,
	}

	err := handleNon200(rsp, nil, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrBadRequest))
	require.Equal(t, ErrBadRequest.Error(), err.Error())
	require.NotEqual(t, ": ", err.Error())
}

func TestHandleNon200_CapturesStatusAndRetryAfter(t *testing.T) {
	body := `{"error":{"message":"busy","type":"ServiceUnavailable"}}`
	rsp := &http.Response{
		StatusCode:    http.StatusServiceUnavailable,
		Header:        http.Header{"Retry-After": {"3"}},
		ContentLength: int64(len(body)),
		Body:          io.NopCloser(bytes.NewBufferString(body)),
	}

	err := handleNon200(rsp, nil, nil)

	var restErr errorResponse
	require.ErrorAs(t, err, &restErr)
	require.Equal(t, http.StatusServiceUnavailable, restErr.statusCode)
	require.Equal(t, "3", restErr.retryAfter)
	require.ErrorIs(t, err, ErrServiceUnavailable)
}

func TestHandleNon200_PreservesStatusOnMalformedBody(t *testing.T) {
	// A non-JSON error page must still carry the HTTP status (so a poller can apply
	// transport-level retry policy) and stay classified as ErrRESTError.
	rsp := &http.Response{
		StatusCode:    http.StatusBadGateway,
		Header:        http.Header{"Retry-After": {"5"}},
		ContentLength: int64(len("not json")),
		Body:          io.NopCloser(bytes.NewBufferString("not json")),
	}

	err := handleNon200(rsp, nil, nil)

	var restErr errorResponse
	require.ErrorAs(t, err, &restErr)
	require.Equal(t, http.StatusBadGateway, restErr.statusCode)
	require.Equal(t, "5", restErr.retryAfter)
	require.ErrorIs(t, err, ErrRESTError)
}

func TestHandleNon200_StatusOverrideAppliesOnMalformedBody(t *testing.T) {
	// The caller-supplied status override carries the semantics of the status
	// line itself, so a 5xx commit with a garbled body must classify the same
	// as a well-formed envelope: ErrCommitStateUnknown. Callers retry on that
	// sentinel; stripping it when a proxy mangles the payload would turn a
	// retryable unknown commit into a fatal decode error. Unmapped statuses
	// keep the plain decode-failure classification.
	commitOverride := map[int]error{
		http.StatusConflict:            ErrCommitFailed,
		http.StatusInternalServerError: ErrCommitStateUnknown,
		http.StatusBadGateway:          ErrCommitStateUnknown,
		http.StatusServiceUnavailable:  ErrCommitStateUnknown,
		http.StatusGatewayTimeout:      ErrCommitStateUnknown,
	}

	wellFormed := func(status int) string {
		return fmt.Sprintf(`{"error":{"message":"%s","type":"ServerException","code":%d}}`,
			http.StatusText(status), status)
	}
	const malformed = `{"error":{"message":"boom`

	tests := []struct {
		name       string
		status     int
		body       string
		wantIs     error
		wantNotIs  error
		wantDecode bool
	}{
		{
			name:   "well-formed 500",
			status: http.StatusInternalServerError,
			body:   wellFormed(http.StatusInternalServerError),
			wantIs: ErrCommitStateUnknown,
		},
		{
			name:       "malformed 500",
			status:     http.StatusInternalServerError,
			body:       malformed,
			wantIs:     ErrCommitStateUnknown,
			wantDecode: true,
		},
		{
			name:   "well-formed 409",
			status: http.StatusConflict,
			body:   wellFormed(http.StatusConflict),
			wantIs: ErrCommitFailed,
		},
		{
			name:       "malformed 409",
			status:     http.StatusConflict,
			body:       malformed,
			wantIs:     ErrCommitFailed,
			wantNotIs:  ErrCommitStateUnknown,
			wantDecode: true,
		},
		{
			name:       "malformed unmapped 404",
			status:     http.StatusNotFound,
			body:       malformed,
			wantIs:     ErrRESTError,
			wantNotIs:  ErrCommitStateUnknown,
			wantDecode: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rsp := &http.Response{
				StatusCode:    tc.status,
				ContentLength: int64(len(tc.body)),
				Body:          io.NopCloser(bytes.NewBufferString(tc.body)),
			}

			err := handleNon200(rsp, commitOverride, nil)
			require.Error(t, err)
			require.ErrorIs(t, err, tc.wantIs)
			if tc.wantNotIs != nil {
				require.NotErrorIs(t, err, tc.wantNotIs)
			}
			if tc.wantDecode {
				require.ErrorContains(t, err, "failed to decode error response")
			}

			var restErr errorResponse
			require.ErrorAs(t, err, &restErr)
			require.Equal(t, tc.status, restErr.statusCode)
		})
	}
}

func TestHandleNon200_ErrorTypeOverride(t *testing.T) {
	t.Parallel()

	sentinelByType := errors.New("mapped by type")
	sentinelByStatus := errors.New("mapped by status")
	typeOverride := map[string]error{"NoSuchThingException": sentinelByType}
	statusOverride := map[int]error{http.StatusNotFound: sentinelByStatus}

	newRsp := func(status int, body string) *http.Response {
		return &http.Response{
			StatusCode:    status,
			ContentLength: int64(len(body)),
			Body:          io.NopCloser(bytes.NewBufferString(body)),
		}
	}

	t.Run("error.type override wins over status override", func(t *testing.T) {
		t.Parallel()

		err := handleNon200(newRsp(http.StatusNotFound, `{"error":{"type":"NoSuchThingException","message":"gone"}}`), statusOverride, typeOverride)
		require.ErrorIs(t, err, sentinelByType)
		assert.NotErrorIs(t, err, sentinelByStatus)
	})

	t.Run("unmatched error.type falls through to status override", func(t *testing.T) {
		t.Parallel()

		err := handleNon200(newRsp(http.StatusNotFound, `{"error":{"type":"SomethingElse","message":"gone"}}`), statusOverride, typeOverride)
		require.ErrorIs(t, err, sentinelByStatus)
		assert.NotErrorIs(t, err, sentinelByType)
	})

	t.Run("empty body falls through to status override", func(t *testing.T) {
		t.Parallel()

		rsp := &http.Response{StatusCode: http.StatusNotFound, ContentLength: 0, Body: http.NoBody}
		err := handleNon200(rsp, statusOverride, typeOverride)
		require.ErrorIs(t, err, sentinelByStatus)
		assert.NotErrorIs(t, err, sentinelByType)
	})

	t.Run("type override ignored on an unmapped status", func(t *testing.T) {
		t.Parallel()

		// The error.type values are defined for 404 in the spec; a 503 carrying
		// one must resolve on status (ErrServiceUnavailable), not a 404 sentinel.
		err := handleNon200(newRsp(http.StatusServiceUnavailable, `{"error":{"type":"NoSuchThingException","message":"down"}}`), statusOverride, typeOverride)
		require.ErrorIs(t, err, ErrServiceUnavailable)
		assert.NotErrorIs(t, err, sentinelByType)
		assert.NotErrorIs(t, err, sentinelByStatus)
	})
}

func TestErrorResponse_ErrorFormattingTypeAndMessage(t *testing.T) {
	err := errorResponse{Type: "ValidationException", Message: "bad request"}
	require.Equal(t, "ValidationException: bad request", err.Error())
}

func TestErrorResponse_ErrorFormattingMessageOnly(t *testing.T) {
	err := errorResponse{Message: "bad request"}
	require.Equal(t, "bad request", err.Error())
}

func TestErrorResponse_ErrorFormattingTypeOnly(t *testing.T) {
	err := errorResponse{Type: "ValidationException"}
	require.Equal(t, "ValidationException", err.Error())
}

func TestErrorResponse_ErrorFormattingWrappingOnly(t *testing.T) {
	err := errorResponse{wrapping: ErrServerError}
	require.Equal(t, ErrServerError.Error(), err.Error())
}

func TestErrorResponse_ErrorFormattingEmpty(t *testing.T) {
	err := errorResponse{}
	require.Equal(t, "unknown REST error", err.Error())
}

func TestToPropsSigv4RegionFallback(t *testing.T) {
	t.Parallel()

	t.Run("sigv4 region propagated as client.region for s3tables", func(t *testing.T) {
		t.Parallel()

		opts := &options{
			enableSigv4:  true,
			sigv4Region:  "us-east-1",
			sigv4Service: "s3tables",
		}
		props := toProps(opts)
		assert.Equal(t, "us-east-1", props["client.region"])
	})

	t.Run("sigv4 region propagated as client.region for s3", func(t *testing.T) {
		t.Parallel()

		opts := &options{
			enableSigv4:  true,
			sigv4Region:  "us-west-2",
			sigv4Service: "s3",
		}
		props := toProps(opts)
		assert.Equal(t, "us-west-2", props["client.region"])
	})

	t.Run("sigv4 region not propagated for non-s3 service", func(t *testing.T) {
		t.Parallel()

		opts := &options{
			enableSigv4:  true,
			sigv4Region:  "us-east-1",
			sigv4Service: "execute-api",
		}
		props := toProps(opts)
		_, ok := props["client.region"]
		assert.False(t, ok)
	})

	t.Run("sigv4 region does not overwrite existing client.region", func(t *testing.T) {
		t.Parallel()

		opts := &options{
			enableSigv4:  true,
			sigv4Region:  "us-east-1",
			sigv4Service: "s3tables",
			additionalProps: map[string]string{
				"client.region": "eu-west-1",
			},
		}
		props := toProps(opts)
		assert.Equal(t, "eu-west-1", props["client.region"])
	})

	t.Run("no client.region when sigv4 disabled", func(t *testing.T) {
		t.Parallel()

		opts := &options{
			sigv4Region: "us-east-1",
		}
		props := toProps(opts)
		_, ok := props["client.region"]
		assert.False(t, ok)
	})

	t.Run("no client.region when sigv4 region empty", func(t *testing.T) {
		t.Parallel()

		opts := &options{
			enableSigv4:  true,
			sigv4Service: "s3tables",
		}
		props := toProps(opts)
		_, ok := props["client.region"]
		assert.False(t, ok)
	})
}

func TestToPropsPreservesOAuthToken(t *testing.T) {
	t.Parallel()

	opts := &options{
		oauthToken: "static-token",
	}
	props := toProps(opts)
	assert.Equal(t, "static-token", props[keyOauthToken])
}

func TestFromPropsReadsOAuthToken(t *testing.T) {
	t.Parallel()

	var opts options
	err := fromProps(iceberg.Properties{
		keyOauthToken: "static-token",
		"custom":      "value",
	}, &opts)
	require.NoError(t, err)

	assert.Equal(t, "static-token", opts.oauthToken)
	assert.Equal(t, iceberg.Properties{"custom": "value"}, opts.additionalProps)
	assert.NotContains(t, opts.additionalProps, keyOauthToken)
}

func TestFromPropsKeepsExistingOAuthToken(t *testing.T) {
	t.Parallel()

	opts := options{
		oauthToken: "caller-token",
	}
	err := fromProps(iceberg.Properties{
		keyOauthToken: "server-token",
	}, &opts)
	require.NoError(t, err)

	assert.Equal(t, "caller-token", opts.oauthToken)
}

func TestFetchConfigTokenOverrideKeepsCallerToken(t *testing.T) {
	t.Parallel()

	var configAuthHeader string
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		configAuthHeader = r.Header.Get(authorizationHeader)
		json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]any{},
			"overrides": map[string]any{
				keyOauthToken: "server-token",
			},
			"endpoints": AllEndpointStrings,
		})
	})

	cat, err := newCatalogFromProps(context.Background(), "rest", srv.URL, iceberg.Properties{
		keyOauthToken: "caller-token",
	})
	require.NoError(t, err)

	assert.Equal(t, "Bearer caller-token", configAuthHeader)
	assert.Equal(t, "caller-token", cat.props[keyOauthToken])
}

func TestConfigOverrideHeadersApplyToCatalogRequests(t *testing.T) {
	t.Parallel()

	const (
		headerName  = "X-Config-Override"
		headerValue = "required"
	)
	var configHeader, catalogHeader string
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		configHeader = r.Header.Get(headerName)
		json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]any{},
			"overrides": map[string]any{
				"header." + headerName: headerValue,
			},
			"endpoints": AllEndpointStrings,
		})
	})
	mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		catalogHeader = r.Header.Get(headerName)
		json.NewEncoder(w).Encode(map[string]any{"namespaces": [][]string{}})
	})

	cat, err := NewCatalog(context.Background(), "rest", srv.URL)
	require.NoError(t, err)
	_, err = cat.ListNamespaces(context.Background(), nil)
	require.NoError(t, err)

	assert.Empty(t, configHeader)
	assert.Equal(t, headerValue, catalogHeader)
}

type closeTrackingTransport struct {
	http.RoundTripper
	closed atomic.Bool
}

func (t *closeTrackingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.RoundTripper == nil {
		return nil, errors.New("unexpected request")
	}

	return t.RoundTripper.RoundTrip(req)
}

func (t *closeTrackingTransport) CloseIdleConnections() {
	t.closed.Store(true)
}

func TestCatalogCloseReleasesOwnedSessionOnce(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"defaults":  map[string]any{},
			"overrides": map[string]any{},
		})
	})

	var transports []*closeTrackingTransport
	cat, err := NewCatalog(context.Background(), "rest", srv.URL, WithTransportFactory(func(*tls.Config) (http.RoundTripper, func()) {
		transport := &closeTrackingTransport{RoundTripper: http.DefaultTransport}
		transports = append(transports, transport)

		return transport, transport.CloseIdleConnections
	}))
	require.NoError(t, err)
	require.Len(t, transports, 2)
	assert.True(t, transports[0].closed.Load(), "bootstrap transport should be closed after config fetch")
	assert.False(t, transports[1].closed.Load(), "catalog transport should remain open until Close")

	require.NoError(t, cat.Close())
	require.NoError(t, cat.Close())
	assert.True(t, transports[1].closed.Load())
}

func TestCatalogCloseDoesNotReleaseCustomTransport(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"defaults":  map[string]any{},
			"overrides": map[string]any{},
		})
	})

	base := &closeTrackingTransport{RoundTripper: http.DefaultTransport}
	cat, err := NewCatalog(context.Background(), "rest", srv.URL, WithCustomTransport(base))
	require.NoError(t, err)
	require.NoError(t, cat.Close())
	assert.False(t, base.closed.Load(), "user-provided transports must remain caller-owned")
}

func TestFetchConfigDoesNotCloseCustomTransport(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"defaults":  map[string]any{},
			"overrides": map[string]any{},
		})
	})

	base := &closeTrackingTransport{RoundTripper: http.DefaultTransport}
	_, err := NewCatalog(context.Background(), "rest", srv.URL, WithCustomTransport(base))
	require.NoError(t, err)
	assert.False(t, base.closed.Load(), "user-provided transports must not be closed during bootstrap")
}

func TestFetchConfigClosesOwnedOAuthTransportWithoutClosingCustomTransport(t *testing.T) {
	catalogMux := http.NewServeMux()
	catalogSrv := httptest.NewServer(catalogMux)
	defer catalogSrv.Close()

	catalogMux.HandleFunc("/v1/config", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"defaults":  map[string]any{},
			"overrides": map[string]any{},
		})
	})

	oauthClosed := make(chan struct{}, 1)
	oauthMux := http.NewServeMux()
	oauthMux.HandleFunc("/oauth/tokens", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": "tok",
			"token_type":   "Bearer",
			"expires_in":   86400,
		})
	})
	oauthSrv := httptest.NewUnstartedServer(oauthMux)
	oauthSrv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			select {
			case oauthClosed <- struct{}{}:
			default:
			}
		}
	}
	oauthSrv.StartTLS()
	defer oauthSrv.Close()

	authURI, err := url.Parse(oauthSrv.URL + "/oauth/tokens")
	require.NoError(t, err)
	oauthTransport, ok := oauthSrv.Client().Transport.(*http.Transport)
	require.True(t, ok)

	base := &closeTrackingTransport{RoundTripper: http.DefaultTransport}
	_, err = NewCatalog(context.Background(), "rest", catalogSrv.URL,
		WithCustomTransport(base),
		WithCredential("secret"),
		WithAuthURI(authURI),
		WithOAuthTLSConfig(oauthTransport.TLSClientConfig.Clone()),
	)
	require.NoError(t, err)
	assert.False(t, base.closed.Load(), "user-provided transports must not be closed during bootstrap")

	select {
	case <-oauthClosed:
	case <-time.After(time.Second):
		t.Fatal("bootstrap OAuth connection was not closed")
	}
}

func TestFetchConfigAuthURLOverridePrecedence(t *testing.T) {
	t.Parallel()

	const overrideAuthURL = "https://override.example.com/token"

	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]any{},
			// The server overrides the token endpoint via the legacy alias while
			// the client supplied oauth2-server-uri; the override must win.
			"overrides": map[string]any{
				keyAuthUrl: overrideAuthURL,
			},
			"endpoints": AllEndpointStrings,
		})
	})

	cat, err := NewCatalog(context.Background(), "rest", srv.URL,
		WithAuthURI(mustParseURL(t, "https://client.example.com/token")))
	require.NoError(t, err)

	assert.Equal(t, overrideAuthURL, cat.props[keyOAuth2ServerURI])
	_, hasLegacy := cat.props[keyAuthUrl]
	assert.False(t, hasLegacy)
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)

	return u
}

func TestEncodeNamespace(t *testing.T) {
	tests := []struct {
		name          string
		separator     string
		namespace     []string
		wantPath      string
		wantQueryPart string
	}{
		{"default empty separator", "", []string{"a", "b"}, "a%1Fb", "a\x1fb"},
		{"explicit unit separator", "%1F", []string{"a", "b"}, "a%1Fb", "a\x1fb"},
		{"dot separator", "%2E", []string{"analytics", "prod"}, "analytics%2Eprod", "analytics.prod"},
		{"single level", "%2E", []string{"db1"}, "db1", "db1"},
		{"level needing escaping", "%2E", []string{"a/b", "c d"}, "a%2Fb%2Ec%20d", "a/b.c d"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Catalog{namespaceSeparator: tt.separator}
			assert.Equal(t, tt.wantPath, r.encodeNamespace(tt.namespace))
			assert.Equal(t, tt.wantQueryPart, r.namespaceToQueryParam(tt.namespace))
		})
	}
}

func TestCheckValidNamespaceRejectsDecodedSeparator(t *testing.T) {
	tests := []struct {
		name      string
		separator string
		namespace table.Identifier
		wantErr   bool
	}{
		{name: "default separator in component", namespace: table.Identifier{"a\x1fb"}, wantErr: true},
		{name: "configured separator in component", separator: "%2E", namespace: table.Identifier{"a.b"}, wantErr: true},
		{name: "separator between components", namespace: table.Identifier{"a", "b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cat := &Catalog{namespaceSeparator: tt.separator}
			err := cat.checkValidNamespace(tt.namespace)
			if tt.wantErr {
				require.ErrorIs(t, err, catalog.ErrNoSuchNamespace)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNamespaceSeparatorFromConfig(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"defaults":  map[string]any{},
			"overrides": map[string]any{"namespace-separator": "%2E"},
		})
	})

	var gotPath string
	mux.HandleFunc("/v1/namespaces/", func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		json.NewEncoder(w).Encode(map[string]any{
			"namespace":  []string{"analytics", "prod"},
			"properties": map[string]any{"owner": "data-team"},
		})
	})

	cat, err := NewCatalog(context.Background(), "rest", srv.URL)
	require.NoError(t, err)
	assert.Equal(t, "%2E", cat.namespaceSeparator)

	props, err := cat.LoadNamespaceProperties(context.Background(), []string{"analytics", "prod"})
	require.NoError(t, err)
	assert.Equal(t, "/v1/namespaces/analytics%2Eprod", gotPath)
	assert.Equal(t, "data-team", props["owner"])
}

func TestNamespaceSeparatorDefaultsToUnitSeparator(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]any{}, "overrides": map[string]any{},
		})
	})

	var gotPath string
	mux.HandleFunc("/v1/namespaces/", func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		json.NewEncoder(w).Encode(map[string]any{
			"namespace": []string{"a", "b"}, "properties": map[string]any{},
		})
	})

	cat, err := NewCatalog(context.Background(), "rest", srv.URL)
	require.NoError(t, err)
	assert.Equal(t, "%1F", cat.namespaceSeparator)

	_, err = cat.LoadNamespaceProperties(context.Background(), []string{"a", "b"})
	require.NoError(t, err)
	assert.Equal(t, "/v1/namespaces/a%1Fb", gotPath)
}

// signerFunc adapts a function to the RequestSigner interface for tests.
type signerFunc func(*http.Request) error

func (f signerFunc) SignRequest(r *http.Request) error { return f(r) }

func TestSessionTransportInvokesSigner(t *testing.T) {
	t.Parallel()

	var signed bool
	var got http.Header
	s := &sessionTransport{
		RoundTripper: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			got = r.Header.Clone()

			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		}),
		defaultHeaders: http.Header{},
		signer: signerFunc(func(r *http.Request) error {
			signed = true
			r.Header.Set("X-Signed", "yes")

			return nil
		}),
	}

	req, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
	require.NoError(t, err)
	_, err = s.RoundTrip(req)
	require.NoError(t, err)
	assert.True(t, signed, "signer.SignRequest should be invoked")
	assert.Equal(t, "yes", got.Get("X-Signed"))
}

func TestSessionTransportNoSignerDoesNotSign(t *testing.T) {
	t.Parallel()

	var got http.Header
	s := &sessionTransport{
		RoundTripper: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			got = r.Header.Clone()

			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		}),
		defaultHeaders: http.Header{},
	}

	req, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
	require.NoError(t, err)
	_, err = s.RoundTrip(req)
	require.NoError(t, err)
	assert.Empty(t, got.Get("x-amz-content-sha256"))
}

func TestSessionTransportSignerErrorAbortsRequest(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("sign failed")
	s := &sessionTransport{
		RoundTripper: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			t.Fatal("request must not be sent when signing fails")

			return nil, nil
		}),
		defaultHeaders: http.Header{},
		signer:         signerFunc(func(_ *http.Request) error { return wantErr }),
	}

	req, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
	require.NoError(t, err)
	_, err = s.RoundTrip(req)
	require.ErrorIs(t, err, wantErr)
}

// TestSessionTransportConcurrentRoundTrip drives one shared sessionTransport
// from many goroutines (each with its own request) to confirm the signing and
// default-header path holds no per-request state that races. Meaningful under
// the race detector; complements the sigv4 package's real-signer concurrency
// test.
func TestSessionTransportConcurrentRoundTrip(t *testing.T) {
	t.Parallel()

	var count atomic.Int64
	s := &sessionTransport{
		RoundTripper: roundTripFunc(func(_ *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		}),
		defaultHeaders: http.Header{"X-Default": {"v"}},
		signer: signerFunc(func(r *http.Request) error {
			count.Add(1)
			r.Header.Set("X-Signed", "yes")

			return nil
		}),
	}

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			req, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
			if err != nil {
				t.Error(err)

				return
			}
			if _, err := s.RoundTrip(req); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()

	assert.Equal(t, int64(50), count.Load())
}

// TestSigV4WithoutBackendReturnsHelpfulError verifies that requesting SigV4
// without a registered signer backend fails with guidance naming the import to
// add. It clears the signer registry for the duration (and restores it) so the
// "no backend" path is exercised deterministically regardless of which backends
// happen to be linked into this test binary; that also precludes t.Parallel.
func TestSigV4WithoutBackendReturnsHelpfulError(t *testing.T) {
	signerMu.Lock()
	saved := signerRegistry
	signerRegistry = map[string]SignerFactory{}
	signerMu.Unlock()
	t.Cleanup(func() {
		signerMu.Lock()
		signerRegistry = saved
		signerMu.Unlock()
	})

	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]any{}, "overrides": map[string]any{},
		})
	})

	_, err := NewCatalog(context.Background(), "rest", srv.URL, WithSigV4())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "catalog/rest/sigv4")
}
