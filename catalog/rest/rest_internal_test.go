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
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

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

func TestSigv4EmptyStringHash(t *testing.T) {
	t.Parallel()
	hash := sha256.New()
	payloadHash := hex.EncodeToString(hash.Sum(nil))
	// Sanity check the constant.
	require.Equal(t, payloadHash, emptyStringHash)
}

func TestSigv4ContentSha256Header(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadDefaultConfig(context.Background(), func(opts *config.LoadOptions) error {
		opts.Credentials = credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
			},
		}

		return nil
	})
	require.NoError(t, err)

	t.Run("header set when sigv4 enabled", func(t *testing.T) {
		t.Parallel()
		var capturedHeader string
		mux := http.NewServeMux()
		srv := httptest.NewServer(mux)
		defer srv.Close()

		mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(map[string]any{
				"defaults": map[string]any{}, "overrides": map[string]any{},
			})
		})

		mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
			capturedHeader = r.Header.Get("x-amz-content-sha256")
			w.WriteHeader(http.StatusOK)
		})

		cat, err := NewCatalog(context.Background(), "rest", srv.URL,
			WithSigV4(),
			WithSigV4RegionSvc("us-east-1", "s3"),
			WithAwsConfig(cfg))
		require.NoError(t, err)

		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/test", nil)
		require.NoError(t, err)

		_, err = cat.cl.Do(req)
		require.NoError(t, err)

		assert.NotEmpty(t, capturedHeader, "x-amz-content-sha256 header should be set when sigv4 is enabled")
		assert.Equal(t, emptyStringHash, capturedHeader, "header should contain hash of empty body")
	})

	t.Run("header not set when sigv4 disabled", func(t *testing.T) {
		t.Parallel()
		var capturedHeader string
		headerPresent := false
		mux := http.NewServeMux()
		srv := httptest.NewServer(mux)
		defer srv.Close()

		mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(map[string]any{
				"defaults": map[string]any{}, "overrides": map[string]any{},
			})
		})

		mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
			capturedHeader = r.Header.Get("x-amz-content-sha256")
			_, headerPresent = r.Header["X-Amz-Content-Sha256"]
			w.WriteHeader(http.StatusOK)
		})

		cat, err := NewCatalog(context.Background(), "rest", srv.URL)
		require.NoError(t, err)

		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/test", nil)
		require.NoError(t, err)

		_, err = cat.cl.Do(req)
		require.NoError(t, err)

		assert.Empty(t, capturedHeader, "x-amz-content-sha256 header should not be set when sigv4 is disabled")
		assert.False(t, headerPresent, "x-amz-content-sha256 header should not be present when sigv4 is disabled")
	})

	t.Run("header contains correct hash for request body", func(t *testing.T) {
		t.Parallel()
		var capturedHeader string
		mux := http.NewServeMux()
		srv := httptest.NewServer(mux)
		defer srv.Close()

		mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(map[string]any{
				"defaults": map[string]any{}, "overrides": map[string]any{},
			})
		})

		mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
			capturedHeader = r.Header.Get("x-amz-content-sha256")
			w.WriteHeader(http.StatusOK)
		})

		cat, err := NewCatalog(context.Background(), "rest", srv.URL,
			WithSigV4(),
			WithSigV4RegionSvc("us-east-1", "s3"),
			WithAwsConfig(cfg))
		require.NoError(t, err)

		body := []byte(`{"test": "data"}`)
		expectedHash := sha256.Sum256(body)
		expectedHashStr := hex.EncodeToString(expectedHash[:])

		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL+"/test", bytes.NewReader(body))
		require.NoError(t, err)

		_, err = cat.cl.Do(req)
		require.NoError(t, err)

		assert.Equal(t, expectedHashStr, capturedHeader, "header should contain correct hash of request body")
	})
}

func TestSigv4ConcurrentSigners(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	srv := httptest.NewUnstartedServer(mux)
	// If we use HTTP 1.1, this test can try to make too many connections
	// and exhaust ephemeral ports.
	srv.EnableHTTP2 = true
	srv.StartTLS() // Using TLS to easily support HTTP/2
	rootCAs := x509.NewCertPool()
	rootCAs.AddCert(srv.Certificate())

	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]any{}, "overrides": map[string]any{},
		})
	})

	cfg, err := config.LoadDefaultConfig(context.Background(), func(opts *config.LoadOptions) error {
		opts.Credentials = credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "abcdefghjklmnop",
				SecretAccessKey: "01234567abcdefgh01234567abcdefgh01234567abcdefgh01234567abcdefgh",
			},
		}

		return nil
	})
	require.NoError(t, err)

	cat, err := NewCatalog(context.Background(), "rest", srv.URL,
		WithSigV4(),
		WithSigV4RegionSvc("abc", "def"),
		WithAwsConfig(cfg),
		WithTLSConfig(&tls.Config{
			RootCAs: rootCAs,
		}))
	require.NoError(t, err)
	assert.NotNil(t, cat)

	// We aren't recreating the signature logic to verify on the server. We're
	// just running many concurrent requests to make sure the race detector
	// doesn't find any data races with how the session transport and signer
	// are used from concurrent goroutines.
	ctx, cancel := context.WithCancel(context.Background())
	grp, ctx := errgroup.WithContext(ctx)
	var count atomic.Uint64
	for range 10 {
		grp.Go(func() error {
			for {
				if err := ctx.Err(); err != nil {
					return nil
				}
				body := make([]byte, 1024)
				if _, err := rand.Read(body); err != nil {
					return err
				}
				// Intentionally using context.Background instead of ctx so that we
				// don't get interrupted when context is cancelled.
				req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, srv.URL, bytes.NewReader(body))
				if err != nil {
					return err
				}
				resp, err := cat.cl.Do(req)
				if err != nil {
					return err
				}
				// We don't actually care about the response, only that it actually made it to the server.
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				count.Add(1)
			}
		})
	}
	time.Sleep(5 * time.Second)
	cancel()
	require.NoError(t, grp.Wait())
	t.Logf("issued %d requests", count.Load())
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

		_, err = do[struct{}](context.Background(), http.MethodGet, baseURI, []string{"test"}, client, nil, false)
		require.Error(t, err)

		assert.True(t, tracker.body.closed,
			"response body should be closed on non-200 status")
	})

	t.Run("doPostAllowNoContent", func(t *testing.T) {
		tracker := &trackingTransport{transport: http.DefaultTransport}
		client := &http.Client{Transport: tracker}

		baseURI, err := url.Parse(srv.URL)
		require.NoError(t, err)

		_, err = doPostAllowNoContent[map[string]string, struct{}](
			context.Background(), baseURI, []string{"test"}, map[string]string{"key": "value"}, client, nil, false)
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

	err := handleNon200(rsp, nil)
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

	err := handleNon200(rsp, nil)
	require.Error(t, err)
	require.Equal(t, "ValidationException: nested error", err.Error())
	require.True(t, errors.Is(err, ErrForbidden))
}

func TestHandleNon200_EmptyBodyFallback(t *testing.T) {
	rsp := &http.Response{
		StatusCode:    http.StatusBadRequest,
		ContentLength: 0,
		Body:          http.NoBody,
	}

	err := handleNon200(rsp, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrBadRequest))
	require.Equal(t, ErrBadRequest.Error(), err.Error())
	require.NotEqual(t, ": ", err.Error())
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
