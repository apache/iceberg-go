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
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"io"
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
		w.WriteHeader(http.StatusOK)
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

func TestScope(t *testing.T) {
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
		assert.Equal(t, values.Get("grant_type"), "client_credentials")
		assert.Equal(t, values.Get("client_secret"), "secret")
		assert.Equal(t, values.Get("scope"), "my_scope")

		w.WriteHeader(http.StatusOK)

		json.NewEncoder(w).Encode(map[string]any{
			"access_token":      "some_jwt_token",
			"token_type":        "Bearer",
			"expires_in":        86400,
			"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
		})
	})

	cat, err := NewCatalog(
		context.Background(),
		"rest",
		srv.URL,
		WithCredential("secret"),
		WithScope("my_scope"),
	)
	require.NoError(t, err)
	assert.NotNil(t, cat)
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
