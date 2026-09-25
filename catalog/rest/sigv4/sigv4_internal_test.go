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

package sigv4

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestSigner(t *testing.T) *signer {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(context.Background(), func(o *config.LoadOptions) error {
		o.Credentials = credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
			},
		}

		return nil
	})
	require.NoError(t, err)
	cfg.Region = "us-east-1"

	return newSigner(cfg, "s3")
}

func TestEmptyStringHash(t *testing.T) {
	t.Parallel()

	h := sha256.New()
	assert.Equal(t, hex.EncodeToString(h.Sum(nil)), emptyStringHash)
}

func TestSignRequestEmptyBodyContentHash(t *testing.T) {
	t.Parallel()

	s := newTestSigner(t)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com/test", nil)
	require.NoError(t, err)
	require.NoError(t, s.SignRequest(req))

	assert.Equal(t, emptyStringHash, req.Header.Get("x-amz-content-sha256"))
	assert.NotEmpty(t, req.Header.Get("Authorization"), "SigV4 should set the Authorization header")
}

func TestSignRequestBodyContentHash(t *testing.T) {
	t.Parallel()

	s := newTestSigner(t)
	body := []byte(`{"test": "data"}`)
	sum := sha256.Sum256(body)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://example.com/test", bytes.NewReader(body))
	require.NoError(t, err)
	require.NoError(t, s.SignRequest(req))

	assert.Equal(t, hex.EncodeToString(sum[:]), req.Header.Get("x-amz-content-sha256"))
}

func TestSignRequestNilGetBodyReturnsError(t *testing.T) {
	t.Parallel()

	s := newTestSigner(t)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://example.com/test", bytes.NewReader([]byte(`{}`)))
	require.NoError(t, err)
	req.GetBody = nil // a hand-built request whose body cannot be re-read

	err = s.SignRequest(req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "GetBody", "error should explain the body is not re-readable")
}

type closeTrackingReadCloser struct {
	*bytes.Reader
	closeErr error
	closed   bool
}

func (r *closeTrackingReadCloser) Close() error {
	r.closed = true

	return r.closeErr
}

func TestSignRequestClosesClonedBody(t *testing.T) {
	t.Parallel()

	s := newTestSigner(t)
	body := []byte(`{"test": "data"}`)
	var cloned *closeTrackingReadCloser

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://example.com/test", bytes.NewReader(body))
	require.NoError(t, err)
	req.GetBody = func() (io.ReadCloser, error) {
		cloned = &closeTrackingReadCloser{Reader: bytes.NewReader(body)}

		return cloned, nil
	}

	require.NoError(t, s.SignRequest(req))
	require.NotNil(t, cloned)
	assert.True(t, cloned.closed)
}

func TestSignRequestReturnsClonedBodyCloseError(t *testing.T) {
	t.Parallel()

	s := newTestSigner(t)
	closeErr := errors.New("close failed")
	body := []byte(`{"test": "data"}`)
	var cloned *closeTrackingReadCloser

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://example.com/test", bytes.NewReader(body))
	require.NoError(t, err)
	req.GetBody = func() (io.ReadCloser, error) {
		cloned = &closeTrackingReadCloser{Reader: bytes.NewReader(body), closeErr: closeErr}

		return cloned, nil
	}

	err = s.SignRequest(req)
	require.ErrorIs(t, err, closeErr)
	require.NotNil(t, cloned)
	assert.True(t, cloned.closed)
}

func TestSignRequestConcurrent(t *testing.T) {
	t.Parallel()

	s := newTestSigner(t)
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com/test", nil)
			if err == nil {
				err = s.SignRequest(req)
			}
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
}

// TestRegisteredBackendEnablesSigV4 verifies that importing this package (its
// init registers the sigv4 signer) lets rest.WithSigV4RegionSvc resolve without
// the caller supplying an explicit signer, and that the resolved signer
// actually signs outbound catalog requests end to end.
func TestRegisteredBackendEnablesSigV4(t *testing.T) {
	// t.Setenv precludes t.Parallel; static credentials let the registered
	// factory's config.LoadDefaultConfig resolve offline (no EC2 IMDS lookup).
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_REGION", "us-east-1")

	var gotAuth, gotSHA string
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotSHA = r.Header.Get("x-amz-content-sha256")
		json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]any{}, "overrides": map[string]any{},
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", srv.URL,
		rest.WithSigV4RegionSvc("us-east-1", "s3"))
	require.NoError(t, err)
	require.NotNil(t, cat)

	// The registered backend must actually sign the bootstrap /v1/config request,
	// not merely let catalog construction succeed.
	assert.Contains(t, gotAuth, "AWS4-HMAC-SHA256", "request should carry a SigV4 Authorization header")
	assert.NotEmpty(t, gotSHA, "request should carry x-amz-content-sha256")
}

// TestConcurrentSignedCatalogRequests drives the full transport+signer stack
// from concurrent goroutines so the race detector can observe the shared signer
// registry lookups and the signing path. Every request the server sees must be
// SigV4-signed. This restores the end-to-end concurrency coverage the previous
// in-package TestSigv4ConcurrentSigners provided before signing moved here.
func TestConcurrentSignedCatalogRequests(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_REGION", "us-east-1")

	var total, signed atomic.Int64
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		total.Add(1)
		if strings.Contains(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256") &&
			r.Header.Get("x-amz-content-sha256") != "" {
			signed.Add(1)
		}
		json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]any{}, "overrides": map[string]any{},
		})
	})

	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			cat, err := rest.NewCatalog(context.Background(), "rest", srv.URL,
				rest.WithSigV4RegionSvc("us-east-1", "s3"))
			if err != nil {
				t.Error(err)

				return
			}
			_ = cat
		})
	}
	wg.Wait()

	require.Positive(t, total.Load())
	assert.Equal(t, total.Load(), signed.Load(), "every request must be SigV4-signed")
}
