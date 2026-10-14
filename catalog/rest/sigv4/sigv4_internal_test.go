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

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	internalaws "github.com/apache/iceberg-go/internal/awsconfig"
	iceio "github.com/apache/iceberg-go/io"
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

	// POSTs with a body so the payload-hashing path (GetBody clone + SHA-256)
	// runs concurrently on a single shared signer, exercising the shared v4
	// signer and aws.Config under the race detector.
	s := newTestSigner(t)
	body := []byte(`{"test":"data"}`)
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://example.com/test", bytes.NewReader(body))
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
	t.Cleanup(func() { _ = cat.Close() })

	// The registered backend must actually sign the bootstrap /v1/config request,
	// not merely let catalog construction succeed.
	assert.Contains(t, gotAuth, "AWS4-HMAC-SHA256", "request should carry a SigV4 Authorization header")
	assert.NotEmpty(t, gotSHA, "request should carry x-amz-content-sha256")
}

// TestWithAwsConfigUsesConfiguredRegionService verifies that sigv4.WithAwsConfig
// signs with the region/service from WithSigV4RegionSvc, not the aws.Config's
// own region. Before WithAwsConfig became a signer factory it froze the scope at
// option-construction time, so the natural migration (keep WithSigV4RegionSvc,
// swap rest.WithAwsConfig for sigv4.WithAwsConfig) silently signed for the wrong
// scope and the server rejected it with a 403.
func TestWithAwsConfigUsesConfiguredRegionService(t *testing.T) {
	t.Parallel()

	var gotAuth string
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		json.NewEncoder(w).Encode(map[string]any{
			"defaults": map[string]any{}, "overrides": map[string]any{},
		})
	})

	cfg := aws.Config{
		Region: "eu-central-1", // must be overridden by WithSigV4RegionSvc below
		Credentials: credentials.StaticCredentialsProvider{Value: aws.Credentials{
			AccessKeyID:     "test-access-key",
			SecretAccessKey: "test-secret-key",
		}},
	}

	cat, err := rest.NewCatalog(context.Background(), "rest", srv.URL,
		rest.WithSigV4RegionSvc("us-west-2", "s3tables"),
		WithAwsConfig(cfg))
	require.NoError(t, err)
	require.NotNil(t, cat)
	t.Cleanup(func() { _ = cat.Close() })

	assert.Contains(t, gotAuth, "/us-west-2/s3tables/aws4_request",
		"signature scope must come from WithSigV4RegionSvc, not aws.Config.Region")
	assert.NotContains(t, gotAuth, "eu-central-1",
		"aws.Config.Region must not leak into the signature scope")
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
			_ = cat.Close()
		})
	}
	wg.Wait()

	require.Positive(t, total.Load())
	assert.Equal(t, total.Load(), signed.Load(), "every request must be SigV4-signed")
}

func TestStaticCredsFromProps(t *testing.T) {
	t.Parallel()

	creds, err := staticCredsFromProps(iceberg.Properties{
		iceio.S3AccessKeyID:     "AK",
		iceio.S3SecretAccessKey: "SK",
		iceio.S3SessionToken:    "ST",
	})
	require.NoError(t, err)
	require.NotNil(t, creds)
	got, err := creds.Retrieve(context.Background())
	require.NoError(t, err)
	require.Equal(t, "AK", got.AccessKeyID)
	require.Equal(t, "SK", got.SecretAccessKey)
	require.Equal(t, "ST", got.SessionToken)

	creds, err = staticCredsFromProps(iceberg.Properties{})
	require.NoError(t, err, "no creds must fall back to the default chain")
	require.Nil(t, creds)

	_, err = staticCredsFromProps(iceberg.Properties{iceio.S3AccessKeyID: "AK"})
	require.ErrorIs(t, err, internalaws.ErrIncompleteStaticCredentials, "a lone access key must be an error, not the ambient identity")

	_, err = staticCredsFromProps(iceberg.Properties{iceio.S3SecretAccessKey: "SK"})
	require.ErrorIs(t, err, internalaws.ErrIncompleteStaticCredentials, "a lone secret key must be an error, not the ambient identity")

	_, err = staticCredsFromProps(iceberg.Properties{iceio.S3SessionToken: "ST"})
	require.ErrorIs(t, err, internalaws.ErrIncompleteStaticCredentials, "a lone session token must be an error, not the ambient identity")

	creds, err = staticCredsFromProps(iceberg.Properties{
		keyRestAccessKeyID:     "RAK",
		keyRestSecretAccessKey: "RSK",
		keyRestSessionToken:    "RST",
	})
	require.NoError(t, err)
	require.NotNil(t, creds)
	got, err = creds.Retrieve(context.Background())
	require.NoError(t, err)
	require.Equal(t, "RAK", got.AccessKeyID)
	require.Equal(t, "RSK", got.SecretAccessKey)
	require.Equal(t, "RST", got.SessionToken)

	creds, err = staticCredsFromProps(iceberg.Properties{
		iceio.S3AccessKeyID:     "AK",
		iceio.S3SecretAccessKey: "SK",
		keyRestAccessKeyID:      "RAK",
		keyRestSecretAccessKey:  "RSK",
	})
	require.NoError(t, err)
	got, err = creds.Retrieve(context.Background())
	require.NoError(t, err)
	require.Equal(t, "AK", got.AccessKeyID, "s3.* keys take precedence over rest.* aliases")
	require.Equal(t, "SK", got.SecretAccessKey)

	_, err = staticCredsFromProps(iceberg.Properties{keyRestAccessKeyID: "RAK"})
	require.ErrorIs(t, err, internalaws.ErrIncompleteStaticCredentials, "a lone rest.* access key must be an error")

	_, err = staticCredsFromProps(iceberg.Properties{
		iceio.S3AccessKeyID:    "AK",
		keyRestSecretAccessKey: "RSK",
	})
	require.ErrorIs(t, err, internalaws.ErrIncompleteStaticCredentials, "a partial pair must not be completed with a field from the other namespace")

	creds, err = staticCredsFromProps(iceberg.Properties{
		iceio.S3AccessKeyID:     "AK",
		iceio.S3SecretAccessKey: "SK",
		keyRestSessionToken:     "RST",
	})
	require.NoError(t, err)
	got, err = creds.Retrieve(context.Background())
	require.NoError(t, err)
	require.Equal(t, "AK", got.AccessKeyID)
	require.Empty(t, got.SessionToken, "a complete s3.* pair must not inherit an unrelated rest.* session token")
}

// TestSigV4SignsWithPropsCredentials pins the wiring end to end: the SigV4
// Authorization header on the bootstrap request must be signed with the
// credentials carried in the catalog properties (via SignerConfig.Props), not
// the AWS default chain.
func TestSigV4SignsWithPropsCredentials(t *testing.T) {
	t.Parallel()

	var gotAuth string
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(map[string]any{"defaults": map[string]any{}, "overrides": map[string]any{}})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", srv.URL,
		rest.WithSigV4RegionSvc("us-east-1", "s3"),
		rest.WithAdditionalProps(iceberg.Properties{
			iceio.S3AccessKeyID:     "AKIDEXAMPLEPROPS",
			iceio.S3SecretAccessKey: "secretexample",
		}))
	require.NoError(t, err)
	require.NotNil(t, cat)
	t.Cleanup(func() { _ = cat.Close() })

	require.Contains(t, gotAuth, "Credential=AKIDEXAMPLEPROPS/",
		"SigV4 must sign with the credentials from catalog properties, not the default chain")
}
