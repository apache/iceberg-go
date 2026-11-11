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

package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	stdio "io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go/internal/remotesigning"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoteSigningFileIO(t *testing.T) {
	t.Parallel()
	data := bytes.Repeat([]byte("data\x00"), 1024)
	var mu sync.Mutex
	var methods []string
	storage := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "storage-signature", r.Header.Get("Authorization"))
		assert.Equal(t, "storage-session", r.Header.Get("X-Amz-Security-Token"))
		assert.Equal(t, "signed", r.URL.Query().Get("from-signer"))
		assert.Equal(t, "UNSIGNED-PAYLOAD", r.Header.Get("X-Amz-Content-Sha256"))
		assert.NotContains(t, r.Header.Get("Content-Encoding"), "aws-chunked")
		assert.Empty(t, r.Header.Get("X-Amz-Trailer"))
		assert.Empty(t, r.Header.Get("X-Signer-Secret"))
		mu.Lock()
		methods = append(methods, r.Method)
		mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			body, err := stdio.ReadAll(r.Body)
			assert.NoError(t, err)
			assert.Equal(t, data, body)
			w.Header().Set("ETag", `"object"`)
		case http.MethodGet:
			if r.URL.Query().Get("list-type") == "2" {
				_, _ = w.Write([]byte(`<ListBucketResult><Name>bucket</Name><KeyCount>1</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated><Contents><Key>object</Key><LastModified>2026-11-11T00:00:00Z</LastModified><ETag>&quot;object&quot;</ETag><Size>5120</Size></Contents></ListBucketResult>`))

				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			_, _ = w.Write(data)
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		case http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer storage.Close()
	signer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer signer-token", r.Header.Get("Authorization"))
		assert.Equal(t, []string{"one", "two"}, r.Header.Values("X-Signer-Secret"))
		var req remoteSignRequest
		if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&req)) {
			return
		}
		assert.Equal(t, "us-east-1", req.Region)
		assert.Equal(t, "s3", req.Provider)
		assert.Nil(t, req.Body, "data file payloads must not be sent to the signer")
		assert.Equal(t, map[string]string{"tenant": "table-a"}, req.Properties)
		assert.Empty(t, req.Headers.Get("Authorization"))
		assert.Empty(t, req.Headers.Get("X-Amz-Security-Token"))
		u, err := url.Parse(req.URI)
		if !assert.NoError(t, err) {
			return
		}
		q := u.Query()
		q.Set("from-signer", "signed")
		u.RawQuery = q.Encode()
		_ = json.NewEncoder(w).Encode(remoteSignResponse{URI: u.String(), Headers: http.Header{
			"authorization": {"storage-signature"}, "x-amz-security-token": {"storage-session"},
		}})
	}))
	defer signer.Close()
	settings := `{"properties":{"tenant":"table-a"},"headers":{"authorization":["Bearer signer-token"],"x-signer-secret":["one","two"]}}`

	shared := &aws.Config{Region: "us-east-1", HTTPClient: storage.Client(), Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		t.Error("local AWS credentials must not be requested")

		return aws.Credentials{}, errors.New("unexpected credentials")
	})}
	ctx := utils.WithAWSConfig(t.Context(), shared)
	props := map[string]string{io.S3EndpointURL: storage.URL, io.S3RemoteSigningEnabled: "true", io.S3SignerURI: signer.URL, remotesigning.ConfigProperty: settings, "token": "ignored"}
	parsed, _ := url.Parse("s3://bucket")
	bucket, err := createS3Bucket(ctx, parsed, props)
	require.NoError(t, err)
	defer bucket.Close()
	require.NoError(t, bucket.WriteAll(ctx, "path with space/+key", data, nil))
	actual, err := bucket.ReadAll(ctx, "path with space/+key")
	require.NoError(t, err)
	require.Equal(t, data, actual)
	exists, err := bucket.Exists(ctx, "path with space/+key")
	require.NoError(t, err)
	require.True(t, exists)
	iterator := bucket.List(nil)
	listed, err := iterator.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, "object", listed.Key)
	require.NoError(t, bucket.Delete(ctx, "path with space/+key"))
	mu.Lock()
	defer mu.Unlock()
	require.Contains(t, methods, http.MethodPut)
	require.Contains(t, methods, http.MethodGet)
	require.Contains(t, methods, http.MethodHead)
	require.Contains(t, methods, http.MethodDelete)
	require.IsType(t, &http.Client{}, shared.HTTPClient, "context config must not be changed")
	require.NotNil(t, shared.Credentials)
}

func remoteSigningSDKClient(t *testing.T, storage *httptest.Server, signer string, attempts int) *s3.Client {
	t.Helper()
	cfg := aws.Config{Region: "us-west-2", HTTPClient: storage.Client(), Retryer: func() aws.Retryer {
		return retry.NewStandard(func(o *retry.StandardOptions) { o.MaxAttempts = attempts; o.MaxBackoff = 0 })
	}}
	require.NoError(t, configureRemoteSigning(t.Context(), map[string]string{remotesigning.EndpointProperty: signer}, &cfg))

	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(storage.URL)
		o.UsePathStyle = true
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.APIOptions = append(o.APIOptions, disableRemoteSigningUploadChecksums)
	})
}

func TestRemoteSigningDeleteObjectsRetry(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int32
	var signedBodies []string
	var mu sync.Mutex
	storage := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.True(t, r.URL.Query().Has("delete"))
		body, err := stdio.ReadAll(r.Body)
		assert.NoError(t, err)
		mu.Lock()
		assert.Equal(t, signedBodies[len(signedBodies)-1], string(body))
		mu.Unlock()
		assert.NotEmpty(t, r.Header.Get("X-Amz-Checksum-Crc32"), "required DeleteObjects checksum must remain")
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`<Error><Code>SlowDown</Code></Error>`))

			return
		}
		_, _ = w.Write([]byte(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Deleted><Key>one &amp; two</Key></Deleted></DeleteResult>`))
	}))
	defer storage.Close()
	signer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req remoteSignRequest
		if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&req)) {
			return
		}
		if !assert.NotNil(t, req.Body) {
			return
		}
		assert.Contains(t, *req.Body, "one &amp; two")
		mu.Lock()
		signedBodies = append(signedBodies, *req.Body)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(remoteSignResponse{URI: req.URI, Headers: http.Header{"Authorization": {"signature"}}})
	}))
	defer signer.Close()
	client := remoteSigningSDKClient(t, storage, signer.URL, 2)
	_, err := client.DeleteObjects(t.Context(), &s3.DeleteObjectsInput{Bucket: aws.String("bucket"), Delete: &s3types.Delete{Objects: []s3types.ObjectIdentifier{{Key: aws.String("one & two")}}}})
	require.NoError(t, err)
	require.EqualValues(t, 2, attempts.Load())
	require.Len(t, signedBodies, 2)
	require.Equal(t, signedBodies[0], signedBodies[1])
}

func TestRemoteSigningMultipart(t *testing.T) {
	t.Parallel()
	data := []byte("part-data")
	storage := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.NotContains(t, r.Header.Get("Content-Encoding"), "aws-chunked")
		assert.Empty(t, r.Header.Get("X-Amz-Trailer"))
		switch {
		case r.URL.Query().Has("uploads"):
			_, _ = w.Write([]byte(`<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>upload</UploadId></InitiateMultipartUploadResult>`))
		case r.Method == http.MethodPut:
			body, err := stdio.ReadAll(r.Body)
			assert.NoError(t, err)
			assert.Equal(t, data, body)
			assert.Equal(t, "explicit-digest", r.Header.Get("X-Amz-Checksum-Crc32"))
			w.Header().Set("ETag", `"part"`)
		case r.Method == http.MethodPost:
			_, _ = w.Write([]byte(`<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><ETag>complete</ETag></CompleteMultipartUploadResult>`))
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer storage.Close()
	signer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req remoteSignRequest
		if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&req)) {
			return
		}
		assert.Nil(t, req.Body, "only DeleteObjects bodies go to the signer")
		_ = json.NewEncoder(w).Encode(remoteSignResponse{URI: req.URI, Headers: http.Header{"Authorization": {"signature"}}})
	}))
	defer signer.Close()
	client := remoteSigningSDKClient(t, storage, signer.URL, 1)
	started, err := client.CreateMultipartUpload(t.Context(), &s3.CreateMultipartUploadInput{Bucket: aws.String("bucket"), Key: aws.String("key")})
	require.NoError(t, err)
	_, err = client.UploadPart(t.Context(), &s3.UploadPartInput{Bucket: aws.String("bucket"), Key: aws.String("key"), UploadId: started.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader(data), ChecksumCRC32: aws.String("explicit-digest"), ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32})
	require.NoError(t, err)
	_, err = client.CompleteMultipartUpload(t.Context(), &s3.CompleteMultipartUploadInput{Bucket: aws.String("bucket"), Key: aws.String("key"), UploadId: started.UploadId, MultipartUpload: &s3types.CompletedMultipartUpload{Parts: []s3types.CompletedPart{{PartNumber: aws.Int32(1), ETag: aws.String("part")}}}})
	require.NoError(t, err)
	_, err = client.AbortMultipartUpload(t.Context(), &s3.AbortMultipartUploadInput{Bucket: aws.String("bucket"), Key: aws.String("key"), UploadId: started.UploadId})
	require.NoError(t, err)
	_, err = client.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("bucket"), Key: aws.String("key"), Body: bytes.NewReader(data), ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32})
	require.ErrorContains(t, err, "requires an explicit checksum value")
}

func TestRemoteSigningFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, body string
		status     int
		want       string
	}{
		{"forbidden", `{"error":{"message":"private details"}}`, http.StatusForbidden, "HTTP 403"},
		{"malformed JSON", `{`, http.StatusOK, "decode remote S3 signature"},
		{"missing URI", `{"headers":{}}`, http.StatusOK, "invalid URI"},
		{"missing headers", `{"uri":"https://storage/object"}`, http.StatusOK, "missing headers"},
		{"invalid URI", `{"uri":"file:///object","headers":{}}`, http.StatusOK, "invalid URI"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			storage := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("storage reached after signer failure") }))
			defer storage.Close()
			signer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer signer.Close()
			client := remoteSigningSDKClient(t, storage, signer.URL, 3)
			_, err := client.HeadObject(t.Context(), &s3.HeadObjectInput{Bucket: aws.String("bucket"), Key: aws.String("key")})
			require.ErrorContains(t, err, tt.want)
			require.NotContains(t, err.Error(), "private details")
			require.EqualValues(t, 1, calls.Load())
		})
	}
}

func TestRemoteSigningCancellation(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	release := make(chan struct{})
	signer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = stdio.Copy(stdio.Discard, r.Body)
		close(entered)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer signer.Close()
	defer close(release)
	storage := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("storage reached after cancellation") }))
	defer storage.Close()
	client := remoteSigningSDKClient(t, storage, signer.URL, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("bucket"), Key: aws.String("key")})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("signer not reached")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("cancellation did not return")
	}
}

func TestRemoteSigningDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	for _, redirectSigner := range []bool{true, false} {
		t.Run(strconv.FormatBool(redirectSigner), func(t *testing.T) {
			target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("signing credentials followed a redirect") }))
			defer target.Close()
			storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
			}))
			defer storage.Close()
			signer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if redirectSigner {
					http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)

					return
				}
				var req remoteSignRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				_ = json.NewEncoder(w).Encode(remoteSignResponse{URI: req.URI, Headers: http.Header{"Authorization": {"secret"}}})
			}))
			defer signer.Close()
			client := remoteSigningSDKClient(t, storage, signer.URL, 1)
			_, err := client.HeadObject(t.Context(), &s3.HeadObjectInput{Bucket: aws.String("bucket"), Key: aws.String("key")})
			require.Error(t, err)
		})
	}
}

func TestRemoteSigningUnsignedStreamingUpload(t *testing.T) {
	t.Parallel()
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := stdio.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.Equal(t, "streaming data", string(body))
	}))
	defer storage.Close()
	signer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req remoteSignRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		assert.Nil(t, req.Body)
		_ = json.NewEncoder(w).Encode(remoteSignResponse{URI: req.URI, Headers: http.Header{}})
	}))
	defer signer.Close()
	client := remoteSigningSDKClient(t, storage, signer.URL, 1)
	// Hide Seek so SDK hashing would fail if the unsigned-payload middleware did
	// not run before the HTTP client.
	_, err := client.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String("bucket"), Key: aws.String("key"), Body: struct{ stdio.Reader }{strings.NewReader("streaming data")}, ContentLength: aws.Int64(14)})
	require.NoError(t, err)
}

func TestRemoteSigningHostAndURI(t *testing.T) {
	t.Parallel()
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "virtual-storage.example", r.Host)
		assert.Equal(t, "/rewritten%2Fobject", r.URL.EscapedPath())
		assert.Equal(t, "a+b", r.URL.Query().Get("signature"))
		body, err := stdio.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.Equal(t, "payload", string(body))
		assert.Equal(t, []string{"first", "second"}, r.Header.Values("X-Signed"))
	}))
	defer storage.Close()
	signer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "virtual-signer.example", r.Host)
		var req remoteSignRequest
		if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&req)) {
			return
		}
		assert.Equal(t, "virtual-input.example", req.Headers.Get("Host"))
		assert.Empty(t, req.Headers.Get("Authorization"))
		assert.Empty(t, req.Headers.Get("X-Amz-Security-Token"))
		_ = json.NewEncoder(w).Encode(remoteSignResponse{URI: storage.URL + "/rewritten%2Fobject?signature=a%2Bb", Headers: http.Header{"Host": {"virtual-storage.example"}, "X-Signed": {"first", "second"}}})
	}))
	defer signer.Close()
	client := &remoteSigningClient{storage: storage.Client(), signer: signer.Client(), endpoint: signer.URL, region: "us-east-1", config: remotesigning.Config{Headers: http.Header{"host": {"virtual-signer.example"}}}}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPut, "https://unsigned.example/original", strings.NewReader("payload"))
	require.NoError(t, err)
	request.Host = "virtual-input.example"
	lowerAuth := strings.ToLower("Authorization")
	lowerToken := strings.ToLower("X-Amz-Security-Token")
	lowerHash := strings.ToLower("X-Amz-Content-Sha256")
	request.Header[lowerAuth] = []string{"local-auth"}
	request.Header[lowerToken] = []string{"local-token"}
	request.Header[lowerHash] = []string{"unsigned-hash"}
	resp, err := client.Do(request)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "https://unsigned.example/original", request.URL.String())
	require.Equal(t, []string{"local-auth"}, request.Header[lowerAuth])
}

type remoteSigningRoundTripFunc func(*http.Request) (*http.Response, error)

func (f remoteSigningRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestRemoteSigningRedactsTransportErrors(t *testing.T) {
	t.Parallel()
	cause := errors.New("transport failed")
	fail := &http.Client{Transport: remoteSigningRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, &url.Error{Op: "Post", URL: "https://nested.example?token=nested-secret", Err: cause}
	})}
	success := &http.Client{Transport: remoteSigningRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: stdio.NopCloser(strings.NewReader(`{"uri":"https://storage.example/object?signature=storage-secret","headers":{}}`))}, nil
	})}
	for _, signerFailure := range []bool{true, false} {
		t.Run(strconv.FormatBool(signerFailure), func(t *testing.T) {
			signer := success
			if signerFailure {
				signer = fail
			}
			client := &remoteSigningClient{storage: fail, signer: signer, endpoint: "https://signer.example/sign?token=signer-secret", region: "us-east-1"}
			request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://storage.example/object", nil)
			require.NoError(t, err)
			_, err = client.Do(request)
			require.ErrorIs(t, err, cause)
			require.NotContains(t, err.Error(), "signer-secret")
			require.NotContains(t, err.Error(), "storage-secret")
			require.NotContains(t, err.Error(), "nested-secret")
			var urlErr *url.Error
			require.ErrorAs(t, err, &urlErr)
		})
	}
}

type remoteSigningTestBody struct {
	stdio.Reader
	closed bool
}

func (b *remoteSigningTestBody) Close() error {
	b.closed = true

	return nil
}

func TestRemoteSigningRejectsStreamingPayload(t *testing.T) {
	t.Parallel()
	never := &http.Client{Transport: remoteSigningRoundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Error("streaming payload reached HTTP transport")

		return nil, errors.New("unexpected request")
	})}
	for _, header := range []http.Header{
		{"content-encoding": {"gzip", "aws-chunked"}},
		{"x-amz-content-sha256": {"UNSIGNED-PAYLOAD", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"}},
	} {
		body := &remoteSigningTestBody{Reader: strings.NewReader("data")}
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPut, "https://storage.example/object", body)
		require.NoError(t, err)
		request.Header = header
		client := &remoteSigningClient{storage: never, signer: never, endpoint: "https://signer.example/sign"}
		_, err = client.Do(request)
		require.ErrorContains(t, err, "does not support aws-chunked")
		require.True(t, body.closed)
	}
}
