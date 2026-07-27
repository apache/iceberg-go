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

package gocloud

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go/io"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithymiddleware "github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseAWSConfigRemoteSigningEnabled(t *testing.T) {
	t.Parallel()

	t.Run("signer uri present with remote signing explicitly enabled", func(t *testing.T) {
		t.Parallel()

		_, err := ParseAWSConfig(context.Background(), map[string]string{
			io.S3SignerURI:            "https://signer.example.com",
			io.S3RemoteSigningEnabled: "true",
		})
		require.ErrorContains(t, err, "remote S3 request signing is not supported")
	})

	t.Run("signer uri present with remote signing explicitly disabled", func(t *testing.T) {
		t.Parallel()

		_, err := ParseAWSConfig(context.Background(), map[string]string{
			io.S3SignerURI:            "https://signer.example.com",
			io.S3RemoteSigningEnabled: "false",
			io.S3Region:               "us-east-1",
		})
		require.NoError(t, err)
	})

	t.Run("signer uri present without remote signing property", func(t *testing.T) {
		t.Parallel()

		_, err := ParseAWSConfig(context.Background(), map[string]string{
			io.S3SignerURI: "https://signer.example.com",
			io.S3Region:    "us-west-2",
		})
		require.NoError(t, err)
	})

	t.Run("remote signing enabled without signer uri", func(t *testing.T) {
		t.Parallel()

		_, err := ParseAWSConfig(context.Background(), map[string]string{
			io.S3RemoteSigningEnabled: "true",
		})
		require.ErrorContains(t, err, "remote S3 request signing is not supported")
	})

	t.Run("no signer properties at all", func(t *testing.T) {
		t.Parallel()

		cfg, err := ParseAWSConfig(context.Background(), map[string]string{
			io.S3Region: "eu-west-1",
		})
		require.NoError(t, err)
		assert.Equal(t, "eu-west-1", cfg.Region)
	})
}

func TestParseAWSConfigInvalidConnectTimeout(t *testing.T) {
	t.Parallel()

	_, err := ParseAWSConfig(context.Background(), map[string]string{
		io.S3Region:         "us-east-1",
		io.S3ConnectTimeout: "not-a-duration",
	})
	require.ErrorContains(t, err, "invalid s3.connect-timeout")
}

func TestParseAWSConfigConnectTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		timeout string
		want    time.Duration
	}{
		{
			name:    "integer seconds",
			timeout: "60",
			want:    60 * time.Second,
		},
		{
			name:    "decimal seconds",
			timeout: "60.0",
			want:    60 * time.Second,
		},
		{
			name:    "fractional seconds",
			timeout: "1.5",
			want:    1500 * time.Millisecond,
		},
		{
			name:    "go duration",
			timeout: "5s",
			want:    5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := ParseAWSConfig(context.Background(), map[string]string{
				io.S3Region:         "us-east-1",
				io.S3ConnectTimeout: tt.timeout,
			})
			require.NoError(t, err)

			client, ok := cfg.HTTPClient.(*awshttp.BuildableClient)
			require.True(t, ok)
			assert.Equal(t, tt.want, client.GetDialer().Timeout)
			assertS3TransportTuning(t, client.GetTransport())
		})
	}
}

func TestParseAWSConfigConnectTimeoutRejectsNonPositiveDurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		timeout string
	}{
		{
			name:    "zero",
			timeout: "0",
		},
		{
			name:    "negative",
			timeout: "-5s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseAWSConfig(context.Background(), map[string]string{
				io.S3Region:         "us-east-1",
				io.S3ConnectTimeout: tt.timeout,
			})
			require.ErrorContains(t, err, "must be a positive duration")
		})
	}
}

func TestParseAWSConfigProxyUsesTunedTransport(t *testing.T) {
	t.Parallel()

	cfg, err := ParseAWSConfig(context.Background(), map[string]string{
		io.S3Region:   "us-east-1",
		io.S3ProxyURI: "http://proxy.example.com:8080",
	})
	require.NoError(t, err)

	client, ok := cfg.HTTPClient.(*awshttp.BuildableClient)
	require.True(t, ok)
	assertS3TransportTuning(t, client.GetTransport())
	assertProxyURL(t, client.GetTransport(), "http://proxy.example.com:8080")
}

func TestParseAWSConfigProxyAndConnectTimeout(t *testing.T) {
	t.Parallel()

	cfg, err := ParseAWSConfig(context.Background(), map[string]string{
		io.S3Region:         "us-east-1",
		io.S3ProxyURI:       "http://proxy.example.com:8080",
		io.S3ConnectTimeout: "5s",
	})
	require.NoError(t, err)

	client, ok := cfg.HTTPClient.(*awshttp.BuildableClient)
	require.True(t, ok)
	assert.Equal(t, 5*time.Second, client.GetDialer().Timeout)
	assertS3TransportTuning(t, client.GetTransport())
	assertProxyURL(t, client.GetTransport(), "http://proxy.example.com:8080")
}

func assertProxyURL(t *testing.T, transport *http.Transport, want string) {
	t.Helper()

	require.NotNil(t, transport)
	proxyFunc := transport.Proxy
	require.NotNil(t, proxyFunc)

	proxyURL, err := proxyFunc(&http.Request{
		URL: &url.URL{Scheme: "https", Host: "bucket.s3.amazonaws.com"},
	})
	require.NoError(t, err)
	require.NotNil(t, proxyURL)
	assert.Equal(t, want, proxyURL.String())
}

func assertS3TransportTuning(t *testing.T, transport *http.Transport) {
	t.Helper()

	require.NotNil(t, transport)
	assert.Equal(t, 256, transport.MaxIdleConns)
	assert.Equal(t, 256, transport.MaxIdleConnsPerHost)
	assert.Equal(t, 2048, transport.MaxConnsPerHost)
	assert.Equal(t, 90*time.Second, transport.IdleConnTimeout)
}

func TestResolveUsePathStyle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		endpoint string
		props    map[string]string
		want     bool
	}{
		{
			name:     "no endpoint defaults to virtual-hosted style",
			endpoint: "",
			props:    nil,
			want:     false,
		},
		{
			name:     "custom endpoint defaults to path-style",
			endpoint: "http://localhost:9000",
			props:    nil,
			want:     true,
		},
		{
			name:     "force virtual-addressing overrides custom endpoint",
			endpoint: "http://localhost:9000",
			props: map[string]string{
				io.S3ForceVirtualAddressing: "true",
			},
			want: false,
		},
		{
			name:     "force virtual-addressing=false with no endpoint",
			endpoint: "",
			props: map[string]string{
				io.S3ForceVirtualAddressing: "false",
			},
			want: true,
		},
		{
			name:     "force virtual-addressing=true with no endpoint",
			endpoint: "",
			props: map[string]string{
				io.S3ForceVirtualAddressing: "true",
			},
			want: false,
		},
		{
			name:     "invalid force-virtual-addressing value ignored, custom endpoint",
			endpoint: "http://localhost:9000",
			props: map[string]string{
				io.S3ForceVirtualAddressing: "not-a-bool",
			},
			want: true,
		},
		{
			name:     "invalid force-virtual-addressing value ignored, no endpoint",
			endpoint: "",
			props: map[string]string{
				io.S3ForceVirtualAddressing: "not-a-bool",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := resolveUsePathStyle(tt.endpoint, tt.props)
			assert.Equal(t, tt.want, got)
		})
	}
}

func compatModeS3Options(endpoint string) func(*s3.Options) {
	return func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.APIOptions = append(o.APIOptions, stripS3InputChecksumAlgorithm, stripGCSIncompatibleSignedHeaders)
		o.UsePathStyle = true
	}
}

func assertNoAwsChunkedWriteHeaders(t *testing.T, captured http.Header, op string) {
	t.Helper()

	require.NotNil(t, captured)

	for header := range captured {
		h := strings.ToLower(header)
		assert.Falsef(t, strings.HasPrefix(h, "x-amz-checksum-"),
			"%s must not send checksum headers against custom endpoints, got %s=%q",
			op, header, captured.Get(header))
		assert.NotEqualf(t, "x-amz-trailer", h,
			"%s must not declare a SigV4 trailer against custom endpoints, got %s=%q",
			op, header, captured.Get(header))
		assert.NotEqualf(t, "x-amz-sdk-checksum-algorithm", h,
			"%s must not declare an SDK checksum algorithm against custom endpoints, got %s=%q",
			op, header, captured.Get(header))
	}

	if ce := captured.Get("Content-Encoding"); ce != "" {
		assert.NotContainsf(t, ce, "aws-chunked",
			"%s must not use aws-chunked transfer encoding against custom endpoints, got Content-Encoding=%q", op, ce)
	}
	if sha := captured.Get("X-Amz-Content-Sha256"); sha != "" {
		assert.NotContainsf(t, sha, "STREAMING-",
			"%s must use a precomputed payload hash against custom endpoints, got X-Amz-Content-Sha256=%q", op, sha)
	}

	for _, h := range []string{"Amz-Sdk-Invocation-Id", "Amz-Sdk-Request"} {
		assert.Emptyf(t, captured.Get(h),
			"%s must not include SDK-internal header %s on the wire, got %q", op, h, captured.Get(h))
	}

	auth := captured.Get("Authorization")
	require.NotEmpty(t, auth, "Authorization header must be set")
	for _, h := range []string{"amz-sdk-invocation-id", "amz-sdk-request", "accept-encoding"} {
		assert.NotContainsf(t, auth, h,
			"SignedHeaders in Authorization must not list GCS-incompatible header %q, got Authorization=%q", h, auth)
	}
}

func TestCompatModePutObjectNoAwsChunked(t *testing.T) {
	t.Parallel()

	var captured http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	cfg := aws.Config{
		Region:      "auto",
		Credentials: credentials.NewStaticCredentialsProvider("AKIA-TEST", "secret-test", ""),
		HTTPClient:  srv.Client(),
	}
	client := s3.NewFromConfig(cfg, compatModeS3Options(srv.URL))

	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("test-bucket"),
		Key:    aws.String("test-key"),
		Body:   strings.NewReader("hello"),
	})
	require.NoError(t, err)

	assertNoAwsChunkedWriteHeaders(t, captured, "PutObject")
}

func TestCompatModeTransferManagerNoAwsChunked(t *testing.T) {
	t.Parallel()

	var captured http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	cfg := aws.Config{
		Region:      "auto",
		Credentials: credentials.NewStaticCredentialsProvider("AKIA-TEST", "secret-test", ""),
		HTTPClient:  srv.Client(),
	}
	client := s3.NewFromConfig(cfg, compatModeS3Options(srv.URL))

	tm := transfermanager.New(client)
	_, err := tm.UploadObject(context.Background(), &transfermanager.UploadObjectInput{
		Bucket:      aws.String("test-bucket"),
		Key:         aws.String("test-key"),
		Body:        strings.NewReader("hello"),
		ContentType: aws.String("application/octet-stream"),
	})
	require.NoError(t, err)

	assertNoAwsChunkedWriteHeaders(t, captured, "transfer-manager PutObject")
}

func TestS3CompatModeEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		props map[string]string
		want  bool
	}{
		{name: "absent defaults to off", props: nil, want: false},
		{name: "explicitly enabled", props: map[string]string{io.S3CompatMode: "true"}, want: true},
		{name: "explicitly disabled", props: map[string]string{io.S3CompatMode: "false"}, want: false},
		{name: "invalid value treated as off", props: map[string]string{io.S3CompatMode: "not-a-bool"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, s3CompatModeEnabled(tt.props))
		})
	}
}

func TestStripS3InputChecksumAlgorithmMiddleware(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input any
		check func(t *testing.T, in any)
	}{
		{
			name:  "PutObjectInput",
			input: &s3.PutObjectInput{ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32},
			check: func(t *testing.T, in any) {
				assert.Equal(t, s3types.ChecksumAlgorithm(""), in.(*s3.PutObjectInput).ChecksumAlgorithm)
			},
		},
		{
			name:  "UploadPartInput",
			input: &s3.UploadPartInput{ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32},
			check: func(t *testing.T, in any) {
				assert.Equal(t, s3types.ChecksumAlgorithm(""), in.(*s3.UploadPartInput).ChecksumAlgorithm)
			},
		},
		{
			name:  "CreateMultipartUploadInput",
			input: &s3.CreateMultipartUploadInput{ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32},
			check: func(t *testing.T, in any) {
				assert.Equal(t, s3types.ChecksumAlgorithm(""), in.(*s3.CreateMultipartUploadInput).ChecksumAlgorithm)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := smithymiddleware.NewStack("test", smithyhttp.NewStackRequest)

			var sawAlgorithm s3types.ChecksumAlgorithm
			anchor := smithymiddleware.InitializeMiddlewareFunc(
				"AWSChecksum:SetupInputContext",
				func(ctx context.Context, in smithymiddleware.InitializeInput, next smithymiddleware.InitializeHandler) (smithymiddleware.InitializeOutput, smithymiddleware.Metadata, error) {
					switch v := in.Parameters.(type) {
					case *s3.PutObjectInput:
						sawAlgorithm = v.ChecksumAlgorithm
					case *s3.UploadPartInput:
						sawAlgorithm = v.ChecksumAlgorithm
					case *s3.CreateMultipartUploadInput:
						sawAlgorithm = v.ChecksumAlgorithm
					}

					return next.HandleInitialize(ctx, in)
				},
			)
			require.NoError(t, s.Initialize.Add(anchor, smithymiddleware.After))
			require.NoError(t, stripS3InputChecksumAlgorithm(s))

			handler := smithymiddleware.DecorateHandler(
				smithymiddleware.HandlerFunc(func(context.Context, any) (any, smithymiddleware.Metadata, error) {
					return nil, smithymiddleware.Metadata{}, nil
				}),
				s,
			)
			_, _, err := handler.Handle(context.Background(), tc.input)
			require.NoError(t, err)
			tc.check(t, tc.input)
			assert.Equal(t, s3types.ChecksumAlgorithm(""), sawAlgorithm,
				"strip middleware must run before AWSChecksum:SetupInputContext so the SDK observes an empty algorithm")
		})
	}
}
