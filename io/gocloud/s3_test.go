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
	"net/url"
	"testing"
	"time"

	"github.com/apache/iceberg-go/io"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
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
