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

func TestParseAWSConfigConnectTimeout(t *testing.T) {
	t.Parallel()

	cfg, err := ParseAWSConfig(context.Background(), map[string]string{
		io.S3ConnectTimeout: "5s",
	})
	require.NoError(t, err)

	client, ok := cfg.HTTPClient.(*awshttp.BuildableClient)
	require.True(t, ok)
	assert.Equal(t, 5*time.Second, client.GetDialer().Timeout)
}

func TestParseAWSConfigInvalidConnectTimeout(t *testing.T) {
	t.Parallel()

	_, err := ParseAWSConfig(context.Background(), map[string]string{
		io.S3ConnectTimeout: "5000",
	})
	require.ErrorContains(t, err, "invalid s3 connect timeout")
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
