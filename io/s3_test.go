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

package io

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseAWSConfigRemoteSigningEnabled(t *testing.T) {
	t.Parallel()

	t.Run("signer uri present with remote signing explicitly enabled", func(t *testing.T) {
		t.Parallel()

		_, err := ParseAWSConfig(t.Context(), map[string]string{
			S3SignerUri:            "https://signer.example.com",
			S3RemoteSigningEnabled: "true",
		})
		require.ErrorContains(t, err, "remote S3 request signing (s3.remote-signing-enabled=true) is not supported")
	})

	t.Run("signer uri present with remote signing explicitly disabled", func(t *testing.T) {
		t.Parallel()

		_, err := ParseAWSConfig(t.Context(), map[string]string{
			S3SignerUri:            "https://signer.example.com",
			S3RemoteSigningEnabled: "false",
			S3Region:               "us-east-1",
		})
		require.NoError(t, err)
	})

	t.Run("signer uri present without remote signing property", func(t *testing.T) {
		t.Parallel()

		_, err := ParseAWSConfig(t.Context(), map[string]string{
			S3SignerUri: "https://signer.example.com",
			S3Region:    "us-west-2",
		})
		require.NoError(t, err)
	})

	t.Run("remote signing enabled without signer uri", func(t *testing.T) {
		t.Parallel()

		_, err := ParseAWSConfig(t.Context(), map[string]string{
			S3RemoteSigningEnabled: "true",
		})
		require.ErrorContains(t, err, "remote S3 request signing (s3.remote-signing-enabled=true) is not supported")
	})

	t.Run("no signer properties at all", func(t *testing.T) {
		t.Parallel()

		cfg, err := ParseAWSConfig(t.Context(), map[string]string{
			S3Region: "eu-west-1",
		})
		require.NoError(t, err)
		assert.Equal(t, "eu-west-1", cfg.Region)
	})
}

func TestParseAWSConfigUnsupportedProperty(t *testing.T) {
	t.Parallel()

	_, err := ParseAWSConfig(t.Context(), map[string]string{
		S3ConnectTimeout: "5000",
	})
	require.ErrorContains(t, err, "unsupported S3 property")
}
