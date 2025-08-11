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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoteSigningTransport(t *testing.T) {
	// Create a mock signer server
	signerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var req RemoteSigningRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Return mock signed headers
		response := RemoteSigningResponse{
			Headers: map[string][]string{
				"Authorization": {"AWS4-HMAC-SHA256 Credential=test/20231201/us-east-1/s3/aws4_request"},
				"X-Amz-Date":    {"20231201T120000Z"},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer signerServer.Close()

	// Create a mock S3 server
	s3Server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check that the request has the signed headers
		assert.Contains(t, r.Header.Get("Authorization"), "AWS4-HMAC-SHA256")
		assert.NotEmpty(t, r.Header.Get("X-Amz-Date"))

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success"))
	}))
	defer s3Server.Close()

	// Create the remote signing transport
	baseTransport := &http.Transport{}
	transport := NewRemoteSigningTransport(baseTransport, signerServer.URL, "", "us-east-1", "", "")

	// Create a test request to the mock S3 server
	req, err := http.NewRequest("GET", s3Server.URL+"/bucket/key", nil)
	require.NoError(t, err)

	// Make the request through the remote signing transport
	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestParseAWSConfigWithRemoteSigner(t *testing.T) {
	// Create a mock signer server
	signerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := RemoteSigningResponse{
			Headers: map[string][]string{
				"Authorization": {"AWS4-HMAC-SHA256 Credential=test/20231201/us-east-1/s3/aws4_request"},
				"X-Amz-Date":    {"20231201T120000Z"},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer signerServer.Close()

	props := map[string]string{
		S3Region:    "us-east-1",
		S3SignerUri: signerServer.URL,
	}

	cfg, err := ParseAWSConfig(context.Background(), props)
	require.NoError(t, err)
	assert.NotNil(t, cfg)
	assert.Equal(t, "us-east-1", cfg.Region)
}

func TestParseAWSConfigWithoutRemoteSigner(t *testing.T) {
	props := map[string]string{
		S3Region:          "us-west-2",
		S3AccessKeyID:     "test-key",
		S3SecretAccessKey: "test-secret",
	}

	cfg, err := ParseAWSConfig(context.Background(), props)
	require.NoError(t, err)
	assert.NotNil(t, cfg)
	assert.Equal(t, "us-west-2", cfg.Region)
}

func TestRemoteSigningTransportIsS3Request(t *testing.T) {
	transport := &RemoteSigningTransport{}

	tests := []struct {
		url      string
		expected bool
	}{
		{"https://s3.amazonaws.com/bucket/key", true},
		{"https://bucket.s3.amazonaws.com/key", true},
		{"https://s3.us-east-1.amazonaws.com/bucket/key", true},
		{"https://custom-endpoint.com/bucket/key", true}, // We allow all when remote signer is configured
		{"https://example.com/path", true},               // We allow all when remote signer is configured
	}

	for _, test := range tests {
		req, err := http.NewRequest("GET", test.url, nil)
		require.NoError(t, err)

		result := transport.isS3Request(req)
		assert.Equal(t, test.expected, result, "URL: %s", test.url)
	}
}

func TestRemoteSigningTransport403Error(t *testing.T) {
	// Create a mock signer server that returns 403
	signerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`{"error": "insufficient permissions", "details": "signer service lacks IAM permissions for this bucket"}`))
	}))
	defer signerServer.Close()

	// Create the remote signing transport
	baseTransport := &http.Transport{}
	transport := NewRemoteSigningTransport(baseTransport, signerServer.URL, "", "us-east-1", "", "")

	// Create a test request
	req, err := http.NewRequest("PUT", "https://example.s3.amazonaws.com/bucket/key", nil)
	require.NoError(t, err)

	// Make the request through the remote signing transport
	_, err = transport.RoundTrip(req)
	require.Error(t, err)

	// Verify the error contains detailed information
	assert.Contains(t, err.Error(), "remote signer authorization denied (403)")
	assert.Contains(t, err.Error(), signerServer.URL)
	assert.Contains(t, err.Error(), "insufficient permissions")
	assert.Contains(t, err.Error(), "Check that the signer service has proper AWS credentials")
	assert.Contains(t, err.Error(), "Request was:")
}

func TestRemoteSigningTransport404Error(t *testing.T) {
	// Create a mock signer server that returns 404
	signerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error": "endpoint not found"}`))
	}))
	defer signerServer.Close()

	// Create the remote signing transport with a wrong endpoint
	baseTransport := &http.Transport{}
	wrongURL := signerServer.URL + "/wrong-path"
	transport := NewRemoteSigningTransport(baseTransport, wrongURL, "", "us-east-1", "", "")

	// Create a test request
	req, err := http.NewRequest("GET", "https://example.s3.amazonaws.com/bucket/key", nil)
	require.NoError(t, err)

	// Make the request through the remote signing transport
	_, err = transport.RoundTrip(req)
	require.Error(t, err)

	// Verify the error contains detailed information
	assert.Contains(t, err.Error(), "remote signer endpoint not found (404)")
	assert.Contains(t, err.Error(), wrongURL)
	assert.Contains(t, err.Error(), "Check the signer URI configuration")
}

func TestRemoteSigningTransportWithAuth(t *testing.T) {
	expectedToken := "test-auth-token-12345"

	// Create a mock signer server that validates auth
	signerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check auth header
		authHeader := r.Header.Get("Authorization")
		if authHeader != "Bearer "+expectedToken {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error": "invalid or missing auth token"}`))
			return
		}

		// Return signed headers if auth is valid
		response := RemoteSigningResponse{
			Headers: map[string][]string{
				"Authorization": {"AWS4-HMAC-SHA256 Credential=test/20231201/us-east-1/s3/aws4_request"},
				"X-Amz-Date":    {"20231201T120000Z"},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer signerServer.Close()

	// Test with valid auth token
	t.Run("ValidAuthToken", func(t *testing.T) {
		baseTransport := &http.Transport{}
		transport := NewRemoteSigningTransport(baseTransport, signerServer.URL, "", "us-east-1", expectedToken, "")

		req, err := http.NewRequest("GET", "https://example.s3.amazonaws.com/bucket/key", nil)
		require.NoError(t, err)

		// This should succeed
		resp, err := transport.getRemoteSignature(req.Context(), req.Method, req.URL.String(), transport.extractHeaders(req))
		require.NoError(t, err)
		assert.NotEmpty(t, resp["Authorization"])
	})

	// Test without auth token
	t.Run("MissingAuthToken", func(t *testing.T) {
		baseTransport := &http.Transport{}
		transport := NewRemoteSigningTransport(baseTransport, signerServer.URL, "", "us-east-1", "", "")

		req, err := http.NewRequest("GET", "https://example.s3.amazonaws.com/bucket/key", nil)
		require.NoError(t, err)

		// This should fail with 401
		_, err = transport.getRemoteSignature(req.Context(), req.Method, req.URL.String(), transport.extractHeaders(req))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "remote signer authentication failed (401)")
	})
}

func TestParseAWSConfigWithRemoteSignerAuth(t *testing.T) {
	// Create a mock signer server that requires auth
	signerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check auth header
		if r.Header.Get("Authorization") != "Bearer my-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		response := RemoteSigningResponse{
			Headers: map[string][]string{
				"Authorization": {"AWS4-HMAC-SHA256 Credential=test/20231201/us-east-1/s3/aws4_request"},
				"X-Amz-Date":    {"20231201T120000Z"},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}))
	defer signerServer.Close()

	props := map[string]string{
		S3Region:          "us-east-1",
		S3SignerUri:       signerServer.URL,
		S3SignerAuthToken: "my-token",
	}

	cfg, err := ParseAWSConfig(context.Background(), props)
	require.NoError(t, err)
	assert.NotNil(t, cfg)
	assert.Equal(t, "us-east-1", cfg.Region)
}
