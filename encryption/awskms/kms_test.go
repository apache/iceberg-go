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

package awskms

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/iceberg-go/encryption"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	cfg := aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RetryMaxAttempts: 1}

	return NewClient(cfg, func(options *kms.Options) { options.BaseEndpoint = aws.String(server.URL) })
}

func TestClientRequestsAndResponses(t *testing.T) {
	requests := make(chan map[string]any, 3)
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.Header.Get("Authorization"), "us-east-1/kms/aws4_request")
		var request map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		request["target"] = r.Header.Get("X-Amz-Target")
		requests <- request
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		switch r.Header.Get("X-Amz-Target") {
		case "TrentService.Encrypt":
			json.NewEncoder(w).Encode(map[string]any{"CiphertextBlob": []byte("wrapped")})
		case "TrentService.Decrypt":
			json.NewEncoder(w).Encode(map[string]any{"Plaintext": []byte("plain")})
		case "TrentService.GenerateDataKey":
			json.NewEncoder(w).Encode(map[string]any{"Plaintext": make([]byte, int(request["NumberOfBytes"].(float64))), "CiphertextBlob": []byte("generated")})
		default:
			t.Errorf("unexpected target %q", r.Header.Get("X-Amz-Target"))
		}
	})
	wrapped, err := client.WrapKey(t.Context(), "alias/table", []byte("plain"))
	require.NoError(t, err)
	require.Equal(t, []byte("wrapped"), wrapped)
	request := <-requests
	require.Equal(t, "alias/table", request["KeyId"])
	require.Equal(t, "SYMMETRIC_DEFAULT", request["EncryptionAlgorithm"])
	require.Equal(t, "cGxhaW4=", request["Plaintext"])
	plain, err := client.UnwrapKey(t.Context(), "alias/table", wrapped)
	require.NoError(t, err)
	require.Equal(t, []byte("plain"), plain)
	request = <-requests
	require.Equal(t, "alias/table", request["KeyId"])
	require.Equal(t, "SYMMETRIC_DEFAULT", request["EncryptionAlgorithm"])
	require.Equal(t, "d3JhcHBlZA==", request["CiphertextBlob"])
	require.True(t, client.SupportsKeyGeneration())
	plain, wrapped, err = client.GenerateKey(t.Context(), "alias/table", 24)
	require.NoError(t, err)
	require.Len(t, plain, 24)
	require.Equal(t, []byte("generated"), wrapped)
	request = <-requests
	require.Equal(t, float64(24), request["NumberOfBytes"])
	require.NotContains(t, request, "KeySpec")
	require.Equal(t, "alias/table", request["KeyId"])
}

func TestClientInputValidation(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { t.Error("invalid request reached KMS") })
	_, err := client.WrapKey(t.Context(), "", []byte("key"))
	require.ErrorIs(t, err, encryption.ErrUnknownKeyID)
	for _, length := range []int{0, 4097} {
		_, err := client.WrapKey(t.Context(), "key", make([]byte, length))
		require.ErrorIs(t, err, encryption.ErrInvalidKeyLength)
	}
	_, err = client.UnwrapKey(t.Context(), "", []byte("key"))
	require.ErrorIs(t, err, encryption.ErrUnknownKeyID)
	_, err = client.UnwrapKey(t.Context(), "key", nil)
	require.ErrorIs(t, err, encryption.ErrCiphertextTooShort)
	_, _, err = client.GenerateKey(t.Context(), "", 16)
	require.ErrorIs(t, err, encryption.ErrUnknownKeyID)
	for _, length := range []int{-1, 0, 1025, int(^uint(0) >> 1)} {
		plain, wrapped, err := client.GenerateKey(t.Context(), "key", length)
		require.ErrorIs(t, err, encryption.ErrInvalidKeyLength)
		require.Nil(t, plain)
		require.Nil(t, wrapped)
	}
}

func TestClientGenerationByteLengths(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var request struct{ NumberOfBytes int }
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		json.NewEncoder(w).Encode(map[string]any{"Plaintext": make([]byte, request.NumberOfBytes), "CiphertextBlob": []byte("wrapped")})
	})
	for _, length := range []int{1, 16, 24, 32, 1024} {
		plain, wrapped, err := client.GenerateKey(t.Context(), "key", length)
		require.NoError(t, err)
		require.Len(t, plain, length)
		require.NotEmpty(t, wrapped)
	}
}

func TestClientPreservesServiceErrors(t *testing.T) {
	for _, code := range []string{"NotFoundException", "DisabledException", "AccessDeniedException", "InvalidCiphertextException", "IncorrectKeyException"} {
		t.Run(code, func(t *testing.T) {
			client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/x-amz-json-1.1")
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]string{"__type": code, "message": "rejected"})
			})
			check := func(err error) {
				t.Helper()
				var apiError smithy.APIError
				require.ErrorAs(t, err, &apiError)
				require.Equal(t, code, apiError.ErrorCode())
				require.NotContains(t, err.Error(), "secret-plaintext")
			}
			plain, err := client.WrapKey(t.Context(), "key", []byte("secret-plaintext"))
			check(err)
			require.Nil(t, plain)
			plain, err = client.UnwrapKey(t.Context(), "key", []byte("ciphertext"))
			check(err)
			require.Nil(t, plain)
			plain, wrapped, err := client.GenerateKey(t.Context(), "key", 32)
			check(err)
			require.Nil(t, plain)
			require.Nil(t, wrapped)
		})
	}
}

func TestClientRejectsIncompleteOutput(t *testing.T) {
	for _, response := range []string{`{}`, `{"Plaintext":"AA=="}`, `{"CiphertextBlob":"AA=="}`, `{"Plaintext":"AA==","CiphertextBlob":"AA=="}`} {
		client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(response)) })
		plain, wrapped, err := client.GenerateKey(t.Context(), "key", 32)
		require.Error(t, err)
		require.Nil(t, plain)
		require.Nil(t, wrapped)
	}
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{}`)) })
	_, err := client.WrapKey(t.Context(), "key", []byte("key"))
	require.Error(t, err)
	_, err = client.UnwrapKey(t.Context(), "key", []byte("wrapped"))
	require.Error(t, err)
}

func TestClientCancellation(t *testing.T) {
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { t.Error("canceled request reached KMS") })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := client.WrapKey(ctx, "key", []byte("key"))
	require.ErrorIs(t, err, context.Canceled)
	_, err = client.UnwrapKey(ctx, "key", []byte("wrapped"))
	require.ErrorIs(t, err, context.Canceled)
	plain, wrapped, err := client.GenerateKey(ctx, "key", 32)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, plain)
	require.Nil(t, wrapped)
}

func TestRegistryUsesContextConfig(t *testing.T) {
	supplied := aws.Config{Region: "us-west-2", Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}
	loaded, err := encryption.LoadKeyManagementClient(utils.WithAWSConfig(t.Context(), &supplied), map[string]string{
		encryption.KMSTypeKey: "aws", "client.region": "eu-west-1",
	})
	require.NoError(t, err)
	client, ok := loaded.(*Client)
	require.True(t, ok)
	require.Equal(t, "eu-west-1", client.client.(*kms.Client).Options().Region)
	require.Equal(t, "us-west-2", supplied.Region)
	require.Contains(t, strings.Join(encryption.GetRegisteredKMSNames(), ","), "aws")
}
