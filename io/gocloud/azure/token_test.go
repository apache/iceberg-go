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

package azure

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/fake"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type tokenTestTransport func(*http.Request) (*http.Response, error)

func (f tokenTestTransport) Do(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestStaticTokenCredential(t *testing.T) {
	// Treat access tokens as opaque: this need not be a parseable JWT.
	credential := staticTokenCredential{token: "opaque-access-token"}
	for range 2 {
		before := time.Now().Add(time.Hour)
		token, err := credential.GetToken(t.Context(), policy.TokenRequestOptions{
			Scopes: []string{"https://storage.azure.com/.default"},
		})
		after := time.Now().Add(time.Hour)
		require.NoError(t, err)
		assert.Equal(t, "opaque-access-token", token.Token)
		assert.False(t, token.ExpiresOn.Before(before))
		assert.False(t, token.ExpiresOn.After(after))
	}
}

func TestAzureTokenAuthentication(t *testing.T) {
	const token = "opaque-access-token"
	tests := []struct {
		name            string
		props           map[string]string
		wantAuthPrefix  string
		wantSAS         string
		wantDefault     bool
		wantManaged     bool
		wantCreateError string
		rejectToken     bool
	}{
		{
			name:           "static token",
			props:          map[string]string{icebergio.ADLSToken: token},
			wantAuthPrefix: "Bearer " + token,
		},
		{
			name: "token precedes managed identity",
			props: map[string]string{
				icebergio.ADLSToken:                  token,
				icebergio.ADLSManagedIdentityEnabled: "true",
				icebergio.ADLSClientID:               "managed-identity-client-id",
			},
			wantAuthPrefix: "Bearer " + token,
		},
		{
			name: "unrelated SAS and connection string do not override token",
			props: map[string]string{
				icebergio.ADLSToken: token,
				icebergio.ADLSSasTokenPrefix + "other.dfs.core.windows.net": "sig=other",
				icebergio.ADLSConnectionStringPrefix + "other":              "invalid-connection-string",
			},
			wantAuthPrefix: "Bearer " + token,
		},
		{
			name: "shared key precedes token",
			props: map[string]string{
				icebergio.ADLSToken:                token,
				icebergio.ADLSSharedKeyAccountName: "testaccount",
				icebergio.ADLSSharedKeyAccountKey:  "YQ==",
			},
			wantAuthPrefix: "SharedKey testaccount:",
		},
		{
			name: "SAS precedes token",
			props: map[string]string{
				icebergio.ADLSToken: token,
				icebergio.ADLSSasTokenPrefix + "testaccount.dfs.core.windows.net": "sig=sas-signature",
			},
			wantSAS: "sas-signature",
		},
		{
			name: "connection string precedes token",
			props: map[string]string{
				icebergio.ADLSToken: token,
				icebergio.ADLSConnectionStringPrefix + "testaccount": "DefaultEndpointsProtocol=https;AccountName=connectionaccount;AccountKey=YQ==;EndpointSuffix=core.windows.net",
			},
			wantAuthPrefix: "SharedKey connectionaccount:",
		},
		{
			name:           "empty token uses default chain",
			props:          map[string]string{icebergio.ADLSToken: ""},
			wantAuthPrefix: "Bearer fake_token",
			wantDefault:    true,
		},
		{
			name: "empty token uses managed identity when enabled",
			props: map[string]string{
				icebergio.ADLSToken:                  "",
				icebergio.ADLSManagedIdentityEnabled: "true",
			},
			wantAuthPrefix: "Bearer fake_token",
			wantManaged:    true,
		},
		{
			name: "incomplete shared key does not fall back to token",
			props: map[string]string{
				icebergio.ADLSToken:                token,
				icebergio.ADLSSharedKeyAccountName: "testaccount",
			},
			wantCreateError: "shared-key requires both",
		},
		{
			name: "invalid connection string does not fall back to token",
			props: map[string]string{
				icebergio.ADLSToken: token,
				icebergio.ADLSConnectionStringPrefix + "testaccount": "invalid-connection-string",
			},
			wantCreateError: "failed container.NewClientFromConnectionString",
		},
		{
			name: "rejected token does not fall back to managed identity",
			props: map[string]string{
				icebergio.ADLSToken:                  token,
				icebergio.ADLSManagedIdentityEnabled: "true",
			},
			wantAuthPrefix: "Bearer " + token,
			rejectToken:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := url.Parse("abfss://container@testaccount.dfs.core.windows.net/path")
			require.NoError(t, err)
			defaultCalled, managedCalled := false, false
			factories := azureCredentialFactories{
				newDefaultCredential: func(*azidentity.DefaultAzureCredentialOptions) (azcore.TokenCredential, error) {
					defaultCalled = true

					return &fake.TokenCredential{}, nil
				},
				newManagedIdentity: func(*azidentity.ManagedIdentityCredentialOptions) (azcore.TokenCredential, error) {
					managedCalled = true

					return &fake.TokenCredential{}, nil
				},
			}
			requests := 0
			options := &container.ClientOptions{ClientOptions: azcore.ClientOptions{
				Retry: policy.RetryOptions{MaxRetries: -1},
				Transport: tokenTestTransport(func(req *http.Request) (*http.Response, error) {
					requests++
					auth := req.Header.Get("Authorization")
					if strings.HasPrefix(tt.wantAuthPrefix, "SharedKey ") {
						assert.True(t, strings.HasPrefix(auth, tt.wantAuthPrefix))
					} else {
						assert.Equal(t, tt.wantAuthPrefix, auth)
					}
					assert.Equal(t, tt.wantSAS, req.URL.Query().Get("sig"))
					assert.NotContains(t, req.URL.String(), token)
					status := http.StatusOK
					header := http.Header{}
					header.Set("Last-Modified", "Mon, 21 Sep 2026 00:00:00 GMT")
					header.Set("x-ms-creation-time", "Mon, 21 Sep 2026 00:00:00 GMT")
					if tt.rejectToken {
						status = http.StatusUnauthorized
						header.Set("x-ms-error-code", "AuthenticationFailed")
					}

					return &http.Response{
						StatusCode: status,
						Header:     header,
						Body:       io.NopCloser(strings.NewReader("")),
						Request:    req,
					}, nil
				}),
			}}
			bucket, err := createAzureBucketWithOptions(t.Context(), parsed, tt.props, factories, options)
			if tt.wantCreateError != "" {
				require.ErrorContains(t, err, tt.wantCreateError)
				assert.NotContains(t, err.Error(), token)
				assert.Zero(t, requests)
			} else {
				require.NoError(t, err)
				defer bucket.Close()
				exists, err := bucket.Exists(context.Background(), "file.parquet")
				if tt.rejectToken {
					require.Error(t, err)
					var responseErr *azcore.ResponseError
					require.ErrorAs(t, err, &responseErr)
					assert.Equal(t, http.StatusUnauthorized, responseErr.StatusCode)
					assert.NotContains(t, err.Error(), token)
				} else {
					require.NoError(t, err)
					assert.True(t, exists)
				}
				assert.Equal(t, 1, requests)
			}
			assert.Equal(t, tt.wantDefault, defaultCalled)
			assert.Equal(t, tt.wantManaged, managedCalled)
		})
	}
}
