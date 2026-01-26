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

package rest

import (
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

type mockTokenSource struct {
	token *oauth2.Token
	err   error
}

func (m *mockTokenSource) Token() (*oauth2.Token, error) {
	return m.token, m.err
}

func TestGoogleAuthManager_AuthHeader(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		mockSrc := &mockTokenSource{
			token: &oauth2.Token{
				AccessToken: "mock-google-token",
			},
		}
		manager := &GoogleAuthManager{
			tokenSource: mockSrc,
		}

		key, value, err := manager.AuthHeader()
		require.NoError(t, err)
		assert.Equal(t, "Authorization", key)
		assert.Equal(t, "Bearer mock-google-token", value)
	})

	t.Run("TokenSourceError", func(t *testing.T) {
		mockSrc := &mockTokenSource{
			err: errors.New("token source error"),
		}
		manager := &GoogleAuthManager{
			tokenSource: mockSrc,
		}

		_, _, err := manager.AuthHeader()
		require.Error(t, err)
		assert.Equal(t, "token source error", err.Error())
	})

	t.Run("Caching", func(t *testing.T) {
		mockSrc := &mockTokenSource{
			token: &oauth2.Token{
				AccessToken: "mock-token-1",
			},
		}
		manager := &GoogleAuthManager{
			tokenSource: mockSrc,
		}

		_, val, err := manager.AuthHeader()
		require.NoError(t, err)
		assert.Equal(t, "Bearer mock-token-1", val)

		mockSrc.token.AccessToken = "mock-token-2"
		_, val, err = manager.AuthHeader()
		require.NoError(t, err)
		assert.Equal(t, "Bearer mock-token-2", val)
	})
}

func TestGoogleAuthManager_NonExistentFile(t *testing.T) {
	manager := &GoogleAuthManager{
		CredentialsPath: "/nonexistent/path/to/creds.json",
	}

	_, _, err := manager.AuthHeader()
	require.Error(t, err)
	assert.ErrorContains(t, err, "no such file or directory")
}

func TestGoogleAuthManager_LoadWithValidFile(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "creds-*.json")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	creds := map[string]string{
		"type":         "service_account",
		"project_id":   "test-project",
		"private_key":  "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDE\n-----END PRIVATE KEY-----\n",
		"client_email": "test@test-project.iam.gserviceaccount.com",
		"token_uri":    "https://oauth2.googleapis.com/token",
	}
	require.NoError(t, json.NewEncoder(tmpFile).Encode(creds))
	require.NoError(t, tmpFile.Close())

	manager := &GoogleAuthManager{
		CredentialsPath: tmpFile.Name(),
		Scopes:          []string{"scope1"},
	}

	assert.Nil(t, manager.tokenSource)

	_, _, err = manager.AuthHeader()
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "no such file or directory")
}