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
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateAzureBucketDefaultCredentialCalled(t *testing.T) {
	ctx := context.Background()

	parsedURL, err := url.Parse("abfs://container@testaccount.dfs.core.windows.net/path")
	assert.NoError(t, err)

	props := map[string]string{}

	bucket, err := createAzureBucket(ctx, parsedURL, props)
	if err != nil {
		// If bucket creation fails, it should be with a DefaultAzureCredential error
		assert.Contains(t, err.Error(), "DefaultAzureCredential",
			"Expected DefaultAzureCredential error but got: %v", err)
		t.Logf("DefaultAzureCredential path was taken and failed at creation: %v", err)

		return
	}

	// If bucket creation succeeds, verify we can't actually use it without proper auth
	assert.NotNil(t, bucket, "Bucket should be created when DefaultAzureCredential client creation succeeds")

	// Test that actual operations fail with DefaultAzureCredential auth errors
	iter := bucket.List(nil)
	_, err = iter.Next(ctx)

	assert.Error(t, err, "Expected List to return an error when List is called")
	// This is expected - we should get an auth error when trying to actually use the bucket
	assert.Contains(t, err.Error(), "DefaultAzureCredential",
		"Expected DefaultAzureCredential error when listing objects but got: %v", err)
}

func TestCreateAzureBucketDefaultCredentialEmptyBucketName(t *testing.T) {
	ctx := context.Background()

	// Create URL with empty bucket name - no container specified in the URL
	parsedURL, err := url.Parse("abfs://testaccount.dfs.core.windows.net/path")
	assert.NoError(t, err)

	props := map[string]string{}

	// This should fail with "container name is required" error since no container is specified in User field
	_, err = createAzureBucket(ctx, parsedURL, props)

	assert.Error(t, err, "Expected error when container name is empty")
	assert.Contains(t, err.Error(), "container name is required",
		"Expected container name error but got: %v", err)
}

func TestCreateAzureBucketSharedKeyMissingAccountKey(t *testing.T) {
	ctx := context.Background()

	parsedURL, err := url.Parse("abfs://container@testaccount.dfs.core.windows.net/path")
	assert.NoError(t, err)

	props := map[string]string{
		"adls.auth.shared-key.account.name": "testaccount",
		// "adls.auth.shared-key.account.key" is intentionally missing
	}
	_, err = createAzureBucket(ctx, parsedURL, props)

	assert.Error(t, err, "Expected error when account key is missing")
	assert.Contains(t, err.Error(), "shared-key requires both",
		"Expected shared-key error but got: %v", err)
}

func TestNewAdlsLocationUriParsing(t *testing.T) {
	tests := []struct {
		uri               string
		expectedAccount   string
		expectedContainer string
		expectedHostname  string
		expectedPath      string
		shouldFail        bool
	}{
		{
			uri:               "abfs://container@account.dfs.core.windows.net/file.txt",
			expectedAccount:   "account",
			expectedContainer: "container",
			expectedHostname:  "account.dfs.core.windows.net",
			expectedPath:      "/file.txt",
			shouldFail:        false,
		},
		{
			uri:               "abfs://container@account.dfs.core.usgovcloudapi.net/file.txt",
			expectedAccount:   "account",
			expectedContainer: "container",
			expectedHostname:  "account.dfs.core.usgovcloudapi.net",
			expectedPath:      "/file.txt",
			shouldFail:        false,
		},
		{
			uri:               "wasb://container@account.blob.core.windows.net/file.txt",
			expectedAccount:   "account",
			expectedContainer: "container",
			expectedHostname:  "account.blob.core.windows.net",
			expectedPath:      "/file.txt",
			shouldFail:        false,
		},
		{
			uri:        "abfs://account.dfs.core.windows.net/path",
			shouldFail: true,
		},
	}

	for _, test := range tests {
		t.Run(test.uri, func(t *testing.T) {
			parsedURL, err := url.Parse(test.uri)
			assert.NoError(t, err)

			location, err := newAdlsLocation(parsedURL)

			if test.shouldFail {
				assert.Error(t, err, "Expected error for URI: %s", test.uri)
				assert.Nil(t, location)
			} else {
				assert.NoError(t, err, "Unexpected error for URI: %s", test.uri)
				assert.NotNil(t, location)
				assert.Equal(t, test.expectedAccount, location.accountName, "Account name mismatch for URI: %s", test.uri)
				assert.Equal(t, test.expectedContainer, location.containerName, "Container name mismatch for URI: %s", test.uri)
				assert.Equal(t, test.expectedHostname, location.hostname, "Hostname mismatch for URI: %s", test.uri)
				assert.Equal(t, test.expectedPath, location.path, "Path mismatch for URI: %s", test.uri)
			}
		})
	}
}

func TestAdlsKeyExtractor(t *testing.T) {
	extractor := adlsKeyExtractor()

	tests := []struct {
		name        string
		input       string
		expectedKey string
		shouldError bool
	}{
		{
			name:        "abfs valid URI",
			input:       "abfs://container@account.dfs.core.windows.net/path/to/file.parquet",
			expectedKey: "path/to/file.parquet",
		},
		{
			name:        "abfss valid URI",
			input:       "abfss://container@account.dfs.core.windows.net/path/to/file.parquet",
			expectedKey: "path/to/file.parquet",
		},
		{
			name:        "wasb valid URI",
			input:       "wasb://container@account.blob.core.windows.net/path/to/file.parquet",
			expectedKey: "path/to/file.parquet",
		},
		{
			name:        "wasbs valid URI",
			input:       "wasbs://container@account.blob.core.windows.net/path/to/file.parquet",
			expectedKey: "path/to/file.parquet",
		},
		{
			name:        "URI with no path",
			input:       "abfs://container@account.dfs.core.windows.net",
			shouldError: true,
		},
		{
			name:        "URI with empty path",
			input:       "abfs://container@account.dfs.core.windows.net/",
			shouldError: true,
		},
		{
			name:        "invalid ADLS location - invalid scheme",
			input:       "s3://bucket/path/to/file.parquet",
			shouldError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key, err := extractor(test.input)

			if test.shouldError {
				assert.Error(t, err, "Expected error for input: %s", test.input)
			} else {
				assert.NoError(t, err, "Unexpected error for input: %s", test.input)
				assert.Equal(t, test.expectedKey, key, "Key mismatch for input: %s", test.input)
			}
		})
	}
}
