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

	// This should fail with "container name is required" error
	_, err = createAzureBucket(ctx, parsedURL, props)

	assert.Error(t, err, "Expected error when container name is empty")
	assert.Contains(t, err.Error(), "container name is required",
		"Expected container name error but got: %v", err)
}
