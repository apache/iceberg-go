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
	"errors"
	"fmt"
	"net/url"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
)

// Constants for Azure configuration options
const (
	ADLS_SAS_TOKEN_PREFIX         = "adls.sas-token."
	ADLS_CONNECTION_STRING_PREFIX = "adls.connection-string."
	ADLS_READ_BLOCK_SIZE          = "adls.read.block-size-bytes"
	ADLS_WRITE_BLOCK_SIZE         = "adls.write.block-size-bytes"
	ADLS_SHARED_KEY_ACCOUNT_NAME  = "adls.auth.shared-key.account.name"
	ADLS_SHARED_KEY_ACCOUNT_KEY   = "adls.auth.shared-key.account.key"
)

// Construct a Azure bucket from a URL
func createAzureBucket(ctx context.Context, parsed *url.URL, props map[string]string) (*blob.Bucket, error) {
	adlsSasTokens := propertiesWithPrefix(props, ADLS_SAS_TOKEN_PREFIX)
	adlsConnectionStrings := propertiesWithPrefix(props, ADLS_CONNECTION_STRING_PREFIX)

	// Construct the client
	accountName := props[ADLS_SHARED_KEY_ACCOUNT_NAME]
	var client *container.Client

	if accountName != "" {
		var sharedKeyCred *azblob.SharedKeyCredential
		var err error

		if accountKey, ok := props[ADLS_SHARED_KEY_ACCOUNT_KEY]; ok {
			svcURL := fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
			containerURL, err := url.JoinPath(svcURL, parsed.Host)
			if err != nil {
				return nil, err
			}
			sharedKeyCred, err = azblob.NewSharedKeyCredential(accountName, accountKey)
			if err != nil {
				return nil, fmt.Errorf("failed azblob.NewSharedKeyCredential: %w", err)
			}

			client, err = container.NewClientWithSharedKeyCredential(containerURL, sharedKeyCred, nil)
			if err != nil {
				return nil, fmt.Errorf("failed container.NewClientWithSharedKeyCredential: %w", err)
			}
		} else if sasToken, ok := adlsSasTokens[accountName]; ok {
			svcURL, err := azureblob.NewServiceURL(&azureblob.ServiceURLOptions{
				AccountName: accountName,
				SASToken:    sasToken,
			})
			if err != nil {
				return nil, err
			}

			containerURL, err := url.JoinPath(string(svcURL), parsed.Host)
			if err != nil {
				return nil, err
			}

			client, err = container.NewClientWithNoCredential(containerURL, nil)
			if err != nil {
				return nil, fmt.Errorf("failed container.NewClientWithNoCredential: %w", err)
			}
		} else if connectionString, ok := adlsConnectionStrings[accountName]; ok {
			client, err = container.NewClientFromConnectionString(connectionString, parsed.Host, nil)
			if err != nil {
				return nil, fmt.Errorf("failed container.NewClientFromConnectionString: %w", err)
			}
		}

		bucket, err := azureblob.OpenBucket(ctx, client, nil)
		if err != nil {
			return nil, err
		}

		return bucket, nil
	}

	return nil, errors.New("xxxx")
}
