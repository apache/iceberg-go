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
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
)

// Constants for Azure configuration options
const (
	AdlsSasTokenPrefix         = "adls.sas-token."
	AdlsConnectionStringPrefix = "adls.connection-string."
	AdlsSharedKeyAccountName   = "adls.auth.shared-key.account.name"
	AdlsSharedKeyAccountKey    = "adls.auth.shared-key.account.key"
	AdlsEndpoint               = "adls.endpoint"
	AdlsProtocol               = "adls.protocol"

	// Not in use yet
	// AdlsReadBlockSize          = "adls.read.block-size-bytes"
	// AdlsWriteBlockSize         = "adls.write.block-size-bytes"
)

// parseAzureURL parses Azure URLs that may contain container@account format
// Returns containerName, accountName from URL, plus any error
func parseAzureURL(parsed *url.URL) (containerName, accountName string, err error) {
	host := parsed.Host
	
	// Check if URL is in container@account.dfs.core.windows.net format
	if strings.Contains(host, "@") {
		parts := strings.Split(host, "@")
		if len(parts) != 2 {
			return "", "", fmt.Errorf("invalid Azure URL format: expected container@account.dfs.core.windows.net, got %s", host)
		}
		containerName = parts[0]
		// Extract account name from account.dfs.core.windows.net
		accountParts := strings.Split(parts[1], ".")
		if len(accountParts) > 0 {
			accountName = accountParts[0]
		}
	} else {
		// Traditional format: abfs://container/path or abfs://account.dfs.core.windows.net/container/path
		if strings.Contains(host, ".") {
			// Format: account.dfs.core.windows.net (container will be in path)
			accountParts := strings.Split(host, ".")
			if len(accountParts) > 0 {
				accountName = accountParts[0]
			}
			// Container name will be derived from path or properties
		} else {
			// Simple container name format: abfs://container/path
			containerName = host
		}
	}
	
	return containerName, accountName, nil
}

// Construct a Azure bucket from a URL
func createAzureBucket(ctx context.Context, parsed *url.URL, props map[string]string) (*blob.Bucket, error) {
	adlsSasTokens := propertiesWithPrefix(props, AdlsSasTokenPrefix)
	adlsConnectionStrings := propertiesWithPrefix(props, AdlsConnectionStringPrefix)

	// Parse the Azure URL to extract container and account names
	urlContainerName, urlAccountName, err := parseAzureURL(parsed)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Azure URL: %w", err)
	}

	// Construct the client
	accountName := props[AdlsSharedKeyAccountName]
	endpoint := props[AdlsEndpoint]
	protocol := props[AdlsProtocol]

	// If account name is not provided in props, use the one from URL
	if accountName == "" && urlAccountName != "" {
		accountName = urlAccountName
	}

	var client *container.Client

	if accountName == "" {
		return nil, errors.New("account name is required for azure bucket")
	}

	// Determine the container name to use
	containerName := urlContainerName
	if containerName == "" {
		// Fallback: check if container name is provided via properties or use parsed.Host
		if propContainer := props["adls.container-name"]; propContainer != "" {
			containerName = propContainer
		} else {
			containerName = parsed.Host
		}
	}

	if accountKey, ok := props[AdlsSharedKeyAccountKey]; ok {
		svcURL, err := azureblob.NewServiceURL(&azureblob.ServiceURLOptions{
			AccountName:   accountName,
			Protocol:      protocol,
			StorageDomain: endpoint,
		})
		if err != nil {
			return nil, err
		}
		containerURL, err := url.JoinPath(string(svcURL), containerName)
		if err != nil {
			return nil, err
		}
		sharedKeyCred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
			return nil, fmt.Errorf("failed azblob.NewSharedKeyCredential: %w", err)
		}

		client, err = container.NewClientWithSharedKeyCredential(containerURL, sharedKeyCred, nil)
		if err != nil {
			return nil, fmt.Errorf("failed container.NewClientWithSharedKeyCredential: %w", err)
		}
	} else if sasToken, ok := adlsSasTokens[accountName]; ok {
		svcURL, err := azureblob.NewServiceURL(&azureblob.ServiceURLOptions{
			AccountName:   accountName,
			SASToken:      sasToken,
			Protocol:      protocol,
			StorageDomain: endpoint,
		})
		if err != nil {
			return nil, err
		}

		containerURL, err := url.JoinPath(string(svcURL), containerName)
		if err != nil {
			return nil, err
		}

		client, err = container.NewClientWithNoCredential(containerURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed container.NewClientWithNoCredential: %w", err)
		}
	} else if connectionString, ok := adlsConnectionStrings[accountName]; ok {
		var err error
		client, err = container.NewClientFromConnectionString(connectionString, containerName, nil)
		if err != nil {
			return nil, fmt.Errorf("failed container.NewClientFromConnectionString: %w", err)
		}
	}

	return azureblob.OpenBucket(ctx, client, nil)
}
