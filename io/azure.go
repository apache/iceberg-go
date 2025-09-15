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
	"regexp"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
)

// adlsURIPattern is taken from the Java implementation:
// https://github.com/apache/iceberg/blob/2114bf631e49af532d66e2ce148ee49dd1dd1f1f/azure/src/main/java/org/apache/iceberg/azure/adlsv2/ADLSLocation.java#L47
var adlsURIPattern = regexp.MustCompile(`^(abfss?|wasbs?)://([^/?#]+)(.*)?$`)

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

// adlsLocation represents the parsed components of an Azure Data Lake Storage URI
type adlsLocation struct {
	accountName   string // Azure storage account name
	containerName string // Container (bucket) name
	path          string // Object path within the container
}

// createServiceURL creates an Azure blob service URL with the given parameters
func createServiceURL(accountName, protocol, endpoint, sasToken string) (string, error) {
	svcURL, err := azureblob.NewServiceURL(&azureblob.ServiceURLOptions{
		AccountName:   accountName,
		SASToken:      sasToken,
		Protocol:      protocol,
		StorageDomain: endpoint,
	})
	if err != nil {
		return "", err
	}

	return string(svcURL), nil
}

// createContainerURL creates a full container URL by building the service URL and joining with container name
func createContainerURL(accountName, protocol, endpoint, sasToken, containerName string) (string, error) {
	serviceURL, err := createServiceURL(accountName, protocol, endpoint, sasToken)
	if err != nil {
		return "", err
	}

	return url.JoinPath(serviceURL, containerName)
}

// getContainerNameFromURI extracts the container name from a URI
func getContainerNameFromURI(uri *url.URL) string {
	return uri.User.Username()
}

// newAdlsLocation parses an Azure Data Lake Storage URI and extracts location components
// Supports the pattern: <scheme>://<file_system>@<account_name>.<endpoint>/<path>
func newAdlsLocation(adlsURI *url.URL) (*adlsLocation, error) {
	// Extract container name from User field
	containerName := getContainerNameFromURI(adlsURI)
	if containerName == "" {
		return nil, errors.New("container name is required for azure bucket")
	}

	hostname := adlsURI.Hostname()
	if hostname == "" {
		return nil, errors.New("hostname is required for azure bucket")
	}

	// Extract account name from hostname (ignore the storage domain)
	parts := strings.Split(hostname, ".")
	if len(parts) == 0 || parts[0] == "" {
		return nil, errors.New("account name is required for azure bucket")
	}
	accountName := parts[0]

	path := adlsURI.Path

	return &adlsLocation{
		accountName:   accountName,
		containerName: containerName,
		path:          path,
	}, nil
}

// Construct a Azure bucket from a URL
func createAzureBucket(ctx context.Context, parsed *url.URL, props map[string]string) (*blob.Bucket, error) {
	adlsSasTokens := propertiesWithPrefix(props, AdlsSasTokenPrefix)
	adlsConnectionStrings := propertiesWithPrefix(props, AdlsConnectionStringPrefix)

	// Construct the client
	location, err := newAdlsLocation(parsed)
	if err != nil {
		return nil, err
	}

	sharedKeyAccountName := props[AdlsSharedKeyAccountName]
	endpoint := props[AdlsEndpoint]
	protocol := props[AdlsProtocol]

	var client *container.Client

	if sharedKeyAccountName != "" {
		sharedKeyAccountKey, ok := props[AdlsSharedKeyAccountKey]
		if !ok || sharedKeyAccountKey == "" {
			return nil, fmt.Errorf("azure authentication: shared-key requires both %s and %s", AdlsSharedKeyAccountName, AdlsSharedKeyAccountKey)
		}

		containerURL, err := createContainerURL(location.accountName, protocol, endpoint, "", location.containerName)
		if err != nil {
			return nil, err
		}

		sharedKeyCred, err := azblob.NewSharedKeyCredential(sharedKeyAccountName, sharedKeyAccountKey)
		if err != nil {
			return nil, fmt.Errorf("failed azblob.NewSharedKeyCredential: %w", err)
		}

		client, err = container.NewClientWithSharedKeyCredential(containerURL, sharedKeyCred, nil)
		if err != nil {
			return nil, fmt.Errorf("failed container.NewClientWithSharedKeyCredential: %w", err)
		}
	} else if sasToken, ok := adlsSasTokens[location.accountName]; ok {
		containerURL, err := createContainerURL(location.accountName, protocol, endpoint, sasToken, location.containerName)
		if err != nil {
			return nil, err
		}

		client, err = container.NewClientWithNoCredential(containerURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed container.NewClientWithNoCredential: %w", err)
		}
	} else if connectionString, ok := adlsConnectionStrings[location.accountName]; ok {
		var err error
		client, err = container.NewClientFromConnectionString(connectionString, location.containerName, nil)
		if err != nil {
			return nil, fmt.Errorf("failed container.NewClientFromConnectionString: %w", err)
		}
	} else {
		containerURL, err := createContainerURL(location.accountName, protocol, endpoint, "", location.containerName)
		if err != nil {
			return nil, err
		}

		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("failed azidentity.NewDefaultAzureCredential: %w", err)
		}

		client, err = container.NewClient(containerURL, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("failed container.NewClient: %w", err)
		}
	}

	return azureblob.OpenBucket(ctx, client, nil)
}

// adlsKeyExtractor creates a key extractor for Azure schemes using the adlsURIPattern pattern
func adlsKeyExtractor() KeyExtractor {
	return func(location string) (string, error) {
		matches := adlsURIPattern.FindStringSubmatch(location)
		if len(matches) < 4 {
			return "", fmt.Errorf("invalid ADLS location: %s", location)
		}

		uriPath := matches[3]
		key := strings.TrimPrefix(uriPath, "/")

		if key == "" {
			return "", fmt.Errorf("URI path is empty: %s", location)
		}

		return key, nil
	}
}
