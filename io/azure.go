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

	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
	"google.golang.org/api/option"
)

// Constants for Azure configuration options
const (
    AzureContainerName = "azure.container.name"
    AzureAccountName = "azure.account.name"
    AzureSasToken = "azure.sas.token"
    AzureStorageDomain = "azure.storage.domain"
    AzureProtocol = "azure.protocol"
)

// Construct
func parseAzureOptions(props map[string]string) *azureblob.ServiceURLOptions {
    opts := azureblob.NewDefaultServiceURLOptions()
    if account := props[AzureAccountName]; account != "" {
        opts.AccountName := account
    }
    if token := props[AzureSasToken]; token != "" {
        opts.SASToken := token
    }
    if domain := props[AzureStorageDomain]; domain != "" {
        opts.StorageDomain := domain
    }
    if protocol := props[AzureProtocol]; protocol != "" {
        opts.Protocol := protocol
    }
    return opts
}


// Construct a Azure bucket from a URL
func createAzureBucket(ctx context.Context, props map[string]string) (*blob.Bucket, error) {
    // Construct the ServiceURL
    opts := parseAzureOptions(props)
    serviceURL, err := azureblob.NewServiceURL(opts)
    if err != nil {
    	return nil, err
    }

    // Construct the client
    client := azureblob.NewDefaultClient(serviceURL, props[AzureContainerName])
    bucket, err := azureblob.OpenBucket(ctx, client, nil)
    if err != nil {
        return nil, err
    }

    return bucket, nil
}