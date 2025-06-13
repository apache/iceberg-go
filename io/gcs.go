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

	"cloud.google.com/go/storage"

	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"google.golang.org/api/option"
)

// Constants for GCS configuration options
const (
	GCSEndpoint   = "gcs.endpoint"
	GCSKeyPath    = "gcs.keypath"
	GCSJSONKey    = "gcs.jsonkey"
	GCSUseJsonAPI = "gcs.usejsonapi" // set to anything to enable
)

// ParseGCSConfig parses GCS properties and returns a configuration.
func ParseGCSConfig(props map[string]string) *gcsblob.Options {
	var o []option.ClientOption
	if url := props[GCSEndpoint]; url != "" {
		o = append(o, option.WithEndpoint(url))
	}
	if key := props[GCSJSONKey]; key != "" {
		o = append(o, option.WithCredentialsJSON([]byte(key)))
	}
	if path := props[GCSKeyPath]; path != "" {
		o = append(o, option.WithCredentialsFile(path))
	}
	if _, ok := props[GCSUseJsonAPI]; ok {
		o = append(o, storage.WithJSONReads())
	}

	return &gcsblob.Options{
		ClientOptions: o,
	}
}

// Construct a GCS bucket from a URL
func createGCSBucket(ctx context.Context, parsed *url.URL, props map[string]string) (*blob.Bucket, error) {
	gcscfg := ParseGCSConfig(props)
	creds, _ := gcp.DefaultCredentials(ctx)
	var client *gcp.HTTPClient
	if creds == nil {
		client = gcp.NewAnonymousHTTPClient(gcp.DefaultTransport())
	} else {
		var err error
		client, err = gcp.NewHTTPClient(
			gcp.DefaultTransport(),
			gcp.CredentialsTokenSource(creds))
		if err != nil {
			return nil, err
		}
	}

	bucket, err := gcsblob.OpenBucket(ctx, client, parsed.Host, gcscfg)
	if err != nil {
		return nil, err
	}

	return bucket, nil
}
