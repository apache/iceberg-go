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

package gocloud

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"

	"cloud.google.com/go/storage"

	"github.com/apache/iceberg-go/io"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

var allowedGCSCredTypes = map[string]option.CredentialsType{
	"service_account":              option.ServiceAccount,
	"authorized_user":              option.AuthorizedUser,
	"impersonated_service_account": option.ImpersonatedServiceAccount,
	"external_account":             option.ExternalAccount,
}

// ParseGCSConfig parses GCS properties and returns a configuration.
func ParseGCSConfig(props map[string]string) *gcsblob.Options {
	var o []option.ClientOption
	if url := props[io.GCSEndpoint]; url != "" {
		o = append(o, option.WithEndpoint(url))
	}
	var credType option.CredentialsType
	if key := props[io.GCSCredType]; key != "" {
		if ct, ok := allowedGCSCredTypes[key]; ok {
			credType = ct
		}
	}
	if key := props[io.GCSJSONKey]; key != "" {
		o = append(o, option.WithAuthCredentialsJSON(credType, []byte(key)))
	}
	if path := props[io.GCSKeyPath]; path != "" {
		o = append(o, option.WithAuthCredentialsFile(credType, path))
	}
	if v, ok := props[io.GCSUseJSONAPI]; ok {
		if parse, err := strconv.ParseBool(v); err == nil && parse {
			o = append(o, storage.WithJSONReads())
		}
	}

	return &gcsblob.Options{
		ClientOptions: o,
	}
}

// gcsScope grants read/write access to GCS objects.
const gcsScope = "https://www.googleapis.com/auth/devstorage.read_write"

// gcsCredentials builds credentials from the gcs.jsonkey / gcs.keypath
// properties (defaulting to a service-account key, honouring gcs.credtype),
// falling back to Application Default Credentials when neither is set.
func gcsCredentials(ctx context.Context, props map[string]string) (*google.Credentials, error) {
	credType := google.ServiceAccount
	if v := props[io.GCSCredType]; v != "" {
		credType = google.CredentialsType(v)
	}

	if jsonKey := props[io.GCSJSONKey]; jsonKey != "" {
		creds, err := google.CredentialsFromJSONWithType(ctx, []byte(jsonKey), credType, gcsScope)
		if err != nil {
			return nil, fmt.Errorf("gcs: parsing %s: %w", io.GCSJSONKey, err)
		}

		return creds, nil
	}

	if path := props[io.GCSKeyPath]; path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("gcs: reading %s %q: %w", io.GCSKeyPath, path, err)
		}
		creds, err := google.CredentialsFromJSONWithType(ctx, data, credType, gcsScope)
		if err != nil {
			return nil, fmt.Errorf("gcs: parsing credentials file %q: %w", path, err)
		}

		return creds, nil
	}

	creds, _ := gcp.DefaultCredentials(ctx)

	return creds, nil
}

// Construct a GCS bucket from a URL
func createGCSBucket(ctx context.Context, parsed *url.URL, props map[string]string) (*blob.Bucket, error) {
	gcscfg := ParseGCSConfig(props)

	creds, err := gcsCredentials(ctx, props)
	if err != nil {
		return nil, err
	}

	var client *gcp.HTTPClient
	if creds == nil {
		client = gcp.NewAnonymousHTTPClient(gcp.DefaultTransport())
	} else {
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
