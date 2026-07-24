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

var allowedGCSCredTypes = map[string]struct{}{
	"service_account":              {},
	"authorized_user":              {},
	"impersonated_service_account": {},
	"external_account":             {},
}

func resolveGCSCredType(props map[string]string) (string, bool) {
	v := props[io.GCSCredType]
	if v == "" {
		return "", false
	}
	if _, ok := allowedGCSCredTypes[v]; !ok {
		return "", false
	}

	return v, true
}

// ParseGCSConfig parses non-credential GCS blob options; credentials are set on
// the client by gcsCredentials, which supersedes any credential option here.
func ParseGCSConfig(props map[string]string) *gcsblob.Options {
	var o []option.ClientOption
	if url := props[io.GCSEndpoint]; url != "" {
		o = append(o, option.WithEndpoint(url))
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

const gcsScope = "https://www.googleapis.com/auth/devstorage.read_write"

func gcsCredentials(ctx context.Context, props map[string]string) (*google.Credentials, error) {
	credType := google.ServiceAccount
	if v, ok := resolveGCSCredType(props); ok {
		credType = google.CredentialsType(v)
	}

	// CredentialsFromJSONWithType requires the key's type to equal credType, so
	// hint at gcs.credtype when a non-service-account key fails against it.
	parse := func(data []byte, source string) (*google.Credentials, error) {
		creds, err := google.CredentialsFromJSONWithType(ctx, data, credType, gcsScope)
		if err != nil {
			return nil, fmt.Errorf("gcs: parsing %s: set %s if the key is not a service account: %w", source, io.GCSCredType, err)
		}

		return creds, nil
	}

	if jsonKey := props[io.GCSJSONKey]; jsonKey != "" {
		return parse([]byte(jsonKey), io.GCSJSONKey)
	}

	if path := props[io.GCSKeyPath]; path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("gcs: reading %s %q: %w", io.GCSKeyPath, path, err)
		}

		return parse(data, fmt.Sprintf("%s %q", io.GCSKeyPath, path))
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
			gcp.CredentialsTokenSource(creds),
		)
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
