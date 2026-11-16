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

// Package googleauth authenticates the REST catalog client with Google
// credentials, for catalogs such as BigLake. It is an optional backend:
// importing it (even as a blank import) registers the "google" auth type, so
// that rest.auth.type=google works, and it keeps the Google auth libraries out
// of the core catalog/rest package.
//
//	import (
//		"github.com/apache/iceberg-go/catalog"
//		_ "github.com/apache/iceberg-go/catalog/rest/googleauth"
//	)
//
//	cat, err := catalog.Load(ctx, "biglake", iceberg.Properties{
//		"type":           "rest",
//		"uri":            "https://biglake.googleapis.com/iceberg/v1/restcatalog",
//		"warehouse":      "gs://my-bucket",
//		"rest.auth.type": "google",
//	})
//
// The properties match Iceberg Java's GoogleAuthManager:
//
//   - gcp.auth.credentials-path: a credentials JSON file, such as a service
//     account key.
//   - gcp.auth.credentials-json: the same JSON, inline.
//   - gcp.auth.scopes: comma-separated OAuth scopes, by default
//     https://www.googleapis.com/auth/cloud-platform.
//
// When neither credentials property is set, Application Default Credentials
// are used. Setting both is an error.
//
// These properties, like rest.auth.type, are read only from the properties the
// client supplies, never from the server's /v1/config response. The
// credentials JSON may be of any type the Google auth library accepts,
// including external account configurations that fetch tokens from other
// hosts, so it must come from a source you control.
//
// Token requests don't go through the catalog's HTTP client, so a transport,
// TLS config or proxy configured on the catalog doesn't apply to them, as in
// Java. They use a client with the default transport and a 60-second timeout.
// To use a specific client, put it in the context's oauth2.HTTPClient value
// when loading the catalog, or build the source with TokenSource and pass
// rest.NewOauth2AuthManager(ts) to rest.WithAuthManager. One exception: on
// Google Cloud compute, Application Default Credentials come from the metadata
// server, which the Google library always reaches through its own client.
package googleauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// Property names, as in Iceberg Java's GoogleAuthManager.
const (
	CredentialsPathKey = "gcp.auth.credentials-path"
	CredentialsJSONKey = "gcp.auth.credentials-json"
	ScopesKey          = "gcp.auth.scopes"
)

// DefaultScopes is the scope requested when gcp.auth.scopes is unset.
const DefaultScopes = "https://www.googleapis.com/auth/cloud-platform"

// refreshTimeout bounds a token request, like the REST client's built-in
// OAuth flow does. It applies only when the catalog's context doesn't already
// carry an oauth2.HTTPClient.
const refreshTimeout = 60 * time.Second

func init() {
	rest.RegisterAuthManager(rest.AuthTypeGoogle, func(ctx context.Context, cfg rest.AuthManagerConfig) (rest.AuthManager, error) {
		ts, err := TokenSource(ctx, cfg.Props)
		if err != nil {
			return nil, err
		}

		return rest.NewOauth2AuthManager(ts), nil
	})
}

// TokenSource returns a token source for the Google credentials that props
// describe, using the gcp.auth.* properties. It is what rest.auth.type=google
// uses, exported for callers that build their own AuthManager.
//
// Tokens are fetched after TokenSource returns, so the source keeps ctx's
// values but not its cancellation.
func TokenSource(ctx context.Context, props iceberg.Properties) (oauth2.TokenSource, error) {
	path, inline := props[CredentialsPathKey], props[CredentialsJSONKey]
	if path != "" && inline != "" {
		return nil, fmt.Errorf("googleauth: cannot set both %s and %s", CredentialsPathKey, CredentialsJSONKey)
	}

	scopes := parseScopes(props.Get(ScopesKey, DefaultScopes))

	ctx = context.WithoutCancel(ctx)
	if _, ok := ctx.Value(oauth2.HTTPClient).(*http.Client); !ok {
		ctx = context.WithValue(ctx, oauth2.HTTPClient, &http.Client{Timeout: refreshTimeout})
	}

	var data []byte
	switch {
	case path != "":
		var err error
		if data, err = os.ReadFile(path); err != nil {
			return nil, fmt.Errorf("googleauth: read %s: %w", CredentialsPathKey, err)
		}
	case inline != "":
		data = []byte(inline)
	default:
		creds, err := google.FindDefaultCredentials(ctx, scopes...)
		if err != nil {
			return nil, fmt.Errorf("googleauth: application default credentials: %w", err)
		}

		return creds.TokenSource, nil
	}

	creds, err := credentialsFromJSON(ctx, data, scopes)
	if err != nil {
		return nil, err
	}

	return creds.TokenSource, nil
}

// credentialsFromJSON loads credentials of whatever type the JSON declares.
// The JSON comes from client-supplied properties only, which is the validation
// google.CredentialsFromJSON's deprecation asks for. Errors never include the
// JSON, which holds a private key.
func credentialsFromJSON(ctx context.Context, data []byte, scopes []string) (*google.Credentials, error) {
	var header struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return nil, errors.New("googleauth: credentials are not valid JSON")
	}
	if header.Type == "" {
		return nil, errors.New(`googleauth: credentials JSON has no "type" field`)
	}

	creds, err := google.CredentialsFromJSONWithType(ctx, data, google.CredentialsType(header.Type), scopes...)
	if err != nil {
		return nil, fmt.Errorf("googleauth: load %q credentials: %w", header.Type, err)
	}

	return creds, nil
}

// parseScopes splits a comma-separated scope list, trimming spaces and
// dropping empty entries, as Java's GoogleAuthManager does.
func parseScopes(s string) []string {
	var scopes []string
	for scope := range strings.SplitSeq(s, ",") {
		if scope = strings.TrimSpace(scope); scope != "" {
			scopes = append(scopes, scope)
		}
	}

	return scopes
}
