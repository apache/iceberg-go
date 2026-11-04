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

package rest_test

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// rotatingTokenSource returns an already expired token first and a valid one
// after that, standing in for a source whose first token has run out.
type rotatingTokenSource struct {
	mu    sync.Mutex
	calls int
}

func (s *rotatingTokenSource) Token() (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls == 1 {
		return &oauth2.Token{AccessToken: "first", Expiry: time.Now().Add(-time.Minute)}, nil
	}

	return &oauth2.Token{AccessToken: "second", Expiry: time.Now().Add(time.Hour)}, nil
}

func TestNewOauth2AuthManagerAuthenticatesCatalogRequests(t *testing.T) {
	var (
		mu      sync.Mutex
		headers []string
	)
	record := func(r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		headers = append(headers, r.URL.Path+" "+r.Header.Get("Authorization"))
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		record(r)
		json.NewEncoder(w).Encode(map[string]any{
			"defaults":  map[string]any{},
			"overrides": map[string]any{},
			"endpoints": rest.AllEndpointStrings,
		})
	})
	mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, r *http.Request) {
		record(r)
		json.NewEncoder(w).Encode(map[string]any{"namespaces": []any{}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ctx := context.Background()
	cat, err := rest.NewCatalog(ctx, "rest", srv.URL,
		rest.WithAuthManager(rest.NewOauth2AuthManager(&rotatingTokenSource{})))
	require.NoError(t, err)
	_, err = cat.ListNamespaces(ctx, nil)
	require.NoError(t, err)

	// The expired first token is sent once, then replaced on the next request.
	require.Equal(t, []string{
		"/v1/config Bearer first",
		"/v1/namespaces Bearer second",
	}, headers)
}

// ExampleNewOauth2AuthManager connects to Google's BigLake Iceberg REST
// catalog with Application Default Credentials.
func ExampleNewOauth2AuthManager() {
	ctx := context.Background()

	// Application Default Credentials: a service-account key file, gcloud user
	// credentials, or workload identity from the metadata server.
	ts, err := google.DefaultTokenSource(ctx, "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		log.Fatal(err)
	}

	cat, err := rest.NewCatalog(ctx, "biglake", "https://biglake.googleapis.com/iceberg/v1/restcatalog",
		rest.WithAuthManager(rest.NewOauth2AuthManager(ts)),
		rest.WithWarehouseLocation("gs://lakehouse-public-data"),
		// The project BigLake bills the requests to.
		rest.WithHeaders(map[string]string{"x-goog-user-project": "my-project"}),
	)
	if err != nil {
		log.Fatal(err)
	}
	_ = cat
}
