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

package rest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

func TestCatalogObjectPagesPreserveWireShapesAndQueries(t *testing.T) {
	for _, tc := range []struct {
		name, route, response string
		ep                    endpoint
		want                  []table.Identifier
	}{
		{
			name: "tables", route: "tables", ep: endpointListTables,
			response: `{"identifiers":[{"namespace":["analytics"],"name":"sales"},{"namespace":["analytics","raw"],"name":"events"}],"next-page-token":"next-page"}`,
			want:     []table.Identifier{{"analytics", "sales"}, {"analytics", "raw", "events"}},
		},
		{
			name: "views", route: "views", ep: endpointListViews,
			response: `{"identifiers":[{"namespace":["analytics"],"name":"monthly"}],"next-page-token":"next-page"}`,
			want:     []table.Identifier{{"analytics", "monthly"}},
		},
		{
			name: "functions", route: "functions", ep: endpointListFunctions,
			response: `{"identifiers":[["analytics","normalize"],["analytics","helpers","parse"]],"next-page-token":"next-page"}`,
			want:     []table.Identifier{{"analytics", "normalize"}, {"analytics", "helpers", "parse"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				requests.Add(1)
				if req.Method != http.MethodGet {
					t.Errorf("method = %q, want GET", req.Method)
				}
				if wantPath := "/v1/namespaces/analytics/" + tc.route; req.URL.Path != wantPath {
					t.Errorf("path = %q, want %q", req.URL.Path, wantPath)
				}
				if got := req.URL.Query().Get("pageToken"); got != "token +/%" {
					t.Errorf("pageToken = %q, want %q", got, "token +/%")
				}
				if got := req.URL.Query().Get("pageSize"); got != "37" {
					t.Errorf("pageSize = %q, want 37", got)
				}
				_, _ = w.Write([]byte(tc.response))
			}))
			defer server.Close()

			baseURI, err := url.Parse(server.URL + "/v1")
			if err != nil {
				t.Fatal(err)
			}
			cat := &Catalog{baseURI: baseURI, cl: server.Client(), endpoints: newEndpointSet([]endpoint{tc.ep})}
			namespace := table.Identifier{"analytics"}
			var got []table.Identifier
			var nextToken string
			switch tc.ep {
			case endpointListTables:
				got, nextToken, err = cat.listTablesPage(context.Background(), namespace, "token +/%", 37)
			case endpointListViews:
				got, nextToken, err = cat.listViewsPage(context.Background(), namespace, "token +/%", 37)
			case endpointListFunctions:
				got, nextToken, err = cat.listFunctionsPage(context.Background(), namespace, "token +/%", 37)
			default:
				t.Fatalf("unexpected endpoint: %v", tc.ep)
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("identifiers = %v, want %v", got, tc.want)
			}
			if nextToken != "next-page" {
				t.Errorf("next token = %q, want next-page", nextToken)
			}
			if requests.Load() != 1 {
				t.Errorf("requests = %d, want 1", requests.Load())
			}
		})
	}
}

func TestCatalogObjectPagesDoNotFetchUnsupportedEndpoints(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	baseURI, err := url.Parse(server.URL + "/v1")
	if err != nil {
		t.Fatal(err)
	}
	cat := &Catalog{baseURI: baseURI, cl: server.Client(), endpoints: newEndpointSet(nil)}

	for _, listPage := range []func(context.Context, table.Identifier, string, int) ([]table.Identifier, string, error){
		cat.listTablesPage, cat.listViewsPage, cat.listFunctionsPage,
	} {
		identifiers, token, err := listPage(context.Background(), table.Identifier{"analytics"}, "", 0)
		if err != nil || len(identifiers) != 0 || token != "" {
			t.Errorf("unsupported listing returned (%v, %q, %v), want empty result", identifiers, token, err)
		}
		_, _, err = listPage(context.Background(), nil, "", 0)
		if err == nil {
			t.Error("invalid namespace must be rejected before endpoint gating")
		}
	}
	if requests.Load() != 0 {
		t.Errorf("requests = %d, want 0", requests.Load())
	}
}

func TestCatalogObjectPagesMapNotFoundToMissingNamespace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	baseURI, err := url.Parse(server.URL + "/v1")
	if err != nil {
		t.Fatal(err)
	}
	cat := &Catalog{
		baseURI: baseURI,
		cl:      server.Client(),
		endpoints: newEndpointSet([]endpoint{
			endpointListTables, endpointListViews, endpointListFunctions,
		}),
	}
	for _, listPage := range []func(context.Context, table.Identifier, string, int) ([]table.Identifier, string, error){
		cat.listTablesPage, cat.listViewsPage, cat.listFunctionsPage,
	} {
		_, _, err := listPage(t.Context(), table.Identifier{"missing"}, "", 0)
		if !errors.Is(err, catalog.ErrNoSuchNamespace) {
			t.Errorf("404 should indicate a missing namespace, got %v", err)
		}
	}
}

func TestPaginationQueryParams(t *testing.T) {
	for _, tc := range []struct {
		token string
		size  int
		want  string
	}{
		{"", 0, ""},
		{"", -1, ""},
		{"abc", 0, "pageToken=abc"},
		{"a+b /%", 42, "pageSize=42&pageToken=a%2Bb+%2F%25"},
	} {
		if got := paginationQueryParams(tc.token, tc.size).Encode(); got != tc.want {
			t.Errorf("paginationQueryParams(%q, %d) = %q, want %q", tc.token, tc.size, got, tc.want)
		}
	}
}
