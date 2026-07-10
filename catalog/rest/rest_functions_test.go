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
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testFunctionMetadataJSON is minimal valid UDF metadata for add_one(int).
const testFunctionMetadataJSON = `{
  "function-uuid": "42fd3f91-bc10-41c1-8a52-92b57dd0a9b2",
  "format-version": 1,
  "definitions": [
    {
      "definition-id": "int",
      "parameters": [{"name": "x", "type": "int"}],
      "return-type": "int",
      "function-type": "udf",
      "versions": [
        {
          "version-id": 1,
          "representations": [{"type": "sql", "dialect": "trino", "sql": "x + 1"}],
          "timestamp-ms": 1000
        }
      ],
      "current-version-id": 1
    }
  ],
  "definition-log": [
    {"timestamp-ms": 1000, "definition-versions": [{"definition-id": "int", "version-id": 1}]}
  ]
}`

func (r *RestCatalogSuite) TestListFunctions200() {
	customPageSize := 100
	namespace := "accounting"
	r.mux.HandleFunc("/v1/namespaces/"+namespace+"/functions", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		pageToken := req.URL.Query().Get("pageToken")
		pageSize := req.URL.Query().Get("pageSize")
		r.Equal("", pageToken)
		r.Equal(strconv.Itoa(customPageSize), pageSize)

		// Function identifiers are CatalogObjectIdentifier values: flat
		// arrays of hierarchy levels, not {namespace, name} objects.
		json.NewEncoder(w).Encode(map[string]any{
			"identifiers": []any{
				[]string{"accounting", "tax", "paid"},
				[]string{"accounting", "tax", "owed"},
			},
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)
	// Passing in a custom page size through context
	ctx := cat.SetPageSize(context.Background(), customPageSize)

	var lastErr error
	functions := make([]table.Identifier, 0)
	for function, err := range cat.ListFunctions(ctx, catalog.ToIdentifier(namespace)) {
		functions = append(functions, function)
		if err != nil {
			lastErr = err
			r.FailNow("unexpected error:", err)
		}
	}

	r.Equal([]table.Identifier{
		{"accounting", "tax", "paid"},
		{"accounting", "tax", "owed"},
	}, functions)
	r.Require().NoError(lastErr)
}

func (r *RestCatalogSuite) TestListFunctionsPagination() {
	namespace := "accounting"
	r.mux.HandleFunc("/v1/namespaces/"+namespace+"/functions", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		switch req.URL.Query().Get("pageToken") {
		case "":
			json.NewEncoder(w).Encode(map[string]any{
				"identifiers":     []any{[]string{"accounting", "add_one"}},
				"next-page-token": "token-1",
			})
		case "token-1":
			json.NewEncoder(w).Encode(map[string]any{
				"identifiers": []any{[]string{"accounting", "add_two"}},
			})
		default:
			r.FailNow("unexpected page token")
		}
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	functions := make([]table.Identifier, 0)
	for function, err := range cat.ListFunctions(context.Background(), catalog.ToIdentifier(namespace)) {
		r.Require().NoError(err)
		functions = append(functions, function)
	}

	r.Equal([]table.Identifier{
		{"accounting", "add_one"},
		{"accounting", "add_two"},
	}, functions)

	// Breaking out of the iteration stops paging without error.
	for function, err := range cat.ListFunctions(context.Background(), catalog.ToIdentifier(namespace)) {
		r.Require().NoError(err)
		r.Equal(table.Identifier{"accounting", "add_one"}, function)

		break
	}
}

func (r *RestCatalogSuite) TestListFunctionsZeroPageSizeNotSent() {
	namespace := "accounting"
	r.mux.HandleFunc("/v1/namespaces/"+namespace+"/functions", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)
		r.Equal("", req.URL.Query().Get("pageSize"), "pageSize must not be sent when set to 0")
		json.NewEncoder(w).Encode(map[string]any{"identifiers": []any{}})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	ctx := cat.SetPageSize(context.Background(), 0)
	for _, err := range cat.ListFunctions(ctx, catalog.ToIdentifier(namespace)) {
		r.Require().NoError(err)
	}
}

func (r *RestCatalogSuite) TestListFunctionsPaginationErrorOnSubsequentPage() {
	namespace := "accounting"
	r.mux.HandleFunc("/v1/namespaces/"+namespace+"/functions", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		pageToken := req.URL.Query().Get("pageToken")

		// First page succeeds
		if pageToken == "" {
			json.NewEncoder(w).Encode(map[string]any{
				"identifiers": []any{
					[]string{namespace, "paid1"},
					[]string{namespace, "paid2"},
				},
				"next-page-token": "token1",
			})

			return
		}

		// Second page fails with an error
		if pageToken == "token1" {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"message": "Token expired or invalid",
					"type":    "NoSuchPageTokenException",
					"code":    404,
				},
			})

			return
		}

		r.FailNow("unexpected page token:", pageToken)
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	functions := make([]table.Identifier, 0)
	var lastErr error
	for function, err := range cat.ListFunctions(context.Background(), catalog.ToIdentifier(namespace)) {
		if err != nil {
			lastErr = err

			break
		}
		functions = append(functions, function)
	}

	// Check that we got the functions from the first page
	r.Equal([]table.Identifier{
		{"accounting", "paid1"},
		{"accounting", "paid2"},
	}, functions)

	// Check that we got the error from the second page
	r.Error(lastErr)
	r.ErrorContains(lastErr, "Token expired or invalid")
}

func (r *RestCatalogSuite) TestListFunctions404() {
	namespace := "nonexistent"
	r.mux.HandleFunc("/v1/namespaces/"+namespace+"/functions", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": "The given namespace does not exist",
				"type":    "NoSuchNamespaceException",
				"code":    404,
			},
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	var lastErr error
	for _, err := range cat.ListFunctions(context.Background(), catalog.ToIdentifier(namespace)) {
		if err != nil {
			lastErr = err
		}
	}
	r.ErrorIs(lastErr, catalog.ErrNoSuchNamespace)
	r.ErrorContains(lastErr, "The given namespace does not exist")
}

func (r *RestCatalogSuite) TestLoadFunction200() {
	r.mux.HandleFunc("/v1/namespaces/accounting/functions/add_one", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.Write([]byte(`{
			"metadata-location": "s3://bucket/functions/add_one/metadata/00000-x.metadata.json",
			"metadata": ` + testFunctionMetadataJSON + `
		}`))
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	fn, err := cat.LoadFunction(context.Background(), catalog.ToIdentifier("accounting", "add_one"))
	r.Require().NoError(err)

	r.Equal(table.Identifier{"accounting", "add_one"}, fn.Identifier())
	r.Equal("s3://bucket/functions/add_one/metadata/00000-x.metadata.json", fn.MetadataLocation())
	r.Equal("42fd3f91-bc10-41c1-8a52-92b57dd0a9b2", fn.Metadata().FunctionUUID().String())

	def, ok := fn.Metadata().DefinitionByID("int")
	r.Require().True(ok, "expected the int overload in the loaded metadata")
	r.Equal(1, def.CurrentVersionID)
	r.Len(fn.Definitions(), 1)
}

func (r *RestCatalogSuite) TestLoadFunction404() {
	r.mux.HandleFunc("/v1/namespaces/accounting/functions/missing_fn", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": "The requested function does not exist",
				"type":    "NoSuchFunctionException",
				"code":    404,
			},
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	_, err = cat.LoadFunction(context.Background(), catalog.ToIdentifier("accounting", "missing_fn"))
	r.ErrorIs(err, catalog.ErrNoSuchFunction)
	r.ErrorContains(err, "The requested function does not exist")
}

func (r *RestCatalogSuite) TestLoadFunctionMalformedMetadata() {
	r.mux.HandleFunc("/v1/namespaces/accounting/functions/broken_fn", func(w http.ResponseWriter, req *http.Request) {
		w.Write([]byte(`{"metadata-location": "s3://x", "metadata": {"format-version": 1}}`))
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	_, err = cat.LoadFunction(context.Background(), catalog.ToIdentifier("accounting", "broken_fn"))
	r.ErrorContains(err, "failed to parse function metadata")
}

func (r *RestCatalogSuite) TestCheckFunctionExists() {
	r.mux.HandleFunc("/v1/namespaces/accounting/functions/add_one", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method, "existence must be checked via GET: the spec has no HEAD endpoint")

		w.Write([]byte(`{"metadata": ` + testFunctionMetadataJSON + `}`))
	})
	r.mux.HandleFunc("/v1/namespaces/accounting/functions/missing_fn", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": "The requested function does not exist",
				"type":    "NoSuchFunctionException",
				"code":    404,
			},
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	exists, err := cat.CheckFunctionExists(context.Background(), catalog.ToIdentifier("accounting", "add_one"))
	r.Require().NoError(err)
	r.True(exists)

	exists, err = cat.CheckFunctionExists(context.Background(), catalog.ToIdentifier("accounting", "missing_fn"))
	r.Require().NoError(err)
	r.False(exists)
}

func (r *RestCatalogSuite) TestLoadFunctionInvalidIdentifier() {
	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	// An invalid identifier reports the function sentinel, not the table one.
	_, err = cat.LoadFunction(context.Background(), table.Identifier{})
	r.ErrorIs(err, catalog.ErrNoSuchFunction)
	r.NotErrorIs(err, catalog.ErrNoSuchTable)
	r.ErrorContains(err, "missing namespace or invalid identifier")

	// The existence check surfaces the invalid identifier as an error, as the
	// table and view existence checks do.
	exists, err := cat.CheckFunctionExists(context.Background(), table.Identifier{})
	r.False(exists)
	r.ErrorIs(err, catalog.ErrNoSuchFunction)
}

func (r *RestCatalogSuite) TestListFunctionsInvalidNamespace() {
	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	var lastErr error
	for _, err := range cat.ListFunctions(context.Background(), table.Identifier{}) {
		if err != nil {
			lastErr = err
		}
	}
	r.Error(lastErr)
}

// TestFunctionEndpointNegotiation drives the catalog against a server that
// does not advertise the function endpoints, which the spec keeps out of
// the assumed default endpoint set.
func TestFunctionEndpointNegotiation(t *testing.T) {
	var functionsHit bool

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, req *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"defaults":  map[string]any{},
			"overrides": map[string]any{},
			// Function endpoints deliberately absent.
			"endpoints": []string{"GET /v1/{prefix}/namespaces"},
		})
	})
	mux.HandleFunc("/v1/namespaces/", func(w http.ResponseWriter, req *http.Request) {
		functionsHit = true
		http.Error(w, "unexpected request to unsupported endpoint", http.StatusInternalServerError)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	cat, err := rest.NewCatalog(context.Background(), "rest", srv.URL, rest.WithOAuthToken(TestToken))
	require.NoError(t, err)

	// An unsupported load fails with the capability sentinel, not a transport error.
	_, err = cat.LoadFunction(context.Background(), table.Identifier{"ns", "fn"})
	assert.ErrorIs(t, err, rest.ErrEndpointNotSupported)
	assert.NotErrorIs(t, err, rest.ErrRESTError)

	// The existence check surfaces the capability error rather than a bogus "not found".
	_, err = cat.CheckFunctionExists(context.Background(), table.Identifier{"ns", "fn"})
	assert.ErrorIs(t, err, rest.ErrEndpointNotSupported)

	// An unsupported list yields empty without erroring or calling the server.
	for _, err := range cat.ListFunctions(context.Background(), table.Identifier{"ns"}) {
		require.NoError(t, err)
	}
	assert.False(t, functionsHit)
}
