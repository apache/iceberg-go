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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/suite"
)

const (
	TestCreds       = "client:secret"
	TestToken       = "some_jwt_token"
	defaultPageSize = 20
)

var (
	TestHeaders = http.Header{
		"X-Client-Version": {"0.14.1"},
		"User-Agent":       {"GoIceberg/(unknown version)"},
		"Authorization":    {"Bearer " + TestToken},
	}
	OAuthTestHeaders = http.Header{
		"Content-Type": {"application/x-www-form-urlencoded"},
	}
)

type RestCatalogSuite struct {
	suite.Suite

	srv *httptest.Server
	mux *http.ServeMux

	configVals url.Values
}

func (r *RestCatalogSuite) SetupTest() {
	r.mux = http.NewServeMux()

	r.mux.HandleFunc("/v1/config", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)
		r.configVals = req.URL.Query()

		json.NewEncoder(w).Encode(map[string]any{
			"defaults":  map[string]any{},
			"overrides": map[string]any{},
		})
	})

	r.srv = httptest.NewServer(r.mux)
}

func (r *RestCatalogSuite) TearDownTest() {
	r.srv.Close()
	r.srv = nil
	r.mux = nil
	r.configVals = nil
}

func (r *RestCatalogSuite) TestToken200() {
	const scope = "myscope"
	r.mux.HandleFunc("/v1/oauth/tokens", func(w http.ResponseWriter, req *http.Request) {
		r.Equal(http.MethodPost, req.Method)

		r.Equal(req.Header.Get("Content-Type"), "application/x-www-form-urlencoded")

		r.Require().NoError(req.ParseForm())
		values := req.PostForm
		r.Equal(values.Get("grant_type"), "client_credentials")
		r.Equal(values.Get("client_id"), "client")
		r.Equal(values.Get("client_secret"), "secret")
		r.Equal(values.Get("scope"), scope)

		w.WriteHeader(http.StatusOK)

		json.NewEncoder(w).Encode(map[string]any{
			"access_token":      TestToken,
			"token_type":        "Bearer",
			"expires_in":        86400,
			"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL,
		rest.WithWarehouseLocation("s3://some-bucket"),
		rest.WithCredential(TestCreds),
		rest.WithScope(scope))
	r.Require().NoError(err)

	r.NotNil(cat)
	r.Equal(r.configVals.Get("warehouse"), "s3://some-bucket")
}

func (r *RestCatalogSuite) TestLoadRegisteredCatalog() {
	ctx := context.Background()
	r.mux.HandleFunc("/v1/oauth/tokens", func(w http.ResponseWriter, req *http.Request) {
		r.Equal(http.MethodPost, req.Method)

		r.Equal(req.Header.Get("Content-Type"), "application/x-www-form-urlencoded")

		r.Require().NoError(req.ParseForm())
		values := req.PostForm
		r.Equal(values.Get("grant_type"), "client_credentials")
		r.Equal(values.Get("client_id"), "client")
		r.Equal(values.Get("client_secret"), "secret")
		r.Equal(values.Get("scope"), "catalog")

		w.WriteHeader(http.StatusOK)

		json.NewEncoder(w).Encode(map[string]any{
			"access_token":      TestToken,
			"token_type":        "Bearer",
			"expires_in":        86400,
			"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
		})
	})

	cat, err := catalog.Load(ctx, "restful", iceberg.Properties{
		"uri":        r.srv.URL,
		"warehouse":  "s3://some-bucket",
		"credential": TestCreds,
	})
	r.NoError(err)

	r.NotNil(cat)
	r.Equal(r.configVals.Get("warehouse"), "s3://some-bucket")
}

func (r *RestCatalogSuite) TestToken400() {
	r.mux.HandleFunc("/v1/oauth/tokens", func(w http.ResponseWriter, req *http.Request) {
		r.Equal(http.MethodPost, req.Method)

		r.Equal(req.Header.Get("Content-Type"), "application/x-www-form-urlencoded")

		w.WriteHeader(http.StatusBadRequest)

		json.NewEncoder(w).Encode(map[string]any{
			"error":             "invalid_client",
			"error_description": "credentials for key invalid_key do not match",
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithCredential(TestCreds))
	r.Nil(cat)

	r.ErrorIs(err, rest.ErrRESTError)
	r.ErrorIs(err, rest.ErrOAuthError)
	r.ErrorContains(err, "invalid_client: credentials for key invalid_key do not match")
}

func (r *RestCatalogSuite) TestToken200AuthUrl() {
	r.mux.HandleFunc("/auth-token-url", func(w http.ResponseWriter, req *http.Request) {
		r.Equal(http.MethodPost, req.Method)

		r.Equal(req.Header.Get("Content-Type"), "application/x-www-form-urlencoded")

		r.Require().NoError(req.ParseForm())
		values := req.PostForm
		r.Equal(values.Get("grant_type"), "client_credentials")
		r.Equal(values.Get("client_id"), "client")
		r.Equal(values.Get("client_secret"), "secret")
		r.Equal(values.Get("scope"), "catalog")

		w.WriteHeader(http.StatusOK)

		json.NewEncoder(w).Encode(map[string]any{
			"access_token":      TestToken,
			"token_type":        "Bearer",
			"expires_in":        86400,
			"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
		})
	})

	authUri, err := url.Parse(r.srv.URL)
	r.Require().NoError(err)
	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL,
		rest.WithWarehouseLocation("s3://some-bucket"),
		rest.WithCredential(TestCreds), rest.WithAuthURI(authUri.JoinPath("auth-token-url")))

	r.Require().NoError(err)

	r.NotNil(cat)
	r.Equal(r.configVals.Get("warehouse"), "s3://some-bucket")
}

func (r *RestCatalogSuite) TestToken401() {
	r.mux.HandleFunc("/v1/oauth/tokens", func(w http.ResponseWriter, req *http.Request) {
		r.Equal(http.MethodPost, req.Method)

		r.Equal(req.Header.Get("Content-Type"), "application/x-www-form-urlencoded")

		w.WriteHeader(http.StatusUnauthorized)

		json.NewEncoder(w).Encode(map[string]any{
			"error":             "invalid_client",
			"error_description": "credentials for key invalid_key do not match",
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithCredential(TestCreds))
	r.Nil(cat)

	r.ErrorIs(err, rest.ErrRESTError)
	r.ErrorIs(err, rest.ErrOAuthError)
	r.ErrorContains(err, "invalid_client: credentials for key invalid_key do not match")
}

func (r *RestCatalogSuite) TestListTables200() {
	namespace := "examples"
	customPageSize := 100
	r.mux.HandleFunc("/v1/namespaces/"+namespace+"/tables", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}
		pageToken := req.URL.Query().Get("pageToken")
		pageSize := req.URL.Query().Get("pageSize")
		r.Equal("", pageToken)
		r.Equal(strconv.Itoa(customPageSize), pageSize)
		json.NewEncoder(w).Encode(map[string]any{
			"identifiers": []any{
				map[string]any{
					"namespace": []string{namespace},
					"name":      "fooshare",
				},
			},
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	ctx := cat.SetPageSize(context.Background(), customPageSize)

	var lastErr error
	tbls := make([]table.Identifier, 0)

	iter := cat.ListTables(ctx, catalog.ToIdentifier(namespace))

	for tbl, err := range iter {
		tbls = append(tbls, tbl)
		if err != nil {
			lastErr = err
			r.FailNow("unexpected error:", err)
		}
	}
	r.Require().NoError(lastErr)
	r.Equal([]table.Identifier{{"examples", "fooshare"}}, tbls)
}

func (r *RestCatalogSuite) TestListTablesPrefixed200() {
	r.mux.HandleFunc("/v1/oauth/tokens", func(w http.ResponseWriter, req *http.Request) {
		r.Equal(http.MethodPost, req.Method)

		r.Equal(req.Header.Get("Content-Type"), "application/x-www-form-urlencoded")

		r.Require().NoError(req.ParseForm())
		values := req.PostForm
		r.Equal(values.Get("grant_type"), "client_credentials")
		r.Equal(values.Get("client_id"), "client")
		r.Equal(values.Get("client_secret"), "secret")
		r.Equal(values.Get("scope"), "catalog")

		w.WriteHeader(http.StatusOK)

		json.NewEncoder(w).Encode(map[string]any{
			"access_token":      TestToken,
			"token_type":        "Bearer",
			"expires_in":        86400,
			"issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
		})
	})

	namespace := "examples"
	r.mux.HandleFunc("/v1/prefix/namespaces/"+namespace+"/tables", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}
		pageToken := req.URL.Query().Get("pageToken")
		pageSize := req.URL.Query().Get("pageSize")
		r.Equal("", pageToken)
		r.Equal(strconv.Itoa(defaultPageSize), pageSize)

		json.NewEncoder(w).Encode(map[string]any{
			"identifiers": []any{
				map[string]any{
					"namespace": []string{namespace},
					"name":      "fooshare",
				},
			},
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL,
		rest.WithPrefix("prefix"),
		rest.WithWarehouseLocation("s3://some-bucket"),
		rest.WithCredential(TestCreds))
	r.Require().NoError(err)

	r.NotNil(cat)
	r.Equal(r.configVals.Get("warehouse"), "s3://some-bucket")

	var lastErr error
	tbls := make([]table.Identifier, 0)

	iter := cat.ListTables(context.Background(), catalog.ToIdentifier(namespace))

	for tbl, err := range iter {
		tbls = append(tbls, tbl)
		if err != nil {
			lastErr = err
			r.FailNow("unexpected error:", err)
		}
	}
	r.Require().NoError(lastErr)
	r.Equal([]table.Identifier{{"examples", "fooshare"}}, tbls)
}

func (r *RestCatalogSuite) TestListTablesPagination() {
	defaultPageSize := 20
	namespace := "accounting"
	r.mux.HandleFunc("/v1/namespaces/"+namespace+"/tables", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		pageToken := req.URL.Query().Get("pageToken")
		pageSize := req.URL.Query().Get("pageSize")
		r.Equal(strconv.Itoa(defaultPageSize), pageSize)

		var response map[string]any
		if pageToken == "" {
			response = map[string]any{
				"identifiers": []any{
					map[string]any{
						"namespace": []string{namespace},
						"name":      "paid1",
					},
					map[string]any{
						"namespace": []string{namespace},
						"name":      "paid2",
					},
				},
				"next-page-token": "token1",
			}
		} else if pageToken == "token1" {
			r.Equal("token1", pageToken)
			response = map[string]any{
				"identifiers": []any{
					map[string]any{
						"namespace": []string{namespace},
						"name":      "pending1",
					},
					map[string]any{
						"namespace": []string{namespace},
						"name":      "pending2",
					},
				},
				"next-page-token": "token2",
			}
		} else {
			r.Equal("token2", pageToken)
			response = map[string]any{
				"identifiers": []any{
					map[string]any{
						"namespace": []string{namespace},
						"name":      "owned",
					},
				},
			}
		}

		json.NewEncoder(w).Encode(response)
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	var lastErr error
	tbls := make([]table.Identifier, 0)
	iter := cat.ListTables(context.Background(), catalog.ToIdentifier(namespace))
	for tbl, err := range iter {
		tbls = append(tbls, tbl)
		if err != nil {
			lastErr = err
			r.FailNow("unexpected error:", err)
		}
	}

	r.Equal([]table.Identifier{
		{"accounting", "paid1"},
		{"accounting", "paid2"},
		{"accounting", "pending1"},
		{"accounting", "pending2"},
		{"accounting", "owned"},
	}, tbls)
	r.Require().NoError(lastErr)
}

func (r *RestCatalogSuite) TestListTablesPaginationErrorOnSubsequentPage() {
	namespace := "accounting"
	r.mux.HandleFunc("/v1/namespaces/"+namespace+"/tables", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		pageToken := req.URL.Query().Get("pageToken")

		// First page succeeds
		if pageToken == "" {
			json.NewEncoder(w).Encode(map[string]any{
				"identifiers": []any{
					map[string]any{
						"namespace": []string{namespace},
						"name":      "paid1",
					},
					map[string]any{
						"namespace": []string{namespace},
						"name":      "paid2",
					},
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

	tbls := make([]table.Identifier, 0)
	var lastErr error
	iter := cat.ListTables(context.Background(), catalog.ToIdentifier(namespace))
	for tbl, err := range iter {
		if err != nil {
			lastErr = err

			break
		}
		tbls = append(tbls, tbl)
	}

	// Check that we got the views from the first page
	r.Equal([]table.Identifier{
		{"accounting", "paid1"},
		{"accounting", "paid2"},
	}, tbls)

	// Check that we got the error from the second page
	r.Error(lastErr)
	r.ErrorContains(lastErr, "Token expired or invalid")
}

func (r *RestCatalogSuite) TestListTables404() {
	namespace := "examples"
	r.mux.HandleFunc("/v1/namespaces/"+namespace+"/tables", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		pageToken := req.URL.Query().Get("pageToken")
		pageSize := req.URL.Query().Get("pageSize")
		r.Equal("", pageToken)
		r.Equal(strconv.Itoa(defaultPageSize), pageSize)
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": "Namespace does not exist: personal in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
				"type":    "NoSuchNamespaceException",
				"code":    404,
			},
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	var lastErr error
	tbls := make([]table.Identifier, 0)

	iter := cat.ListTables(context.Background(), catalog.ToIdentifier(namespace))

	for tbl, err := range iter {
		tbls = append(tbls, tbl)
		if err != nil {
			lastErr = err
		}
	}
	r.ErrorIs(lastErr, catalog.ErrNoSuchNamespace)
	r.ErrorContains(lastErr, "Namespace does not exist")
}

func (r *RestCatalogSuite) TestListNamespaces200() {
	r.mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		json.NewEncoder(w).Encode(map[string]any{
			"namespaces": []table.Identifier{
				{"default"}, {"examples"}, {"fokko"}, {"system"},
			},
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	results, err := cat.ListNamespaces(context.Background(), nil)
	r.Require().NoError(err)

	r.Equal([]table.Identifier{{"default"}, {"examples"}, {"fokko"}, {"system"}}, results)
}

func (r *RestCatalogSuite) TestListNamespaceWithParent200() {
	r.mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)
		r.Require().Equal("accounting", req.URL.Query().Get("parent"))

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		json.NewEncoder(w).Encode(map[string]any{
			"namespaces": []table.Identifier{
				{"accounting", "tax"},
			},
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	results, err := cat.ListNamespaces(context.Background(), catalog.ToIdentifier("accounting"))
	r.Require().NoError(err)

	r.Equal([]table.Identifier{{"accounting", "tax"}}, results)
}

func (r *RestCatalogSuite) TestListNamespaces400() {
	r.mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": "Namespace does not exist: personal in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
				"type":    "NoSuchNamespaceException",
				"code":    404,
			},
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	_, err = cat.ListNamespaces(context.Background(), catalog.ToIdentifier("accounting"))
	r.ErrorIs(err, catalog.ErrNoSuchNamespace)
	r.ErrorContains(err, "Namespace does not exist: personal in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e")
}

func (r *RestCatalogSuite) TestCreateNamespace200() {
	r.mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodPost, req.Method)
		r.Require().Equal("application/json", req.Header.Get("Content-Type"))

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		defer req.Body.Close()
		dec := json.NewDecoder(req.Body)
		body := struct {
			Namespace table.Identifier   `json:"namespace"`
			Props     iceberg.Properties `json:"properties"`
		}{}

		r.Require().NoError(dec.Decode(&body))
		r.Equal(table.Identifier{"leden"}, body.Namespace)
		r.Empty(body.Props)

		json.NewEncoder(w).Encode(map[string]any{
			"namespace": []string{"leden"}, "properties": map[string]any{},
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	r.Require().NoError(cat.CreateNamespace(context.Background(), catalog.ToIdentifier("leden"), nil))
}

func (r *RestCatalogSuite) TestCheckNamespaceExists204() {
	r.mux.HandleFunc("/v1/namespaces/leden", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodHead, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}
		w.WriteHeader(http.StatusNoContent)
	})
	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	exists, err := cat.CheckNamespaceExists(context.Background(), catalog.ToIdentifier("leden"))
	r.Require().NoError(err)
	r.Require().True(exists)
}

func (r *RestCatalogSuite) TestCheckNamespaceExists404() {
	r.mux.HandleFunc("/v1/namespaces/noneexistent", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodHead, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.WriteHeader(http.StatusNotFound)
		err := json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": "The given namespace does not exist",
				"type":    "NoSuchNamespaceException",
				"code":    404,
			},
		})
		if err != nil {
			return
		}
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	exists, err := cat.CheckNamespaceExists(context.Background(), catalog.ToIdentifier("noneexistent"))
	r.Require().NoError(err)
	r.False(exists)
}

func (r *RestCatalogSuite) TestCreateNamespaceWithProps200() {
	r.mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodPost, req.Method)
		r.Require().Equal("application/json", req.Header.Get("Content-Type"))

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		defer req.Body.Close()
		dec := json.NewDecoder(req.Body)
		body := struct {
			Namespace table.Identifier   `json:"namespace"`
			Props     iceberg.Properties `json:"properties"`
		}{}

		r.Require().NoError(dec.Decode(&body))
		r.Equal(table.Identifier{"leden"}, body.Namespace)
		r.Equal(iceberg.Properties{"foo": "bar", "super": "duper"}, body.Props)

		json.NewEncoder(w).Encode(map[string]any{
			"namespace": []string{"leden"}, "properties": body.Props,
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	r.Require().NoError(cat.CreateNamespace(context.Background(), catalog.ToIdentifier("leden"), iceberg.Properties{"foo": "bar", "super": "duper"}))
}

func (r *RestCatalogSuite) TestCreateNamespace409() {
	r.mux.HandleFunc("/v1/namespaces", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodPost, req.Method)
		r.Require().Equal("application/json", req.Header.Get("Content-Type"))

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		defer req.Body.Close()
		dec := json.NewDecoder(req.Body)
		body := struct {
			Namespace table.Identifier   `json:"namespace"`
			Props     iceberg.Properties `json:"properties"`
		}{}

		r.Require().NoError(dec.Decode(&body))
		r.Equal(table.Identifier{"fokko"}, body.Namespace)
		r.Empty(body.Props)

		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": "Namespace already exists: fokko in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
				"type":    "AlreadyExistsException",
				"code":    409,
			},
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	err = cat.CreateNamespace(context.Background(), catalog.ToIdentifier("fokko"), nil)
	r.ErrorIs(err, catalog.ErrNamespaceAlreadyExists)
	r.ErrorContains(err, "fokko in warehouse")
}

func (r *RestCatalogSuite) TestDropNamespace204() {
	r.mux.HandleFunc("/v1/namespaces/examples", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodDelete, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.WriteHeader(http.StatusNoContent)
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	r.NoError(cat.DropNamespace(context.Background(), catalog.ToIdentifier("examples")))
}

func (r *RestCatalogSuite) TestDropNamespace404() {
	r.mux.HandleFunc("/v1/namespaces/examples", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodDelete, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": "Namespace does not exist: examples in warehouse",
				"type":    "NoSuchNamespaceException",
				"code":    404,
			},
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	err = cat.DropNamespace(context.Background(), catalog.ToIdentifier("examples"))
	r.ErrorIs(err, catalog.ErrNoSuchNamespace)
	r.ErrorContains(err, "examples in warehouse")
}

func (r *RestCatalogSuite) TestLoadNamespaceProps200() {
	r.mux.HandleFunc("/v1/namespaces/leden", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{
			"namespace":  []string{"fokko"},
			"properties": map[string]any{"prop": "yes"},
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	props, err := cat.LoadNamespaceProperties(context.Background(), catalog.ToIdentifier("leden"))
	r.Require().NoError(err)
	r.Equal(iceberg.Properties{"prop": "yes"}, props)
}

func (r *RestCatalogSuite) TestLoadNamespaceProps404() {
	r.mux.HandleFunc("/v1/namespaces/leden", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": "Namespace does not exist: fokko22 in warehouse",
				"type":    "NoSuchNamespaceException",
				"code":    404,
			},
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	_, err = cat.LoadNamespaceProperties(context.Background(), catalog.ToIdentifier("leden"))
	r.ErrorIs(err, catalog.ErrNoSuchNamespace)
	r.ErrorContains(err, "Namespace does not exist: fokko22 in warehouse")
}

func (r *RestCatalogSuite) TestUpdateNamespaceProps200() {
	r.mux.HandleFunc("/v1/namespaces/fokko/properties", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodPost, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		json.NewEncoder(w).Encode(map[string]any{
			"removed": []string{},
			"updated": []string{"prop"},
			"missing": []string{"abc"},
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	summary, err := cat.UpdateNamespaceProperties(context.Background(), table.Identifier([]string{"fokko"}),
		[]string{"abc"}, iceberg.Properties{"prop": "yes"})
	r.Require().NoError(err)

	r.Equal(catalog.PropertiesUpdateSummary{
		Removed: []string{},
		Updated: []string{"prop"},
		Missing: []string{"abc"},
	}, summary)
}

func (r *RestCatalogSuite) TestUpdateNamespaceProps404() {
	r.mux.HandleFunc("/v1/namespaces/fokko/properties", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodPost, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": "Namespace does not exist: does_not_exist in warehouse",
				"type":    "NoSuchNamespaceException",
				"code":    404,
			},
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	_, err = cat.UpdateNamespaceProperties(context.Background(),
		table.Identifier{"fokko"}, []string{"abc"}, iceberg.Properties{"prop": "yes"})
	r.ErrorIs(err, catalog.ErrNoSuchNamespace)
	r.ErrorContains(err, "Namespace does not exist: does_not_exist in warehouse")
}

var (
	exampleTableMetadataNoSnapshotV1 = `{
	"format-version": 1,
	"table-uuid": "bf289591-dcc0-4234-ad4f-5c3eed811a29",
	"location": "s3://warehouse/database/table",
	"last-updated-ms": 1657810967051,
	"last-column-id": 3,
	"schema": {
		"type": "struct",
		"schema-id": 0,
		"identifier-field-ids": [2],
		"fields": [
			{"id": 1, "name": "foo", "required": false, "type": "string"},
			{"id": 2, "name": "bar", "required": true, "type": "int"},
			{"id": 3, "name": "baz", "required": false, "type": "boolean"}
		]
	},
	"current-schema-id": 0,
	"schemas": [
		{
			"type": "struct",
			"schema-id": 0,
			"identifier-field-ids": [2],
			"fields": [
				{"id": 1, "name": "foo", "required": false, "type": "string"},
				{"id": 2, "name": "bar", "required": true, "type": "int"},
				{"id": 3, "name": "baz", "required": false, "type": "boolean"}
			]
		}
	],
	"partition-spec": [],
	"default-spec-id": 0,
	"last-partition-id": 999,
	"default-sort-order-id": 0,
	"sort-orders": [{"order-id": 0, "fields": []}],
	"properties": {
		"write.delete.parquet.compression-codec": "zstd",
		"write.metadata.compression-codec": "gzip",
		"write.summary.partition-limit": "100",
		"write.parquet.compression-codec": "zstd"
	},
	"current-snapshot-id": -1,
	"refs": {},
	"snapshots": [],
	"snapshot-log": [],
	"metadata-log": []
}`

	createTableRestExample = fmt.Sprintf(`{
	"metadata-location": "s3://warehouse/database/table/metadata.json",
	"metadata": %s,
	"config": {
		"client.factory": "io.tabular.iceberg.catalog.TabularAwsClientFactory",
		"region": "us-west-2"
	}
}`, exampleTableMetadataNoSnapshotV1)

	tableSchemaSimple = iceberg.NewSchemaWithIdentifiers(1, []int{2},
		iceberg.NestedField{ID: 1, Name: "foo", Type: iceberg.StringType{}, Required: false},
		iceberg.NestedField{ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool, Required: false},
	)
)

func (r *RestCatalogSuite) TestCreateTable200() {
	r.mux.HandleFunc("/v1/namespaces/fokko/tables", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodPost, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.Write([]byte(createTableRestExample))
	})

	t := createTableRestExample
	_ = t
	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	tbl, err := cat.CreateTable(
		context.Background(),
		catalog.ToIdentifier("fokko", "fokko2"),
		tableSchemaSimple,
	)
	r.Require().NoError(err)

	r.Equal(catalog.ToIdentifier("fokko", "fokko2"), tbl.Identifier())
	r.Equal("s3://warehouse/database/table/metadata.json", tbl.MetadataLocation())
	r.EqualValues(1, tbl.Metadata().Version())
	r.Equal("bf289591-dcc0-4234-ad4f-5c3eed811a29", tbl.Metadata().TableUUID().String())
	r.EqualValues(1657810967051, tbl.Metadata().LastUpdatedMillis())
	r.Equal(3, tbl.Metadata().LastColumnID())
	r.Zero(tbl.Schema().ID)
	r.Zero(tbl.Metadata().DefaultPartitionSpec())
	r.Equal(999, *tbl.Metadata().LastPartitionSpecID())
	r.Equal(table.UnsortedSortOrder, tbl.SortOrder())
}

func (r *RestCatalogSuite) TestCreateTable409() {
	// Mock the create table endpoint with 409 response
	r.mux.HandleFunc("/v1/namespaces/fokko/tables", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodPost, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.WriteHeader(http.StatusConflict)
		errorResponse := map[string]interface{}{
			"error": map[string]interface{}{
				"message": "Table already exists: fokko.already_exists in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e",
				"type":    "AlreadyExistsException",
				"code":    409,
			},
		}
		json.NewEncoder(w).Encode(errorResponse)
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	// Attempt to create table with properties
	_, err = cat.CreateTable(
		context.Background(),
		catalog.ToIdentifier("fokko", "fokko2"),
		tableSchemaSimple,
		catalog.WithProperties(map[string]string{"owner": "fokko"}),
	)

	// Verify error
	r.Error(err)
	r.Contains(err.Error(), "Table already exists")
	r.ErrorIs(err, catalog.ErrTableAlreadyExists)
}

func (r *RestCatalogSuite) TestCheckTableExists204() {
	r.mux.HandleFunc("/v1/namespaces/fokko/tables/fokko2", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodHead, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}
		w.WriteHeader(http.StatusNoContent)
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	exists, err := cat.CheckTableExists(context.Background(), catalog.ToIdentifier("fokko", "fokko2"))
	r.Require().NoError(err)
	r.True(exists)
}

func (r *RestCatalogSuite) TestCheckTableExists404() {
	r.mux.HandleFunc("/v1/namespaces/fokko/tables/nonexistent", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodHead, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": "Table not found",
				"type":    "NoSuchTableException",
				"code":    http.StatusNotFound,
			},
		})
	})
	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	exists, err := cat.CheckTableExists(context.Background(), catalog.ToIdentifier("fokko", "nonexistent"))
	r.Require().NoError(err)
	r.False(exists)
}

func (r *RestCatalogSuite) TestLoadTable200() {
	r.mux.HandleFunc("/v1/namespaces/fokko/tables/table", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.Write([]byte(`{			
			"metadata-location": "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json",
			"metadata": {
				"format-version": 1,
				"table-uuid": "b55d9dda-6561-423a-8bfc-787980ce421f",
				"location": "s3://warehouse/database/table",
				"last-updated-ms": 1646787054459,
				"last-column-id": 2,
				"schema": {
					"type": "struct",
					"schema-id": 0,
					"fields": [
						{"id": 1, "name": "id", "required": false, "type": "int"},
						{"id": 2, "name": "data", "required": false, "type": "string"}
					]
				},
				"current-schema-id": 0,
				"schemas": [
					{
						"type": "struct",
						"schema-id": 0,
						"fields": [
							{"id": 1, "name": "id", "required": false, "type": "int"},
							{"id": 2, "name": "data", "required": false, "type": "string"}
						]
					}
				],
				"partition-spec": [],
				"default-spec-id": 0,
				"partition-specs": [{"spec-id": 0, "fields": []}],
				"last-partition-id": 999,
				"default-sort-order-id": 0,
				"sort-orders": [{"order-id": 0, "fields": []}],
				"properties": {"owner": "bryan", "write.metadata.compression-codec": "gzip"},
				"current-snapshot-id": 3497810964824022504,
				"refs": {"main": {"snapshot-id": 3497810964824022504, "type": "branch"}},
				"snapshots": [
					{
						"snapshot-id": 3497810964824022504,
						"timestamp-ms": 1646787054459,
						"summary": {
							"operation": "append",
							"spark.app.id": "local-1646787004168",
							"added-data-files": "1",
							"added-records": "1",
							"added-files-size": "697",
							"changed-partition-count": "1",
							"total-records": "1",
							"total-files-size": "697",
							"total-data-files": "1",
							"total-delete-files": "0",
							"total-position-deletes": "0",
							"total-equality-deletes": "0"
						},
						"manifest-list": "s3://warehouse/database/table/metadata/snap-3497810964824022504-1-c4f68204-666b-4e50-a9df-b10c34bf6b82.avro",
						"schema-id": 0
					}
				],
				"snapshot-log": [{"timestamp-ms": 1646787054459, "snapshot-id": 3497810964824022504}],
				"metadata-log": [
					{
						"timestamp-ms": 1646787031514,
						"metadata-file": "s3://warehouse/database/table/metadata/00000-88484a1c-00e5-4a07-a787-c0e7aeffa805.gz.metadata.json"
					}
				]
			}
		}`))
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	tbl, err := cat.LoadTable(context.Background(), catalog.ToIdentifier("fokko", "table"))
	r.Require().NoError(err)

	r.Equal(catalog.ToIdentifier("fokko", "table"), tbl.Identifier())
	r.Equal("s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json", tbl.MetadataLocation())
	r.EqualValues(1, tbl.Metadata().Version())
	r.Equal("b55d9dda-6561-423a-8bfc-787980ce421f", tbl.Metadata().TableUUID().String())
	r.EqualValues(1646787054459, tbl.Metadata().LastUpdatedMillis())
	r.Equal(2, tbl.Metadata().LastColumnID())
	r.Zero(tbl.Schema().ID)
	r.Zero(tbl.Metadata().DefaultPartitionSpec())
	r.Equal(999, *tbl.Metadata().LastPartitionSpecID())
	r.Equal(table.UnsortedSortOrder, tbl.SortOrder())
	r.EqualValues(3497810964824022504, tbl.CurrentSnapshot().SnapshotID)
	zero := 0
	r.True(tbl.SnapshotByName("main").Equals(table.Snapshot{
		SnapshotID:   3497810964824022504,
		TimestampMs:  1646787054459,
		SchemaID:     &zero,
		ManifestList: "s3://warehouse/database/table/metadata/snap-3497810964824022504-1-c4f68204-666b-4e50-a9df-b10c34bf6b82.avro",
		Summary: &table.Summary{
			Operation: table.OpAppend,
			Properties: map[string]string{
				"spark.app.id":            "local-1646787004168",
				"added-data-files":        "1",
				"added-records":           "1",
				"added-files-size":        "697",
				"changed-partition-count": "1",
				"total-records":           "1",
				"total-files-size":        "697",
				"total-data-files":        "1",
				"total-delete-files":      "0",
				"total-position-deletes":  "0",
				"total-equality-deletes":  "0",
			},
		},
	}))
}

func (r *RestCatalogSuite) TestRenameTable200() {
	// Mock the rename table endpoint
	r.mux.HandleFunc("/v1/tables/rename", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodPost, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		var payload struct {
			Source struct {
				Namespace []string `json:"namespace"`
				Name      string   `json:"name"`
			} `json:"source"`
			Destination struct {
				Namespace []string `json:"namespace"`
				Name      string   `json:"name"`
			} `json:"destination"`
		}
		r.NoError(json.NewDecoder(req.Body).Decode(&payload))
		r.Equal([]string{"fokko"}, payload.Source.Namespace)
		r.Equal("source", payload.Source.Name)
		r.Equal([]string{"fokko"}, payload.Destination.Namespace)
		r.Equal("destination", payload.Destination.Name)

		w.WriteHeader(http.StatusOK)
	})

	// Mock the get table endpoint for loading the renamed table
	r.mux.HandleFunc("/v1/namespaces/fokko/tables/destination", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.Write([]byte(createTableRestExample))
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	fromIdent := catalog.ToIdentifier("fokko", "source")
	toIdent := catalog.ToIdentifier("fokko", "destination")

	renamedTable, err := cat.RenameTable(context.Background(), fromIdent, toIdent)
	r.Require().NoError(err)

	r.Equal(catalog.ToIdentifier("fokko", "destination"), renamedTable.Identifier())
	r.Equal("s3://warehouse/database/table/metadata.json", renamedTable.MetadataLocation())
	r.EqualValues(1, renamedTable.Metadata().Version())
	r.Equal("bf289591-dcc0-4234-ad4f-5c3eed811a29", renamedTable.Metadata().TableUUID().String())
	r.EqualValues(1657810967051, renamedTable.Metadata().LastUpdatedMillis())
	r.Equal(3, renamedTable.Metadata().LastColumnID())
	r.Zero(renamedTable.Schema().ID)
	r.Zero(renamedTable.Metadata().DefaultPartitionSpec())
	r.Equal(999, *renamedTable.Metadata().LastPartitionSpecID())
	r.Equal(table.UnsortedSortOrder, renamedTable.SortOrder())
}

func (r *RestCatalogSuite) TestDropTable204() {
	// Mock the drop table endpoint
	r.mux.HandleFunc("/v1/namespaces/fokko/tables/table", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodDelete, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		// Return 204 No Content for successful deletion
		w.WriteHeader(http.StatusNoContent)
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	err = cat.DropTable(context.Background(), catalog.ToIdentifier("fokko", "table"))
	r.NoError(err)
}

func (r *RestCatalogSuite) TestDropTable404() {
	// Mock the drop table endpoint with 404 response
	r.mux.HandleFunc("/v1/namespaces/fokko/tables/table", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodDelete, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.WriteHeader(http.StatusNotFound)
		errorResponse := map[string]interface{}{
			"error": map[string]interface{}{
				"message": "Table does not exist: fokko.table",
				"type":    "NoSuchTableException",
				"code":    404,
			},
		}
		json.NewEncoder(w).Encode(errorResponse)
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	err = cat.DropTable(context.Background(), catalog.ToIdentifier("fokko", "table"))
	r.Error(err)
	r.ErrorIs(err, catalog.ErrNoSuchTable)
	r.ErrorContains(err, "Table does not exist: fokko.table")
}

func (r *RestCatalogSuite) TestRegisterTable200() {
	r.mux.HandleFunc("/v1/namespaces/fokko/tables/fokko2", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodPost, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		var payload struct {
			Name        string `json:"name"`
			MetadataLoc string `json:"metadata-location"`
		}
		r.NoError(json.NewDecoder(req.Body).Decode(&payload))
		r.Equal("fokko2", payload.Name)
		r.Equal("s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json", payload.MetadataLoc)

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
  "metadata-location": "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json",
  "metadata": {
    "format-version": 1,
    "table-uuid": "d55d9dda-6561-423a-8bfc-787980ce421f",
    "location": "s3://warehouse/database/table",
    "last-updated-ms": 1646787054459,
    "last-column-id": 2,
    "schema": {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {
          "id": 1,
          "name": "id",
          "required": false,
          "type": "int"
        },
        {
          "id": 2,
          "name": "data",
          "required": false,
          "type": "string"
        }
      ]
    },
    "current-schema-id": 0,
    "schemas": [
      {
        "type": "struct",
        "schema-id": 0,
        "fields": [
          {
            "id": 1,
            "name": "id",
            "required": false,
            "type": "int"
          },
          {
            "id": 2,
            "name": "data",
            "required": false,
            "type": "string"
          }
        ]
      }
    ],
    "partition-spec": [
      
    ],
    "default-spec-id": 0,
    "partition-specs": [
      {
        "spec-id": 0,
        "fields": [
          
        ]
      }
    ],
    "last-partition-id": 999,
    "default-sort-order-id": 0,
    "sort-orders": [
      {
        "order-id": 0,
        "fields": [
          
        ]
      }
    ],
    "properties": {
      "owner": "bryan",
      "write.metadata.compression-codec": "gzip"
    },
    "current-snapshot-id": 3497810964824022504,
    "refs": {
      "main": {
        "snapshot-id": 3497810964824022504,
        "type": "branch"
      }
    },
    "snapshots": [
      {
        "snapshot-id": 3497810964824022504,
        "timestamp-ms": 1646787054459,
        "summary": {
          "operation": "append",
          "spark.app.id": "local-1646787004168",
          "added-data-files": "1",
          "added-records": "1",
          "added-files-size": "697",
          "changed-partition-count": "1",
          "total-records": "1",
          "total-files-size": "697",
          "total-data-files": "1",
          "total-delete-files": "0",
          "total-position-deletes": "0",
          "total-equality-deletes": "0"
        },
        "manifest-list": "s3://warehouse/database/table/metadata/snap-3497810964824022504-1-c4f68204-666b-4e50-a9df-b10c34bf6b82.avro",
        "schema-id": 0
      }
    ],
    "snapshot-log": [
      {
        "timestamp-ms": 1646787054459,
        "snapshot-id": 3497810964824022504
      }
    ],
    "metadata-log": [
      {
        "timestamp-ms": 1646787031514,
        "metadata-file": "s3://warehouse/database/table/metadata/00000-88484a1c-00e5-4a07-a787-c0e7aeffa805.gz.metadata.json"
      }
    ]
  }
}`))
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	tbl, err := cat.RegisterTable(context.Background(), catalog.ToIdentifier("fokko", "fokko2"), "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json")
	r.Require().NoError(err)

	r.Equal(catalog.ToIdentifier("fokko", "fokko2"), tbl.Identifier())
	r.Equal("s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json", tbl.MetadataLocation())
	r.EqualValues(1, tbl.Metadata().Version())
	r.Equal("d55d9dda-6561-423a-8bfc-787980ce421f", tbl.Metadata().TableUUID().String())
	r.Equal("bryan", tbl.Metadata().Properties()["owner"])
}

func (r *RestCatalogSuite) TestRegisterTable404() {
	r.mux.HandleFunc("/v1/namespaces/nonexistent/tables/fokko2", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodPost, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		var payload struct {
			Name        string `json:"name"`
			MetadataLoc string `json:"metadata-location"`
		}

		r.NoError(json.NewDecoder(req.Body).Decode(&payload))
		r.Equal("fokko2", payload.Name)
		r.Equal("s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json", payload.MetadataLoc)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{
			  "error": {
				"message": "The given namespace does not exist",
			    "type": "NoSuchNamespaceException",
			    "code": 404
			  }
			}`))
	})
	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	_, err = cat.RegisterTable(context.Background(), catalog.ToIdentifier("nonexistent", "fokko2"), "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json")
	r.ErrorIs(err, catalog.ErrNoSuchNamespace)
	r.ErrorContains(err, "The given namespace does not exist")
}

func (r *RestCatalogSuite) TestRegisterTable409() {
	r.mux.HandleFunc("/v1/namespaces/fokko/tables/alreadyexist", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodPost, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		var payload struct {
			Name        string `json:"name"`
			MetadataLoc string `json:"metadata-location"`
		}

		r.NoError(json.NewDecoder(req.Body).Decode(&payload))
		r.Equal("alreadyexist", payload.Name)
		r.Equal("s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json", payload.MetadataLoc)
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte(`{
			"error": {
				"message": "The given table already exists",
				"type": "AlreadyExistsException",
				"code": 409
			}
		}`))
	})
	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	_, err = cat.RegisterTable(context.Background(), catalog.ToIdentifier("fokko", "alreadyexist"), "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json")
	r.ErrorIs(err, catalog.ErrTableAlreadyExists)
	r.ErrorContains(err, "The given table already exists")
}

func (r *RestCatalogSuite) TestListViews200() {
	customPageSize := 100
	namespace := "accounting"
	r.mux.HandleFunc("/v1/namespaces/"+namespace+"/views", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		pageToken := req.URL.Query().Get("pageToken")
		pageSize := req.URL.Query().Get("pageSize")
		r.Equal("", pageToken)
		r.Equal(strconv.Itoa(customPageSize), pageSize)

		json.NewEncoder(w).Encode(map[string]any{
			"identifiers": []any{
				map[string]any{
					"namespace": []string{"accounting", "tax"},
					"name":      "paid",
				},
				map[string]any{
					"namespace": []string{"accounting", "tax"},
					"name":      "owed",
				},
			},
		})
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)
	// Passing in a custom page size through context
	ctx := cat.SetPageSize(context.Background(), customPageSize)

	var lastErr error
	views := make([]table.Identifier, 0)
	iter := cat.ListViews(ctx, catalog.ToIdentifier(namespace))

	for view, err := range iter {
		views = append(views, view)
		if err != nil {
			lastErr = err
			r.FailNow("unexpected error:", err)
		}
	}

	r.Equal([]table.Identifier{
		{"accounting", "tax", "paid"},
		{"accounting", "tax", "owed"},
	}, views)
	r.Require().NoError(lastErr)
}

func (r *RestCatalogSuite) TestListViewsPagination() {
	defaultPageSize := 20
	namespace := "accounting"
	r.mux.HandleFunc("/v1/namespaces/"+namespace+"/views", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		pageToken := req.URL.Query().Get("pageToken")
		pageSize := req.URL.Query().Get("pageSize")
		r.Equal(strconv.Itoa(defaultPageSize), pageSize)

		var response map[string]any
		if pageToken == "" {
			response = map[string]any{
				"identifiers": []any{
					map[string]any{
						"namespace": []string{"accounting", "tax"},
						"name":      "paid1",
					},
					map[string]any{
						"namespace": []string{"accounting", "tax"},
						"name":      "paid2",
					},
				},
				"next-page-token": "token1",
			}
		} else if pageToken == "token1" {
			r.Equal("token1", pageToken)
			response = map[string]any{
				"identifiers": []any{
					map[string]any{
						"namespace": []string{"accounting", "tax"},
						"name":      "pending1",
					},
					map[string]any{
						"namespace": []string{"accounting", "tax"},
						"name":      "pending2",
					},
				},
				"next-page-token": "token2",
			}
		} else {
			r.Equal("token2", pageToken)
			response = map[string]any{
				"identifiers": []any{
					map[string]any{
						"namespace": []string{"accounting", "tax"},
						"name":      "owned",
					},
				},
			}
		}

		json.NewEncoder(w).Encode(response)
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	var lastErr error
	views := make([]table.Identifier, 0)
	iter := cat.ListViews(context.Background(), catalog.ToIdentifier(namespace))
	for view, err := range iter {
		views = append(views, view)
		if err != nil {
			lastErr = err
			r.FailNow("unexpected error:", err)
		}
	}

	r.Equal([]table.Identifier{
		{"accounting", "tax", "paid1"},
		{"accounting", "tax", "paid2"},
		{"accounting", "tax", "pending1"},
		{"accounting", "tax", "pending2"},
		{"accounting", "tax", "owned"},
	}, views)
	r.Require().NoError(lastErr)
}

func (r *RestCatalogSuite) TestListViewsPaginationErrorOnSubsequentPage() {
	namespace := "accounting"
	r.mux.HandleFunc("/v1/namespaces/"+namespace+"/views", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		pageToken := req.URL.Query().Get("pageToken")

		// First page succeeds
		if pageToken == "" {
			json.NewEncoder(w).Encode(map[string]any{
				"identifiers": []any{
					map[string]any{
						"namespace": []string{"accounting", "tax"},
						"name":      "paid1",
					},
					map[string]any{
						"namespace": []string{"accounting", "tax"},
						"name":      "paid2",
					},
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

	views := make([]table.Identifier, 0)
	var lastErr error
	iter := cat.ListViews(context.Background(), catalog.ToIdentifier(namespace))
	for view, err := range iter {
		if err != nil {
			lastErr = err

			break
		}
		views = append(views, view)
	}

	// Check that we got the views from the first page
	r.Equal([]table.Identifier{
		{"accounting", "tax", "paid1"},
		{"accounting", "tax", "paid2"},
	}, views)

	// Check that we got the error from the second page
	r.Error(lastErr)
	r.ErrorContains(lastErr, "Token expired or invalid")
}

func (r *RestCatalogSuite) TestListViews404() {
	namespace := "nonexistent"
	r.mux.HandleFunc("/v1/namespaces/"+namespace+"/views", func(w http.ResponseWriter, req *http.Request) {
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
	iter := cat.ListViews(context.Background(), catalog.ToIdentifier(namespace))
	views := make([]table.Identifier, 0)
	for view, err := range iter {
		views = append(views, view)
		if err != nil {
			lastErr = err
		}
	}
	r.ErrorIs(lastErr, catalog.ErrNoSuchNamespace)
	r.ErrorContains(lastErr, "The given namespace does not exist")
}

func (r *RestCatalogSuite) TestDropView204() {
	r.mux.HandleFunc("/v1/namespaces/fokko/views/fokko2", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodDelete, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.WriteHeader(http.StatusNoContent)
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	err = cat.DropView(context.Background(), catalog.ToIdentifier("fokko", "fokko2"))
	r.NoError(err)
}

func (r *RestCatalogSuite) TestDropView404() {
	r.mux.HandleFunc("/v1/namespaces/fokko/views/nonexistent", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodDelete, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.WriteHeader(http.StatusNotFound)
		errorResponse := map[string]interface{}{
			"error": map[string]interface{}{
				"message": "The given view does not exist",
				"type":    "NoSuchViewException",
				"code":    404,
			},
		}
		json.NewEncoder(w).Encode(errorResponse)
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	err = cat.DropView(context.Background(), catalog.ToIdentifier("fokko", "nonexistent"))
	r.Error(err)
	r.ErrorIs(err, catalog.ErrNoSuchView)
	r.ErrorContains(err, "The given view does not exist")
}

func (r *RestCatalogSuite) TestCheckViewExists204() {
	r.mux.HandleFunc("/v1/namespaces/fokko/views/fokko2", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodHead, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}
		w.WriteHeader(http.StatusNoContent)
	})
	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	exists, err := cat.CheckViewExists(context.Background(), catalog.ToIdentifier("fokko", "fokko2"))
	r.Require().NoError(err)
	r.Require().True(exists)
}

func (r *RestCatalogSuite) TestCheckViewExists404() {
	r.mux.HandleFunc("/v1/namespaces/fokko/views/nonexistent", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodHead, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.WriteHeader(http.StatusNotFound)
		err := json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": "The given view does not exist",
				"type":    "NoSuchViewException",
				"code":    404,
			},
		})
		if err != nil {
			return
		}
	})

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	exists, err := cat.CheckViewExists(context.Background(), catalog.ToIdentifier("fokko", "nonexistent"))
	r.Require().NoError(err)
	r.False(exists)
}

type RestTLSCatalogSuite struct {
	suite.Suite

	srv *httptest.Server
	mux *http.ServeMux

	configVals url.Values
}

func (r *RestTLSCatalogSuite) SetupTest() {
	r.mux = http.NewServeMux()

	r.mux.HandleFunc("/v1/config", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)
		r.configVals = req.URL.Query()

		json.NewEncoder(w).Encode(map[string]any{
			"defaults":  map[string]any{},
			"overrides": map[string]any{},
		})
	})

	r.srv = httptest.NewTLSServer(r.mux)
}

func (r *RestTLSCatalogSuite) TearDownTest() {
	r.srv.Close()
	r.srv = nil
	r.mux = nil
	r.configVals = nil
}

func (r *RestTLSCatalogSuite) TestSSLFail() {
	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken))
	r.Nil(cat)

	r.ErrorContains(err, "tls: failed to verify certificate")
}

func (r *RestTLSCatalogSuite) TestSSLLoadRegisteredCatalog() {
	ctx := context.Background()
	cat, err := catalog.Load(ctx, "foobar", iceberg.Properties{
		"uri":                  r.srv.URL,
		"warehouse":            "s3://some-bucket",
		"token":                TestToken,
		"rest.tls.skip-verify": "true",
	})
	r.NoError(err)

	r.NotNil(cat)
	r.Equal(r.configVals.Get("warehouse"), "s3://some-bucket")
}

func (r *RestTLSCatalogSuite) TestSSLConfig() {
	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken),
		rest.WithWarehouseLocation("s3://some-bucket"),
		rest.WithTLSConfig(&tls.Config{InsecureSkipVerify: true}))
	r.NoError(err)

	r.NotNil(cat)
	r.Equal(r.configVals.Get("warehouse"), "s3://some-bucket")
}

func (r *RestTLSCatalogSuite) TestSSLCerts() {
	certs := x509.NewCertPool()
	for _, c := range r.srv.TLS.Certificates {
		roots, err := x509.ParseCertificates(c.Certificate[len(c.Certificate)-1])
		r.Require().NoError(err)
		for _, root := range roots {
			certs.AddCert(root)
		}
	}

	cat, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL, rest.WithOAuthToken(TestToken),
		rest.WithWarehouseLocation("s3://some-bucket"),
		rest.WithTLSConfig(&tls.Config{RootCAs: certs}))
	r.NoError(err)

	r.NotNil(cat)
	r.Equal(r.configVals.Get("warehouse"), "s3://some-bucket")
}

func TestRestCatalog(t *testing.T) {
	suite.Run(t, new(RestCatalogSuite))
	suite.Run(t, new(RestTLSCatalogSuite))
}

type errorResponse struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    int    `json:"code"`
}

type createViewRequest struct {
	Name        string             `json:"name"`
	Schema      *iceberg.Schema    `json:"schema"`
	SQL         string             `json:"sql"`
	Props       iceberg.Properties `json:"properties"`
	ViewVersion struct {
		VersionID       int               `json:"version-id"`
		TimestampMs     int64             `json:"timestamp-ms"`
		SchemaID        int               `json:"schema-id"`
		Summary         map[string]string `json:"summary"`
		Representations []struct {
			Type    string `json:"type"`
			SQL     string `json:"sql"`
			Dialect string `json:"dialect"`
		} `json:"representations"`
		DefaultCatalog   string   `json:"default-catalog"`
		DefaultNamespace []string `json:"default-namespace"`
	} `json:"view-version"`
}

type viewResponse struct {
	MetadataLoc string             `json:"metadata-location"`
	Config      iceberg.Properties `json:"config"`
}

func (r *RestCatalogSuite) TestCreateView200() {
	ns := "ns"
	view := "view"
	identifier := table.Identifier{ns, view}
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID:       1,
		Name:     "id",
		Type:     iceberg.PrimitiveTypes.Int32,
		Required: true,
	})
	sql := "SELECT * FROM table"
	viewVersionJSON, _ := json.Marshal(map[string]interface{}{
		"version-id":   1,
		"timestamp-ms": time.Now().UnixMilli(),
		"schema-id":    schema.ID,
		"summary":      map[string]string{"sql": sql},
		"representations": []map[string]string{
			{"type": "sql", "sql": sql, "dialect": "default"},
		},
		"default-catalog":   "default-catalog",
		"default-namespace": []string{ns},
	})
	props := iceberg.Properties{
		"comment":      "Example view created via REST catalog",
		"owner":        "admin",
		"view-version": string(viewVersionJSON),
		"view-format":  "iceberg",
		"view-sql":     sql,
	}
	r.mux.HandleFunc("/v1/namespaces/"+ns+"/views", func(w http.ResponseWriter, req *http.Request) {
		r.Equal(http.MethodPost, req.Method)
		r.Equal("application/json", req.Header.Get("Content-Type"))

		var payload createViewRequest
		err := json.NewDecoder(req.Body).Decode(&payload)
		r.NoError(err)
		r.Equal(view, payload.Name)
		r.Equal(sql, payload.SQL)
		r.Equal(schema.ID, payload.Schema.ID)
		r.Equal(1, payload.ViewVersion.VersionID)
		r.Equal(0, payload.ViewVersion.SchemaID)
		r.Equal("sql", payload.ViewVersion.Representations[0].Type)
		r.Equal(sql, payload.ViewVersion.Representations[0].SQL)
		r.Equal("default", payload.ViewVersion.Representations[0].Dialect)
		r.Equal("rest", payload.ViewVersion.DefaultCatalog)
		r.Equal([]string{ns}, payload.ViewVersion.DefaultNamespace)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(viewResponse{
			MetadataLoc: "metadata-location",
			Config:      iceberg.Properties{},
		})
	})

	ctlg, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL)
	r.NoError(err)

	err = ctlg.CreateView(context.Background(), identifier, schema, sql, props)
	r.NoError(err)
}

func (r *RestCatalogSuite) TestCreateView409() {
	ns := "ns"
	view := "view"
	identifier := table.Identifier{ns, view}
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID:       1,
		Name:     "id",
		Type:     iceberg.PrimitiveTypes.Int32,
		Required: true,
	})
	sql := "SELECT * FROM table"

	r.mux.HandleFunc("/v1/namespaces/"+ns+"/views", func(w http.ResponseWriter, req *http.Request) {
		r.Equal(http.MethodPost, req.Method)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(errorResponse{
			Message: "The given view already exists",
			Type:    "AlreadyExistsException",
			Code:    409,
		})
	})

	ctlg, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL)
	r.NoError(err)

	err = ctlg.CreateView(context.Background(), identifier, schema, sql, nil)
	r.Error(err)
	r.ErrorIs(err, catalog.ErrViewAlreadyExists)
}

func (r *RestCatalogSuite) TestCreateView404() {
	ns := "ns"
	view := "view"
	identifier := table.Identifier{ns, view}
	schema := iceberg.NewSchema(1, iceberg.NestedField{
		ID:       1,
		Name:     "id",
		Type:     iceberg.PrimitiveTypes.Int32,
		Required: true,
	})
	sql := "SELECT * FROM table"

	r.mux.HandleFunc("/v1/namespaces/"+ns+"/views", func(w http.ResponseWriter, req *http.Request) {
		r.Equal(http.MethodPost, req.Method)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(errorResponse{
			Message: "namespace does not exist",
			Type:    "NoSuchNamespaceException",
			Code:    404,
		})
	})

	ctlg, err := rest.NewCatalog(context.Background(), "rest", r.srv.URL)
	r.NoError(err)

	err = ctlg.CreateView(context.Background(), identifier, schema, sql, nil)
	r.Error(err)
	r.ErrorIs(err, catalog.ErrNoSuchNamespace)
}
