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

package nessie_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/nessie"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/suite"
)

const (
	TestCreds = "client:secret"
	TestToken = "some_jwt_token"
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

type NessieCatalogSuite struct {
	suite.Suite

	srv *httptest.Server
	mux *http.ServeMux

	configVals url.Values
}

func (r *NessieCatalogSuite) SetupTest() {
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

func (r *NessieCatalogSuite) TearDownTest() {
	r.srv.Close()
	r.srv = nil
	r.mux = nil
	r.configVals = nil
}

func (r *NessieCatalogSuite) TestToken200() {
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

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL,
		nessie.WithWarehouseLocation("s3://some-bucket"),
		nessie.WithCredential(TestCreds),
		nessie.WithScope(scope))
	r.Require().NoError(err)

	r.NotNil(cat)
	r.Equal(r.configVals.Get("warehouse"), "s3://some-bucket")
}

func (r *NessieCatalogSuite) TestLoadRegisteredCatalog() {
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

	cat, err := catalog.Load(ctx, "nessie", iceberg.Properties{
		"uri":        r.srv.URL,
		"warehouse":  "s3://some-bucket",
		"credential": TestCreds,
	})
	r.NoError(err)

	r.NotNil(cat)
	r.Equal(r.configVals.Get("warehouse"), "s3://some-bucket")
}

func (r *NessieCatalogSuite) TestToken400() {
	r.mux.HandleFunc("/v1/oauth/tokens", func(w http.ResponseWriter, req *http.Request) {
		r.Equal(http.MethodPost, req.Method)

		r.Equal(req.Header.Get("Content-Type"), "application/x-www-form-urlencoded")

		w.WriteHeader(http.StatusBadRequest)

		json.NewEncoder(w).Encode(map[string]any{
			"error":             "invalid_client",
			"error_description": "credentials for key invalid_key do not match",
		})
	})

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithCredential(TestCreds))
	r.Nil(cat)

	r.ErrorIs(err, nessie.ErrRESTError)
	r.ErrorIs(err, nessie.ErrOAuthError)
	r.ErrorContains(err, "invalid_client: credentials for key invalid_key do not match")
}

func (r *NessieCatalogSuite) TestToken200AuthUrl() {
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
	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL,
		nessie.WithWarehouseLocation("s3://some-bucket"),
		nessie.WithCredential(TestCreds), nessie.WithAuthURI(authUri.JoinPath("auth-token-url")))

	r.Require().NoError(err)

	r.NotNil(cat)
	r.Equal(r.configVals.Get("warehouse"), "s3://some-bucket")
}

func (r *NessieCatalogSuite) TestToken401() {
	r.mux.HandleFunc("/v1/oauth/tokens", func(w http.ResponseWriter, req *http.Request) {
		r.Equal(http.MethodPost, req.Method)

		r.Equal(req.Header.Get("Content-Type"), "application/x-www-form-urlencoded")

		w.WriteHeader(http.StatusUnauthorized)

		json.NewEncoder(w).Encode(map[string]any{
			"error":             "invalid_client",
			"error_description": "credentials for key invalid_key do not match",
		})
	})

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithCredential(TestCreds))
	r.Nil(cat)

	r.ErrorIs(err, nessie.ErrRESTError)
	r.ErrorIs(err, nessie.ErrOAuthError)
	r.ErrorContains(err, "invalid_client: credentials for key invalid_key do not match")
}

func (r *NessieCatalogSuite) TestListTables200() {
	namespace := "examples"
	r.mux.HandleFunc("/v1/namespaces/"+namespace+"/tables", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		json.NewEncoder(w).Encode(map[string]any{
			"identifiers": []any{
				map[string]any{
					"namespace": []string{namespace},
					"name":      "fooshare",
				},
			},
		})
	})

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	tables, err := cat.ListTables(context.Background(), catalog.ToIdentifier(namespace))
	r.Require().NoError(err)
	r.Equal([]table.Identifier{{"examples", "fooshare"}}, tables)
}

func (r *NessieCatalogSuite) TestListTablesPrefixed200() {
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

		json.NewEncoder(w).Encode(map[string]any{
			"identifiers": []any{
				map[string]any{
					"namespace": []string{namespace},
					"name":      "fooshare",
				},
			},
		})
	})

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL,
		nessie.WithPrefix("prefix"),
		nessie.WithWarehouseLocation("s3://some-bucket"),
		nessie.WithCredential(TestCreds))
	r.Require().NoError(err)

	r.NotNil(cat)
	r.Equal(r.configVals.Get("warehouse"), "s3://some-bucket")

	tables, err := cat.ListTables(context.Background(), catalog.ToIdentifier(namespace))
	r.Require().NoError(err)
	r.Equal([]table.Identifier{{"examples", "fooshare"}}, tables)
}

func (r *NessieCatalogSuite) TestListTables404() {
	namespace := "examples"
	r.mux.HandleFunc("/v1/namespaces/"+namespace+"/tables", func(w http.ResponseWriter, req *http.Request) {
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

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	_, err = cat.ListTables(context.Background(), catalog.ToIdentifier(namespace))
	r.ErrorIs(err, catalog.ErrNoSuchNamespace)
	r.ErrorContains(err, "Namespace does not exist: personal in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e")
}

func (r *NessieCatalogSuite) TestListNamespaces200() {
	r.mux.HandleFunc("/v1/namespaces/main", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		type namespaceItem struct {
			Type     string   `json:"type"`
			ID       string   `json:"id"`
			Elements []string `json:"elements"`
		}

		type rsptype struct {
			Namespaces []namespaceItem `json:"namespaces"`
		}

		json.NewEncoder(w).Encode(rsptype{
			Namespaces: []namespaceItem{
				{
					Type:     "NAMESPACE",
					ID:       "default",
					Elements: []string{"default"},
				},
				{
					Type:     "NAMESPACE",
					ID:       "examples",
					Elements: []string{"examples"},
				},
				{
					Type:     "NAMESPACE",
					ID:       "fokko",
					Elements: []string{"fokko"},
				},
				{
					Type:     "NAMESPACE",
					ID:       "system",
					Elements: []string{"system"},
				},
			},
		})
	})

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	ctx := iceberg.WithRef(context.Background(), "main")
	results, err := cat.ListNamespaces(ctx, nil)
	r.Require().NoError(err)

	r.Equal([]table.Identifier{{"default"}, {"examples"}, {"fokko"}, {"system"}}, results)
}

func (r *NessieCatalogSuite) TestListNamespaceWithParent200() {
	r.mux.HandleFunc("/v1/namespaces/", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)
		r.Require().Equal("accounting", req.URL.Query().Get("parent"))

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		json.NewEncoder(w).Encode(map[string]any{
			"namespaces": []map[string]any{
				{
					"type":     "NAMESPACE",
					"id":       "accounting.tax",
					"elements": []string{"accounting", "tax"},
				},
			},
		})
	})

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	results, err := cat.ListNamespaces(context.Background(), catalog.ToIdentifier("accounting"))
	r.Require().NoError(err)

	r.Equal([]table.Identifier{{"accounting", "tax"}}, results)
}

func (r *NessieCatalogSuite) TestListNamespaces400() {
	r.mux.HandleFunc("/v1/namespaces/main", func(w http.ResponseWriter, req *http.Request) {
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

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	ctx := iceberg.WithRef(context.Background(), "main")
	_, err = cat.ListNamespaces(ctx, catalog.ToIdentifier("accounting"))
	r.ErrorIs(err, catalog.ErrNoSuchNamespace)
	r.ErrorContains(err, "Namespace does not exist: personal in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e")
}

func (r *NessieCatalogSuite) TestCreateNamespace200() {
	r.mux.HandleFunc("/v1/namespaces/namespace/main/leden", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodPut, req.Method)
		r.Require().Equal("application/json", req.Header.Get("Content-Type"))

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		defer req.Body.Close()
		dec := json.NewDecoder(req.Body)
		body := struct {
			Type       string             `json:"type"`
			Elements   table.Identifier   `json:"elements"`
			Properties iceberg.Properties `json:"properties"`
		}{}

		r.Require().NoError(dec.Decode(&body))

		r.Equal("NAMESPACE", body.Type)
		r.Equal(table.Identifier{"leden"}, body.Elements)
		r.Empty(body.Properties)

		json.NewEncoder(w).Encode(map[string]any{
			"namespace": []string{"leden"}, "properties": map[string]any{},
		})
	})

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	ctx := iceberg.WithRef(context.Background(), "main")

	r.Require().NoError(cat.CreateNamespace(ctx, catalog.ToIdentifier("leden"), nil))
}

func (r *NessieCatalogSuite) TestCheckNamespaceExists204() {
	r.mux.HandleFunc("/v1/namespaces/leden", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodHead, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}
		w.WriteHeader(http.StatusNoContent)
	})
	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	exists, err := cat.CheckNamespaceExists(context.Background(), catalog.ToIdentifier("leden"))
	r.Require().NoError(err)
	r.Require().True(exists)
}

func (r *NessieCatalogSuite) TestCheckNamespaceExists404() {
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

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	exists, err := cat.CheckNamespaceExists(context.Background(), catalog.ToIdentifier("noneexistent"))
	r.Require().NoError(err)
	r.False(exists)
}

func (r *NessieCatalogSuite) TestCreateNamespaceWithProps200() {
	r.mux.HandleFunc("/v1/namespaces/namespace/main/leden", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodPut, req.Method)
		r.Require().Equal("application/json", req.Header.Get("Content-Type"))

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		defer req.Body.Close()
		dec := json.NewDecoder(req.Body)
		body := struct {
			Type       string             `json:"type"`
			Elements   []string           `json:"elements"`
			Properties iceberg.Properties `json:"properties"`
		}{}

		r.Require().NoError(dec.Decode(&body))
		r.Equal("NAMESPACE", body.Type)
		r.Equal([]string{"leden"}, body.Elements)
		r.Equal(iceberg.Properties{"foo": "bar", "super": "duper"}, body.Properties)

		json.NewEncoder(w).Encode(map[string]any{
			"namespace": []string{"leden"}, "properties": body.Properties,
		})
	})

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	ctx := iceberg.WithRef(context.Background(), "main")

	r.Require().NoError(cat.CreateNamespace(ctx, catalog.ToIdentifier("leden"), iceberg.Properties{"foo": "bar", "super": "duper"}))
}

func (r *NessieCatalogSuite) TestCreateNamespace409() {
	r.mux.HandleFunc("/v1/namespaces/namespace/main/fokko", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodPut, req.Method)
		r.Require().Equal("application/json", req.Header.Get("Content-Type"))

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		defer req.Body.Close()
		dec := json.NewDecoder(req.Body)
		body := struct {
			Type       string             `json:"type"`
			Elements   []string           `json:"elements"`
			Properties iceberg.Properties `json:"properties"`
		}{}

		r.Require().NoError(dec.Decode(&body))
		r.Equal("NAMESPACE", body.Type)
		r.Equal([]string{"fokko"}, body.Elements)
		r.Empty(body.Properties)

		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]any{
			"message": "Namespace already exists: fokko in warehouse",
			"type":    "AlreadyExistsException",
			"code":    409,
		})
	})

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	ctx := iceberg.WithRef(context.Background(), "main")
	err = cat.CreateNamespace(ctx, catalog.ToIdentifier("fokko"), nil)
	r.ErrorIs(err, catalog.ErrNamespaceAlreadyExists)
	r.ErrorContains(err, "fokko in warehouse")
}

func (r *NessieCatalogSuite) TestDropNamespace204() {
	r.mux.HandleFunc("/v1/namespaces/namespace/main/examples", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodDelete, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.WriteHeader(http.StatusNoContent)
	})

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	ctx := iceberg.WithRef(context.Background(), "main")

	r.NoError(cat.DropNamespace(ctx, catalog.ToIdentifier("examples")))
}

func (r *NessieCatalogSuite) TestDropNamespace404() {
	r.mux.HandleFunc("/v1/namespaces/namespace/main/examples", func(w http.ResponseWriter, req *http.Request) {
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

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	ctx := iceberg.WithRef(context.Background(), "main")

	err = cat.DropNamespace(ctx, catalog.ToIdentifier("examples"))
	r.ErrorIs(err, catalog.ErrNoSuchNamespace)
	r.ErrorContains(err, "examples in warehouse")
}

func (r *NessieCatalogSuite) TestLoadNamespaceProps200() {
	r.mux.HandleFunc("/v1/namespaces/namespace/main/leden", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{
			"namespace": []map[string]any{
				{
					"type":     "NAMESPACE",
					"id":       "leden",
					"elements": []string{"leden"},
				},
			},
			"properties": map[string]any{"prop": "yes"},
		})
	})

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	ctx := iceberg.WithRef(context.Background(), "main")

	props, err := cat.LoadNamespaceProperties(ctx, catalog.ToIdentifier("leden"))
	r.Require().NoError(err)
	r.Equal(iceberg.Properties{"prop": "yes"}, props)
}

func (r *NessieCatalogSuite) TestLoadNamespaceProps404() {
	r.mux.HandleFunc("/v1/namespaces/namespace/main/leden", func(w http.ResponseWriter, req *http.Request) {
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

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	ctx := iceberg.WithRef(context.Background(), "main")

	_, err = cat.LoadNamespaceProperties(ctx, catalog.ToIdentifier("leden"))
	r.ErrorIs(err, catalog.ErrNoSuchNamespace)
	r.ErrorContains(err, "Namespace does not exist: fokko22 in warehouse")
}

func (r *NessieCatalogSuite) TestUpdateNamespaceProps200() {
	r.mux.HandleFunc("/v1/namespaces/namespace/main/fokko", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodPost, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		json.NewEncoder(w).Encode(catalog.PropertiesUpdateSummary{
			Removed: []string{},
			Updated: []string{"prop"},
			Missing: []string{"abc"},
		})
	})

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	ctx := iceberg.WithRef(context.Background(), "main")

	summary, err := cat.UpdateNamespaceProperties(ctx, table.Identifier([]string{"fokko"}),
		[]string{"abc"}, iceberg.Properties{"prop": "yes"})
	r.Require().NoError(err)

	r.Equal(catalog.PropertiesUpdateSummary{
		Removed: []string{},
		Updated: []string{"prop"},
		Missing: []string{"abc"},
	}, summary)
}

func (r *NessieCatalogSuite) TestUpdateNamespaceProps404() {
	r.mux.HandleFunc("/v1/namespaces/namespace/main/fokko", func(w http.ResponseWriter, req *http.Request) {
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

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	ctx := iceberg.WithRef(context.Background(), "main")

	_, err = cat.UpdateNamespaceProperties(ctx,
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

	createTableNessieExample = fmt.Sprintf(`{
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

func (r *NessieCatalogSuite) TestCreateTable200() {
	r.mux.HandleFunc("/v1/namespaces/fokko/tables", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodPost, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.Write([]byte(createTableNessieExample))
	})

	t := createTableNessieExample
	_ = t
	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	tbl, err := cat.CreateTable(
		context.Background(),
		catalog.ToIdentifier("fokko", "fokko2"),
		tableSchemaSimple,
	)
	r.Require().NoError(err)

	r.Equal(catalog.ToIdentifier("nessie", "fokko", "fokko2"), tbl.Identifier())
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

func (r *NessieCatalogSuite) TestCreateTable409() {
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

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
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

func (r *NessieCatalogSuite) TestCheckTableExists204() {
	r.mux.HandleFunc("/v1/namespaces/fokko/tables/fokko2", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodHead, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}
		w.WriteHeader(http.StatusNoContent)
	})

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	exists, err := cat.CheckTableExists(context.Background(), catalog.ToIdentifier("fokko", "fokko2"))
	r.Require().NoError(err)
	r.True(exists)
}

func (r *NessieCatalogSuite) TestCheckTableExists404() {
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
	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	exists, err := cat.CheckTableExists(context.Background(), catalog.ToIdentifier("fokko", "nonexistent"))
	r.Require().NoError(err)
	r.False(exists)
}

func (r *NessieCatalogSuite) TestLoadTable200() {
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

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	tbl, err := cat.LoadTable(context.Background(), catalog.ToIdentifier("fokko", "table"), nil)
	r.Require().NoError(err)

	r.Equal(catalog.ToIdentifier("nessie", "fokko", "table"), tbl.Identifier())
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

func (r *NessieCatalogSuite) TestRenameTable200() {
	// Mock the rename table endpoint
	r.mux.HandleFunc("/v1/tables/rename", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodPost, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		var payload struct {
			From struct {
				Namespace []string `json:"namespace"`
				Name      string   `json:"name"`
			} `json:"from"`
			To struct {
				Namespace []string `json:"namespace"`
				Name      string   `json:"name"`
			} `json:"to"`
		}
		r.NoError(json.NewDecoder(req.Body).Decode(&payload))
		r.Equal([]string{"fokko"}, payload.From.Namespace)
		r.Equal("source", payload.From.Name)
		r.Equal([]string{"fokko"}, payload.To.Namespace)
		r.Equal("destination", payload.To.Name)

		w.WriteHeader(http.StatusOK)
	})

	// Mock the get table endpoint for loading the renamed table
	r.mux.HandleFunc("/v1/namespaces/fokko/tables/destination", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodGet, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		w.Write([]byte(createTableNessieExample))
	})

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	fromIdent := catalog.ToIdentifier("fokko", "source")
	toIdent := catalog.ToIdentifier("fokko", "destination")

	renamedTable, err := cat.RenameTable(context.Background(), fromIdent, toIdent)
	r.Require().NoError(err)

	r.Equal(catalog.ToIdentifier("nessie", "fokko", "destination"), renamedTable.Identifier())
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

func (r *NessieCatalogSuite) TestDropTable204() {
	// Mock the drop table endpoint
	r.mux.HandleFunc("/v1/namespaces/fokko/tables/table", func(w http.ResponseWriter, req *http.Request) {
		r.Require().Equal(http.MethodDelete, req.Method)

		for k, v := range TestHeaders {
			r.Equal(v, req.Header.Values(k))
		}

		// Return 204 No Content for successful deletion
		w.WriteHeader(http.StatusNoContent)
	})

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	err = cat.DropTable(context.Background(), catalog.ToIdentifier("fokko", "table"))
	r.NoError(err)
}

func (r *NessieCatalogSuite) TestDropTable404() {
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

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	err = cat.DropTable(context.Background(), catalog.ToIdentifier("fokko", "table"))
	r.Error(err)
	r.ErrorIs(err, catalog.ErrNoSuchTable)
	r.ErrorContains(err, "Table does not exist: fokko.table")
}

func (r *NessieCatalogSuite) TestRegisterTable200() {
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

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	tbl, err := cat.RegisterTable(context.Background(), catalog.ToIdentifier("fokko", "fokko2"), "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json")
	r.Require().NoError(err)

	r.Equal(catalog.ToIdentifier("nessie", "fokko", "fokko2"), tbl.Identifier())
	r.Equal("s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json", tbl.MetadataLocation())
	r.EqualValues(1, tbl.Metadata().Version())
	r.Equal("d55d9dda-6561-423a-8bfc-787980ce421f", tbl.Metadata().TableUUID().String())
	r.Equal("bryan", tbl.Metadata().Properties()["owner"])
}

func (r *NessieCatalogSuite) TestRegisterTable404() {
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
	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	_, err = cat.RegisterTable(context.Background(), catalog.ToIdentifier("nonexistent", "fokko2"), "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json")
	r.ErrorIs(err, catalog.ErrNoSuchNamespace)
	r.ErrorContains(err, "The given namespace does not exist")
}

func (r *NessieCatalogSuite) TestRegisterTable409() {
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
	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	_, err = cat.RegisterTable(context.Background(), catalog.ToIdentifier("fokko", "alreadyexist"), "s3://warehouse/database/table/metadata/00001-5f2f8166-244c-4eae-ac36-384ecdec81fc.gz.metadata.json")
	r.ErrorIs(err, catalog.ErrTableAlreadyExists)
	r.ErrorContains(err, "The given table already exists")
}

type NessieTLSCatalogSuite struct {
	suite.Suite

	srv *httptest.Server
	mux *http.ServeMux

	configVals url.Values
}

func (r *NessieTLSCatalogSuite) SetupTest() {
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

func (r *NessieTLSCatalogSuite) TearDownTest() {
	r.srv.Close()
	r.srv = nil
	r.mux = nil
	r.configVals = nil
}

func (r *NessieTLSCatalogSuite) TestSSLFail() {
	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken))
	r.Nil(cat)

	r.ErrorContains(err, "tls: failed to verify certificate")
}

func (r *NessieTLSCatalogSuite) TestSSLLoadRegisteredCatalog() {
	ctx := context.Background()
	cat, err := catalog.Load(ctx, "foobar", iceberg.Properties{
		"uri":                  r.srv.URL,
		"warehouse":            "s3://some-bucket",
		"token":                TestToken,
		"nessie.tls.skip-verify": "true",
	})
	r.NoError(err)

	r.NotNil(cat)
	r.Equal(r.configVals.Get("warehouse"), "s3://some-bucket")
}

func (r *NessieTLSCatalogSuite) TestSSLConfig() {
	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken),
		nessie.WithWarehouseLocation("s3://some-bucket"),
		nessie.WithTLSConfig(&tls.Config{InsecureSkipVerify: true}))
	r.NoError(err)

	r.NotNil(cat)
	r.Equal(r.configVals.Get("warehouse"), "s3://some-bucket")
}

func (r *NessieTLSCatalogSuite) TestSSLCerts() {
	certs := x509.NewCertPool()
	for _, c := range r.srv.TLS.Certificates {
		roots, err := x509.ParseCertificates(c.Certificate[len(c.Certificate)-1])
		r.Require().NoError(err)
		for _, root := range roots {
			certs.AddCert(root)
		}
	}

	cat, err := nessie.NewCatalog(context.Background(), "nessie", r.srv.URL, nessie.WithOAuthToken(TestToken),
		nessie.WithWarehouseLocation("s3://some-bucket"),
		nessie.WithTLSConfig(&tls.Config{RootCAs: certs}))
	r.NoError(err)

	r.NotNil(cat)
	r.Equal(r.configVals.Get("warehouse"), "s3://some-bucket")
}

func TestNessieCatalog(t *testing.T) {
	suite.Run(t, new(NessieCatalogSuite))
	suite.Run(t, new(NessieTLSCatalogSuite))
}
