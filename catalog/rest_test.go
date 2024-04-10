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

package catalog_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
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

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL,
		catalog.WithWarehouseLocation("s3://some-bucket"),
		catalog.WithCredential(TestCreds))
	r.Require().NoError(err)

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

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithCredential(TestCreds))
	r.Nil(cat)

	r.ErrorIs(err, catalog.ErrRESTError)
	r.ErrorIs(err, catalog.ErrOAuthError)
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
	cat, err := catalog.NewRestCatalog("rest", r.srv.URL,
		catalog.WithWarehouseLocation("s3://some-bucket"),
		catalog.WithCredential(TestCreds), catalog.WithAuthURI(authUri.JoinPath("auth-token-url")))

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

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithCredential(TestCreds))
	r.Nil(cat)

	r.ErrorIs(err, catalog.ErrRESTError)
	r.ErrorIs(err, catalog.ErrOAuthError)
	r.ErrorContains(err, "invalid_client: credentials for key invalid_key do not match")
}

func (r *RestCatalogSuite) TestListTables200() {
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

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	tables, err := cat.ListTables(context.Background(), catalog.ToRestIdentifier(namespace))
	r.Require().NoError(err)
	r.Equal([]table.Identifier{{"examples", "fooshare"}}, tables)
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

		json.NewEncoder(w).Encode(map[string]any{
			"identifiers": []any{
				map[string]any{
					"namespace": []string{namespace},
					"name":      "fooshare",
				},
			},
		})
	})

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL,
		catalog.WithPrefix("prefix"),
		catalog.WithWarehouseLocation("s3://some-bucket"),
		catalog.WithCredential(TestCreds))
	r.Require().NoError(err)

	r.NotNil(cat)
	r.Equal(r.configVals.Get("warehouse"), "s3://some-bucket")

	tables, err := cat.ListTables(context.Background(), catalog.ToRestIdentifier(namespace))
	r.Require().NoError(err)
	r.Equal([]table.Identifier{{"examples", "fooshare"}}, tables)
}

func (r *RestCatalogSuite) TestListTables404() {
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

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	_, err = cat.ListTables(context.Background(), catalog.ToRestIdentifier(namespace))
	r.ErrorIs(err, catalog.ErrNoSuchNamespace)
	r.ErrorContains(err, "Namespace does not exist: personal in warehouse 8bcb0838-50fc-472d-9ddb-8feb89ef5f1e")
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

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithOAuthToken(TestToken))
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

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	results, err := cat.ListNamespaces(context.Background(), catalog.ToRestIdentifier("accounting"))
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

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	_, err = cat.ListNamespaces(context.Background(), catalog.ToRestIdentifier("accounting"))
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

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	r.Require().NoError(cat.CreateNamespace(context.Background(), catalog.ToRestIdentifier("leden"), nil))
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

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	r.Require().NoError(cat.CreateNamespace(context.Background(), catalog.ToRestIdentifier("leden"), iceberg.Properties{"foo": "bar", "super": "duper"}))
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

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	err = cat.CreateNamespace(context.Background(), catalog.ToRestIdentifier("fokko"), nil)
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

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	r.NoError(cat.DropNamespace(context.Background(), catalog.ToRestIdentifier("examples")))
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

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	err = cat.DropNamespace(context.Background(), catalog.ToRestIdentifier("examples"))
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

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	props, err := cat.LoadNamespaceProperties(context.Background(), catalog.ToRestIdentifier("leden"))
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

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	_, err = cat.LoadNamespaceProperties(context.Background(), catalog.ToRestIdentifier("leden"))
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

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithOAuthToken(TestToken))
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

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	_, err = cat.UpdateNamespaceProperties(context.Background(),
		table.Identifier{"fokko"}, []string{"abc"}, iceberg.Properties{"prop": "yes"})
	r.ErrorIs(err, catalog.ErrNoSuchNamespace)
	r.ErrorContains(err, "Namespace does not exist: does_not_exist in warehouse")
}

func (r *RestCatalogSuite) TestLoadTable200() {
	r.T().Skip("haven't implemented rest with objstore bucket")
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

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithOAuthToken(TestToken))
	r.Require().NoError(err)

	tbl, err := cat.LoadTable(context.Background(), catalog.ToRestIdentifier("fokko", "table"), nil)
	r.Require().NoError(err)

	r.Equal(catalog.ToRestIdentifier("rest", "fokko", "table"), tbl.Identifier())
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
	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithOAuthToken(TestToken))
	r.Nil(cat)

	r.ErrorContains(err, "tls: failed to verify certificate")
}

func (r *RestTLSCatalogSuite) TestSSLConfig() {
	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithOAuthToken(TestToken),
		catalog.WithWarehouseLocation("s3://some-bucket"),
		catalog.WithTLSConfig(&tls.Config{InsecureSkipVerify: true}))
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

	cat, err := catalog.NewRestCatalog("rest", r.srv.URL, catalog.WithOAuthToken(TestToken),
		catalog.WithWarehouseLocation("s3://some-bucket"),
		catalog.WithTLSConfig(&tls.Config{RootCAs: certs}))
	r.NoError(err)

	r.NotNil(cat)
	r.Equal(r.configVals.Get("warehouse"), "s3://some-bucket")
}

func TestRestCatalog(t *testing.T) {
	suite.Run(t, new(RestCatalogSuite))
	suite.Run(t, new(RestTLSCatalogSuite))
}
