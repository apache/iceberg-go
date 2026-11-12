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
	"encoding/json"
	"fmt"
	stdio "io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal/remotesigning"
	iceio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud/s3"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func remoteSigningMetadata(t *testing.T) table.Metadata {
	t.Helper()
	meta, err := table.NewMetadata(iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32}), nil, table.UnsortedSortOrder, "s3://bucket/table", nil)
	require.NoError(t, err)

	return meta
}

func TestRESTRemoteSigningLifecycle(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"load", "create", "register"} {
		t.Run(operation, func(t *testing.T) {
			var signs atomic.Int32
			var legacySigns atomic.Int32
			legacy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				legacySigns.Add(1)
				http.Error(w, "legacy signer must not be used", http.StatusBadRequest)
			}))
			defer legacy.Close()
			storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "storage-auth", r.Header.Get("Authorization"))
				assert.Empty(t, r.Header.Get("X-Table-Secret"))
				assert.Empty(t, r.Header.Get("X-Catalog-Secret"))
				_, _ = w.Write([]byte("file content"))
			}))
			defer storage.Close()
			meta := remoteSigningMetadata(t)
			mux := http.NewServeMux()
			mux.HandleFunc("/base/v1/config", func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"defaults": map[string]string{}, "overrides": map[string]string{}, "endpoints": []string{endpointLoadTable.String(), endpointCreateTable.String(), endpointRegisterTable.String(), endpointTableCredentials.String()}})
			})
			respond := func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "Bearer catalog-token", r.Header.Get("Authorization"))
				if r.Header.Get("If-None-Match") != "" {
					w.WriteHeader(http.StatusNotModified)

					return
				}
				w.Header().Set("ETag", `"table-1"`)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"metadata-location": "s3://bucket/table/metadata/1.json", "metadata": meta,
					"config": map[string]string{
						iceio.S3RemoteSigningEnabled: "true", iceio.S3Region: "us-east-1", iceio.S3EndpointURL: storage.URL,
						"token": "table-token", "signer.endpoint": "legacy/sign", "signer.uri": legacy.URL, iceio.S3SignerURI: legacy.URL,
					},
					"remote-signing-config": map[string]any{"properties": map[string]string{"tenant": "table"}, "headers": map[string][]string{"x-table-secret": {"one", "two"}}},
				})
			}
			mux.HandleFunc("/base/v1/prefix/namespaces/db/tables/tbl", respond)
			mux.HandleFunc("/base/v1/prefix/namespaces/db/tables", respond)
			mux.HandleFunc("/base/v1/prefix/namespaces/db/register", respond)
			mux.HandleFunc("/base/v1/prefix/namespaces/db/tables/tbl/credentials", func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "Bearer catalog-token", r.Header.Get("Authorization"))
				_ = json.NewEncoder(w).Encode(map[string]any{"storage-credentials": []StorageCredential{{Prefix: "s3://bucket/", Config: iceberg.Properties{iceio.S3RemoteSigningEnabled: "true"}}}})
			})
			mux.HandleFunc("/base/v1/prefix/namespaces/db/tables/tbl/sign", func(w http.ResponseWriter, r *http.Request) {
				signs.Add(1)
				assert.Equal(t, "Bearer table-token", r.Header.Get("Authorization"))
				assert.Equal(t, []string{"one", "two"}, r.Header.Values("X-Table-Secret"))
				var req struct {
					URI        string            `json:"uri"`
					Properties map[string]string `json:"properties"`
					Headers    http.Header       `json:"headers"`
				}
				if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&req)) {
					return
				}
				assert.Equal(t, map[string]string{"tenant": "table"}, req.Properties)
				assert.Empty(t, req.Headers.Get("Authorization"))
				assert.Empty(t, req.Headers.Get("X-Catalog-Secret"))
				_ = json.NewEncoder(w).Encode(map[string]any{"uri": req.URI, "headers": map[string][]string{"Authorization": {"storage-auth"}}})
			})
			server := httptest.NewServer(mux)
			defer server.Close()
			cat, err := NewCatalog(t.Context(), "rest", server.URL+"/base", WithPrefix("prefix"), WithOAuthToken("catalog-token"), WithHeaders(map[string]string{"X-Catalog-Secret": "catalog-secret"}))
			require.NoError(t, err)
			defer cat.Close()
			var tbl *table.Table
			ident := table.Identifier{"db", "tbl"}
			switch operation {
			case "load":
				tbl, err = cat.LoadTable(t.Context(), ident)
			case "create":
				tbl, err = cat.CreateTable(t.Context(), ident, meta.CurrentSchema())
			case "register":
				tbl, err = cat.RegisterTable(t.Context(), ident, "s3://bucket/table/metadata/1.json")
			}
			require.NoError(t, err)
			read := func(tbl *table.Table) {
				fs, err := tbl.FS(t.Context())
				require.NoError(t, err)
				file, err := fs.Open(t.Context(), "s3://bucket/table/data/file")
				require.NoError(t, err)
				data, err := stdio.ReadAll(file)
				require.NoError(t, err)
				require.Equal(t, "file content", string(data))
				require.NoError(t, file.Close())
			}
			read(tbl)
			refreshed, err := cat.RefreshTableCredentials(t.Context(), tbl)
			require.NoError(t, err)
			read(refreshed)
			require.Equal(t, tbl.SavedConfig(), refreshed.SavedConfig())
			// Cached 304 responses retain the same remote signing configuration.
			loaded, err := cat.LoadTable(t.Context(), ident)
			require.NoError(t, err)
			read(loaded)
			loaded, err = cat.LoadTable(t.Context(), ident)
			require.NoError(t, err)
			read(loaded)
			require.GreaterOrEqual(t, signs.Load(), int32(4))
			require.Zero(t, legacySigns.Load())
		})
	}
}

type rotatingSigningAuth struct{ calls atomic.Int32 }

func (a *rotatingSigningAuth) AuthHeader() (string, string, error) {
	return "Authorization", fmt.Sprintf("Bearer token-%d", a.calls.Add(1)), nil
}

func TestRESTRemoteSigningAuthIsolation(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		external   bool
		tableToken string
		host       string
		header     http.Header
		want       string
	}{
		{name: "refreshing catalog auth", want: "Bearer token-"},
		{name: "explicit static authorization", header: http.Header{"Authorization": {"static-secret"}}, want: "static-secret"},
		{name: "explicit empty authorization", header: http.Header{"Authorization": {}}, want: ""},
		{name: "table token", tableToken: "table", want: "Bearer table"},
		{name: "different signer Host", host: "other.example", want: ""},
		{name: "different Host with table token", host: "other.example", tableToken: "table", want: "Bearer table"},
		{name: "external signer", external: true, want: ""},
		{name: "external table token", external: true, tableToken: "table", want: "Bearer table"},
	} {
		t.Run(test.name, func(t *testing.T) {
			auth := &rotatingSigningAuth{}
			var got http.Header
			record := func(w http.ResponseWriter, r *http.Request) { got = r.Header.Clone(); _, _ = w.Write([]byte(`{}`)) }
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/config", serveEmptyConfig)
			mux.HandleFunc("/sign", record)
			server := httptest.NewServer(mux)
			defer server.Close()
			external := httptest.NewServer(http.HandlerFunc(record))
			defer external.Close()
			cat, err := NewCatalog(t.Context(), "rest", server.URL, WithAuthManager(auth), WithHeaders(map[string]string{"X-Catalog-Secret": "private", "User-Agent": "catalog-private"}))
			require.NoError(t, err)
			defer cat.Close()
			target := server.URL + "/sign"
			if test.external {
				target = external.URL
			}
			// Even the separately configured OAuth origin must receive no catalog
			// defaults from the signing client.
			session := cat.cl.Transport.(*sessionTransport)
			session.authOrigin, _ = url.Parse(external.URL)
			client := cat.remoteSigningHTTPClient()
			send := func() {
				req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, target, strings.NewReader(`{}`))
				require.NoError(t, err)
				req.Host = test.host
				req.Header = test.header.Clone()
				if req.Header == nil {
					req.Header = make(http.Header)
				}
				if test.tableToken != "" {
					req.Header.Set("Authorization", "Bearer "+test.tableToken)
				}
				resp, err := client.Do(req)
				require.NoError(t, err)
				require.NoError(t, resp.Body.Close())
				if test.want == "Bearer token-" {
					require.True(t, strings.HasPrefix(got.Get("Authorization"), test.want))
				} else {
					require.Equal(t, test.want, got.Get("Authorization"))
				}
				if test.external || test.host != "" {
					require.Empty(t, got.Get("X-Catalog-Secret"))
					require.NotEqual(t, "catalog-private", got.Get("User-Agent"))
				} else {
					require.Equal(t, "private", got.Get("X-Catalog-Secret"))
				}
			}
			send()
			first := got.Get("Authorization")
			send()
			if test.want == "Bearer token-" {
				require.NotEqual(t, first, got.Get("Authorization"))
			}
		})
	}
}

func TestRESTRemoteSigningDoesNotOverrideCatalogAuthPolicy(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", serveEmptyConfig)
	var got string
	mux.HandleFunc("/sign", func(w http.ResponseWriter, r *http.Request) { got = r.Header.Get("Authorization") })
	server := httptest.NewServer(mux)
	defer server.Close()
	cat, err := NewCatalog(t.Context(), "rest", server.URL, WithSigner(markingSigner{}))
	require.NoError(t, err)
	defer cat.Close()
	signing := cat.remoteSigningHTTPClient()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/sign", nil)
	req.Header.Set("Authorization", "table-auth")
	resp, err := signing.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, "table-auth", got)
	req, _ = http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/sign", nil)
	req.Header.Set("Authorization", "table-auth")
	resp, err = cat.cl.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Contains(t, got, "AWS4-HMAC-SHA256")
}

func TestRESTRemoteSigningEndpoint(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.HandleFunc("/base/v1/config", serveEmptyConfig)
	server := httptest.NewServer(mux)
	defer server.Close()
	cat, err := NewCatalog(t.Context(), "rest", server.URL+"/base", WithPrefix("prefix"))
	require.NoError(t, err)
	defer cat.Close()
	props := iceberg.Properties{"signer.endpoint": "custom/sign"}
	session, err := cat.remoteSigningSession([]string{"db", "tbl"})
	require.NoError(t, err)
	props["uri"] = session.CatalogURI
	endpoint, err := remotesigning.Endpoint(props, session.Endpoint)
	require.NoError(t, err)
	require.Equal(t, server.URL+"/base/custom/sign", endpoint)
	props = iceberg.Properties{}
	session, err = cat.remoteSigningSession([]string{"db one", "table+name%2F"})
	require.NoError(t, err)
	endpoint, err = remotesigning.Endpoint(props, session.Endpoint)
	require.NoError(t, err)
	require.Equal(t, server.URL+"/base/v1/prefix/namespaces/db%20one/tables/table%2Bname%252F/sign", endpoint)
}

func TestRESTRemoteSigningPlanIO(t *testing.T) {
	t.Parallel()
	registry := &iceio.Registry{}
	var gotProps map[string]string
	var gotClient *http.Client
	var gotEndpoint string
	require.NoError(t, registry.Register("isolated", func(ctx context.Context, _ *url.URL, props map[string]string) (iceio.IO, error) {
		gotClient = remotesigning.SessionFromContext(ctx).Client
		gotEndpoint = remotesigning.SessionFromContext(ctx).Endpoint
		gotProps = maps.Clone(props)

		return iceio.NewMemFS(), nil
	}))
	cat := newScanPlanningTestCatalog(t, []endpoint{endpointPlanTableScan}, func(mux *http.ServeMux) {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/plan", func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"status":"completed","plan-id":"plan-1","storage-credentials":[{"prefix":"isolated://bucket/","config":{"s3.remote-signing-enabled":"true"}}]}`))
		})
	}, WithIORegistry(registry))
	req := planFilesReq()
	req.MetadataLocation = "isolated://bucket/table/metadata.json"
	req.FileIOProperties = iceberg.Properties{remotesigning.ConfigProperty: `{"headers":{"Authorization":["plan-auth"]},"properties":{"tenant":"table"}}`}
	result, err := cat.PlanFiles(t.Context(), req)
	require.NoError(t, err)
	require.NotNil(t, result.IO)
	defer result.IO.Close()
	fs, err := result.IO.Load(t.Context())
	require.NoError(t, err)
	_, err = fs.Open(t.Context(), "isolated://bucket/data")
	require.Error(t, err, "the recording filesystem is empty")
	require.NotNil(t, gotClient)
	require.Equal(t, req.FileIOProperties[remotesigning.ConfigProperty], gotProps[remotesigning.ConfigProperty])
	require.True(t, strings.HasSuffix(gotEndpoint, "/v1/namespaces/db/tables/tbl/sign"))
}

func TestRESTRemoteSigningAutomaticCredentialRenewal(t *testing.T) {
	t.Parallel()
	for _, token := range []struct {
		name        string
		replace     bool
		value, want string
	}{
		{name: "keep load token", want: "Bearer table-token"},
		{name: "new token", replace: true, value: "renewed-token", want: "Bearer renewed-token"},
		{name: "clear token", replace: true, value: "", want: ""},
	} {
		t.Run(token.name, func(t *testing.T) {
			storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "storage-auth", r.Header.Get("Authorization"))
				_, _ = w.Write([]byte("renewed"))
			}))
			defer storage.Close()
			registry := &iceio.Registry{}
			var sessions []remotesigning.Session
			require.NoError(t, registry.Register("isolated", func(ctx context.Context, _ *url.URL, props map[string]string) (iceio.IO, error) {
				sessions = append(sessions, remotesigning.SessionFromContext(ctx))

				return iceio.LoadFS(ctx, props, "s3://bucket/table/metadata.json")
			}))
			meta := remoteSigningMetadata(t)
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"defaults": map[string]string{}, "overrides": map[string]string{}, "endpoints": []string{endpointLoadTable.String(), endpointTableCredentials.String()}})
			})
			mux.HandleFunc("/v1/namespaces/db/tables/tbl", func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"metadata-location": "isolated://bucket/table/metadata.json", "metadata": meta,
					"config":              map[string]string{"token": "table-token", iceio.S3EndpointURL: storage.URL, iceio.S3Region: "us-east-1"},
					"storage-credentials": []StorageCredential{{Prefix: "isolated://bucket/", Config: iceberg.Properties{iceio.S3AccessKeyID: "initial", iceio.S3SecretAccessKey: "secret", keyS3TokenExpiresAtMs: "1"}}},
				})
			})
			mux.HandleFunc("/v1/namespaces/db/tables/tbl/credentials", func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"storage-credentials": []StorageCredential{{Prefix: "isolated://bucket/", Config: func() iceberg.Properties {
					props := iceberg.Properties{iceio.S3RemoteSigningEnabled: "true", "signer.endpoint": "renewed/sign?tenant=table"}
					if token.replace {
						props["token"] = token.value
					}

					return props
				}()}}})
			})
			mux.HandleFunc("/renewed/sign", func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "table", r.URL.Query().Get("tenant"))
				assert.Equal(t, token.want, r.Header.Get("Authorization"))
				var request struct {
					URI string `json:"uri"`
				}
				if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&request)) {
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"uri": request.URI, "headers": map[string][]string{"Authorization": {"storage-auth"}}})
			})
			server := httptest.NewServer(mux)
			defer server.Close()
			cat, err := NewCatalog(t.Context(), "rest", server.URL, WithOAuthToken("catalog-token"), WithIORegistry(registry))
			require.NoError(t, err)
			defer cat.Close()
			tbl, err := cat.LoadTable(t.Context(), table.Identifier{"db", "tbl"})
			require.NoError(t, err)
			first, err := tbl.FS(t.Context())
			require.NoError(t, err)
			second, err := tbl.FS(t.Context())
			require.NoError(t, err)
			require.NotSame(t, first, second)
			file, err := second.Open(t.Context(), "s3://bucket/table/data")
			require.NoError(t, err)
			defer file.Close()
			data, err := stdio.ReadAll(file)
			require.NoError(t, err)
			require.Equal(t, "renewed", string(data))
			require.Len(t, sessions, 2)
			require.NotNil(t, sessions[0].Client)
			require.NotNil(t, sessions[1].Client)
			require.NoError(t, closeOptionalIO(first))
			require.NoError(t, closeOptionalIO(second))
		})
	}
}

func TestRESTRemoteSigningConfigOverrideBaseURI(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.HandleFunc("/initial/v1/config", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"defaults": map[string]string{}, "overrides": map[string]string{"uri": "https://effective.example/catalog"}})
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	cat, err := NewCatalog(t.Context(), "rest", server.URL+"/initial", WithPrefix("prefix"))
	require.NoError(t, err)
	defer cat.Close()
	session, err := cat.remoteSigningSession([]string{"db", "tbl"})
	require.NoError(t, err)
	require.Equal(t, "https://effective.example/catalog", session.CatalogURI)
	endpoint, err := remotesigning.Endpoint(map[string]string{"signer.endpoint": "custom/sign?tenant=one", "uri": session.CatalogURI}, session.Endpoint)
	require.NoError(t, err)
	require.Equal(t, "https://effective.example/catalog/custom/sign?tenant=one", endpoint)
	require.Equal(t, "https://effective.example/catalog/v1/prefix/namespaces/db/tables/tbl/sign", session.Endpoint)
}
