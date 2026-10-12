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
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/metrics"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ioPropsRecorder registers an IO scheme that records the properties handed to
// every filesystem load. That is how these tests observe whether vended
// credentials actually reached a table's FileIO, which is otherwise private to
// the table.
type ioPropsRecorder struct {
	mu    sync.Mutex
	loads []map[string]string
}

func (rec *ioPropsRecorder) register(t *testing.T, scheme string) {
	t.Helper()

	iceio.Register(scheme, func(_ context.Context, _ *url.URL, props map[string]string) (iceio.IO, error) {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		rec.loads = append(rec.loads, maps.Clone(props))

		return iceio.NewMemFS(), nil
	})
	t.Cleanup(func() { iceio.Unregister(scheme) })
}

// lastLoad returns the properties the most recent filesystem load was built
// from, failing the test if no load happened.
func (rec *ioPropsRecorder) lastLoad(t *testing.T) map[string]string {
	t.Helper()

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.NotEmpty(t, rec.loads, "no filesystem was loaded")

	return rec.loads[len(rec.loads)-1]
}

type credsCatalogOpts struct {
	// endpoints is what /v1/config advertises; nil advertises every endpoint.
	endpoints []string
	// defaults is the /v1/config defaults block, which the catalog folds into
	// the properties it later builds table FileIO from.
	defaults map[string]string
	// creds handles GET /v1/namespaces/db/tables/tbl/credentials. When nil, the
	// endpoint is left unrouted so a request to it would 404.
	creds http.HandlerFunc
	// loadTable handles GET /v1/namespaces/db/tables/tbl. When nil, the
	// endpoint is left unrouted so a request to it would 404.
	loadTable http.HandlerFunc
	// routes are further handlers, keyed by request path.
	routes map[string]http.HandlerFunc
}

func newCredsTestCatalog(t *testing.T, opts credsCatalogOpts) *rest.Catalog {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, _ *http.Request) {
		endpoints := opts.endpoints
		if endpoints == nil {
			endpoints = rest.AllEndpointStrings
		}
		defaults := opts.defaults
		if defaults == nil {
			defaults = map[string]string{}
		}
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"defaults":  defaults,
			"overrides": map[string]any{},
			"endpoints": endpoints,
		}))
	})
	if opts.creds != nil {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl/credentials", opts.creds)
	}
	if opts.loadTable != nil {
		mux.HandleFunc("/v1/namespaces/db/tables/tbl", opts.loadTable)
	}
	for path, handler := range opts.routes {
		mux.HandleFunc(path, handler)
	}

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cat, err := rest.NewCatalog(context.Background(), "rest", srv.URL, rest.WithOAuthToken(TestToken))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, cat.Close()) })

	return cat
}

// newExternalTable builds a table the way a caller outside the catalog would:
// straight through table.New, with a plain property-based FileIO and no vended
// credentials seeded into it. Any opts are passed through to table.New.
func newExternalTable(t *testing.T, cat *rest.Catalog, scheme string, opts ...table.Option) (*table.Table, string) {
	t.Helper()

	// The shared fixture is written against s3://; point it at the recording
	// scheme so loading its FileIO stays offline and observable.
	meta, err := table.ParseMetadataString(
		strings.ReplaceAll(exampleTableMetadataNoSnapshotV1, "s3://", scheme+"://"))
	require.NoError(t, err)

	metadataLoc := scheme + "://warehouse/database/table/metadata/00000-a.metadata.json"

	return table.New(
		catalog.ToIdentifier("db", "tbl"),
		meta,
		metadataLoc,
		iceio.LoadFSFunc(nil, metadataLoc),
		cat,
		opts...,
	), metadataLoc
}

func storageCredentialsBody(prefix string, config map[string]string) map[string]any {
	return map[string]any{
		"storage-credentials": []any{
			map[string]any{"prefix": prefix, "config": config},
		},
	}
}

// TestRefreshTableCredentialsSeedsExternallyCreatedTable covers the case a
// caller cannot reach through LoadTable: a table handed to table.New directly,
// whose FileIO was never seeded with vended credentials, is brought up to date
// without a metadata reload.
func TestRefreshTableCredentialsSeedsExternallyCreatedTable(t *testing.T) {
	const scheme = "restcreds-seed"

	rec := &ioPropsRecorder{}
	rec.register(t, scheme)

	var credsCalls atomic.Int32
	cat := newCredsTestCatalog(t, credsCatalogOpts{
		defaults: map[string]string{"s3.region": "us-west-2"},
		creds: func(w http.ResponseWriter, req *http.Request) {
			credsCalls.Add(1)
			assert.Equal(t, http.MethodGet, req.Method)
			assert.NoError(t, json.NewEncoder(w).Encode(storageCredentialsBody(
				scheme+"://warehouse/database/table",
				map[string]string{
					"s3.access-key-id":     "vended-key",
					"s3.secret-access-key": "vended-secret",
					"s3.session-token":     "vended-token",
				},
			)))
		},
	})

	// Save config the way the catalog does for tables it loads: the table's
	// metadata properties (the fixture sets this codec) plus table-specific
	// FileIO settings such as a region or client factory.
	savedConfig := iceberg.Properties{
		"write.parquet.compression-codec": "zstd",
		// Overrides the catalog default above.
		iceio.S3Region:   "eu-central-1",
		"client.factory": "com.example.CustomClientFactory",
	}
	tbl, metadataLoc := newExternalTable(t, cat, scheme, table.WithSavedConfig(savedConfig))

	// The premise: as built, the table's FileIO carries no credentials.
	_, err := tbl.FS(context.Background())
	require.NoError(t, err)
	assert.NotContains(t, rec.lastLoad(t), "s3.access-key-id")

	refreshed, err := cat.RefreshTableCredentials(context.Background(), tbl)
	require.NoError(t, err)
	require.NotNil(t, refreshed)
	assert.Equal(t, int32(1), credsCalls.Load())

	// Only the FileIO configuration changes: identity, metadata and the metadata
	// location a later commit targets all carry over untouched.
	assert.Equal(t, catalog.ToIdentifier("db", "tbl"), refreshed.Identifier())
	assert.Equal(t, metadataLoc, refreshed.MetadataLocation())
	assert.Equal(t, tbl.Metadata(), refreshed.Metadata())
	assert.Equal(t, tbl.Location(), refreshed.Location())

	_, err = refreshed.FS(context.Background())
	require.NoError(t, err)

	props := rec.lastLoad(t)
	assert.Equal(t, "vended-key", props["s3.access-key-id"])
	assert.Equal(t, "vended-secret", props["s3.secret-access-key"])
	assert.Equal(t, "vended-token", props["s3.session-token"])
	// The table's saved config survives the merge, winning over the catalog's
	// own config, rather than being replaced by the credentials alone.
	for k, v := range savedConfig {
		assert.Equal(t, v, props[k], "FileIO property %q", k)
	}

	// The saved config also carries over to the refreshed table itself, without
	// any changes - the vended credentials are not merged in it.
	refreshedConfig := refreshed.SavedConfig()
	assert.Equal(t, savedConfig, refreshedConfig,
		"saved config must round-trip unchanged: no catalog props, no vended credentials")

	// The table the caller passed in, and the map it saved, are left alone.
	_, err = tbl.FS(context.Background())
	require.NoError(t, err)
	assert.NotContains(t, rec.lastLoad(t), "s3.access-key-id")
	assert.Equal(t, savedConfig, tbl.SavedConfig())
	assert.NotContains(t, savedConfig, "s3.access-key-id")
}

// customReporter is a non-nop metrics reporter a caller might attach to a
// table, distinguishable from the catalog's default by pointer identity.
type customReporter struct{}

func (*customReporter) Report(context.Context, metrics.MetricsReport) {}
func (*customReporter) Close() error                                  { return nil }

// TestRefreshTableCredentialsPreservesMetricsReporter checks that a reporter
// the caller set on the table survives the refresh instead of being replaced
// by the catalog's default reporter.
func TestRefreshTableCredentialsPreservesMetricsReporter(t *testing.T) {
	const scheme = "restcreds-reporter"

	cat := newCredsTestCatalog(t, credsCatalogOpts{
		creds: func(w http.ResponseWriter, _ *http.Request) {
			assert.NoError(t, json.NewEncoder(w).Encode(storageCredentialsBody(
				scheme+"://warehouse/database/table",
				map[string]string{"s3.access-key-id": "vended-key"},
			)))
		},
	})

	reporter := &customReporter{}
	tbl, _ := newExternalTable(t, cat, scheme, table.WithMetricsReporter(reporter))

	refreshed, err := cat.RefreshTableCredentials(context.Background(), tbl)
	require.NoError(t, err)
	require.NotSame(t, tbl, refreshed)
	assert.Same(t, reporter, refreshed.MetricsReporter())
}

// TestRefreshTableCredentialsAfterLoadTable covers a table loaded through
// LoadTable: the per-table config block of the load response is in neither the
// catalog's props nor the table's metadata properties, so only the table's
// saved config can carry it through a credential refresh.
func TestRefreshTableCredentialsAfterLoadTable(t *testing.T) {
	const scheme = "restcreds-loaded"

	rec := &ioPropsRecorder{}
	rec.register(t, scheme)

	metadataLoc := scheme + "://warehouse/database/table/metadata/00000-a.metadata.json"
	tableConfig := map[string]string{
		// Overrides the catalog default below.
		iceio.S3Region:   "eu-central-1",
		"client.factory": "com.example.CustomClientFactory",
	}

	cat := newCredsTestCatalog(t, credsCatalogOpts{
		defaults: map[string]string{iceio.S3Region: "us-west-2"},
		loadTable: func(w http.ResponseWriter, req *http.Request) {
			assert.Equal(t, http.MethodGet, req.Method)
			assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"metadata-location": metadataLoc,
				"metadata": json.RawMessage(strings.ReplaceAll(
					exampleTableMetadataNoSnapshotV1, "s3://", scheme+"://")),
				"config": tableConfig,
			}))
		},
		creds: func(w http.ResponseWriter, _ *http.Request) {
			assert.NoError(t, json.NewEncoder(w).Encode(storageCredentialsBody(
				scheme+"://warehouse/database/table",
				map[string]string{
					iceio.S3AccessKeyID:     "vended-key",
					iceio.S3SecretAccessKey: "vended-secret",
					iceio.S3SessionToken:    "vended-token",
				},
			)))
		},
	})

	tbl, err := cat.LoadTable(context.Background(), catalog.ToIdentifier("db", "tbl"))
	require.NoError(t, err)

	// The premise: the loaded table's FileIO carries the per-table config but no
	// credentials, since the load response vended none.
	_, err = tbl.FS(context.Background())
	require.NoError(t, err)
	props := rec.lastLoad(t)
	for k, v := range tableConfig {
		require.Equal(t, v, props[k], "loaded FileIO property %q", k)
	}
	require.NotContains(t, props, iceio.S3AccessKeyID)

	refreshed, err := cat.RefreshTableCredentials(context.Background(), tbl)
	require.NoError(t, err)
	require.NotNil(t, refreshed)
	assert.Equal(t, tbl.Identifier(), refreshed.Identifier())
	assert.Equal(t, metadataLoc, refreshed.MetadataLocation())
	assert.Equal(t, tbl.Metadata(), refreshed.Metadata())

	_, err = refreshed.FS(context.Background())
	require.NoError(t, err)

	props = rec.lastLoad(t)
	for k, v := range tableConfig {
		assert.Equal(t, v, props[k], "refreshed FileIO property %q", k)
	}
	assert.Equal(t, "zstd", props["write.parquet.compression-codec"],
		"metadata properties merged at load time should survive too")
	assert.Equal(t, "vended-key", props[iceio.S3AccessKeyID])
	assert.Equal(t, "vended-secret", props[iceio.S3SecretAccessKey])
	assert.Equal(t, "vended-token", props[iceio.S3SessionToken])
	// Saved configs are identical.
	assert.Equal(t, tbl.SavedConfig(), refreshed.SavedConfig())
}

// TestRefreshTableCredentialsSeededTableRenewsOnExpiry checks the refreshed
// table's FileIO keeps the refresher wiring, so credentials that expire are
// re-fetched instead of being pinned at their first value.
func TestRefreshTableCredentialsSeededTableRenewsOnExpiry(t *testing.T) {
	const scheme = "restcreds-renew"

	rec := &ioPropsRecorder{}
	rec.register(t, scheme)

	var credsCalls atomic.Int32
	cat := newCredsTestCatalog(t, credsCatalogOpts{
		creds: func(w http.ResponseWriter, _ *http.Request) {
			n := credsCalls.Add(1)
			assert.NoError(t, json.NewEncoder(w).Encode(storageCredentialsBody(
				scheme+"://warehouse/database/table",
				map[string]string{
					"s3.access-key-id": "vended-key",
					// Already past its expiry, so the very next FileIO load must
					// go back to the catalog rather than reuse it.
					"s3.session-token-expires-at-ms": "1",
					"s3.session-token":               "token-" + strings.Repeat("x", int(n)),
				},
			)))
		},
	})

	tbl, _ := newExternalTable(t, cat, scheme)

	refreshed, err := cat.RefreshTableCredentials(context.Background(), tbl)
	require.NoError(t, err)
	require.Equal(t, int32(1), credsCalls.Load())

	_, err = refreshed.FS(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "token-x", rec.lastLoad(t)["s3.session-token"])

	_, err = refreshed.FS(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int32(2), credsCalls.Load(),
		"expired credentials should be renewed through the catalog")
	assert.Equal(t, "token-xx", rec.lastLoad(t)["s3.session-token"])
}

func TestRefreshTableCredentialsNoCredentialsVended(t *testing.T) {
	const scheme = "restcreds-empty"

	cat := newCredsTestCatalog(t, credsCatalogOpts{
		creds: func(w http.ResponseWriter, _ *http.Request) {
			assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"storage-credentials": []any{},
			}))
		},
	})

	tbl, _ := newExternalTable(t, cat, scheme)

	refreshed, err := cat.RefreshTableCredentials(context.Background(), tbl)
	require.NoError(t, err)
	assert.Same(t, tbl, refreshed,
		"a server that vends nothing should leave the table as-is")
}

// TestRefreshTableCredentialsPrefixMismatch pins that credentials are resolved
// against the table's metadata location: a credential for some other prefix
// must not be applied.
func TestRefreshTableCredentialsPrefixMismatch(t *testing.T) {
	const scheme = "restcreds-mismatch"

	cat := newCredsTestCatalog(t, credsCatalogOpts{
		creds: func(w http.ResponseWriter, _ *http.Request) {
			assert.NoError(t, json.NewEncoder(w).Encode(storageCredentialsBody(
				scheme+"://warehouse/other-database/other-table",
				map[string]string{"s3.access-key-id": "vended-key"},
			)))
		},
	})

	tbl, _ := newExternalTable(t, cat, scheme)

	refreshed, err := cat.RefreshTableCredentials(context.Background(), tbl)
	require.NoError(t, err)
	assert.Same(t, tbl, refreshed,
		"a server that vends a different prefix credential should leave the table as-is")
}

func TestRefreshTableCredentialsEndpointNotAdvertised(t *testing.T) {
	const scheme = "restcreds-unsupported"

	// Advertise load-table but not the credentials endpoint.
	cat := newCredsTestCatalog(t, credsCatalogOpts{
		endpoints: []string{"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}"},
	})

	tbl, _ := newExternalTable(t, cat, scheme)

	refreshed, err := cat.RefreshTableCredentials(context.Background(), tbl)
	assert.Nil(t, refreshed)
	assert.ErrorIs(t, err, rest.ErrEndpointNotSupported)
}

func TestRefreshTableCredentialsNotFound(t *testing.T) {
	const scheme = "restcreds-notfound"

	cat := newCredsTestCatalog(t, credsCatalogOpts{
		creds: func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"message": "Table does not exist: db.tbl",
					"type":    "NoSuchTableException",
					"code":    404,
				},
			}))
		},
	})

	tbl, _ := newExternalTable(t, cat, scheme)

	refreshed, err := cat.RefreshTableCredentials(context.Background(), tbl)
	assert.Nil(t, refreshed)
	assert.ErrorIs(t, err, catalog.ErrNoSuchTable)
}

// TestRefreshTableCredentialsClearsADLSAuthShadowingVendedToken checks that a
// refreshed adls.token is not shadowed by shared-key, SAS-token, or
// connection-string credentials left in the table's saved config for the same
// storage account, which the Azure FileIO would otherwise select ahead of it.
// Another account's credentials are kept. The recording scheme's host,
// "warehouse", stands in for the storage account.
func TestRefreshTableCredentialsClearsADLSAuthShadowingVendedToken(t *testing.T) {
	const (
		scheme    = "restcreds-stale-adls"
		otherHost = "other.dfs.core.windows.net"
	)

	rec := &ioPropsRecorder{}
	rec.register(t, scheme)

	cat := newCredsTestCatalog(t, credsCatalogOpts{
		creds: func(w http.ResponseWriter, _ *http.Request) {
			assert.NoError(t, json.NewEncoder(w).Encode(storageCredentialsBody(
				scheme+"://warehouse/database/table",
				map[string]string{iceio.ADLSToken: "vended-token"})))
		},
	})
	tbl, _ := newExternalTable(t, cat, scheme, table.WithSavedConfig(iceberg.Properties{
		iceio.ADLSSharedKeyAccountName:                 "warehouse",
		iceio.ADLSSharedKeyAccountKey:                  "stale-key",
		iceio.ADLSSasTokenPrefix + "warehouse":         "stale-sas",
		iceio.ADLSConnectionStringPrefix + "warehouse": "stale-conn",
		iceio.ADLSSasTokenPrefix + otherHost:           "other-sas",
	}))

	refreshed, err := cat.RefreshTableCredentials(context.Background(), tbl)
	require.NoError(t, err)
	_, err = refreshed.FS(context.Background())
	require.NoError(t, err)

	props := rec.lastLoad(t)
	assert.Equal(t, "vended-token", props[iceio.ADLSToken])
	for _, k := range []string{
		iceio.ADLSSharedKeyAccountName,
		iceio.ADLSSharedKeyAccountKey,
		iceio.ADLSSasTokenPrefix + "warehouse",
		iceio.ADLSConnectionStringPrefix + "warehouse",
	} {
		assert.NotContains(t, props, k, "stale credential %q must not survive the refresh", k)
	}
	assert.Equal(t, "other-sas", props[iceio.ADLSSasTokenPrefix+otherHost])
}

// tableResponseCalls are the catalog methods that build a table from a
// load-table response, each of which merges the response's storage credentials
// over the table-wide config.
var tableResponseCalls = []struct {
	name string
	call func(context.Context, *rest.Catalog) (*table.Table, error)
}{
	{"CreateTable", func(ctx context.Context, cat *rest.Catalog) (*table.Table, error) {
		return cat.CreateTable(ctx, catalog.ToIdentifier("db", "tbl"), tableSchemaSimple)
	}},
	{"RegisterTable", func(ctx context.Context, cat *rest.Catalog) (*table.Table, error) {
		return cat.RegisterTable(ctx, catalog.ToIdentifier("db", "tbl"), "unused-metadata-location")
	}},
	{"LoadTable", func(ctx context.Context, cat *rest.Catalog) (*table.Table, error) {
		return cat.LoadTable(ctx, catalog.ToIdentifier("db", "tbl"))
	}},
}

// tableResponseFSProps calls each of tableResponseCalls against a server that
// answers with a load-table response carrying tableConfig and one storage
// credential, vended, for metadataLoc's prefix. It returns the properties each
// call's table FileIO was built from, keyed by call name.
func tableResponseFSProps(t *testing.T, scheme, metadataLoc string, tableConfig, vended map[string]string) map[string]map[string]string {
	t.Helper()

	rec := &ioPropsRecorder{}
	rec.register(t, scheme)

	respond := func(w http.ResponseWriter, _ *http.Request) {
		body := storageCredentialsBody(metadataLoc[:strings.LastIndex(metadataLoc, "/metadata/")], vended)
		body["metadata-location"] = metadataLoc
		body["metadata"] = json.RawMessage(strings.ReplaceAll(
			exampleTableMetadataNoSnapshotV1, "s3://", scheme+"://"))
		body["config"] = tableConfig
		assert.NoError(t, json.NewEncoder(w).Encode(body))
	}
	cat := newCredsTestCatalog(t, credsCatalogOpts{
		loadTable: respond,
		routes: map[string]http.HandlerFunc{
			"/v1/namespaces/db/tables":   respond,
			"/v1/namespaces/db/register": respond,
		},
	})

	got := make(map[string]map[string]string, len(tableResponseCalls))
	for _, tc := range tableResponseCalls {
		tbl, err := tc.call(context.Background(), cat)
		require.NoError(t, err, tc.name)
		_, err = tbl.FS(context.Background())
		require.NoError(t, err, tc.name)
		got[tc.name] = rec.lastLoad(t)
	}

	return got
}

// TestTableResponseVendedADLSTokenClearsShadowingCredentials covers the
// table-wide merge sites, where config is the whole table config rather than a
// per-location copy. The table's FileIO serves only the metadata location's
// account, so a vended adls.token drops what would apply to that account ahead
// of it or set its lifetime: that account's SAS token and connection string,
// any shared key, and every inherited credential expiry. Another account's SAS token and
// connection string never apply to it and are kept.
func TestTableResponseVendedADLSTokenClearsShadowingCredentials(t *testing.T) {
	const (
		scheme    = "restcreds-table-adls"
		host      = "acct.dfs.core.windows.net"
		otherHost = "other.dfs.core.windows.net"
	)

	got := tableResponseFSProps(t, scheme,
		scheme+"://container@"+host+"/table/metadata/00000-a.metadata.json",
		map[string]string{
			// The factory would sign this account's requests with another
			// account's shared key.
			iceio.ADLSSharedKeyAccountName:              "other",
			iceio.ADLSSharedKeyAccountKey:               "other-key",
			iceio.ADLSSasTokenPrefix + host:             "stale-sas",
			"adls.sas-token-expires-at-ms." + host:      "1000",
			iceio.ADLSConnectionStringPrefix + "acct":   "stale-conn",
			iceio.ADLSSasTokenPrefix + otherHost:        "other-sas",
			"adls.sas-token-expires-at-ms." + otherHost: "1000",
			iceio.ADLSConnectionStringPrefix + "other":  "other-conn",
			"expiration-time":                           "1000",
		},
		map[string]string{iceio.ADLSToken: "vended-token"})

	for name, props := range got {
		assert.Equal(t, "vended-token", props[iceio.ADLSToken], name)
		for _, k := range []string{
			iceio.ADLSSharedKeyAccountName,
			iceio.ADLSSharedKeyAccountKey,
			iceio.ADLSSasTokenPrefix + host,
			iceio.ADLSConnectionStringPrefix + "acct",
		} {
			assert.NotContains(t, props, k, "%s: credential %q would be selected ahead of the token", name, k)
		}
		for _, k := range []string{
			"adls.sas-token-expires-at-ms." + host,
			"adls.sas-token-expires-at-ms." + otherHost,
			"expiration-time",
		} {
			assert.NotContains(t, props, k, "%s: inherited expiry %q would set the token's lifetime", name, k)
		}
		assert.Equal(t, "other-sas", props[iceio.ADLSSasTokenPrefix+otherHost], name)
		assert.Equal(t, "other-conn", props[iceio.ADLSConnectionStringPrefix+"other"], name)
	}
}

// TestTableResponseLayersPartialS3Credential pins the existing behavior at the
// table-wide merge sites: a partial vended S3 credential is layered over the
// table config rather than replacing the key pair there. Whether a partial
// credential should replace instead is a separate decision from adls.token
// support.
func TestTableResponseLayersPartialS3Credential(t *testing.T) {
	const scheme = "restcreds-table-s3"

	got := tableResponseFSProps(t, scheme,
		scheme+"://warehouse/database/table/metadata/00000-a.metadata.json",
		map[string]string{
			iceio.S3AccessKeyID:     "static-key",
			iceio.S3SecretAccessKey: "static-secret",
		},
		map[string]string{iceio.S3SessionToken: "vended-token"})

	for name, props := range got {
		assert.Equal(t, "static-key", props[iceio.S3AccessKeyID], name)
		assert.Equal(t, "static-secret", props[iceio.S3SecretAccessKey], name)
		assert.Equal(t, "vended-token", props[iceio.S3SessionToken], name)
	}
}
