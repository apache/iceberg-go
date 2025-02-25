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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	_ "github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/config"
	"github.com/stretchr/testify/assert"
)

func TestCatalogRegistry(t *testing.T) {
	ctx := context.Background()
	assert.ElementsMatch(t, []string{
		"rest",
		"http",
		"https",
		"glue",
	}, catalog.GetRegisteredCatalogs())

	catalog.Register("foobar", catalog.RegistrarFunc(func(ctx context.Context, s string, p iceberg.Properties) (catalog.Catalog, error) {
		assert.Equal(t, "foobar", s)
		assert.Equal(t, "baz", p.Get("foo", ""))

		return nil, nil
	}))

	assert.ElementsMatch(t, []string{
		"rest",
		"http",
		"foobar",
		"https",
		"glue",
	}, catalog.GetRegisteredCatalogs())

	c, err := catalog.Load(ctx, "foobar", iceberg.Properties{"foo": "baz"})
	assert.Nil(t, c)
	assert.ErrorIs(t, err, catalog.ErrCatalogNotFound)

	catalog.Register("foobar", catalog.RegistrarFunc(func(ctx context.Context, s string, p iceberg.Properties) (catalog.Catalog, error) {
		assert.Equal(t, "not found", s)
		assert.Equal(t, "baz", p.Get("foo", ""))

		return nil, nil
	}))

	c, err = catalog.Load(ctx, "not found", iceberg.Properties{"type": "foobar", "foo": "baz"})
	assert.Nil(t, c)
	assert.NoError(t, err)

	catalog.Register("foobar", catalog.RegistrarFunc(func(ctx context.Context, s string, p iceberg.Properties) (catalog.Catalog, error) {
		assert.Equal(t, "not found", s)
		assert.Equal(t, "foobar://helloworld", p.Get("uri", ""))
		assert.Equal(t, "baz", p.Get("foo", ""))

		return nil, nil
	}))

	c, err = catalog.Load(ctx, "not found", iceberg.Properties{
		"uri": "foobar://helloworld",
		"foo": "baz",
	})
	assert.Nil(t, c)
	assert.NoError(t, err)

	catalog.Unregister("foobar")
	assert.ElementsMatch(t, []string{
		"rest",
		"http",
		"https",
		"glue",
	}, catalog.GetRegisteredCatalogs())
}

func TestRegistryPanic(t *testing.T) {
	assert.PanicsWithValue(t, "catalog: RegisterCatalog catalog factory is nil", func() { catalog.Register("foobar", nil) })
}

func TestCatalogWithEmptyName(t *testing.T) {
	ctx := context.Background()
	config.EnvConfig.DefaultCatalog = "test-default"
	config.EnvConfig.Catalogs = map[string]config.CatalogConfig{
		"test-default": {
			URI:         "http://localhost:8181/",
			Credential:  "default-credential",
			Warehouse:   "/default/warehouse",
			CatalogType: "mock",
		},
	}
	catalog.Register("mock", catalog.RegistrarFunc(func(ctx context.Context, name string, props iceberg.Properties) (catalog.Catalog, error) {
		// Ensure the correct name and properties are passed
		assert.Equal(t, "test-default", name)
		assert.Equal(t, "http://localhost:8181/", props.Get("uri", ""))
		assert.Equal(t, "default-credential", props.Get("credential", ""))
		assert.Equal(t, "/default/warehouse", props.Get("warehouse", ""))

		return nil, nil
	}))
	c, err := catalog.Load(ctx, "", nil)
	assert.Nil(t, c)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{
		"rest",
		"http",
		"https",
		"glue",
		"mock",
	}, catalog.GetRegisteredCatalogs())
	catalog.Unregister("mock")
}

func TestCatalogLoadInvalidURI(t *testing.T) {
	ctx := context.Background()
	config.EnvConfig.DefaultCatalog = "default"
	config.EnvConfig.Catalogs = map[string]config.CatalogConfig{
		"default": {
			URI:         "http://localhost:8181/",
			Credential:  "default-credential",
			Warehouse:   "/default/warehouse",
			CatalogType: "mock",
		},
	}

	catalog.Register("mock", catalog.RegistrarFunc(func(ctx context.Context, name string, props iceberg.Properties) (catalog.Catalog, error) {
		return nil, nil
	}))
	props := iceberg.Properties{
		"uri": "://invalid-uri", // This will cause url.Parse to fail
	}
	c, err := catalog.Load(ctx, "mock", props)

	assert.Nil(t, c)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse catalog URI")
	catalog.Unregister("mock")
}

func TestRegistryFromConfig(t *testing.T) {
	ctx := context.Background()
	var params url.Values

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, r *http.Request) {
		params = r.URL.Query()

		json.NewEncoder(w).Encode(map[string]any{
			"default":   map[string]any{},
			"overrides": map[string]any{},
		})
	})

	srv := httptest.NewServer(mux)

	defer func(cats map[string]config.CatalogConfig) {
		config.EnvConfig.Catalogs = cats
	}(config.EnvConfig.Catalogs)

	config.EnvConfig.Catalogs = map[string]config.CatalogConfig{
		"foobar": {
			CatalogType: "rest",
			URI:         srv.URL,
			Warehouse:   "catalog_name",
		},
	}

	c, err := catalog.Load(ctx, "foobar", nil)
	assert.NoError(t, err)
	assert.IsType(t, &rest.Catalog{}, c)
	assert.Equal(t, "foobar", c.(*rest.Catalog).Name())
	assert.Equal(t, "catalog_name", params.Get("warehouse"))

	c, err = catalog.Load(ctx, "foobar", iceberg.Properties{"warehouse": "overridden"})
	assert.NoError(t, err)
	assert.IsType(t, &rest.Catalog{}, c)
	assert.Equal(t, "foobar", c.(*rest.Catalog).Name())
	assert.Equal(t, "overridden", params.Get("warehouse"))

	srv.Close()

	srv2 := httptest.NewServer(mux)
	defer srv2.Close()

	c, err = catalog.Load(ctx, "foobar", iceberg.Properties{"uri": srv2.URL})
	assert.NoError(t, err)
	assert.IsType(t, &rest.Catalog{}, c)
	assert.Equal(t, "foobar", c.(*rest.Catalog).Name())
	assert.Equal(t, "catalog_name", params.Get("warehouse"))
}
