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

package catalog

import (
	"context"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"sync"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/config"
)

// Registry holds catalog factories independently of the package-level default.
// The zero value is ready to use, and its methods are safe for concurrent use.
type Registry struct {
	mu         sync.RWMutex
	registrars map[string]Registrar
}

// NewRegistry returns an empty registry. Importing a catalog backend registers
// it with the package-level default, not with independently created registries.
// Use CloneDefaultRegistry to start with those registrations.
func NewRegistry() *Registry {
	return &Registry{registrars: make(map[string]Registrar)}
}

// Clone returns a snapshot with independent registrations and synchronization.
// Existing Registrar implementations are shared; callers should make sure
// their implementations are safe to use concurrently.
func (r *Registry) Clone() *Registry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return &Registry{registrars: maps.Clone(r.registrars)}
}

func (r *Registry) get(catalogType string) (Registrar, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	registrar, ok := r.registrars[strings.ToLower(catalogType)]

	return registrar, ok
}

var defaultRegistry = NewRegistry()

// Registrar is a factory for creating Catalog instances, used for registering to use
// with LoadCatalog.
type Registrar interface {
	GetCatalog(ctx context.Context, catalogName string, props iceberg.Properties) (Catalog, error)
}

type RegistrarFunc func(context.Context, string, iceberg.Properties) (Catalog, error)

func (f RegistrarFunc) GetCatalog(ctx context.Context, catalogName string, props iceberg.Properties) (Catalog, error) {
	return f(ctx, catalogName, props)
}

// Register adds a catalog factory to this registry. An existing catalog type
// is replaced. An empty name or nil registrar returns ErrInvalidArgument.
func (r *Registry) Register(catalogType string, reg Registrar) error {
	if catalogType == "" {
		return fmt.Errorf("%w: catalog registry type must not be empty", iceberg.ErrInvalidArgument)
	}
	if reg == nil {
		return fmt.Errorf("%w: catalog registrar must not be nil", iceberg.ErrInvalidArgument)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.registrars == nil {
		r.registrars = make(map[string]Registrar)
	}
	r.registrars[strings.ToLower(catalogType)] = reg

	return nil
}

// MustRegister registers a catalog factory or panics. Intended for package init.
func (r *Registry) MustRegister(catalogType string, reg Registrar) {
	if err := r.Register(catalogType, reg); err != nil {
		panic(err)
	}
}

// Unregister removes a catalog factory from this registry.
func (r *Registry) Unregister(catalogType string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.registrars, strings.ToLower(catalogType))
}

// GetRegisteredCatalogs lists catalog types in sorted order.
func (r *Registry) GetRegisteredCatalogs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return slices.Sorted(maps.Keys(r.registrars))
}

// Register adds a factory to the process-wide default registry. An existing
// catalog type is replaced. Invalid registrations return an error instead of
// panicking. Use MustRegister for package initialization.
func Register(catalogType string, reg Registrar) error {
	return defaultRegistry.Register(catalogType, reg)
}

// MustRegister registers a catalog factory in the default registry or panics.
func MustRegister(catalogType string, reg Registrar) {
	defaultRegistry.MustRegister(catalogType, reg)
}

// Unregister removes a catalog factory from the default registry.
func Unregister(catalogType string) {
	defaultRegistry.Unregister(catalogType)
}

// GetRegisteredCatalogs returns the registered catalog types in sorted order.
func GetRegisteredCatalogs() []string {
	return defaultRegistry.GetRegisteredCatalogs()
}

// CloneDefaultRegistry returns an independent snapshot of the current default
// registry, including backends registered by side-effect imports.
func CloneDefaultRegistry() *Registry {
	return defaultRegistry.Clone()
}

// Load returns a catalog that must be closed by the caller to release its
// reporter, transports, and other catalog-owned resources.
//
// Load allows loading a specific catalog by URI and properties.
//
// This is utilized alongside Register/Unregister to not only allow
// easier catalog loading but also to allow for custom catalog implementations to
// be registered and loaded external to this module.
//
// The name parameter is used to look up the catalog configuration, if one exists,
// that was loaded from the configuration file, ".iceberg-go.yaml". By default,
// the config file is loaded from the user's home directory, but the directory can
// be changed by setting the GOICEBERG_HOME environment variable to the path of the
// directory containing the ".iceberg-go.yaml" file. The name will also be passed to
// the registered GetCatalog function.
//
// The catalog registry will be checked for the catalog's type. The "type" property
// is used first to search the catalog registry, with the passed properties taking
// priority over any loaded config.
//
// If there is no "type" in the configuration and no "type" in the passed in properties,
// then the "uri" property is used to look up the catalog by checking the scheme. Again,
// if there is an "uri" key set in the passed in "props" it will take priority over the
// configuration.
//
// Currently, the following catalog types are registered by default:
//
//   - "glue" for AWS Glue Data Catalog, the rest of the URI is ignored, all configuration
//     should be provided using the properties. "glue.region", "glue.endpoint",
//     "glue.max-retries", etc. Default AWS credentials are used if found, or can be
//     overridden by setting "glue.access-key-id", "glue.secret-access-key", and "glue.session-token".
//
//   - "rest" for a REST API catalog, if the properties have an "uri" key, then that will be used
//     as the REST endpoint, otherwise the URI is used as the endpoint. The REST catalog also
//     registers "http" and "https" so that Load with a http/s URI will automatically
//     load the REST Catalog.
func Load(ctx context.Context, name string, props iceberg.Properties) (Catalog, error) {
	return defaultRegistry.Load(ctx, name, props)
}

// Load resolves a catalog using only the factories registered in this registry.
// Configuration resolution matches the package-level Load function.
func (r *Registry) Load(ctx context.Context, name string, props iceberg.Properties) (Catalog, error) {
	if name == "" {
		name = config.EnvConfig.DefaultCatalog
	}

	conf := config.EnvConfig.Catalogs[name]
	if props == nil {
		props = iceberg.Properties{
			"uri":        conf.URI,
			"credential": conf.Credential,
			"warehouse":  conf.Warehouse,
		}
	} else {
		props = maps.Clone(props)
		props["uri"] = props.Get("uri", conf.URI)
		props["credential"] = props.Get("credential", conf.Credential)
		props["warehouse"] = props.Get("warehouse", conf.Warehouse)
	}

	catalogType := props.Get("type", conf.CatalogType)
	if catalogType == "" {
		if strings.Contains(props["uri"], "://") {
			uri, err := url.Parse(props["uri"])
			if err != nil {
				return nil, fmt.Errorf("failed to parse catalog URI: %w", err)
			}
			catalogType = uri.Scheme
		}
	}

	cat, ok := r.get(catalogType)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCatalogNotFound, catalogType)
	}

	return cat.GetCatalog(ctx, name, props)
}
