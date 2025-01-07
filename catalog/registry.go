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
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"sync"

	"github.com/apache/iceberg-go"
)

type registry map[string]Registrar

func (r registry) getKeys() []string {
	regMutex.Lock()
	defer regMutex.Unlock()
	return slices.Collect(maps.Keys(r))
}

func (r registry) set(catalogType string, reg Registrar) {
	regMutex.Lock()
	defer regMutex.Unlock()
	r[catalogType] = reg
}

func (r registry) get(catalogType string) (Registrar, bool) {
	regMutex.Lock()
	defer regMutex.Unlock()
	reg, ok := r[catalogType]
	return reg, ok
}

func (r registry) remove(catalogType string) {
	regMutex.Lock()
	defer regMutex.Unlock()
	delete(r, catalogType)
}

var (
	regMutex        sync.Mutex
	defaultRegistry = registry{}
)

// Registrar is a factory for creating Catalog instances, used for registering to use
// with LoadCatalog.
type Registrar interface {
	GetCatalog(catalogURI string, props iceberg.Properties) (Catalog, error)
}

type RegistrarFunc func(string, iceberg.Properties) (Catalog, error)

func (f RegistrarFunc) GetCatalog(catalogURI string, props iceberg.Properties) (Catalog, error) {
	return f(catalogURI, props)
}

// Register adds the new catalog type to the registry. If the catalog type is already registered, it will be replaced.
func Register(catalogType string, reg Registrar) {
	if reg == nil {
		panic("catalog: RegisterCatalog catalog factory is nil")
	}
	defaultRegistry.set(catalogType, reg)
}

// Unregister removes the requested catalog factory from the registry.
func Unregister(catalogType string) {
	defaultRegistry.remove(catalogType)
}

// GetRegsisteredCatalogs returns the list of registered catalog names that can
// be looked up via LoadCatalog.
func GetRegisteredCatalogs() []string {
	return defaultRegistry.getKeys()
}

// Load allows loading a specific catalog by URI and properties.
//
// This is utilized alongside RegisterCatalog/UnregisterCatalog to not only allow
// easier catalog loading but also to allow for custom catalog implementations to
// be registered and loaded external to this module.
//
// The URI is used to determine the catalog type by first checking if it contains
// the string "://" indicating the presence of a scheme. If so, the schema is used
// to lookup the registered catalog. i.e. "glue://..." would return the Glue catalog
// implementation, passing the URI and properties to NewGlueCatalog. If no scheme is
// present, then the URI is used as-is to lookup the catalog factory function.
//
// Currently the following catalogs are registered by default:
//
//   - "glue" for AWS Glue Data Catalog, the rest of the URI is ignored, all configuration
//     should be provided using the properties. "glue.region", "glue.endpoint",
//     "glue.max-retries", etc. Default AWS credentials are used if found, or can be
//     overridden by setting "glue.access-key", "glue.secret-access-key", and "glue.session-token".
//
//   - "rest" for a REST API catalog, if the properties have a "uri" key, then that will be used
//     as the REST endpoint, otherwise the URI is used as the endpoint. The REST catalog also
//     registers "http" and "https" so that Load with an http/s URI will automatically
//     load the REST Catalog.
func Load(catalogURI string, props iceberg.Properties) (Catalog, error) {
	var catalogType string
	if strings.Contains(catalogURI, "://") {
		parsed, err := url.Parse(catalogURI)
		if err != nil {
			return nil, err
		}
		catalogType = parsed.Scheme
	} else {
		catalogType = catalogURI
	}

	cat, ok := defaultRegistry.get(catalogType)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrCatalogNotFound, catalogType)
	}

	return cat.GetCatalog(catalogURI, props)
}
