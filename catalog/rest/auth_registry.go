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
	"fmt"
	"maps"
	"strings"
	"sync"

	"github.com/apache/iceberg-go"
)

// keyAuthType selects the catalog's authentication, as in Iceberg Java's
// AuthProperties.AUTH_TYPE.
const keyAuthType = "rest.auth.type"

// Values of the rest.auth.type property. AuthTypeNone and AuthTypeOAuth2 are
// built in; other values name an auth manager registered with
// RegisterAuthManager.
const (
	// AuthTypeNone sends no authorization header, even when token or
	// credential is set.
	AuthTypeNone = "none"
	// AuthTypeOAuth2 uses the token or credential properties. It is the
	// default when rest.auth.type is unset.
	AuthTypeOAuth2 = "oauth2"
	// AuthTypeGoogle is the name the googleauth backend registers under.
	AuthTypeGoogle = "google"
)

// AuthManagerConfig carries what an auth manager backend may configure itself
// from.
type AuthManagerConfig struct {
	// Props holds the properties the client supplied, before the server's
	// /v1/config defaults and overrides are merged in. A server can't select
	// or configure a client's authentication.
	Props iceberg.Properties
}

// AuthManagerFactory builds an AuthManager for a catalog. The context is the
// one passed to NewCatalog or catalog.Load; an AuthManager that refreshes
// credentials later must not depend on its cancellation.
type AuthManagerFactory func(ctx context.Context, cfg AuthManagerConfig) (AuthManager, error)

var (
	authManagerMu       sync.RWMutex
	authManagerRegistry = map[string]AuthManagerFactory{}
)

// RegisterAuthManager registers factory under name so that setting
// rest.auth.type to name selects it. Names are matched case-insensitively.
// It is intended to be called from a backend package's init function, so a
// blank import of the backend is enough to enable it. Registering a name again
// replaces the previous factory. It panics if factory is nil or if name is
// empty or one of the built-in types, none and oauth2.
func RegisterAuthManager(name string, factory AuthManagerFactory) {
	if factory == nil {
		panic(fmt.Sprintf("rest: RegisterAuthManager: nil factory for %q", name))
	}

	key := strings.ToLower(name)
	switch key {
	case "", AuthTypeNone, AuthTypeOAuth2:
		panic(fmt.Sprintf("rest: RegisterAuthManager: %q is reserved", name))
	}

	authManagerMu.Lock()
	defer authManagerMu.Unlock()
	authManagerRegistry[key] = factory
}

func lookupAuthManager(name string) (AuthManagerFactory, bool) {
	authManagerMu.RLock()
	defer authManagerMu.RUnlock()
	f, ok := authManagerRegistry[name]

	return f, ok
}

// authTypeImportHints names the package to import for an auth type that
// iceberg-go provides as an optional backend.
var authTypeImportHints = map[string]string{
	AuthTypeGoogle: "github.com/apache/iceberg-go/catalog/rest/googleauth",
}

// resolveAuthType applies rest.auth.type from the client-supplied properties.
// It runs once, before /v1/config, so the server can neither select nor
// configure the client's authentication. An AuthManager passed with
// WithAuthManager takes precedence over the property.
//
// Unset and oauth2 keep the token / credential flow. none disables it. Any
// other value builds the manager registered under that name, which is then
// used for both the /v1/config request and the catalog session.
func resolveAuthType(ctx context.Context, ops *options, clientProps iceberg.Properties) error {
	raw, ok := clientProps[keyAuthType]
	if !ok || ops.authManager != nil {
		return nil
	}

	authType := strings.ToLower(strings.TrimSpace(raw))
	switch authType {
	case AuthTypeOAuth2:
		return nil
	case AuthTypeNone:
		ops.disableAuth = true

		return nil
	}

	factory, ok := lookupAuthManager(authType)
	if !ok {
		if authType == SignerNameSigV4 {
			return fmt.Errorf("rest: %s=%s is not supported; set %s=true to sign requests with SigV4",
				keyAuthType, raw, keyRestSigV4)
		}
		if pkg, hint := authTypeImportHints[authType]; hint {
			return fmt.Errorf("rest: %s=%s requires its backend; add a blank import: import _ %q",
				keyAuthType, raw, pkg)
		}

		return fmt.Errorf("rest: unknown %s %q: no auth manager is registered under that name",
			keyAuthType, raw)
	}

	am, err := factory(ctx, AuthManagerConfig{Props: maps.Clone(clientProps)})
	if err != nil {
		return fmt.Errorf("rest: %s=%s: %w", keyAuthType, raw, err)
	}
	if am == nil {
		return fmt.Errorf("rest: %s=%s: auth manager factory returned nil", keyAuthType, raw)
	}
	ops.authManager = am

	return nil
}
