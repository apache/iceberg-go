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

package encryption

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
)

// KMSTypeKey is the catalog property that selects a [KeyManagementClient]
// implementation registered with a [KMSRegistry] (e.g. "memory").
// It is an iceberg-go-specific property and is not portable to other Iceberg
// implementations.
const KMSTypeKey = "kms-type"

var (
	// ErrKMSTypeNotFound is returned by [KMSRegistry.Load] when the
	// requested KMS has not been registered.
	ErrKMSTypeNotFound = errors.New("encryption: kms type not found")

	// ErrKMSAlreadyRegistered is returned when a factory name is already in use.
	ErrKMSAlreadyRegistered = errors.New("encryption: kms factory already registered")

	// ErrInvalidKMSRegistration is returned for an empty name or nil factory.
	ErrInvalidKMSRegistration = errors.New("encryption: invalid kms registration")
)

// KMSFactory creates a [KeyManagementClient] from catalog/table properties.
// Factories receive the caller's context and full property map.
type KMSFactory func(ctx context.Context, props map[string]string) (KeyManagementClient, error)

// KMSRegistry owns the named factories used to construct KMS clients.
// A registry is safe for concurrent use and must not be copied after first use.
// Its zero value is an empty, usable registry. Use [NewKMSRegistry] to clone
// the global default, which includes the built-in "memory" factory (for testing).
//
// Separate registries let tests and catalog clients register the same name
// without modifying each other's factories.
type KMSRegistry struct {
	mu        sync.RWMutex
	factories map[string]KMSFactory
}

func builtInKMSRegistry() *KMSRegistry {
	return &KMSRegistry{
		factories: map[string]KMSFactory{
			"memory": func(_ context.Context, _ map[string]string) (KeyManagementClient, error) {
				return NewInMemoryKeyManagementClient(), nil
			},
		},
	}
}

// defaultKMSRegistry receives package-level registrations, including those
// made by cloud KMS providers through side-effect imports.
var defaultKMSRegistry = builtInKMSRegistry()

// NewKMSRegistry returns an independent snapshot of the default registry.
// Providers should be imported before calling it. Initializing a package-level
// variable with NewKMSRegistry can miss registrations made by other packages.
func NewKMSRegistry() *KMSRegistry {
	return defaultKMSRegistry.Clone()
}

// Clone copies the registered factories into a separate registry. Factories
// themselves are shared, but future registrations and removals are isolated.
func (r *KMSRegistry) Clone() *KMSRegistry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return &KMSRegistry{factories: maps.Clone(r.factories)}
}

// Register adds a named KMS factory. It returns an error for an empty name,
// nil factory, or duplicate name. The registry is unchanged on error.
func (r *KMSRegistry) Register(name string, factory KMSFactory) error {
	if name == "" {
		return fmt.Errorf("%w: name must not be empty", ErrInvalidKMSRegistration)
	}
	if factory == nil {
		return fmt.Errorf("%w: factory for %q is nil", ErrInvalidKMSRegistration, name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.factories[name]; exists {
		return fmt.Errorf("%w: %q", ErrKMSAlreadyRegistered, name)
	}
	if r.factories == nil {
		r.factories = make(map[string]KMSFactory)
	}
	r.factories[name] = factory

	return nil
}

// MustRegister registers a factory or panics. It is intended for init functions
// in KMS provider packages where registration failure is unrecoverable.
func (r *KMSRegistry) MustRegister(name string, factory KMSFactory) {
	if err := r.Register(name, factory); err != nil {
		panic(err)
	}
}

// Unregister removes a named factory. Removing an unknown name is a no-op.
func (r *KMSRegistry) Unregister(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.factories, name)
}

// Names returns the currently registered factory names in sorted order.
// The returned slice is independent of the registry.
func (r *KMSRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.factories))
	for name := range r.factories {
		names = append(names, name)
	}
	slices.Sort(names)

	return names
}

// Load resolves and constructs a [KeyManagementClient] from props. It looks
// up the name under [KMSTypeKey] and returns an error wrapping
// [ErrKMSTypeNotFound] when the property is empty or not registered.
// The factory is invoked outside the lock, so it may update this registry.
func (r *KMSRegistry) Load(ctx context.Context, props map[string]string) (KeyManagementClient, error) {
	name := props[KMSTypeKey]
	if name == "" {
		return nil, fmt.Errorf("%w: %q is not set", ErrKMSTypeNotFound, KMSTypeKey)
	}

	r.mu.RLock()
	factory, ok := r.factories[name]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrKMSTypeNotFound, name)
	}

	return factory(ctx, props)
}

// RegisterKMS adds a factory to the process-wide default registry. As in
// previous releases, invalid or duplicate registrations panic. Use a separate
// KMSRegistry and its Register method to handle registration errors.
func RegisterKMS(name string, factory KMSFactory) {
	if factory == nil {
		panic("encryption: RegisterKMS factory is nil")
	}
	if err := defaultKMSRegistry.Register(name, factory); err != nil {
		if errors.Is(err, ErrKMSAlreadyRegistered) {
			panic("encryption: RegisterKMS called twice for name " + name)
		}
		panic(err)
	}
}

// MustRegisterKMS registers a global KMS factory or panics. It is intended
// for use by KMS providers during package initialization.
func MustRegisterKMS(name string, factory KMSFactory) {
	RegisterKMS(name, factory)
}

// UnregisterKMS removes a factory from the default registry. Existing
// independent registry snapshots are unaffected.
func UnregisterKMS(name string) {
	defaultKMSRegistry.Unregister(name)
}

// GetRegisteredKMSNames returns globally registered factory names, sorted.
func GetRegisteredKMSNames() []string {
	return defaultKMSRegistry.Names()
}

// LoadKeyManagementClient constructs a KMS client using the default registry.
// Its context is forwarded to the selected factory.
func LoadKeyManagementClient(ctx context.Context, props map[string]string) (KeyManagementClient, error) {
	return defaultKMSRegistry.Load(ctx, props)
}
