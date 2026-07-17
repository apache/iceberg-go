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
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
)

// Catalog/table property keys used to select and configure a KeyManagementClient.
const (
	// KMSTypeKey selects a built-in KeyManagementClient implementation
	// registered via [RegisterKMS] (e.g. "memory").
	KMSTypeKey = "encryption.kms-type"

	// KMSImplKey selects a custom KeyManagementClient implementation by a
	// name registered via [RegisterKMS]. It is an alternative to
	// [KMSTypeKey] for vendors that ship their own KMS integration under a
	// distinct name; the two properties share the same registry and
	// resolution mechanism.
	KMSImplKey = "encryption.kms-impl"
)

// ErrKMSTypeNotFound is returned by [LoadKeyManagementClient] when the
// requested kmsType has not been registered via [RegisterKMS].
var ErrKMSTypeNotFound = errors.New("encryption: kms type not found")

// KMSFactory creates a [KeyManagementClient] from catalog/table properties.
// props is the full property map (typically catalog properties); factories
// should read whichever keys they need (e.g. key material, endpoints,
// credentials) directly from it.
type KMSFactory func(props map[string]string) (KeyManagementClient, error)

var (
	kmsRegMutex sync.RWMutex
	kmsRegistry = map[string]KMSFactory{}
)

// RegisterKMS adds a new named [KMSFactory] to the registry so it can later
// be selected via the [KMSTypeKey] or [KMSImplKey] catalog property. It
// panics if factory is nil or if name is already registered.
func RegisterKMS(name string, factory KMSFactory) {
	if factory == nil {
		panic("encryption: RegisterKMS factory is nil")
	}

	kmsRegMutex.Lock()
	defer kmsRegMutex.Unlock()

	if _, dup := kmsRegistry[name]; dup {
		panic("encryption: RegisterKMS called twice for name " + name)
	}
	kmsRegistry[name] = factory
}

// UnregisterKMS removes the requested named factory from the registry.
func UnregisterKMS(name string) {
	kmsRegMutex.Lock()
	defer kmsRegMutex.Unlock()
	delete(kmsRegistry, name)
}

// GetRegisteredKMSNames returns the names of all currently registered KMS
// factories.
func GetRegisteredKMSNames() []string {
	kmsRegMutex.RLock()
	defer kmsRegMutex.RUnlock()

	return slices.Collect(maps.Keys(kmsRegistry))
}

// LoadKeyManagementClient resolves and constructs a [KeyManagementClient]
// from props. It looks up the KMS name under [KMSTypeKey] first, falling
// back to [KMSImplKey] if present; it returns an error wrapping
// [ErrKMSTypeNotFound] if neither property is set or the named KMS has not
// been registered via [RegisterKMS].
func LoadKeyManagementClient(props map[string]string) (KeyManagementClient, error) {
	name := props[KMSTypeKey]
	if name == "" {
		name = props[KMSImplKey]
	}

	if name == "" {
		return nil, fmt.Errorf("%w: neither %q nor %q is set", ErrKMSTypeNotFound, KMSTypeKey, KMSImplKey)
	}

	kmsRegMutex.RLock()
	factory, ok := kmsRegistry[name]
	kmsRegMutex.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrKMSTypeNotFound, name)
	}

	return factory(props)
}

func init() {
	RegisterKMS("memory", func(_ map[string]string) (KeyManagementClient, error) {
		return NewInMemoryKeyManagementClient(), nil
	})
}
