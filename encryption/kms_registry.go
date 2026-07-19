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

// KMSTypeKey is the catalog/table property that selects a
// [KeyManagementClient] implementation registered via [RegisterKMS] (e.g.
// "memory"). It is a registered short name, not a fully-qualified class
// name.
const KMSTypeKey = "encryption.kms-type"

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
// be selected via the [KMSTypeKey] catalog property. It panics if factory is
// nil or if name is already registered.
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
// from props. It looks up the KMS name under [KMSTypeKey]; it returns an
// error wrapping [ErrKMSTypeNotFound] if the property is unset or the named
// KMS has not been registered via [RegisterKMS].
func LoadKeyManagementClient(props map[string]string) (KeyManagementClient, error) {
	name := props[KMSTypeKey]
	if name == "" {
		return nil, fmt.Errorf("%w: %q is not set", ErrKMSTypeNotFound, KMSTypeKey)
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
