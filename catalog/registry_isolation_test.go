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
	"fmt"
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func stubRegistrar() catalog.Registrar {
	return catalog.RegistrarFunc(func(context.Context, string, iceberg.Properties) (catalog.Catalog, error) {
		return nil, nil
	})
}

func TestCatalogRegistryIsolation(t *testing.T) {
	first := catalog.NewRegistry()
	second := catalog.NewRegistry()

	require.NoError(t, first.Register("MyType", stubRegistrar()))
	assert.Equal(t, []string{"mytype"}, first.GetRegisteredCatalogs())
	assert.Empty(t, second.GetRegisteredCatalogs())

	_, err := first.Load(context.Background(), "test", iceberg.Properties{"type": "MYTYPE"})
	require.NoError(t, err)

	_, err = second.Load(context.Background(), "test", iceberg.Properties{"type": "mytype"})
	require.ErrorIs(t, err, catalog.ErrCatalogNotFound)

	_, err = catalog.Load(context.Background(), "test", iceberg.Properties{"type": "mytype"})
	require.ErrorIs(t, err, catalog.ErrCatalogNotFound)
}

func TestCatalogRegistryCloneIsIndependent(t *testing.T) {
	source := catalog.NewRegistry()
	require.NoError(t, source.Register("mock", stubRegistrar()))
	clone := source.Clone()
	source.Unregister("mock")

	assert.Empty(t, source.GetRegisteredCatalogs())
	assert.Equal(t, []string{"mock"}, clone.GetRegisteredCatalogs())

	_, err := clone.Load(context.Background(), "test", iceberg.Properties{"type": "mock"})
	require.NoError(t, err)

	// Clones of the zero-value registry also support registration.
	var zero catalog.Registry
	require.NoError(t, zero.Clone().Register("other", stubRegistrar()))
	assert.Empty(t, zero.GetRegisteredCatalogs())
}

func TestCatalogCloneDefaultRegistryDoesNotAffectGlobalRegistrations(t *testing.T) {
	const name = "catalog-clone-test"
	require.NoError(t, catalog.Register(name, stubRegistrar()))
	t.Cleanup(func() { catalog.Unregister(name) })

	cloned := catalog.CloneDefaultRegistry()
	assert.Contains(t, cloned.GetRegisteredCatalogs(), name)

	cloned.Unregister(name)
	assert.NotContains(t, cloned.GetRegisteredCatalogs(), name)
	assert.Contains(t, catalog.GetRegisteredCatalogs(), name)

	_, err := cloned.Load(context.Background(), "test", iceberg.Properties{"type": name})
	require.ErrorIs(t, err, catalog.ErrCatalogNotFound)
	_, err = catalog.Load(context.Background(), "test", iceberg.Properties{"type": name})
	require.NoError(t, err)
}

func TestCatalogRegistryRejectsInvalidRegistration(t *testing.T) {
	reg := catalog.NewRegistry()
	require.ErrorIs(t, reg.Register("", stubRegistrar()), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, reg.Register("mock", nil), iceberg.ErrInvalidArgument)
	assert.Empty(t, reg.GetRegisteredCatalogs())
	assert.Panics(t, func() { reg.MustRegister("mock", nil) })
}

func TestCatalogRegistryConcurrentAccess(t *testing.T) {
	reg := catalog.NewRegistry()
	const workers = 16
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("mock-%d", i)
			if err := reg.Register(name, stubRegistrar()); err != nil {
				t.Errorf("register %s: %v", name, err)
				return
			}
			if _, err := reg.Load(context.Background(), "test", iceberg.Properties{"type": name}); err != nil {
				t.Errorf("load %s: %v", name, err)
			}
			_ = reg.GetRegisteredCatalogs()
			_ = reg.Clone()
		}(i)
	}
	wg.Wait()
	assert.Len(t, reg.GetRegisteredCatalogs(), workers)
}
