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
	"errors"
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
	assert.Contains(t, first.Names(), "mytype")
	assert.NotContains(t, second.Names(), "mytype")
	assert.Contains(t, first.Names(), "rest")
	assert.Contains(t, second.Names(), "rest")

	_, err := first.Load(context.Background(), "test", iceberg.Properties{"type": "MYTYPE"})
	require.NoError(t, err)

	_, err = second.Load(context.Background(), "test", iceberg.Properties{"type": "mytype"})
	require.ErrorIs(t, err, catalog.ErrCatalogNotFound)

	_, err = catalog.Load(context.Background(), "test", iceberg.Properties{"type": "mytype"})
	require.ErrorIs(t, err, catalog.ErrCatalogNotFound)
}

func TestCatalogRegistriesWithSameTypeAreIsolated(t *testing.T) {
	first := catalog.NewRegistry()
	second := catalog.NewRegistry()

	firstErr := errors.New("first factory")
	secondErr := errors.New("second factory")
	registrarFor := func(err error) catalog.Registrar {
		return catalog.RegistrarFunc(func(context.Context, string, iceberg.Properties) (catalog.Catalog, error) {
			return nil, err
		})
	}

	require.NoError(t, first.Register("shared", registrarFor(firstErr)))
	require.NoError(t, second.Register("SHARED", registrarFor(secondErr)))

	_, err := first.Load(context.Background(), "test", iceberg.Properties{"type": "shared"})
	require.ErrorIs(t, err, firstErr)

	_, err = second.Load(context.Background(), "test", iceberg.Properties{"type": "SHARED"})
	require.ErrorIs(t, err, secondErr)

	first.Unregister("SHARED")
	_, err = first.Load(context.Background(), "test", iceberg.Properties{"type": "shared"})
	require.ErrorIs(t, err, catalog.ErrCatalogNotFound)

	_, err = second.Load(context.Background(), "test", iceberg.Properties{"type": "shared"})
	require.ErrorIs(t, err, secondErr)
}

func TestCatalogRegistryZeroValue(t *testing.T) {
	var reg catalog.Registry
	assert.Empty(t, reg.Names())

	require.NoError(t, reg.Register("Mock", stubRegistrar()))
	assert.Equal(t, []string{"mock"}, reg.Names())

	_, err := reg.Load(context.Background(), "test", iceberg.Properties{"type": "MOCK"})
	require.NoError(t, err)

	reg.Unregister("MOCK")
	assert.Empty(t, reg.Names())
}

func TestCatalogRegistryCloneIsIndependent(t *testing.T) {
	source := catalog.NewRegistry()
	require.NoError(t, source.Register("mock", stubRegistrar()))
	clone := source.Clone()
	source.Unregister("mock")

	assert.NotContains(t, source.Names(), "mock")
	assert.Contains(t, clone.Names(), "mock")

	_, err := clone.Load(context.Background(), "test", iceberg.Properties{"type": "mock"})
	require.NoError(t, err)

	// Clones of the zero-value registry also support registration.
	var zero catalog.Registry
	require.NoError(t, zero.Clone().Register("other", stubRegistrar()))
	assert.Empty(t, zero.Names())
}

func TestCatalogNewRegistryDoesNotAffectGlobalRegistrations(t *testing.T) {
	const name = "catalog-clone-test"
	catalog.Register(name, stubRegistrar())
	t.Cleanup(func() { catalog.Unregister(name) })

	cloned := catalog.NewRegistry()
	assert.Contains(t, cloned.Names(), name)

	cloned.Unregister(name)
	assert.NotContains(t, cloned.Names(), name)
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
	assert.NotContains(t, reg.Names(), "mock")
	assert.Panics(t, func() { reg.MustRegister("mock", nil) })
}

func TestCatalogRegistryConcurrentAccess(t *testing.T) {
	reg := catalog.NewRegistry()
	const workers = 16
	baseCount := len(reg.Names())
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
			_ = reg.Names()
			_ = reg.Clone()
		}(i)
	}
	wg.Wait()
	assert.Len(t, reg.Names(), baseCount+workers)
}

func TestCatalogNewRegistrySnapshotsDefaultRegistrations(t *testing.T) {
	const name = "catalog-snapshot-test"
	before := catalog.NewRegistry()
	assert.NotContains(t, before.Names(), name)

	catalog.Register(name, stubRegistrar())
	t.Cleanup(func() { catalog.Unregister(name) })
	after := catalog.NewRegistry()

	assert.NotContains(t, before.Names(), name)
	assert.Contains(t, after.Names(), name)
	catalog.Unregister(name)
	assert.Contains(t, after.Names(), name)

	_, err := after.Load(context.Background(), "test", iceberg.Properties{"type": name})
	require.NoError(t, err)
}

func TestCatalogRegistryDuplicateRegistrationReplacesFactory(t *testing.T) {
	reg := catalog.NewRegistry()
	firstErr := errors.New("first factory")
	secondErr := errors.New("replacement factory")

	require.NoError(t, reg.Register("replace-test", catalog.RegistrarFunc(
		func(context.Context, string, iceberg.Properties) (catalog.Catalog, error) {
			return nil, firstErr
		},
	)))
	require.NoError(t, reg.Register("REPLACE-TEST", catalog.RegistrarFunc(
		func(context.Context, string, iceberg.Properties) (catalog.Catalog, error) {
			return nil, secondErr
		},
	)))

	_, err := reg.Load(context.Background(), "test", iceberg.Properties{"type": "replace-test"})
	require.ErrorIs(t, err, secondErr)
}
