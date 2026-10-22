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

package encryption_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/apache/iceberg-go/encryption"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func memoryKMSFactory(_ map[string]string) (encryption.KeyManagementClient, error) {
	return encryption.NewInMemoryKeyManagementClient(), nil
}

func TestKMSRegistry_LoadMemory(t *testing.T) {
	registry := encryption.NewKMSRegistry()
	kms, err := registry.Load(map[string]string{
		encryption.KMSTypeKey: "memory",
	})
	require.NoError(t, err)
	assert.IsType(t, &encryption.InMemoryKeyManagementClient{}, kms)
}

func TestKMSRegistry_NoPropertiesSet(t *testing.T) {
	_, err := encryption.NewKMSRegistry().Load(map[string]string{})
	require.ErrorIs(t, err, encryption.ErrKMSTypeNotFound)
}

func TestKMSRegistry_UnregisteredName(t *testing.T) {
	_, err := encryption.NewKMSRegistry().Load(map[string]string{
		encryption.KMSTypeKey: "does-not-exist",
	})
	require.ErrorIs(t, err, encryption.ErrKMSTypeNotFound)
}

func TestKMSRegistry_DuplicateRegistration(t *testing.T) {
	registry := encryption.NewKMSRegistry()
	require.NoError(t, registry.Register("custom", memoryKMSFactory))
	err := registry.Register("custom", memoryKMSFactory)
	require.ErrorIs(t, err, encryption.ErrKMSAlreadyRegistered)

	_, err = registry.Load(map[string]string{encryption.KMSTypeKey: "custom"})
	require.NoError(t, err)
}

func TestKMSRegistry_InvalidRegistration(t *testing.T) {
	registry := encryption.NewKMSRegistry()
	require.ErrorIs(t, registry.Register("invalid", nil), encryption.ErrInvalidKMSRegistration)
	require.ErrorIs(t, registry.Register("", memoryKMSFactory), encryption.ErrInvalidKMSRegistration)
	assert.Equal(t, []string{"memory"}, registry.Names())
}

func TestKMSRegistry_IsolatedRegistries(t *testing.T) {
	first := encryption.NewKMSRegistry()
	second := encryption.NewKMSRegistry()
	require.NoError(t, first.Register("shared", memoryKMSFactory))
	require.NoError(t, second.Register("shared", memoryKMSFactory))

	first.Unregister("shared")
	_, err := first.Load(map[string]string{encryption.KMSTypeKey: "shared"})
	require.ErrorIs(t, err, encryption.ErrKMSTypeNotFound)

	_, err = second.Load(map[string]string{encryption.KMSTypeKey: "shared"})
	require.NoError(t, err)
	assert.Equal(t, []string{"memory", "shared"}, second.Names())
}

func TestKMSRegistry_NamesSortedAndIndependent(t *testing.T) {
	registry := encryption.NewKMSRegistry()
	require.NoError(t, registry.Register("z", memoryKMSFactory))
	require.NoError(t, registry.Register("a", memoryKMSFactory))

	names := registry.Names()
	assert.Equal(t, []string{"a", "memory", "z"}, names)

	names[0] = "changed"
	assert.Equal(t, []string{"a", "memory", "z"}, registry.Names())
}

func TestKMSRegistry_ZeroValue(t *testing.T) {
	var registry encryption.KMSRegistry
	assert.Empty(t, registry.Names())
	require.NoError(t, registry.Register("custom", memoryKMSFactory))

	_, err := registry.Load(map[string]string{encryption.KMSTypeKey: "custom"})
	require.NoError(t, err)
	registry.Unregister("custom")
	assert.Empty(t, registry.Names())
}

func TestKMSRegistry_FactoryError(t *testing.T) {
	registry := encryption.NewKMSRegistry()
	wantErr := assert.AnError
	require.NoError(t, registry.Register("error-kms", func(_ map[string]string) (encryption.KeyManagementClient, error) {
		return nil, wantErr
	}))

	_, err := registry.Load(map[string]string{encryption.KMSTypeKey: "error-kms"})
	require.ErrorIs(t, err, wantErr)
}

func TestKMSRegistry_PassesPropertiesToFactory(t *testing.T) {
	registry := encryption.NewKMSRegistry()
	wantProps := map[string]string{
		encryption.KMSTypeKey:   "props-kms",
		"encryption.kms.region": "us-east-1",
	}

	var gotProps map[string]string
	require.NoError(t, registry.Register("props-kms", func(props map[string]string) (encryption.KeyManagementClient, error) {
		gotProps = props

		return encryption.NewInMemoryKeyManagementClient(), nil
	}))

	_, err := registry.Load(wantProps)
	require.NoError(t, err)
	assert.Equal(t, wantProps, gotProps)
}

func TestKMSRegistry_FactoryMayUpdateRegistry(t *testing.T) {
	registry := encryption.NewKMSRegistry()
	require.NoError(t, registry.Register("nested", func(_ map[string]string) (encryption.KeyManagementClient, error) {
		if err := registry.Register("new-factory", memoryKMSFactory); err != nil {
			return nil, err
		}

		return encryption.NewInMemoryKeyManagementClient(), nil
	}))

	_, err := registry.Load(map[string]string{encryption.KMSTypeKey: "nested"})
	require.NoError(t, err)
	_, err = registry.Load(map[string]string{encryption.KMSTypeKey: "new-factory"})
	require.NoError(t, err)
}

func TestKMSRegistry_ConcurrentUse(t *testing.T) {
	registry := encryption.NewKMSRegistry()
	const workerCount = 32

	var wg sync.WaitGroup
	failures := make(chan error, workerCount)
	for i := range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()

			name := fmt.Sprintf("worker-%d", i)
			if err := registry.Register(name, memoryKMSFactory); err != nil {
				failures <- err

				return
			}
			if _, err := registry.Load(map[string]string{encryption.KMSTypeKey: name}); err != nil {
				failures <- err
			}
			_ = registry.Names()
			registry.Unregister(name)
		}()
	}
	wg.Wait()
	close(failures)

	for err := range failures {
		require.NoError(t, err)
	}
}
