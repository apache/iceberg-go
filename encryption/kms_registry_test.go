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
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/apache/iceberg-go/encryption"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func memoryKMSFactory(_ context.Context, _ map[string]string) (encryption.KeyManagementClient, error) {
	return encryption.NewInMemoryKeyManagementClient(), nil
}

func TestKMSRegistry_LoadMemory(t *testing.T) {
	registry := encryption.NewKMSRegistry()
	kms, err := registry.Load(t.Context(), map[string]string{
		encryption.KMSTypeKey: "memory",
	})
	require.NoError(t, err)
	assert.IsType(t, &encryption.InMemoryKeyManagementClient{}, kms)
}

func TestKMSRegistry_NoPropertiesSet(t *testing.T) {
	_, err := encryption.NewKMSRegistry().Load(t.Context(), map[string]string{})
	require.ErrorIs(t, err, encryption.ErrKMSTypeNotFound)
}

func TestKMSRegistry_UnregisteredName(t *testing.T) {
	_, err := encryption.NewKMSRegistry().Load(t.Context(), map[string]string{
		encryption.KMSTypeKey: "does-not-exist",
	})
	require.ErrorIs(t, err, encryption.ErrKMSTypeNotFound)
}

func TestKMSRegistry_DuplicateRegistration(t *testing.T) {
	registry := encryption.NewKMSRegistry()
	require.NoError(t, registry.Register("custom", memoryKMSFactory))
	err := registry.Register("custom", func(_ context.Context, _ map[string]string) (encryption.KeyManagementClient, error) {
		return nil, assert.AnError
	})
	require.ErrorIs(t, err, encryption.ErrKMSAlreadyRegistered)

	_, err = registry.Load(t.Context(), map[string]string{encryption.KMSTypeKey: "custom"})
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
	_, err := first.Load(t.Context(), map[string]string{encryption.KMSTypeKey: "shared"})
	require.ErrorIs(t, err, encryption.ErrKMSTypeNotFound)

	_, err = second.Load(t.Context(), map[string]string{encryption.KMSTypeKey: "shared"})
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

	_, err := registry.Load(t.Context(), map[string]string{encryption.KMSTypeKey: "custom"})
	require.NoError(t, err)
	registry.Unregister("custom")
	assert.Empty(t, registry.Names())
}

func TestKMSRegistry_FactoryError(t *testing.T) {
	registry := encryption.NewKMSRegistry()
	wantErr := assert.AnError
	require.NoError(t, registry.Register("error-kms", func(_ context.Context, _ map[string]string) (encryption.KeyManagementClient, error) {
		return nil, wantErr
	}))

	_, err := registry.Load(t.Context(), map[string]string{encryption.KMSTypeKey: "error-kms"})
	require.ErrorIs(t, err, wantErr)
}

func TestKMSRegistry_PassesContextToFactory(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(t.Context(), contextKey{}, "request")
	registry := encryption.NewKMSRegistry()
	called := false
	require.NoError(t, registry.Register("ctx-kms", func(got context.Context, _ map[string]string) (encryption.KeyManagementClient, error) {
		called = true
		assert.Equal(t, "request", got.Value(contextKey{}))

		return encryption.NewInMemoryKeyManagementClient(), nil
	}))

	_, err := registry.Load(ctx, map[string]string{encryption.KMSTypeKey: "ctx-kms"})
	require.NoError(t, err)
	assert.True(t, called, "factory must receive the caller context")
}

func TestKMSRegistry_PropagatesCanceledContext(t *testing.T) {
	registry := encryption.NewKMSRegistry()
	require.NoError(t, registry.Register("context-kms", func(ctx context.Context, _ map[string]string) (encryption.KeyManagementClient, error) {
		return nil, ctx.Err()
	}))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := registry.Load(ctx, map[string]string{encryption.KMSTypeKey: "context-kms"})
	require.ErrorIs(t, err, context.Canceled)
}

func TestKMSRegistry_PassesPropertiesToFactory(t *testing.T) {
	registry := encryption.NewKMSRegistry()
	wantProps := map[string]string{
		encryption.KMSTypeKey:   "props-kms",
		"encryption.kms.region": "us-east-1",
	}

	var gotProps map[string]string
	require.NoError(t, registry.Register("props-kms", func(_ context.Context, props map[string]string) (encryption.KeyManagementClient, error) {
		gotProps = props

		return encryption.NewInMemoryKeyManagementClient(), nil
	}))

	_, err := registry.Load(t.Context(), wantProps)
	require.NoError(t, err)
	assert.Equal(t, wantProps, gotProps)
}

func TestKMSRegistry_FactoryMayUpdateRegistry(t *testing.T) {
	registry := encryption.NewKMSRegistry()
	require.NoError(t, registry.Register("nested", func(_ context.Context, _ map[string]string) (encryption.KeyManagementClient, error) {
		if err := registry.Register("new-factory", memoryKMSFactory); err != nil {
			return nil, err
		}

		return encryption.NewInMemoryKeyManagementClient(), nil
	}))

	_, err := registry.Load(t.Context(), map[string]string{encryption.KMSTypeKey: "nested"})
	require.NoError(t, err)
	_, err = registry.Load(t.Context(), map[string]string{encryption.KMSTypeKey: "new-factory"})
	require.NoError(t, err)
}

func TestKMSRegistry_ConcurrentUse(t *testing.T) {
	registry := encryption.NewKMSRegistry()
	const workerCount = 32

	var wg sync.WaitGroup
	failures := make(chan error, workerCount)
	for i := range workerCount {
		wg.Go(func() {
			name := fmt.Sprintf("worker-%d", i)
			if err := registry.Register(name, memoryKMSFactory); err != nil {
				failures <- err

				return
			}
			if _, err := registry.Load(t.Context(), map[string]string{encryption.KMSTypeKey: name}); err != nil {
				failures <- err
			}
			_ = registry.Names()
			registry.Unregister(name)
		})
	}
	wg.Wait()
	close(failures)

	for err := range failures {
		require.NoError(t, err)
	}
}

func TestKMSRegistry_DefaultRegistrySnapshot(t *testing.T) {
	const name = "test-default-kms-registry-snapshot"
	before := encryption.NewKMSRegistry()

	// Mimic a provider registering through a side-effect import.
	encryption.RegisterKMS(name, memoryKMSFactory)
	t.Cleanup(func() { encryption.UnregisterKMS(name) })

	after := encryption.NewKMSRegistry()
	assert.Contains(t, encryption.GetRegisteredKMSNames(), name)

	_, err := before.Load(t.Context(), map[string]string{encryption.KMSTypeKey: name})
	require.ErrorIs(t, err, encryption.ErrKMSTypeNotFound)
	_, err = after.Load(t.Context(), map[string]string{encryption.KMSTypeKey: name})
	require.NoError(t, err)

	// Removing a global registration must not mutate an existing snapshot.
	encryption.UnregisterKMS(name)
	_, err = after.Load(t.Context(), map[string]string{encryption.KMSTypeKey: name})
	require.NoError(t, err)
	_, err = encryption.NewKMSRegistry().Load(t.Context(), map[string]string{encryption.KMSTypeKey: name})
	require.ErrorIs(t, err, encryption.ErrKMSTypeNotFound)
}

func TestKMSRegistry_CloneIsIndependent(t *testing.T) {
	registry := encryption.NewKMSRegistry()
	clone := registry.Clone()
	require.NoError(t, clone.Register("only-clone", memoryKMSFactory))

	_, err := registry.Load(t.Context(), map[string]string{encryption.KMSTypeKey: "only-clone"})
	require.ErrorIs(t, err, encryption.ErrKMSTypeNotFound)
	_, err = clone.Load(t.Context(), map[string]string{encryption.KMSTypeKey: "only-clone"})
	require.NoError(t, err)

	registry.Unregister("memory")
	_, err = clone.Load(t.Context(), map[string]string{encryption.KMSTypeKey: "memory"})
	require.NoError(t, err)

	// Cloning the zero-value registry must also create an independent instance.
	var empty encryption.KMSRegistry
	copied := empty.Clone()
	require.NoError(t, copied.Register("custom", memoryKMSFactory))
	assert.Empty(t, empty.Names())
}

func TestKMSRegistry_GlobalRegistrationCompatibility(t *testing.T) {
	const name = "test-global-kms-registration"
	encryption.MustRegisterKMS(name, memoryKMSFactory)
	t.Cleanup(func() { encryption.UnregisterKMS(name) })

	assert.Panics(t, func() {
		encryption.RegisterKMS(name, func(_ context.Context, _ map[string]string) (encryption.KeyManagementClient, error) {
			return nil, assert.AnError
		})
	})
	assert.Panics(t, func() { encryption.RegisterKMS("test-nil-global-kms", nil) })

	kms, err := encryption.LoadKeyManagementClient(t.Context(), map[string]string{encryption.KMSTypeKey: name})
	require.NoError(t, err)
	assert.IsType(t, &encryption.InMemoryKeyManagementClient{}, kms)

	// Registering only on an instance must not change the process-wide default.
	require.NoError(t, encryption.NewKMSRegistry().Register("instance-only", memoryKMSFactory))
	_, err = encryption.LoadKeyManagementClient(t.Context(), map[string]string{encryption.KMSTypeKey: "instance-only"})
	require.ErrorIs(t, err, encryption.ErrKMSTypeNotFound)
}

func TestKMSRegistry_GlobalLoaderPassesContext(t *testing.T) {
	const name = "test-global-kms-context"
	encryption.MustRegisterKMS(name, func(ctx context.Context, _ map[string]string) (encryption.KeyManagementClient, error) {
		return nil, ctx.Err()
	})
	t.Cleanup(func() { encryption.UnregisterKMS(name) })

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := encryption.LoadKeyManagementClient(ctx, map[string]string{encryption.KMSTypeKey: name})
	require.ErrorIs(t, err, context.Canceled)
}
