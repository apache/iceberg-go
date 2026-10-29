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
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

type capabilityTestDecorator struct {
	Catalog
}

func (d *capabilityTestDecorator) Unwrap() Catalog { return d.Catalog }

type interceptingTestDecorator struct {
	Catalog
}

func (d *interceptingTestDecorator) Unwrap() Catalog { return d.Catalog }

func (d *interceptingTestDecorator) CommitTransaction(context.Context, []table.TableCommit) error {
	return nil
}

func TestAsCatalogCapabilities(t *testing.T) {
	backend := &fullStubCatalog{}
	inner := &capabilityTestDecorator{Catalog: backend}
	outer := &capabilityTestDecorator{Catalog: inner}

	capability, ok := As[TransactionalCatalog](outer)
	require.True(t, ok)
	require.Same(t, backend, capability)

	concrete, ok := As[*fullStubCatalog](outer)
	require.True(t, ok)
	require.Same(t, backend, concrete)

	_, ok = As[RegisterableCatalog](outer)
	require.False(t, ok)

	_, ok = As[TransactionalCatalog](nil)
	require.False(t, ok)

	// Embedding Catalog alone hides optional capabilities without Unwrap.
	opaque := struct{ Catalog }{Catalog: backend}
	_, ok = As[TransactionalCatalog](opaque)
	require.False(t, ok)
}

func TestAsPrefersOutermostCapability(t *testing.T) {
	backend := &fullStubCatalog{}
	decorator := &interceptingTestDecorator{Catalog: backend}

	capability, ok := As[TransactionalCatalog](decorator)
	require.True(t, ok)
	require.Same(t, decorator, capability)
}

func TestNewMultiTableTransactionUnwrapsCatalog(t *testing.T) {
	backend := &fullStubCatalog{}
	wrapped := &capabilityTestDecorator{
		Catalog: &capabilityTestDecorator{Catalog: backend},
	}
	mtx, err := NewMultiTableTransaction(wrapped)
	require.NoError(t, err)

	tx := mtxTestTable(t, "db", "t").NewTransaction()
	require.NoError(t, tx.SetProperties(map[string]string{"k": "v"}))
	require.NoError(t, mtx.AddTransaction(tx))
	require.NoError(t, mtx.Commit(context.Background()))

	require.Len(t, backend.commits, 1)
	require.Len(t, backend.commits[0], 1)
}
