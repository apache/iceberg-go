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

package catalogtest

import (
	"testing"

	"github.com/apache/iceberg-go/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type closeSpyCatalog struct {
	catalog.Catalog
	closeCount *int
}

func (s *closeSpyCatalog) Close() error {
	(*s.closeCount)++

	return nil
}

func TestManagedCatalogClosesAfterTestCleanup(t *testing.T) {
	closes := 0
	ok := t.Run("catalog", func(t *testing.T) {
		cat := managedCatalog(t, func(*testing.T) catalog.Catalog {
			return &closeSpyCatalog{closeCount: &closes}
		})
		require.NotNil(t, cat)
		t.Cleanup(func() { assert.Zero(t, closes, "close must run after per-test cleanup") })
		assert.Zero(t, closes)
	})

	require.True(t, ok)
	assert.Equal(t, 1, closes, "the suite must close each catalog exactly once")
}
