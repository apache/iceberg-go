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

package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCachedReporter(t *testing.T) {
	t.Run("builds once and caches", func(t *testing.T) {
		var builds int
		Register("cached-once", func(map[string]string) (Reporter, error) {
			builds++

			return &countingReporter{}, nil
		})
		t.Cleanup(func() { Deregister("cached-once") })

		var c CachedReporter
		props := map[string]string{ReporterImplKey: "cached-once"}
		r1, err := c.Get(props)
		require.NoError(t, err)
		r2, err := c.Get(props)
		require.NoError(t, err)
		assert.Same(t, r1, r2, "Get must return the same cached reporter")
		assert.Equal(t, 1, builds, "the reporter must be built only once, not per Get")
	})

	t.Run("Close releases the built reporter", func(t *testing.T) {
		cr := &countingReporter{}
		Register("cached-close", func(map[string]string) (Reporter, error) { return cr, nil })
		t.Cleanup(func() { Deregister("cached-close") })

		var c CachedReporter
		_, err := c.Get(map[string]string{ReporterImplKey: "cached-close"})
		require.NoError(t, err)
		require.NoError(t, c.Close())
		assert.Equal(t, 1, cr.closeCalls(), "Close must close the built reporter")
	})

	t.Run("Close without a prior Get is a no-op", func(t *testing.T) {
		var c CachedReporter
		assert.NoError(t, c.Close())
	})

	t.Run("Close is idempotent and does not re-close", func(t *testing.T) {
		cr := &countingReporter{}
		Register("cached-idempotent", func(map[string]string) (Reporter, error) { return cr, nil })
		t.Cleanup(func() { Deregister("cached-idempotent") })

		var c CachedReporter
		_, err := c.Get(map[string]string{ReporterImplKey: "cached-idempotent"})
		require.NoError(t, err)
		require.NoError(t, c.Close())
		require.NoError(t, c.Close())
		assert.Equal(t, 1, cr.closeCalls(), "a second Close must not re-close the reporter")
	})

	t.Run("Get after Close returns the nop reporter", func(t *testing.T) {
		cr := &countingReporter{}
		Register("cached-post-close", func(map[string]string) (Reporter, error) { return cr, nil })
		t.Cleanup(func() { Deregister("cached-post-close") })

		var c CachedReporter
		props := map[string]string{ReporterImplKey: "cached-post-close"}
		_, err := c.Get(props)
		require.NoError(t, err)
		require.NoError(t, c.Close())

		got, err := c.Get(props)
		require.NoError(t, err)
		assert.IsType(t, NopReporter{}, got, "Get after Close must not hand back the released reporter")
	})

	t.Run("a build error is cached and not retried", func(t *testing.T) {
		var c CachedReporter
		props := map[string]string{ReporterImplKey: "does-not-exist"}
		_, err1 := c.Get(props)
		require.Error(t, err1)
		_, err2 := c.Get(props)
		assert.Equal(t, err1, err2, "the cached error must be returned without rebuilding")
	})
}
