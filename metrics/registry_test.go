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

func TestFromProperties(t *testing.T) {
	t.Run("absent name yields NopReporter", func(t *testing.T) {
		r, err := FromProperties(nil)
		require.NoError(t, err)
		assert.IsType(t, NopReporter{}, r)
	})

	t.Run("empty name yields NopReporter", func(t *testing.T) {
		r, err := FromProperties(map[string]string{ReporterImplKey: ""})
		require.NoError(t, err)
		assert.IsType(t, NopReporter{}, r)
	})

	t.Run("nop name yields NopReporter", func(t *testing.T) {
		r, err := FromProperties(map[string]string{ReporterImplKey: "nop"})
		require.NoError(t, err)
		assert.IsType(t, NopReporter{}, r)
	})

	t.Run("logging name yields LoggingReporter", func(t *testing.T) {
		r, err := FromProperties(map[string]string{ReporterImplKey: "logging"})
		require.NoError(t, err)
		assert.IsType(t, &LoggingReporter{}, r)
	})

	t.Run("unknown name is an error", func(t *testing.T) {
		_, err := FromProperties(map[string]string{ReporterImplKey: "does-not-exist"})
		assert.Error(t, err)
	})
}

func TestRegister(t *testing.T) {
	t.Run("factory is selectable via FromProperties", func(t *testing.T) {
		Register("test-custom", func(props map[string]string) (Reporter, error) {
			return &InMemoryReporter{}, nil
		})
		r, err := FromProperties(map[string]string{ReporterImplKey: "test-custom"})
		require.NoError(t, err)
		assert.IsType(t, &InMemoryReporter{}, r)
	})

	t.Run("empty name panics", func(t *testing.T) {
		assert.Panics(t, func() { Register("", func(map[string]string) (Reporter, error) { return NopReporter{}, nil }) })
	})

	t.Run("nil factory panics", func(t *testing.T) {
		assert.Panics(t, func() { Register("test-nil-factory", nil) })
	})

	t.Run("duplicate name panics", func(t *testing.T) {
		assert.Panics(t, func() { Register("logging", func(map[string]string) (Reporter, error) { return NopReporter{}, nil }) })
	})
}
