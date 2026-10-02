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

package iceberg

import (
	"testing"

	"github.com/apache/arrow-go/v18/parquet/variant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLiteralSetVariant(t *testing.T) {
	build := func(v any) VariantLiteral {
		var b variant.Builder
		require.NoError(t, b.Append(v))
		val, err := b.Build()
		require.NoError(t, err)

		return VariantLiteral(val)
	}

	t.Run("contains", func(t *testing.T) {
		set := newLiteralSet(build(int64(1)), build(int64(2)))
		require.Equal(t, 2, set.Len())

		assert.True(t, set.Contains(build(int64(1))))
		assert.True(t, set.Contains(build(int64(2))))
		assert.False(t, set.Contains(build(int64(3))))
	})

	t.Run("same value bytes with different metadata", func(t *testing.T) {
		objA := build(map[string]any{"a": int64(1)})
		objB := build(map[string]any{"b": int64(1)})
		// Guard the premise: only the metadata dictionary tells these apart.
		require.Equal(t, variant.Value(objA).Bytes(), variant.Value(objB).Bytes())
		require.NotEqual(t, variant.Value(objA).Metadata().Bytes(), variant.Value(objB).Metadata().Bytes())

		set := newLiteralSet(objA, objB)
		assert.Equal(t, 2, set.Len())

		set = newLiteralSet(objA)
		assert.True(t, set.Contains(build(map[string]any{"a": int64(1)})))
		assert.False(t, set.Contains(objB))
	})
}
