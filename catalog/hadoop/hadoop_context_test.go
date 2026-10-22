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

package hadoop

import (
	"context"
	"net/url"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

// TestNewCatalogPropagatesContext checks both the direct constructor and the
// catalog registry, which must forward the context to the filesystem loader.
func TestNewCatalogPropagatesContext(t *testing.T) {
	const scheme = "hadoop-context-constructor-test"

	type contextKey struct{}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "present"))
	cancel()

	icebergio.Register(scheme, func(got context.Context, _ *url.URL, _ map[string]string) (icebergio.IO, error) {
		if got.Value(contextKey{}) != "present" {
			return nil, context.DeadlineExceeded
		}

		return nil, got.Err()
	})
	t.Cleanup(func() { icebergio.Unregister(scheme) })

	warehouse := scheme + "://bucket/warehouse"
	t.Run("direct constructor", func(t *testing.T) {
		_, err := NewCatalog(ctx, "test", warehouse, iceberg.Properties{"allow-unsafe-commits": "true"})
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("catalog registry", func(t *testing.T) {
		_, err := catalog.Load(ctx, "test", iceberg.Properties{
			"type":                 string(catalog.Hadoop),
			"warehouse":            warehouse,
			"allow-unsafe-commits": "true",
		})
		require.ErrorIs(t, err, context.Canceled)
	})
}
