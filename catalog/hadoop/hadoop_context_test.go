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
	"io/fs"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

type hadoopContextKey struct{}

// contextBoundHadoopFS models cloud filesystems that retain their construction
// context and use it for later operations. LocalFS provides the other
// methods; the overridden operations check the retained context.
type contextBoundHadoopFS struct {
	icebergio.LocalFS
	ctx  context.Context
	root string
}

func (f *contextBoundHadoopFS) localPath(name string) string {
	parsed, err := url.Parse(name)
	if err != nil {
		return filepath.Join(f.root, name)
	}

	return filepath.Join(f.root, parsed.Host, filepath.FromSlash(strings.TrimPrefix(parsed.Path, "/")))
}

func (f *contextBoundHadoopFS) Stat(name string) (fs.FileInfo, error) {
	if err := f.ctx.Err(); err != nil {
		return nil, err
	}

	return f.LocalFS.Stat(f.localPath(name))
}

func (f *contextBoundHadoopFS) MkdirAll(name string) error {
	if err := f.ctx.Err(); err != nil {
		return err
	}

	return f.LocalFS.MkdirAll(f.localPath(name))
}

func (f *contextBoundHadoopFS) ReadFile(name string) ([]byte, error) {
	if err := f.ctx.Err(); err != nil {
		return nil, err
	}

	return f.LocalFS.ReadFile(f.localPath(name))
}

func (f *contextBoundHadoopFS) WriteFile(name string, content []byte) error {
	if err := f.ctx.Err(); err != nil {
		return err
	}

	return f.LocalFS.WriteFile(f.localPath(name), content)
}

func registerHadoopContextFS(t *testing.T, scheme string, factory icebergio.SchemeFactory) {
	t.Helper()
	icebergio.Register(scheme, factory)
	t.Cleanup(func() { icebergio.Unregister(scheme) })
}

// TestNewCatalogFilesystemContextLifetime checks both the direct constructor
// and catalog registry. Filesystem operations must keep working after the
// caller cancels the context used to initialize the catalog.
func TestNewCatalogFilesystemContextLifetime(t *testing.T) {
	for _, useRegistry := range []bool{false, true} {
		name := "direct constructor"
		if useRegistry {
			name = "catalog registry"
		}

		t.Run(name, func(t *testing.T) {
			const scheme = "hadoop-context-lifetime-test"
			root := t.TempDir()
			warehouse := scheme + "://bucket/warehouse"
			ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), hadoopContextKey{}, "present"), time.Hour)
			defer cancel()

			var factoryContext context.Context
			registerHadoopContextFS(t, scheme, func(got context.Context, _ *url.URL, _ map[string]string) (icebergio.IO, error) {
				factoryContext = got

				return &contextBoundHadoopFS{ctx: got, root: root}, nil
			})

			props := iceberg.Properties{
				"warehouse":            warehouse,
				"allow-unsafe-commits": "true",
			}
			var cat catalog.Catalog
			var err error
			if useRegistry {
				props["type"] = string(catalog.Hadoop)
				cat, err = catalog.Load(ctx, "test", props)
			} else {
				cat, err = NewCatalog(ctx, "test", warehouse, props)
			}
			require.NoError(t, err)
			require.Equal(t, "present", factoryContext.Value(hadoopContextKey{}))
			require.Nil(t, factoryContext.Done(), "filesystem context must outlive the caller")
			_, hasDeadline := factoryContext.Deadline()
			require.False(t, hasDeadline, "filesystem context must not inherit the caller deadline")

			cancel()
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			require.NoError(t, factoryContext.Err())
			require.NoError(t, cat.CreateNamespace(context.Background(), table.Identifier{"ns"}, nil))

			fs := cat.(*Catalog).filesystem
			location := warehouse + "/ns/after-cancel.txt"
			content := []byte("still writable")
			require.NoError(t, fs.WriteFile(location, content))
			readBack, err := fs.ReadFile(location)
			require.NoError(t, err)
			require.Equal(t, content, readBack)
		})
	}
}

func TestNewCatalogHonorsContextCancellation(t *testing.T) {
	for _, useRegistry := range []bool{false, true} {
		name := "direct constructor"
		if useRegistry {
			name = "catalog registry"
		}

		t.Run(name, func(t *testing.T) {
			const scheme = "hadoop-context-canceled-test"
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			factoryCalled := false
			registerHadoopContextFS(t, scheme, func(context.Context, *url.URL, map[string]string) (icebergio.IO, error) {
				factoryCalled = true

				return nil, nil
			})

			warehouse := scheme + "://bucket/warehouse"
			props := iceberg.Properties{
				"warehouse":            warehouse,
				"allow-unsafe-commits": "true",
			}
			var err error
			if useRegistry {
				props["type"] = string(catalog.Hadoop)
				_, err = catalog.Load(ctx, "test", props)
			} else {
				_, err = NewCatalog(ctx, "test", warehouse, props)
			}

			require.ErrorIs(t, err, context.Canceled)
			require.False(t, factoryCalled, "filesystem initialization must not run for a canceled context")
		})
	}
}

func TestNewCatalogHonorsCancellationDuringFilesystemLoad(t *testing.T) {
	const scheme = "hadoop-context-load-cancel-test"
	root := t.TempDir()
	loadStarted := make(chan struct{})
	finishLoad := make(chan struct{})
	registerHadoopContextFS(t, scheme, func(got context.Context, _ *url.URL, _ map[string]string) (icebergio.IO, error) {
		close(loadStarted)
		<-finishLoad

		return &contextBoundHadoopFS{ctx: got, root: root}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	warehouse := scheme + "://bucket/warehouse"
	result := make(chan error, 1)
	go func() {
		_, err := NewCatalog(ctx, "test", warehouse, iceberg.Properties{"allow-unsafe-commits": "true"})
		result <- err
	}()

	<-loadStarted
	cancel()
	close(finishLoad)
	require.ErrorIs(t, <-result, context.Canceled)
}
