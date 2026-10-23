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
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/url"
	"slices"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

func (c *Catalog) CreateNamespace(_ context.Context, ns table.Identifier, props iceberg.Properties) error {
	if err := validateIdentifier(ns); err != nil {
		return err
	}

	if len(props) > 0 {
		return fmt.Errorf("%w: hadoop catalog: namespace properties are not supported", iceberg.ErrNotImplemented)
	}

	// Raise an error if the namespace already exists
	if err := c.checkedMkdirAll(ns); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("%w: %s", catalog.ErrNamespaceAlreadyExists, strings.Join(ns, "."))
		}

		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: parent namespace does not exist for %s",
				catalog.ErrNoSuchNamespace, strings.Join(ns, "."))
		}

		return fmt.Errorf("hadoop catalog: failed to create namespace: %w", err)
	}

	return nil
}

func (c *Catalog) DropNamespace(_ context.Context, ns table.Identifier) error {
	if err := validateIdentifier(ns); err != nil {
		return err
	}

	path := c.namespaceToPath(ns)

	// Walk the namespace directory directly so existence and type checks use the
	// same filesystem view. The root entry preserves file-at-namespace handling.
	rootNotDir := false
	foundEntries := false
	err := c.filesystem.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if p == path {
			rootNotDir = !d.IsDir()

			return nil
		}

		foundEntries = true

		return fs.SkipAll
	})
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, strings.Join(ns, "."))
		}

		return fmt.Errorf("hadoop catalog: failed to read namespace directory: %w", err)
	}

	if rootNotDir {
		return fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, strings.Join(ns, "."))
	}

	if foundEntries {
		return fmt.Errorf("%w: %s", catalog.ErrNamespaceNotEmpty, strings.Join(ns, "."))
	}

	return c.filesystem.Remove(path)
}

func (c *Catalog) CheckNamespaceExists(_ context.Context, ns table.Identifier) (bool, error) {
	if err := validateIdentifier(ns); err != nil {
		return false, err
	}

	path := c.namespaceToPath(ns)

	info, err := c.filesystem.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return info.IsDir(), nil
}

func (c *Catalog) ListNamespaces(_ context.Context, parent table.Identifier) ([]table.Identifier, error) {
	if len(parent) > 0 {
		if err := validateIdentifier(parent); err != nil {
			return nil, err
		}
	}

	var path string

	if len(parent) == 0 {
		path = c.warehouse
	} else {
		path = c.namespaceToPath(parent)

		info, err := c.filesystem.Stat(path)
		if errors.Is(err, fs.ErrNotExist) || (err == nil && !info.IsDir()) {
			return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, strings.Join(parent, "."))
		}
	}

	result := []table.Identifier{}
	err := c.filesystem.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if p != path && shouldSuppressPermissionError(err) {
				log.Printf("hadoop catalog: unable to inspect path %s while listing namespaces: %v", p, err)

				return fs.SkipDir
			}

			return err
		}
		if p == path {
			return nil
		}

		if !d.IsDir() {
			// skip plain files
			return nil
		}
		isTable, err := isTableDirForListing(c.filesystem, c.isLocal, p)
		if err != nil {
			return err
		}
		if isTable {
			// if a table, not a namespace, don't descend
			return fs.SkipDir
		}
		child := append(slices.Clone(parent), d.Name())
		result = append(result, child)
		// found a namespace dir, don't recurse into it
		return fs.SkipDir
	})
	if err != nil {
		return nil, fmt.Errorf("hadoop catalog: failed to read directory: %w", err)
	}

	return result, nil
}

// LoadNamespaceProperties returns a synthetic "location" property for the
// namespace. This is a Go-only convenience — the Hadoop catalog does not
// persist user-defined namespace properties.
func (c *Catalog) LoadNamespaceProperties(_ context.Context, ns table.Identifier) (iceberg.Properties, error) {
	if err := validateIdentifier(ns); err != nil {
		return nil, err
	}

	path := c.namespaceToPath(ns)

	info, err := c.filesystem.Stat(path)
	if errors.Is(err, fs.ErrNotExist) || (err == nil && !info.IsDir()) {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, strings.Join(ns, "."))
	}

	if err != nil {
		return nil, fmt.Errorf("hadoop catalog: failed to stat namespace: %w", err)
	}

	var loc string
	if c.isLocal {
		loc = (&url.URL{Scheme: "file", Path: path}).String()
	} else {
		// the path variable contains the proper scheme
		// already if it is not a local file, so we can
		// use it directly
		loc = path
	}

	return iceberg.Properties{"location": loc}, nil
}

func (c *Catalog) UpdateNamespaceProperties(_ context.Context, _ table.Identifier, _ []string, _ iceberg.Properties) (catalog.PropertiesUpdateSummary, error) {
	return catalog.PropertiesUpdateSummary{}, fmt.Errorf("%w: hadoop catalog: namespace properties are not supported", iceberg.ErrNotImplemented)
}

// checkedMkdirAll is a helper function that checks
// all subdirectories of a given identifier path exist before creating the full path.
// This function is not atomic and may not return ErrNamespaceAlreadyExists if
// called concurrently with other calls that change the same identifier
func (c *Catalog) checkedMkdirAll(id table.Identifier) error {
	path := c.namespaceToPath(id)
	// Start at index 1 to skip the root warehouse directory
	for pathIndex := 1; pathIndex < len(id); pathIndex++ {
		subPath := id[:pathIndex]
		parentPath := c.namespaceToPath(subPath)
		if _, err := c.filesystem.Stat(parentPath); err != nil {
			return err
		}
	}
	// Check the final element in the path
	if _, err := c.filesystem.Stat(path); err == nil {
		// If there is no error and stat returns successfully,
		// it means that it must already exist
		return fs.ErrExist
	}

	return c.filesystem.MkdirAll(path)
}
