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
	"iter"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

func init() {
	catalog.Register(string(catalog.Hadoop), catalog.RegistrarFunc(
		func(_ context.Context, name string, props iceberg.Properties) (catalog.Catalog, error) {
			warehouse := props.Get("warehouse", "")

			return NewCatalog(name, warehouse, props)
		},
	))
}

var _ catalog.Catalog = (*Catalog)(nil)

// versionPattern matches Hadoop catalog metadata filenames:
// v1.metadata.json, v42.metadata.json, v1.gz.metadata.json, etc.
var versionPattern = regexp.MustCompile(`^v([0-9]+)(?:\.gz)?\.metadata\.json$`)

// Catalog is a filesystem-based Iceberg catalog that requires no external
// metastore. All state lives on disk as directories and versioned JSON
// metadata files.
type Catalog struct {
	name      string
	warehouse string
	props     iceberg.Properties
}

// NewCatalog creates a new Hadoop catalog rooted at the given warehouse path.
// The warehouse directory is not created on construction; it is created
// implicitly by the first CreateNamespace call.
func NewCatalog(name, warehouse string, props iceberg.Properties) (*Catalog, error) {
	if warehouse == "" {
		return nil, errors.New("hadoop catalog requires a warehouse path")
	}

	u, err := url.Parse(warehouse)
	if err != nil {
		return nil, fmt.Errorf("hadoop catalog: invalid warehouse path: %w", err)
	}

	if u.Scheme != "" && u.Scheme != "file" {
		return nil, fmt.Errorf("hadoop catalog: unsupported warehouse scheme %q, must be file:// or a local path", u.Scheme)
	}

	if u.Opaque != "" {
		warehouse = u.Opaque
	} else {
		warehouse = u.Path
	}

	warehouse = strings.TrimRight(warehouse, "/")

	return &Catalog{
		name:      name,
		warehouse: warehouse,
		props:     props,
	}, nil
}

func (c *Catalog) CatalogType() catalog.Type {
	return catalog.Hadoop
}

func (c *Catalog) namespaceToPath(ns table.Identifier) string {
	return filepath.Join(append([]string{c.warehouse}, ns...)...)
}

func (c *Catalog) tableToPath(ident table.Identifier) string {
	return filepath.Join(append([]string{c.warehouse}, ident...)...)
}

func (c *Catalog) metadataDir(ident table.Identifier) string {
	return filepath.Join(c.tableToPath(ident), "metadata")
}

func (c *Catalog) metadataFilePath(ident table.Identifier, version int) string {
	return filepath.Join(c.metadataDir(ident), fmt.Sprintf("v%d.metadata.json", version))
}

func (c *Catalog) versionHintPath(ident table.Identifier) string {
	return filepath.Join(c.metadataDir(ident), "version-hint.text")
}

func (c *Catalog) defaultTableLocation(ident table.Identifier) string {
	return c.tableToPath(ident)
}

// isTableDir reports whether path is a table directory by checking for
// v*.metadata.json files in its metadata/ subdirectory.
func isTableDir(path string) bool {
	metaDir := filepath.Join(path, "metadata")

	entries, err := os.ReadDir(metaDir)
	if err != nil {
		return false
	}

	for _, e := range entries {
		if !e.IsDir() && versionPattern.MatchString(e.Name()) {
			return true
		}
	}

	return false
}

func (c *Catalog) readVersionHint(ident table.Identifier) int {
	data, err := os.ReadFile(c.versionHintPath(ident))
	if err != nil {
		return 0
	}

	ver, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || ver <= 0 {
		return 0
	}

	return ver
}

func (c *Catalog) writeVersionHint(ident table.Identifier, version int) {
	dir := c.metadataDir(ident)
	tempPath := filepath.Join(dir, uuid.New().String()+"-version-hint.temp")
	hintPath := c.versionHintPath(ident)

	content := []byte(strconv.Itoa(version))
	if err := os.WriteFile(tempPath, content, 0o644); err != nil {
		log.Printf("hadoop catalog: failed to write version hint temp file: %v", err)

		return
	}

	if err := os.Rename(tempPath, hintPath); err != nil {
		log.Printf("hadoop catalog: failed to rename version hint: %v", err)
		os.Remove(tempPath)
	}
}

// metadataVersionExists checks whether a metadata file for the given version
// exists in either plain or gzip-compressed form.
func (c *Catalog) metadataVersionExists(ident table.Identifier, version int) bool {
	dir := c.metadataDir(ident)
	plain := filepath.Join(dir, fmt.Sprintf("v%d.metadata.json", version))

	if _, err := os.Stat(plain); err == nil {
		return true
	}

	gz := filepath.Join(dir, fmt.Sprintf("v%d.gz.metadata.json", version))

	_, err := os.Stat(gz)

	return err == nil
}

func (c *Catalog) scanForward(ident table.Identifier, start int) int {
	ver := start
	for c.metadataVersionExists(ident, ver+1) {
		ver++
	}

	return ver
}

func (c *Catalog) findVersion(ident table.Identifier) (int, error) {
	hint := c.readVersionHint(ident)
	if hint > 0 && c.metadataVersionExists(ident, hint) {
		return c.scanForward(ident, hint), nil
	}

	dir := c.metadataDir(ident)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("hadoop catalog: cannot read metadata directory for %s: %w",
			strings.Join(ident, "."), catalog.ErrNoSuchTable)
	}

	maxVer := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}

		matches := versionPattern.FindStringSubmatch(e.Name())
		if len(matches) == 2 {
			v, _ := strconv.Atoi(matches[1])
			if v > maxVer {
				maxVer = v
			}
		}
	}

	if maxVer == 0 {
		return 0, fmt.Errorf("hadoop catalog: no metadata files found for table %s: %w",
			strings.Join(ident, "."), catalog.ErrNoSuchTable)
	}

	return c.scanForward(ident, maxVer), nil
}

func (c *Catalog) CreateTable(_ context.Context, _ table.Identifier, _ *iceberg.Schema, _ ...catalog.CreateTableOpt) (*table.Table, error) {
	return nil, errors.New("hadoop catalog: CreateTable not yet implemented")
}

func (c *Catalog) CommitTable(_ context.Context, _ table.Identifier, _ []table.Requirement, _ []table.Update) (table.Metadata, string, error) {
	return nil, "", errors.New("hadoop catalog: CommitTable not yet implemented")
}

func (c *Catalog) ListTables(_ context.Context, ns table.Identifier) iter.Seq2[table.Identifier, error] {
	return func(yield func(table.Identifier, error) bool) {
		if len(ns) == 0 {
			yield(nil, errors.New("hadoop catalog: namespace identifier must not be empty"))

			return
		}

		nsPath := c.namespaceToPath(ns)

		info, err := os.Stat(nsPath)
		if os.IsNotExist(err) || (err == nil && !info.IsDir()) {
			yield(nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, strings.Join(ns, ".")))

			return
		}

		if err != nil {
			yield(nil, fmt.Errorf("hadoop catalog: failed to stat namespace: %w", err))

			return
		}

		entries, err := os.ReadDir(nsPath)
		if err != nil {
			yield(nil, fmt.Errorf("hadoop catalog: failed to read namespace directory: %w", err))

			return
		}

		for _, e := range entries {
			if !e.IsDir() {
				continue
			}

			child := filepath.Join(nsPath, e.Name())
			if !isTableDir(child) {
				continue
			}

			ident := make(table.Identifier, len(ns)+1)
			copy(ident, ns)
			ident[len(ns)] = e.Name()
			if !yield(ident, nil) {
				return
			}
		}
	}
}

func (c *Catalog) DropTable(_ context.Context, ident table.Identifier) error {
	if len(ident) < 2 {
		return errors.New("hadoop catalog: table identifier must have at least a namespace and table name")
	}

	tablePath := c.tableToPath(ident)
	if !isTableDir(tablePath) {
		return fmt.Errorf("%w: %s", catalog.ErrNoSuchTable, strings.Join(ident, "."))
	}

	return os.RemoveAll(tablePath)
}

func (c *Catalog) RenameTable(_ context.Context, _, _ table.Identifier) (*table.Table, error) {
	return nil, errors.New("hadoop catalog: rename table is not supported")
}

func (c *Catalog) LoadTable(_ context.Context, _ table.Identifier) (*table.Table, error) {
	return nil, errors.New("hadoop catalog: LoadTable not yet implemented")
}

func (c *Catalog) CheckTableExists(_ context.Context, _ table.Identifier) (bool, error) {
	return false, errors.New("hadoop catalog: CheckTableExists not yet implemented")
}

func (c *Catalog) CreateNamespace(_ context.Context, ns table.Identifier, props iceberg.Properties) error {
	if len(ns) == 0 {
		return errors.New("hadoop catalog: namespace identifier must not be empty")
	}

	if len(props) > 0 {
		return errors.New("hadoop catalog: namespace properties are not supported")
	}

	path := c.namespaceToPath(ns)

	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("%w: %s", catalog.ErrNamespaceAlreadyExists, strings.Join(ns, "."))
	}

	return os.MkdirAll(path, 0o755)
}

func (c *Catalog) DropNamespace(_ context.Context, ns table.Identifier) error {
	if len(ns) == 0 {
		return errors.New("hadoop catalog: namespace identifier must not be empty")
	}

	path := c.namespaceToPath(ns)

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, strings.Join(ns, "."))
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("hadoop catalog: failed to read namespace directory: %w", err)
	}

	if len(entries) > 0 {
		return fmt.Errorf("%w: %s", catalog.ErrNamespaceNotEmpty, strings.Join(ns, "."))
	}

	return os.Remove(path)
}

func (c *Catalog) CheckNamespaceExists(_ context.Context, ns table.Identifier) (bool, error) {
	path := c.namespaceToPath(ns)

	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return info.IsDir(), nil
}

func (c *Catalog) ListNamespaces(_ context.Context, parent table.Identifier) ([]table.Identifier, error) {
	var path string

	if len(parent) == 0 {
		path = c.warehouse
	} else {
		path = c.namespaceToPath(parent)

		info, err := os.Stat(path)
		if os.IsNotExist(err) || (err == nil && !info.IsDir()) {
			return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, strings.Join(parent, "."))
		}
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("hadoop catalog: failed to read directory: %w", err)
	}

	result := []table.Identifier{}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		child := filepath.Join(path, e.Name())
		if isTableDir(child) {
			continue
		}

		result = append(result, table.Identifier{e.Name()})
	}

	return result, nil
}

func (c *Catalog) LoadNamespaceProperties(_ context.Context, ns table.Identifier) (iceberg.Properties, error) {
	path := c.namespaceToPath(ns)

	info, err := os.Stat(path)
	if os.IsNotExist(err) || (err == nil && !info.IsDir()) {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, strings.Join(ns, "."))
	}

	if err != nil {
		return nil, fmt.Errorf("hadoop catalog: failed to stat namespace: %w", err)
	}

	return iceberg.Properties{"location": path}, nil
}

func (c *Catalog) UpdateNamespaceProperties(_ context.Context, _ table.Identifier, _ []string, _ iceberg.Properties) (catalog.PropertiesUpdateSummary, error) {
	return catalog.PropertiesUpdateSummary{}, errors.New("hadoop catalog: UpdateNamespaceProperties not yet implemented")
}
