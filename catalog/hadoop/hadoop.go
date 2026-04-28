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
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
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

	warehouse = strings.TrimPrefix(warehouse, "file://")
	warehouse = strings.TrimPrefix(warehouse, "file:")
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

func (c *Catalog) CreateTable(_ context.Context, _ table.Identifier, _ *iceberg.Schema, _ ...catalog.CreateTableOpt) (*table.Table, error) {
	return nil, errors.New("hadoop catalog: CreateTable not yet implemented")
}

func (c *Catalog) CommitTable(_ context.Context, _ table.Identifier, _ []table.Requirement, _ []table.Update) (table.Metadata, string, error) {
	return nil, "", errors.New("hadoop catalog: CommitTable not yet implemented")
}

func (c *Catalog) ListTables(_ context.Context, _ table.Identifier) iter.Seq2[table.Identifier, error] {
	return func(yield func(table.Identifier, error) bool) {
		yield(nil, errors.New("hadoop catalog: ListTables not yet implemented"))
	}
}

func (c *Catalog) LoadTable(_ context.Context, _ table.Identifier) (*table.Table, error) {
	return nil, errors.New("hadoop catalog: LoadTable not yet implemented")
}

func (c *Catalog) DropTable(_ context.Context, _ table.Identifier) error {
	return errors.New("hadoop catalog: DropTable not yet implemented")
}

func (c *Catalog) RenameTable(_ context.Context, _, _ table.Identifier) (*table.Table, error) {
	return nil, errors.New("hadoop catalog: RenameTable not yet implemented")
}

func (c *Catalog) CheckTableExists(_ context.Context, _ table.Identifier) (bool, error) {
	return false, errors.New("hadoop catalog: CheckTableExists not yet implemented")
}

func (c *Catalog) ListNamespaces(_ context.Context, _ table.Identifier) ([]table.Identifier, error) {
	return nil, errors.New("hadoop catalog: ListNamespaces not yet implemented")
}

func (c *Catalog) CreateNamespace(_ context.Context, _ table.Identifier, _ iceberg.Properties) error {
	return errors.New("hadoop catalog: CreateNamespace not yet implemented")
}

func (c *Catalog) DropNamespace(_ context.Context, _ table.Identifier) error {
	return errors.New("hadoop catalog: DropNamespace not yet implemented")
}

func (c *Catalog) CheckNamespaceExists(_ context.Context, _ table.Identifier) (bool, error) {
	return false, errors.New("hadoop catalog: CheckNamespaceExists not yet implemented")
}

func (c *Catalog) LoadNamespaceProperties(_ context.Context, _ table.Identifier) (iceberg.Properties, error) {
	return nil, errors.New("hadoop catalog: LoadNamespaceProperties not yet implemented")
}

func (c *Catalog) UpdateNamespaceProperties(_ context.Context, _ table.Identifier, _ []string, _ iceberg.Properties) (catalog.PropertiesUpdateSummary, error) {
	return catalog.PropertiesUpdateSummary{}, errors.New("hadoop catalog: UpdateNamespaceProperties not yet implemented")
}
