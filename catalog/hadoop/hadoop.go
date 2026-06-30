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
	"iter"
	"log"
	"net/url"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/internal"
	"github.com/apache/iceberg-go/io"
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

// uuidMetadataPattern matches UUID-style metadata filenames produced by
// Java/PyIceberg catalogs: 00000-<uuid>.metadata.json or
// 00000-<uuid>.gz.metadata.json. The sequence is a 5-digit zero-padded
// number and the UUID is in canonical 8-4-4-4-12 hex format.
var uuidMetadataPattern = regexp.MustCompile(
	`^[0-9]{5}-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}(?:\.gz)?\.metadata\.json$`,
)

// validateIdentifier checks that an identifier is non-empty and that each
// component is safe for use as a path segment. It rejects nil/empty
// identifiers, empty parts, path traversal sequences, and components
// containing path separators.
//
// Note: this is POSIX-best-effort validation — it does not catch NUL bytes
// or Windows reserved names (NUL, CON, COM1, etc.).
func validateIdentifier(ident table.Identifier) error {
	return validateIdentifierParts(ident, catalog.ErrNoSuchNamespace, "namespace identifier", "identifier component")
}

func validateTableIdentifier(ident table.Identifier) error {
	if len(ident) < 2 {
		return fmt.Errorf("%w: table identifier must have at least a namespace and table name", catalog.ErrNoSuchTable)
	}

	return validateIdentifierParts(ident, catalog.ErrNoSuchTable, "table identifier", "table identifier component")
}

func validateIdentifierParts(ident table.Identifier, errType error, identifierName, componentName string) error {
	if len(ident) == 0 {
		return fmt.Errorf("%w: %s must not be empty", errType, identifierName)
	}

	for _, part := range ident {
		if part == "" {
			return fmt.Errorf("%w: %s must not be empty", errType, componentName)
		}

		if part == "." || part == ".." {
			return fmt.Errorf("%w: invalid %s %q", errType, componentName, part)
		}

		if strings.ContainsAny(part, "/\\") {
			return fmt.Errorf("%w: %s must not contain path separators: %q", errType, componentName, part)
		}
	}

	return nil
}

// Catalog is a filesystem-based Iceberg catalog that requires no external
// metastore. All state lives on disk as directories and versioned JSON
// metadata files. Currently only local filesystem paths are supported.
var _ catalog.PurgeableTable = (*Catalog)(nil)

type Catalog struct {
	name       string
	warehouse  string
	isLocal    bool
	filesystem HadoopCatalogFS
	props      iceberg.Properties
}

// NewCatalog creates a new Hadoop catalog rooted at the given warehouse path.
// When using a local filesystem, the warehouse directory
// is not created on construction; it is created implicitly by the first CreateNamespace
// call. When using other schemes, the property `allow-unsafe-commits` must be set to
// true since custom schemes and blob filesystems do not have the same atomicity guarantees.
func NewCatalog(name, warehouse string, props iceberg.Properties) (*Catalog, error) {
	if warehouse == "" {
		return nil, errors.New("hadoop catalog requires a warehouse path")
	}

	u, err := url.Parse(warehouse)
	if err != nil {
		return nil, fmt.Errorf("hadoop catalog: invalid warehouse path: %w", err)
	}

	isLocal := u.Scheme == "" || u.Scheme == "file"
	allowUnsafeCommits := props.GetBool("allow-unsafe-commits", false)

	if !isLocal && !allowUnsafeCommits {
		return nil, fmt.Errorf("hadoop catalog: when using warehouse scheme %q, `allow-unsafe-commits` must be set to true", u.Scheme)
	}

	if isLocal {
		if u.Opaque != "" {
			warehouse = u.Opaque
		} else {
			warehouse = u.Path
		}

		if warehouse == "" || warehouse == "/" {
			return nil, errors.New("hadoop catalog: local filesystem requires a non-root warehouse path")
		}

		warehouse = strings.TrimRight(warehouse, "/")

		// Normalize to absolute path so the synthetic "location" property
		// always produces a valid file:// URI.
		absWarehouse, err := filepath.Abs(warehouse)
		if err != nil {
			return nil, fmt.Errorf("hadoop catalog: failed to resolve absolute warehouse path: %w", err)
		}

		warehouse = absWarehouse
	}
	// TODO: propagate caller context once NewCatalog accepts one
	filesystem, err := io.LoadFS(context.Background(), props, warehouse)
	if err != nil {
		return nil, fmt.Errorf("hadoop catalog: failed to load filesystem: %w", err)
	}

	hadoopFs, ok := filesystem.(HadoopCatalogFS)
	if !ok {
		return nil, fmt.Errorf("hadoop catalog: %T does not implement HadoopCatalogFS", filesystem)
	}

	return &Catalog{
		name:      name,
		warehouse: warehouse,
		isLocal:   isLocal,
		// filesystem is resolved dynamically from the IO registry based on the warehouse scheme
		filesystem: hadoopFs,
		props:      props,
	}, nil
}

func (c *Catalog) CatalogType() catalog.Type {
	return catalog.Hadoop
}

// joinPath is a helper that allows paths to be joined as both local filesystem
// paths or as remote URIs needed for filesystems like blob stores.
func joinPath(isLocal bool, base string, parts ...string) string {
	if isLocal {
		// Local filesystems can be joined using path.Join
		// without any special handling.
		return filepath.Join(append([]string{base}, parts...)...)
	}

	baseWithoutTrailingSlash := strings.TrimRight(base, "/")
	if len(parts) == 0 {
		// Remote warehouse roots should not keep a trailing slash.
		return baseWithoutTrailingSlash
	}

	// Remote paths need POSIX separators without removing the URI authority.
	joinedParts := path.Join(parts...)
	// joined returns . if all parts are empty; if this is the case,
	// we return the base without the trailing slash
	if joinedParts == "." {
		return baseWithoutTrailingSlash
	}

	return baseWithoutTrailingSlash + "/" + joinedParts
}

func (c *Catalog) namespaceToPath(ns table.Identifier) string {
	return joinPath(c.isLocal, c.warehouse, ns...)
}

func (c *Catalog) tableToPath(ident table.Identifier) string {
	return joinPath(c.isLocal, c.warehouse, ident...)
}

func (c *Catalog) metadataDir(ident table.Identifier) string {
	return joinPath(c.isLocal, c.tableToPath(ident), "metadata")
}

func (c *Catalog) metadataFilePath(ident table.Identifier, version int) string {
	return joinPath(c.isLocal, c.metadataDir(ident), fmt.Sprintf("v%d.metadata.json", version))
}

func (c *Catalog) versionHintPath(ident table.Identifier) string {
	return joinPath(c.isLocal, c.metadataDir(ident), "version-hint.text")
}

func (c *Catalog) defaultTableLocation(ident table.Identifier) string {
	return c.tableToPath(ident)
}

// isTableDir reports whether path is a table directory by checking for
// metadata files in its metadata/ subdirectory. It recognizes:
//   - v*.metadata.json (Hadoop catalog format)
//   - <seq>-<uuid>.metadata.json (Java/PyIceberg format)
//   - version-hint.text
func isTableDir(filesystem HadoopCatalogFS, isLocal bool, path string) bool {
	metaDir := joinPath(isLocal, path, "metadata")

	foundMetadata := false
	err := filesystem.WalkDir(metaDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip the root itself.
		if path == metaDir {
			return nil
		}

		// Don't descend into subdirectories.
		if d.IsDir() {
			return fs.SkipDir
		}

		name := d.Name()
		if versionPattern.MatchString(name) ||
			uuidMetadataPattern.MatchString(name) ||
			name == "version-hint.text" {
			foundMetadata = true

			return fs.SkipAll
		}

		return nil
	})
	if err != nil {
		return false
	}

	return foundMetadata
}

func (c *Catalog) readVersionHint(ident table.Identifier) int {
	data, err := c.filesystem.ReadFile(c.versionHintPath(ident))
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
	tempPath := joinPath(c.isLocal, dir, uuid.New().String()+"-version-hint.temp")
	hintPath := c.versionHintPath(ident)

	content := []byte(strconv.Itoa(version))
	if err := c.filesystem.WriteFile(tempPath, content); err != nil {
		log.Printf("hadoop catalog: failed to write version hint temp file: %v", err)

		return
	}

	if err := c.filesystem.Rename(tempPath, hintPath); err != nil {
		log.Printf("hadoop catalog: failed to rename version hint: %v", err)
		_ = c.filesystem.Remove(tempPath)
	}
}

// metadataVersionExists checks whether a metadata file for the given version
// exists in either plain or gzip-compressed form.
func (c *Catalog) metadataVersionExists(ident table.Identifier, version int) bool {
	dir := c.metadataDir(ident)
	plain := joinPath(c.isLocal, dir, fmt.Sprintf("v%d.metadata.json", version))

	if _, err := c.filesystem.Stat(plain); err == nil {
		return true
	}

	gz := joinPath(c.isLocal, dir, fmt.Sprintf("v%d.gz.metadata.json", version))

	_, err := c.filesystem.Stat(gz)

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

	maxVer := 0
	// Walk just one directory level to find metadata files,
	// ignoring any subdirectories that may exist
	err := c.filesystem.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == dir {
			return nil
		}
		if d.IsDir() {
			return fs.SkipDir
		}

		name := d.Name()
		matches := versionPattern.FindStringSubmatch(name)
		if len(matches) == 2 {
			v, _ := strconv.Atoi(matches[1])
			if v > maxVer {
				maxVer = v
			}
		}

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("hadoop catalog: cannot read metadata directory for %s: %w",
			strings.Join(ident, "."), catalog.ErrNoSuchTable)
	}

	if maxVer == 0 {
		return 0, fmt.Errorf("hadoop catalog: no metadata files found for table %s: %w",
			strings.Join(ident, "."), catalog.ErrNoSuchTable)
	}

	return c.scanForward(ident, maxVer), nil
}

func (c *Catalog) CreateTable(ctx context.Context, ident table.Identifier, sc *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	var cfg catalog.CreateTableCfg
	for _, opt := range opts {
		opt(&cfg)
	}

	if err := validateTableIdentifier(ident); err != nil {
		return nil, err
	}

	ns := catalog.NamespaceFromIdent(ident)
	nsPath := c.namespaceToPath(ns)

	info, err := c.filesystem.Stat(nsPath)
	if errors.Is(err, fs.ErrNotExist) || (err == nil && !info.IsDir()) {
		return nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, strings.Join(ns, "."))
	}

	if err != nil {
		return nil, fmt.Errorf("hadoop catalog: failed to stat namespace directory: %w", err)
	}

	loc := c.defaultTableLocation(ident)
	if cfg.Location != "" && cfg.Location != loc {
		return nil, errors.New("hadoop catalog: custom table locations are not supported")
	}

	if isTableDir(c.filesystem, c.isLocal, loc) {
		return nil, fmt.Errorf("%w: %s", catalog.ErrTableAlreadyExists, strings.Join(ident, "."))
	}

	metadata, err := table.NewMetadata(sc, cfg.PartitionSpec, cfg.SortOrder, loc, cfg.Properties)
	if err != nil {
		return nil, fmt.Errorf("hadoop catalog: failed to create table metadata: %w", err)
	}

	metaDir := c.metadataDir(ident)
	if err := c.filesystem.MkdirAll(metaDir); err != nil {
		return nil, fmt.Errorf("hadoop catalog: failed to create metadata directory: %w", err)
	}

	version := 1
	metaPath := c.metadataFilePath(ident, version)
	tempPath := joinPath(c.isLocal, metaDir, uuid.New().String()+".metadata.json")

	compression := table.MetadataCompressionDefault
	if cfg.Properties != nil {
		if v, ok := cfg.Properties[table.MetadataCompressionKey]; ok {
			compression = v
		}
	}

	if err := internal.WriteTableMetadata(metadata, c.filesystem, tempPath, compression); err != nil {
		_ = c.filesystem.Remove(tempPath)

		return nil, fmt.Errorf("hadoop catalog: failed to write table metadata: %w", err)
	}

	if err := c.commitMetadataFile(ident, tempPath, metaPath, catalog.ErrTableAlreadyExists); err != nil {
		return nil, err
	}

	c.writeVersionHint(ident, version)

	tbl := table.New(
		ident,
		metadata,
		metaPath,
		io.LoadFSFunc(c.props, metaPath),
		c,
	)

	return tbl, nil
}

func (c *Catalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	if err := validateTableIdentifier(ident); err != nil {
		return nil, err
	}

	ver, err := c.findVersion(ident)
	if err != nil {
		return nil, err
	}

	metaPath := c.metadataFilePath(ident, ver)

	return table.NewFromLocation(ctx, ident, metaPath, io.LoadFSFunc(c.props, metaPath), c)
}

func (c *Catalog) CheckTableExists(_ context.Context, ident table.Identifier) (bool, error) {
	if err := validateTableIdentifier(ident); err != nil {
		return false, nil
	}

	return isTableDir(c.filesystem, c.isLocal, c.tableToPath(ident)), nil
}

func (c *Catalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	if err := validateTableIdentifier(ident); err != nil {
		return nil, "", err
	}

	// Step 1: Load current table (nil for create-via-commit).
	current, err := c.LoadTable(ctx, ident)
	if err != nil && !errors.Is(err, catalog.ErrNoSuchTable) {
		return nil, "", err
	}

	// Step 2: Validate requirements against current metadata.
	if current != nil {
		for _, r := range reqs {
			if err := r.Validate(current.Metadata()); err != nil {
				return nil, "", err
			}
		}
	}

	// Step 3: Apply updates to produce new metadata.
	var baseMeta table.Metadata
	var currentMetadataLoc string

	if current != nil {
		baseMeta = current.Metadata()
		currentMetadataLoc = current.MetadataLocation()
	} else {
		baseMeta, err = table.NewMetadata(iceberg.NewSchema(0), nil, table.UnsortedSortOrder, "", nil)
		if err != nil {
			return nil, "", fmt.Errorf("hadoop catalog: failed to create base metadata: %w", err)
		}
	}

	updated, err := internal.UpdateTableMetadata(baseMeta, updates, currentMetadataLoc)
	if err != nil {
		return nil, "", fmt.Errorf("hadoop catalog: failed to apply updates: %w", err)
	}

	// Step 4: Validate table location has not changed.
	if current != nil && updated.Location() != current.Location() {
		return nil, "", errors.New("hadoop catalog: table location cannot be changed")
	}

	// Step 5: Reject write.metadata.location property.
	if v := updated.Properties().Get(table.WriteMetadataLocationKey, ""); v != "" {
		return nil, "", fmt.Errorf("hadoop catalog: %s property is not supported", table.WriteMetadataLocationKey)
	}

	// Step 6: Determine next version number.
	var currentVersion int

	if current != nil {
		currentVersion, err = c.findVersion(ident)
		if err != nil {
			return nil, "", err
		}
	}

	newVersion := currentVersion + 1

	// Step 7: Create metadata directory if needed (create-via-commit).
	metaDir := c.metadataDir(ident)
	if err := c.filesystem.MkdirAll(metaDir); err != nil {
		return nil, "", fmt.Errorf("hadoop catalog: failed to create metadata directory: %w", err)
	}

	newMetaPath := c.metadataFilePath(ident, newVersion)
	tempPath := joinPath(c.isLocal, metaDir, uuid.New().String()+".metadata.json")

	compression := updated.Properties().Get(table.MetadataCompressionKey, table.MetadataCompressionDefault)

	if err := internal.WriteTableMetadata(updated, c.filesystem, tempPath, compression); err != nil {
		_ = c.filesystem.Remove(tempPath)

		return nil, "", fmt.Errorf("hadoop catalog: failed to write table metadata: %w", err)
	}

	if err := c.commitMetadataFile(ident, tempPath, newMetaPath, table.ErrCommitFailed); err != nil {
		return nil, "", err
	}

	// Step 8: Best-effort version hint update.
	c.writeVersionHint(ident, newVersion)

	return updated, newMetaPath, nil
}

func (c *Catalog) commitMetadataFile(ident table.Identifier, tempPath, metaPath string, conflictErr error) error {
	if err := c.filesystem.RenameNoReplace(tempPath, metaPath); err != nil {
		_ = c.filesystem.Remove(tempPath)

		if errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("%w: metadata file already exists for table %s: %s",
				conflictErr, strings.Join(ident, "."), metaPath)
		}

		return fmt.Errorf("hadoop catalog: failed to commit metadata file: %w", err)
	}

	return nil
}

func (c *Catalog) ListTables(_ context.Context, ns table.Identifier) iter.Seq2[table.Identifier, error] {
	return func(yield func(table.Identifier, error) bool) {
		if err := validateIdentifier(ns); err != nil {
			yield(nil, err)

			return
		}

		nsPath := c.namespaceToPath(ns)

		info, err := c.filesystem.Stat(nsPath)
		if errors.Is(err, fs.ErrNotExist) || (err == nil && !info.IsDir()) {
			yield(nil, fmt.Errorf("%w: %s", catalog.ErrNoSuchNamespace, strings.Join(ns, ".")))

			return
		}

		if err != nil {
			yield(nil, fmt.Errorf("hadoop catalog: failed to stat namespace: %w", err))

			return
		}

		err = c.filesystem.WalkDir(nsPath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			// Anything that is a file is not a namespace or table, so skip it.
			if !d.IsDir() {
				return nil
			}

			// Skip the namespace directory itself.
			if path == nsPath {
				return nil
			}

			// Skip anything that is not a table directory.
			if !isTableDir(c.filesystem, c.isLocal, path) {
				return fs.SkipDir
			}
			ident := make(table.Identifier, len(ns)+1)
			copy(ident, ns)
			ident[len(ns)] = d.Name()
			if !yield(ident, nil) {
				return fs.SkipAll
			}
			// If a table has been found, then that directory
			// doesn't need to be walked further
			return fs.SkipDir
		})
		if err != nil {
			yield(nil, fmt.Errorf("hadoop catalog: failed to read namespace directory: %w", err))

			return
		}
	}
}

func (c *Catalog) DropTable(_ context.Context, ident table.Identifier) error {
	if err := validateTableIdentifier(ident); err != nil {
		return err
	}

	tablePath := c.tableToPath(ident)
	if !isTableDir(c.filesystem, c.isLocal, tablePath) {
		return fmt.Errorf("%w: %s", catalog.ErrNoSuchTable, strings.Join(ident, "."))
	}

	return c.filesystem.RemoveAll(tablePath)
}

func (c *Catalog) PurgeTable(ctx context.Context, identifier table.Identifier) error {
	if err := validateTableIdentifier(identifier); err != nil {
		return err
	}

	tbl, err := c.LoadTable(ctx, identifier)
	if err != nil {
		return err
	}

	// For Hadoop catalog, physical files walk must run BEFORE deleting the table directory root
	if purgeErr := tbl.PurgeFiles(ctx); purgeErr != nil {
		log.Printf("WARNING: failing to purge some files in Hadoop table %s: %v", identifier, purgeErr)
	}

	// Delete the table directory root from the local storage
	return c.DropTable(ctx, identifier)
}

func (c *Catalog) RenameTable(_ context.Context, _, _ table.Identifier) (*table.Table, error) {
	return nil, errors.New("hadoop catalog: rename table is not supported")
}

func (c *Catalog) CreateNamespace(_ context.Context, ns table.Identifier, props iceberg.Properties) error {
	if err := validateIdentifier(ns); err != nil {
		return err
	}

	if len(props) > 0 {
		return errors.New("hadoop catalog: namespace properties are not supported")
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
			return err
		}
		if p == path {
			return nil
		}

		if !d.IsDir() {
			// skip plain files
			return nil
		}
		if isTableDir(c.filesystem, c.isLocal, p) {
			// if a table, not a namespace, don't descend
			return fs.SkipDir
		}
		result = append(result, table.Identifier{d.Name()})
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
	return catalog.PropertiesUpdateSummary{}, errors.New("hadoop catalog: UpdateNamespaceProperties not yet implemented")
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
