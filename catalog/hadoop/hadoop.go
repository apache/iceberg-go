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

// Package hadoop implements a catalog over a Hadoop-style warehouse directory.
//
// It links no cloud SDK. Callers on cloud storage must blank-import the backend for their scheme.
// For example io/gocloud/s3.
//
// Otherwise the first LoadFS for a cloud path fails with io.ErrIOSchemeNotFound naming the package to import.
package hadoop

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/url"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/metrics"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

func init() {
	catalog.Register(string(catalog.Hadoop), catalog.RegistrarFunc(
		func(ctx context.Context, name string, props iceberg.Properties) (catalog.Catalog, error) {
			warehouse := props.Get("warehouse", "")

			return NewCatalog(ctx, name, warehouse, props)
		},
	))
}

var _ catalog.Catalog = (*Catalog)(nil)

// versionPattern matches Hadoop catalog metadata filenames:
// v1.metadata.json, v42.metadata.json, v1.gz.metadata.json, etc.
var versionPattern = regexp.MustCompile(`^v([0-9]+)(?:\.gz)?\.metadata\.json$`)

// uuidMetadataPattern matches UUID-style metadata filenames produced by
// Java/PyIceberg catalogs: 00000-<uuid>.metadata.json or
// 00000-<uuid>.gz.metadata.json. The numeric sequence has a minimum width
// of 5 digits and the UUID is in canonical 8-4-4-4-12 hex format.
var uuidMetadataPattern = regexp.MustCompile(
	`^([0-9]{5,})-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}(?:\.gz)?\.metadata\.json$`,
)

// Claims expire aggressively so crashed writers do not permanently block a
// metadata version. Slow live writers may lose a claim and have to retry, but
// the protocol still fails closed instead of publishing duplicate versions.
const metadataClaimStaleAfter = time.Minute

type metadataFile struct {
	location   string
	version    int
	hadoopName bool
	compressed bool
}

func metadataFileFromName(path, name string) (metadataFile, bool) {
	if matches := versionPattern.FindStringSubmatch(name); len(matches) == 2 {
		version, err := strconv.Atoi(matches[1])
		if err != nil || version <= 0 {
			return metadataFile{}, false
		}

		return metadataFile{
			location:   path,
			version:    version,
			hadoopName: true,
			compressed: strings.Contains(name, ".gz.metadata.json"),
		}, true
	}

	if matches := uuidMetadataPattern.FindStringSubmatch(name); len(matches) == 2 {
		version, err := strconv.Atoi(matches[1])
		if err != nil || version < 0 {
			return metadataFile{}, false
		}

		return metadataFile{
			location:   path,
			version:    version,
			compressed: strings.Contains(name, ".gz.metadata.json"),
		}, true
	}

	return metadataFile{}, false
}

func (m metadataFile) betterThan(current metadataFile) bool {
	switch {
	case current.location == "":
		return true
	case m.version != current.version:
		return m.version > current.version
	case m.hadoopName != current.hadoopName:
		// If both naming styles exist for one version, prefer the Hadoop vN
		// file because this catalog writes and conflict-checks that sequence.
		return m.hadoopName
	case m.compressed != current.compressed:
		return !m.compressed
	default:
		// This keeps scans deterministic under map iteration; within one
		// metadata directory it reduces to a stable filename tie-break.
		return m.location > current.location
	}
}

func shouldSuppressPermissionError(err error) bool {
	return errors.Is(err, fs.ErrPermission) ||
		(err != nil && strings.Contains(err.Error(), "AuthorizationPermissionMismatch"))
}

// validateIdentifier checks that an identifier is non-empty and that each
// component is safe for use as a path segment. It rejects nil/empty
// identifiers, empty parts, path traversal sequences, and components
// containing path separators.
//
// Note: this is POSIX-best-effort validation — it does not catch NUL bytes
// or Windows reserved names (NUL, CON, COM1, etc.).
func validateIdentifier(ident table.Identifier) error {
	return validateIdentifierParts(ident, "namespace identifier", "identifier component")
}

func validateTableIdentifier(ident table.Identifier) error {
	if len(ident) < 2 {
		return fmt.Errorf("%w: table identifier must have at least a namespace and table name", catalog.ErrInvalidIdentifier)
	}

	return validateIdentifierParts(ident, "table identifier", "table identifier component")
}

func validateIdentifierParts(ident table.Identifier, identifierName, componentName string) error {
	if len(ident) == 0 {
		return fmt.Errorf("%w: %s must not be empty", catalog.ErrInvalidIdentifier, identifierName)
	}

	for _, part := range ident {
		if part == "" {
			return fmt.Errorf("%w: %s must not be empty", catalog.ErrInvalidIdentifier, componentName)
		}

		if part == "." || part == ".." {
			return fmt.Errorf("%w: invalid %s %q", catalog.ErrInvalidIdentifier, componentName, part)
		}

		if strings.ContainsAny(part, "/\\") {
			return fmt.Errorf("%w: %s must not contain path separators: %q", catalog.ErrInvalidIdentifier, componentName, part)
		}
	}

	return nil
}

// Catalog is a filesystem-based Iceberg catalog that requires no external
// metastore. All state lives on disk as directories and versioned JSON
// metadata files. Currently only local filesystem paths are supported.
var _ catalog.Catalog = (*Catalog)(nil)

type Catalog struct {
	name       string
	warehouse  string
	isLocal    bool
	filesystem HadoopCatalogFS
	props      iceberg.Properties
	// reporter builds and caches the catalog's metrics reporter once, so it is
	// constructed per-catalog rather than per table load. Released by Close.
	reporter metrics.CachedReporter
}

// NewCatalog creates a new Hadoop catalog rooted at the given warehouse path.
// The caller context is checked for cancellation around filesystem setup.
// Its values, but not its cancellation or deadline, are kept for later I/O.
// When using a local filesystem, the warehouse directory
// is not created on construction; it is created implicitly by the first CreateNamespace
// call. When using other schemes, the property `allow-unsafe-commits` must be set to
// true since custom schemes and blob filesystems do not have the same atomicity guarantees.
func NewCatalog(ctx context.Context, name, warehouse string, props iceberg.Properties) (*Catalog, error) {
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
		return nil, fmt.Errorf("hadoop catalog: when using warehouse scheme %q, `allow-unsafe-commits` must be set to true; this implementation is not atomic and can lose data with concurrent commits", u.Scheme)
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
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("hadoop catalog: failed to load filesystem: %w", err)
	}

	// Some filesystem implementations retain their construction context for
	// every later operation. Keep the caller's values, but not its cancellation
	// or deadline, for the lifetime of the catalog. The original context is
	// checked before and after initialization so canceled setup still fails.
	filesystem, err := io.LoadFS(context.WithoutCancel(ctx), props, warehouse)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, fmt.Errorf("hadoop catalog: failed to load filesystem: %w", ctxErr)
	}
	if err != nil {
		return nil, fmt.Errorf("hadoop catalog: failed to load filesystem: %w", err)
	}

	hadoopFs, ok := filesystem.(HadoopCatalogFS)
	if !ok {
		return nil, fmt.Errorf("hadoop catalog: %T does not implement HadoopCatalogFS", filesystem)
	}

	cat := &Catalog{
		name:      name,
		warehouse: warehouse,
		isLocal:   isLocal,
		// filesystem is resolved dynamically from the IO registry based on the warehouse scheme
		filesystem: hadoopFs,
		props:      props,
	}

	// Resolve the metrics reporter once, up front. CachedReporter caches the
	// first result, so building it here means a bad metrics-reporter-impl fails
	// construction — rather than surfacing later as a spurious CreateTable error
	// after the table is already on disk, or as a cached error that wedges every
	// later table op on this catalog.
	if _, err := cat.reporter.Get(props); err != nil {
		return nil, fmt.Errorf("failed to initialize metrics reporter: %w", err)
	}

	return cat, nil
}

func (c *Catalog) CatalogType() catalog.Type {
	return catalog.Hadoop
}

// Close releases the catalog's metrics reporter. Close is required by [catalog.Catalog].
func (c *Catalog) Close() error { return c.reporter.Close() }

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

func (c *Catalog) metadataFilePathForCompression(ident table.Identifier, version int, compression string) (string, error) {
	var suffix string
	switch compression {
	case table.MetadataCompressionCodecNone:
		suffix = ".metadata.json"
	case table.MetadataCompressionCodecGzip:
		suffix = ".gz.metadata.json"
	default:
		return "", fmt.Errorf("unsupported write metadata compression codec: %s", compression)
	}

	return joinPath(c.isLocal, c.metadataDir(ident), fmt.Sprintf("v%d%s", version, suffix)), nil
}

func (c *Catalog) metadataVersionClaimPath(ident table.Identifier, version int) string {
	return joinPath(c.isLocal, c.metadataDir(ident), fmt.Sprintf("v%d.metadata.json.claim", version))
}

func (c *Catalog) versionHintPath(ident table.Identifier) string {
	return joinPath(c.isLocal, c.metadataDir(ident), "version-hint.text")
}

func (c *Catalog) defaultTableLocation(ident table.Identifier) string {
	return c.tableToPath(ident)
}

// isTableDir reports whether path is a table directory by checking for
// loadable metadata files in its metadata/ subdirectory. It recognizes:
//   - v*.metadata.json (Hadoop catalog format)
//   - <seq>-<uuid>.metadata.json (Java/PyIceberg format)
func isTableDir(filesystem HadoopCatalogFS, isLocal bool, tablePath string) (bool, error) {
	metaDir := joinPath(isLocal, tablePath, "metadata")

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

		if _, ok := metadataFileFromName(path, d.Name()); ok {
			foundMetadata = true

			return fs.SkipAll
		}

		return nil
	})
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}

		return false, err
	}

	return foundMetadata, nil
}

func isTableDirForListing(filesystem HadoopCatalogFS, isLocal bool, tablePath string) (bool, error) {
	isTable, err := isTableDir(filesystem, isLocal, tablePath)
	if err != nil && shouldSuppressPermissionError(err) {
		log.Printf("hadoop catalog: unable to inspect metadata directory %s: %v", joinPath(isLocal, tablePath, "metadata"), err)

		return false, nil
	}

	return isTable, err
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
		// Best-effort cleanup of the version-hint temp file; no caller context.
		_ = c.filesystem.Remove(context.Background(), tempPath)
	}
}

func (c *Catalog) scanMetadataFiles(ident table.Identifier) (map[int]metadataFile, metadataFile, error) {
	dir := c.metadataDir(ident)
	byVersion := map[int]metadataFile{}
	var latest metadataFile

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

		file, ok := metadataFileFromName(path, d.Name())
		if !ok {
			return nil
		}

		if file.betterThan(byVersion[file.version]) {
			byVersion[file.version] = file
		}
		if file.betterThan(latest) {
			latest = file
		}

		return nil
	})

	return byVersion, latest, err
}

func (c *Catalog) metadataVersionLocation(ident table.Identifier, version int) (string, bool, error) {
	files, _, err := c.scanMetadataFiles(ident)
	if err != nil {
		return "", false, err
	}

	file, ok := files[version]

	return file.location, ok, nil
}

func (c *Catalog) findMetadataLocation(ident table.Identifier) (string, int, error) {
	_, latest, err := c.scanMetadataFiles(ident)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", 0, fmt.Errorf("hadoop catalog: cannot read metadata directory for %s: %w: %w",
				strings.Join(ident, "."), catalog.ErrNoSuchTable, err)
		}

		return "", 0, fmt.Errorf("hadoop catalog: cannot read metadata directory for %s: %w",
			strings.Join(ident, "."), err)
	}

	if latest.location == "" {
		return "", 0, fmt.Errorf("hadoop catalog: no metadata files found for table %s: %w",
			strings.Join(ident, "."), catalog.ErrNoSuchTable)
	}

	return latest.location, latest.version, nil
}

func (c *Catalog) findVersion(ident table.Identifier) (int, error) {
	_, version, err := c.findMetadataLocation(ident)
	if err != nil {
		return 0, err
	}

	return version, nil
}
