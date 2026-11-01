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
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/internal"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/maintenance"
	"github.com/google/uuid"
)

func (c *Catalog) CreateTable(ctx context.Context, ident table.Identifier, sc *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	var cfg catalog.CreateTableCfg
	for _, opt := range opts {
		opt(&cfg)
	}

	if err := validateTableIdentifier(ident); err != nil {
		return nil, err
	}

	// Resolve the reporter before any catalog mutation: a bad
	// metrics-reporter-impl must fail here, not after the metadata file and
	// version hint are written, which would report a failure for a table that
	// was actually created (and make the retry hit ErrTableAlreadyExists).
	reporter, err := c.reporter.Get(c.props)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metrics reporter: %w", err)
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

	exists, err := isTableDir(ctx, c.filesystem, c.isLocal, loc)
	if err != nil {
		return nil, fmt.Errorf("hadoop catalog: failed to inspect table directory: %w", err)
	}
	if exists {
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
	compression := table.MetadataCompressionDefault
	if cfg.Properties != nil {
		if v, ok := cfg.Properties[table.MetadataCompressionKey]; ok {
			compression = v
		}
	}

	metaPath, err := c.metadataFilePathForCompression(ident, version, compression)
	if err != nil {
		return nil, fmt.Errorf("hadoop catalog: failed to determine metadata file path: %w", err)
	}

	tempPath := joinPath(c.isLocal, metaDir, uuid.New().String()+".metadata.json")

	if err := internal.WriteTableMetadata(ctx, metadata, c.filesystem, tempPath, compression); err != nil {
		_ = c.filesystem.Remove(context.WithoutCancel(ctx), tempPath)

		return nil, fmt.Errorf("hadoop catalog: failed to write table metadata: %w", err)
	}

	if err := c.commitMetadataFile(ctx, ident, version, tempPath, metaPath, catalog.ErrTableAlreadyExists); err != nil {
		return nil, err
	}

	c.writeVersionHint(ctx, ident, version)

	tbl := table.New(
		ident,
		metadata,
		metaPath,
		internal.LoadFSFuncWithRegistry(c.ioRegistry, c.props, metaPath),
		c,
		table.WithMetricsReporter(reporter),
		table.WithScanPlanningIOProperties(c.props),
	)

	return tbl, nil
}

func (c *Catalog) loadTable(ctx context.Context, ident table.Identifier) (*table.Table, int, error) {
	if err := validateTableIdentifier(ident); err != nil {
		return nil, 0, err
	}

	metaPath, version, err := c.findMetadataLocation(ctx, ident)
	if err != nil {
		return nil, 0, err
	}

	reporter, err := c.reporter.Get(c.props)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to initialize metrics reporter: %w", err)
	}

	tbl, err := table.NewFromLocation(
		ctx, ident, metaPath, internal.LoadFSFuncWithRegistry(c.ioRegistry, c.props, metaPath), c,
		table.WithMetricsReporter(reporter),
		table.WithScanPlanningIOProperties(c.props),
	)
	if err != nil {
		return nil, 0, err
	}

	return tbl, version, nil
}

func (c *Catalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	tbl, _, err := c.loadTable(ctx, ident)

	return tbl, err
}

func (c *Catalog) CheckTableExists(ctx context.Context, ident table.Identifier) (bool, error) {
	if err := validateTableIdentifier(ident); err != nil {
		return false, err
	}

	// Direct existence checks surface metadata directory read errors; listing
	// paths use isTableDirForListing to skip unreadable entries.
	exists, err := isTableDir(ctx, c.filesystem, c.isLocal, c.tableToPath(ident))
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (c *Catalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	if err := validateTableIdentifier(ident); err != nil {
		return nil, "", err
	}

	// Step 1: Load current table (nil for create-via-commit).
	current, currentVersion, err := c.loadTable(ctx, ident)
	if err != nil && !errors.Is(err, catalog.ErrNoSuchTable) {
		return nil, "", err
	}

	// Step 2: Validate requirements against current metadata.
	var currentMetadata table.Metadata
	if current != nil {
		currentMetadata = current.Metadata()
	}
	if err := internal.ValidateRequirements(ident, currentMetadata, reqs); err != nil {
		return nil, "", err
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
	newVersion := currentVersion + 1

	// Step 7: Create metadata directory if needed (create-via-commit).
	metaDir := c.metadataDir(ident)
	if err := c.filesystem.MkdirAll(metaDir); err != nil {
		return nil, "", fmt.Errorf("hadoop catalog: failed to create metadata directory: %w", err)
	}

	compression := updated.Properties().Get(table.MetadataCompressionKey, table.MetadataCompressionDefault)
	newMetaPath, err := c.metadataFilePathForCompression(ident, newVersion, compression)
	if err != nil {
		return nil, "", fmt.Errorf("hadoop catalog: failed to determine metadata file path: %w", err)
	}

	tempPath := joinPath(c.isLocal, metaDir, uuid.New().String()+".metadata.json")

	if err := internal.WriteTableMetadata(ctx, updated, c.filesystem, tempPath, compression); err != nil {
		_ = c.filesystem.Remove(context.WithoutCancel(ctx), tempPath)

		return nil, "", fmt.Errorf("hadoop catalog: failed to write table metadata: %w", err)
	}

	if err := c.commitMetadataFile(ctx, ident, newVersion, tempPath, newMetaPath, table.ErrCommitFailed); err != nil {
		return nil, "", err
	}

	// Step 8: Best-effort version hint update.
	c.writeVersionHint(ctx, ident, newVersion)

	return updated, newMetaPath, nil
}

func (c *Catalog) commitMetadataFile(ctx context.Context, ident table.Identifier, version int, tempPath, metaPath string, conflictErr error) error {
	// Cleanup on a failed commit must not inherit ctx's cancellation, or a
	// canceled commit would leave the temp file or the version claim behind.
	cleanupCtx := context.WithoutCancel(ctx)
	claimPath := c.metadataVersionClaimPath(ident, version)
	for {
		if err := c.filesystem.RenameNoReplace(tempPath, claimPath); err != nil {
			if !errors.Is(err, fs.ErrExist) {
				_ = c.filesystem.Remove(cleanupCtx, tempPath)

				return fmt.Errorf("hadoop catalog: failed to claim metadata version: %w", err)
			}

			existingPath, exists, err := c.metadataVersionLocation(ctx, ident, version)
			if err != nil {
				_ = c.filesystem.Remove(cleanupCtx, tempPath)

				return fmt.Errorf("hadoop catalog: failed to inspect metadata directory for version %d: %w",
					version, err)
			}
			if exists {
				_ = c.filesystem.Remove(cleanupCtx, tempPath)

				return fmt.Errorf("%w: metadata file already exists for table %s: %s",
					conflictErr, strings.Join(ident, "."), existingPath)
			}

			claimInfo, err := c.filesystem.Stat(claimPath)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}

				_ = c.filesystem.Remove(cleanupCtx, tempPath)

				return fmt.Errorf("hadoop catalog: failed to inspect stale metadata claim %s: %w", claimPath, err)
			}
			if time.Since(claimInfo.ModTime()) < metadataClaimStaleAfter {
				_ = c.filesystem.Remove(cleanupCtx, tempPath)

				return fmt.Errorf("%w: metadata version already claimed for table %s: %s",
					conflictErr, strings.Join(ident, "."), claimPath)
			}

			if err := c.filesystem.Remove(ctx, claimPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
				_ = c.filesystem.Remove(cleanupCtx, tempPath)

				return fmt.Errorf("hadoop catalog: failed to clear stale metadata claim %s: %w", claimPath, err)
			}

			continue
		}

		break
	}

	removeClaim := true
	defer func() {
		if removeClaim {
			_ = c.filesystem.Remove(cleanupCtx, claimPath)
		}
	}()

	existingPath, exists, err := c.metadataVersionLocation(ctx, ident, version)
	if err != nil {
		return fmt.Errorf("hadoop catalog: failed to inspect metadata directory for version %d: %w",
			version, err)
	}
	if exists {
		return fmt.Errorf("%w: metadata file already exists for table %s: %s",
			conflictErr, strings.Join(ident, "."), existingPath)
	}

	if err := c.filesystem.RenameNoReplace(claimPath, metaPath); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("%w: metadata file already exists for table %s: %s",
				conflictErr, strings.Join(ident, "."), metaPath)
		}
		if errors.Is(err, fs.ErrNotExist) {
			existingPath, exists, err := c.metadataVersionLocation(ctx, ident, version)
			if err != nil {
				return fmt.Errorf("hadoop catalog: failed to inspect metadata directory for version %d: %w",
					version, err)
			}
			if exists {
				return fmt.Errorf("%w: metadata file already exists for table %s: %s",
					conflictErr, strings.Join(ident, "."), existingPath)
			}

			return fmt.Errorf("%w: metadata claim for table %s and version %d was removed before publish",
				conflictErr, strings.Join(ident, "."), version)
		}

		return fmt.Errorf("hadoop catalog: failed to commit metadata file: %w", err)
	}

	removeClaim = false

	return nil
}

func (c *Catalog) ListTables(ctx context.Context, ns table.Identifier) iter.Seq2[table.Identifier, error] {
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

		err = c.filesystem.WalkDir(ctx, nsPath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				if path != nsPath && shouldSuppressPermissionError(err) {
					log.Printf("hadoop catalog: unable to inspect path %s while listing tables: %v", path, err)

					return fs.SkipDir
				}

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
			isTable, err := isTableDirForListing(ctx, c.filesystem, c.isLocal, path)
			if err != nil {
				return err
			}
			if !isTable {
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

func (c *Catalog) DropTable(ctx context.Context, ident table.Identifier) error {
	if _, _, err := c.loadTable(ctx, ident); err != nil {
		return err
	}

	tablePath := c.tableToPath(ident)

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
	if purgeErr := maintenance.PurgeFiles(ctx, tbl); purgeErr != nil {
		log.Printf("WARNING: failing to purge some files in Hadoop table %s: %v", identifier, purgeErr)
	}

	if !tbl.Metadata().Properties().GetBool("gc.enabled", true) {
		return c.filesystem.RemoveAll(c.metadataDir(identifier))
	}

	return c.filesystem.RemoveAll(c.tableToPath(identifier))
}

func (c *Catalog) RenameTable(_ context.Context, _, _ table.Identifier) (*table.Table, error) {
	return nil, fmt.Errorf("%w: hadoop catalog: rename table is not supported", iceberg.ErrNotImplemented)
}
