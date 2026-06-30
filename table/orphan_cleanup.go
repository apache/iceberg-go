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

package table

import (
	"context"
	"errors"
	"fmt"
	stdfs "io/fs"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"golang.org/x/sync/errgroup"
)

// PrefixMismatchMode defines how to handle cases where candidate files have different
// URI schemes or authorities compared to table location during orphan cleanup.
// This is useful when files may be referenced using different but equivalent schemes
// (e.g., s3:// vs s3a:// vs s3n://) or when cleaning up files across different locations.
type PrefixMismatchMode int

const (
	// PrefixMismatchError causes cleanup to fail with an error when candidate files
	// have URI schemes/authorities that don't match the table location and are not
	// covered by configured equivalences. This is the safest default behavior.
	PrefixMismatchError PrefixMismatchMode = iota // default

	// PrefixMismatchIgnore skips candidate files that have mismatched URI schemes/authorities
	// without treating it as an error. Files are silently ignored and not considered for deletion.
	PrefixMismatchIgnore

	// PrefixMismatchDelete treats candidate files with mismatched URI schemes/authorities
	// as orphans and includes them for deletion. Use with caution as this may delete
	// files from unexpected locations.
	PrefixMismatchDelete
)

func (p PrefixMismatchMode) String() string {
	switch p {
	case PrefixMismatchError:
		return "ERROR"
	case PrefixMismatchIgnore:
		return "IGNORE"
	case PrefixMismatchDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// OrphanCleanupConfig holds configuration for orphan file cleanup operations.
type orphanCleanupConfig struct {
	location           string
	olderThan          time.Duration
	dryRun             bool
	deleteFunc         func(string) error
	maxConcurrency     int
	prefixMismatchMode PrefixMismatchMode
	equalSchemes       map[string]string
	equalAuthorities   map[string]string
}

type OrphanCleanupOption func(*orphanCleanupConfig)

func WithLocation(location string) OrphanCleanupOption {
	return func(cfg *orphanCleanupConfig) {
		cfg.location = location
	}
}

func WithFilesOlderThan(duration time.Duration) OrphanCleanupOption {
	return func(cfg *orphanCleanupConfig) {
		cfg.olderThan = duration
	}
}

func WithDryRun(enabled bool) OrphanCleanupOption {
	return func(cfg *orphanCleanupConfig) {
		cfg.dryRun = enabled
	}
}

// WithDeleteFunc sets a custom delete function. If not provided, the table's FileIO
// delete method will be used.
func WithDeleteFunc(deleteFunc func(string) error) OrphanCleanupOption {
	return func(cfg *orphanCleanupConfig) {
		cfg.deleteFunc = deleteFunc
	}
}

// WithMaxConcurrency sets the maximum number of goroutines for parallel deletion.
// Defaults to a reasonable number based on the system. Only used when deleteFunc is nil or when
// the FileIO doesn't support bulk operations.
func WithMaxConcurrency(maxWorkers int) OrphanCleanupOption {
	return func(cfg *orphanCleanupConfig) {
		if maxWorkers > 0 {
			cfg.maxConcurrency = maxWorkers
		}
	}
}

// WithPrefixMismatchMode sets how to handle situations when metadata references files
// that match listed files except for authority/scheme differences.
func WithPrefixMismatchMode(mode PrefixMismatchMode) OrphanCleanupOption {
	return func(cfg *orphanCleanupConfig) {
		cfg.prefixMismatchMode = mode
	}
}

// WithEqualSchemes specifies schemes that should be considered equivalent.
// For example, map["s3,s3a,s3n"] = "s3" treats all S3 scheme variants as equivalent.
// The key can be a comma-separated list of schemes that map to the value scheme.
func WithEqualSchemes(schemes map[string]string) OrphanCleanupOption {
	return func(cfg *orphanCleanupConfig) {
		if cfg.equalSchemes == nil {
			cfg.equalSchemes = make(map[string]string)
		}
		for k, v := range schemes {
			cfg.equalSchemes[k] = v
		}
	}
}

// WithEqualAuthorities specifies authorities that should be considered equivalent.
// For example, map["endpoint1.s3.amazonaws.com,endpoint2.s3.amazonaws.com"] = "s3.amazonaws.com"
// treats different S3 endpoints as equivalent. The key can be a comma-separated list.
func WithEqualAuthorities(authorities map[string]string) OrphanCleanupOption {
	return func(cfg *orphanCleanupConfig) {
		if cfg.equalAuthorities == nil {
			cfg.equalAuthorities = make(map[string]string)
		}
		for k, v := range authorities {
			cfg.equalAuthorities[k] = v
		}
	}
}

type OrphanCleanupResult struct {
	OrphanFileLocations []string
	DeletedFiles        []string
	// TotalSizeBytes is the combined size of orphan files only, not all scanned files.
	TotalSizeBytes int64
}

// DeleteOrphanFiles identifies files under a table location that are no longer
// referenced by table metadata and deletes them unless dry-run is enabled.
//
// The table filesystem must implement iceio.ListableIO so orphan cleanup can
// fully enumerate candidate files before deciding what is safe to delete.
func (t Table) DeleteOrphanFiles(ctx context.Context, opts ...OrphanCleanupOption) (OrphanCleanupResult, error) {
	cfg := &orphanCleanupConfig{
		location:           "",             // empty means use table's data location
		olderThan:          72 * time.Hour, // 3 days ago
		dryRun:             false,
		deleteFunc:         nil,
		maxConcurrency:     runtime.GOMAXPROCS(0), // default to number of CPUs
		prefixMismatchMode: PrefixMismatchError,   // default to safest mode
		equalSchemes:       nil,                   // no scheme equivalence by default
		equalAuthorities:   nil,                   // no authority equivalence by default
	}

	// Apply functional options
	for _, opt := range opts {
		opt(cfg)
	}

	return t.executeOrphanCleanup(ctx, cfg)
}

type scannedFile struct {
	path string
	size int64
}

func (t Table) executeOrphanCleanup(ctx context.Context, cfg *orphanCleanupConfig) (OrphanCleanupResult, error) {
	fs, err := t.fsF(ctx)
	if err != nil {
		return OrphanCleanupResult{}, fmt.Errorf("failed to get filesystem: %w", err)
	}

	scanLocation := cfg.location
	if scanLocation == "" {
		scanLocation = t.metadata.Location()
	}

	// Run the S3 walk and referenced-file collection concurrently.
	// Each goroutine owns its variable exclusively — no shared writes.
	var referencedFiles map[string]bool
	var scannedFiles []scannedFile

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		referencedFiles, err = t.getReferencedFiles(gctx, fs, cfg.maxConcurrency, true)

		return err
	})

	g.Go(func() error {
		cutoff := time.Now().Add(-cfg.olderThan)

		return walkDirectory(fs, scanLocation, func(path string, info stdfs.FileInfo) error {
			if gctx.Err() != nil {
				return gctx.Err()
			}
			if info.IsDir() || !info.ModTime().Before(cutoff) {
				return nil
			}
			scannedFiles = append(scannedFiles, scannedFile{path: path, size: info.Size()})

			return nil
		})
	})

	if err = g.Wait(); err != nil {
		return OrphanCleanupResult{}, err
	}

	// Identify orphans.
	normalizedRef := make(map[string]string, len(referencedFiles))
	for refPath := range referencedFiles {
		normalizedPath := normalizeFilePathWithConfig(refPath, cfg)
		normalizedRef[normalizedPath] = refPath
		normalizedRef[refPath] = refPath
	}

	var orphanFiles []string
	var totalOrphanSize int64
	for _, f := range scannedFiles {
		isOrphan, err := isFileOrphan(f.path, referencedFiles, normalizedRef, cfg)
		if err != nil {
			return OrphanCleanupResult{}, fmt.Errorf("failed to identify orphan %s: %w", f.path, err)
		}
		if isOrphan {
			orphanFiles = append(orphanFiles, f.path)
			totalOrphanSize += f.size
		}
	}

	result := OrphanCleanupResult{
		OrphanFileLocations: orphanFiles,
		TotalSizeBytes:      totalOrphanSize,
	}

	if cfg.dryRun {
		return result, nil
	}
	deletedFiles, err := deleteFiles(ctx, fs, orphanFiles, cfg)
	if err != nil {
		return OrphanCleanupResult{}, fmt.Errorf("failed to delete orphan files: %w", err)
	}

	result.DeletedFiles = deletedFiles

	return result, nil
}

// getReferencedFiles collects all files referenced by table metadata: previous metadata
// files, statistics and partition-statistics paths (Puffin, etc.), and all paths reachable
// from current snapshots (manifest lists, manifests, data files).
//
// The collection uses a two-pass approach: first it reads every snapshot's manifest list
// (small files) to discover the set of unique manifest file paths, then it reads each
// unique manifest's entries in parallel. Manifests are immutable and shared across
// snapshots via copy-on-write, so deduplicating avoids redundant I/O that would
// otherwise grow as O(snapshots × manifests-per-snapshot).
//
// If the table has snapshots, fs must not be nil, otherwise an error is returned.
// All returned paths are normalized using the package-level normalizeFilePath function.
// The bool value distinguishes data files (true) from metadata files (false), which
// is used by PurgeFiles to respect gc.enabled.
func (t Table) getReferencedFiles(ctx context.Context, fs iceio.IO, maxConcurrency int, discardDeleted bool) (map[string]bool, error) {
	referenced := make(map[string]bool)
	metadata := t.metadata

	for entry := range metadata.PreviousFiles() {
		referenced[normalizeFilePath(entry.MetadataFile)] = false
	}
	referenced[normalizeFilePath(t.metadataLocation)] = false

	// Add version hint file (for Hadoop-style tables)
	// Following Java's ReachableFileUtil.versionHintLocation() logic:
	versionHintPath := versionHintLocation(metadata.Location())
	referenced[normalizeFilePath(versionHintPath)] = false

	for sf := range metadata.Statistics() {
		// Guard against malformed metadata; statistics-path is required per spec.
		if sf.StatisticsPath != "" {
			referenced[normalizeFilePath(sf.StatisticsPath)] = false
		}
	}
	for psf := range metadata.PartitionStatistics() {
		// Guard against malformed metadata; statistics-path is required per spec.
		if psf.StatisticsPath != "" {
			referenced[normalizeFilePath(psf.StatisticsPath)] = false
		}
	}

	if len(metadata.Snapshots()) > 0 && fs == nil {
		return nil, errors.New("fs cannot be nil when table has snapshots")
	}

	// Pass 1: Read manifest lists (lightweight) to collect unique manifests.
	uniqueManifests := make(map[string]iceberg.ManifestFile)
	for _, snapshot := range metadata.Snapshots() {
		if snapshot.ManifestList != "" {
			referenced[normalizeFilePath(snapshot.ManifestList)] = false
		}

		manifestFiles, err := snapshot.Manifests(fs)
		if err != nil {
			return nil, fmt.Errorf("failed to read manifests for snapshot %d: %w", snapshot.SnapshotID, err)
		}

		for _, manifest := range manifestFiles {
			path := manifest.FilePath()
			if _, ok := uniqueManifests[path]; !ok {
				uniqueManifests[path] = manifest
				referenced[normalizeFilePath(path)] = false
			}
		}
	}

	if len(uniqueManifests) == 0 {
		return referenced, nil
	}

	// Pass 2: Read entries from each unique manifest in parallel.
	type refEntry struct {
		path   string
		isData bool
	}
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(max(min(maxConcurrency, len(uniqueManifests)), 1))
	for _, m := range uniqueManifests {
		g.Go(func() error {
			var entries []refEntry
			// discardDeleted=true: skip DELETED-status entries when
			// computing the reachable file set. A DELETED entry is
			// not live in this snapshot and should not pin the file
			// against orphan cleanup once the snapshot that
			// originally held it live has been expired.
			// This matches iceberg-java and pyiceberg behavior.
			for entry, err := range m.Entries(fs, discardDeleted) {
				if err != nil {
					return fmt.Errorf("manifest %s: %w", m.FilePath(), err)
				}
				if gctx.Err() != nil {
					return gctx.Err()
				}
				// All files tracked within a manifest (data files, equality deletes, position deletes)
				// are considered "data files" for the purposes of gc.enabled.
				entries = append(entries, refEntry{
					path:   normalizeFilePath(entry.DataFile().FilePath()),
					isData: true,
				})
				if ref := entry.DataFile().ReferencedDataFile(); ref != nil {
					// This is a deletion vector entry referencing a data file.
					// Its FilePath() is the deletion vector (.dv) file itself (added above).
					// We must also mark the referenced data file as referenced.
					entries = append(entries, refEntry{
						path:   normalizeFilePath(*ref),
						isData: true,
					})
				}
			}

			mu.Lock()
			for _, e := range entries {
				referenced[e.path] = e.isData
			}
			mu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("failed to read manifest entries: %w", err)
	}

	return referenced, nil
}

func walkDirectory(fsys iceio.IO, root string, fn func(path string, info stdfs.FileInfo) error) error {
	if listable, ok := fsys.(iceio.ListableIO); ok {
		return listable.WalkDir(root, func(path string, d stdfs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() {
				return nil
			}

			info, err := d.Info()
			if err != nil {
				return err
			}

			return fn(path, info)
		})
	}

	return fmt.Errorf("filesystem %T does not implement iceio.ListableIO", fsys)
}

func isFileOrphan(file string, referencedFiles map[string]bool, normalizedReferencedFiles map[string]string, cfg *orphanCleanupConfig) (bool, error) {
	normalizedFile := normalizeFilePathWithConfig(file, cfg)

	// Any presence in referencedFiles means referenced;
	// the bool distinguishes data vs metadata for gc.enabled, not membership"
	if _, ok := referencedFiles[file]; ok {
		return false, nil
	}
	if _, ok := referencedFiles[normalizedFile]; ok {
		return false, nil
	}

	if originalPath, exists := normalizedReferencedFiles[normalizedFile]; exists {
		err := checkPrefixMismatch(originalPath, file, cfg)
		if err != nil {
			return false, err
		}

		return false, nil
	}

	return true, nil
}

func deleteFiles(ctx context.Context, fs iceio.IO, orphanFiles []string, cfg *orphanCleanupConfig) ([]string, error) {
	if len(orphanFiles) == 0 {
		return nil, nil
	}

	// Use bulk delete when available and no custom deleteFunc is set.
	if cfg.deleteFunc == nil {
		if bulk, ok := fs.(iceio.BulkRemovableIO); ok {
			return bulk.DeleteFiles(ctx, orphanFiles)
		}
	}

	if cfg.maxConcurrency == 1 {
		return deleteFilesSequential(fs, orphanFiles, cfg)
	}

	return deleteFilesParallel(fs, orphanFiles, cfg)
}

func deleteFilesSequential(fs iceio.IO, orphanFiles []string, cfg *orphanCleanupConfig) ([]string, error) {
	var deletedFiles []string

	deleteFunc := fs.Remove
	if cfg.deleteFunc != nil {
		deleteFunc = cfg.deleteFunc
	}

	var result error
	for _, file := range orphanFiles {
		if err := deleteFunc(file); err != nil {
			result = errors.Join(result, fmt.Errorf("failed to delete orphan file %s: %w", file, err))

			continue
		}
		deletedFiles = append(deletedFiles, file)
	}

	return deletedFiles, result
}

func deleteFilesParallel(fs iceio.IO, orphanFiles []string, cfg *orphanCleanupConfig) ([]string, error) {
	deleteFunc := fs.Remove
	if cfg.deleteFunc != nil {
		deleteFunc = cfg.deleteFunc
	}

	in := make(chan string, cfg.maxConcurrency)
	out := make(chan string, cfg.maxConcurrency)
	errList := make([][]error, cfg.maxConcurrency)

	go func() {
		defer close(in)
		for _, file := range orphanFiles {
			in <- file
		}
	}()

	var wg sync.WaitGroup
	wg.Add(cfg.maxConcurrency)
	for i := 0; i < cfg.maxConcurrency; i++ {
		go func(workerID int) {
			defer wg.Done()
			for file := range in {
				if err := deleteFunc(file); err != nil {
					errList[workerID] = append(errList[workerID], fmt.Errorf("failed to delete orphan file %s: %w", file, err))
				} else {
					out <- file
				}
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	deletedFiles := make([]string, 0, len(orphanFiles))
	for file := range out {
		deletedFiles = append(deletedFiles, file)
	}

	var allErrors []error
	for _, workerErrors := range errList {
		allErrors = append(allErrors, workerErrors...)
	}
	err := errors.Join(allErrors...)

	return deletedFiles, err
}

// normalizeFilePath normalizes file paths for comparison by handling different
// path representations that might refer to the same file, with support for
// scheme/authority equivalence as specified in the configuration.
//
// This implementation is based on Apache Iceberg's Java DeleteOrphanFiles action:
// https://github.com/apache/iceberg/blob/07c088fce9c54369864dcb6da16006e78206048b/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/DeleteOrphanFilesSparkAction.java#L1
//
// The normalization logic specifically follows the ToFileURI.toFileURI() method (lines 542-548):
// - Line 545: scheme = equalSchemes.getOrDefault(uri.getScheme(), uri.getScheme())
// - Line 546: authority = equalAuthorities.getOrDefault(uri.getAuthority(), uri.getAuthority())
//
// See also: https://iceberg.apache.org/docs/latest/maintenance/#remove-orphan-files
//
// Path normalization is essential for orphan cleanup because:
//  1. Files may be referenced using different but equivalent URI schemes (e.g., s3:// vs s3a:// vs s3n://)
//  2. Different authorities/endpoints may refer to the same storage (e.g., region-specific S3 endpoints)
//  3. Path separators and casing may vary across different systems and configurations
//  4. Without normalization, semantically identical paths would be treated as different,
//     leading to false positives in orphan detection
//
// normalizeFilePath normalizes a file path for comparison, handling schemes, authorities, and separators.
// It also aligns file:// URIs and bare local file system paths so they normalize to the same format.
func normalizeFilePath(path string) string {
	return normalizeFilePathWithConfig(path, nil)
}

func normalizeFilePathWithConfig(path string, cfg *orphanCleanupConfig) string {
	if strings.HasPrefix(path, "file:") {
		if u, err := url.Parse(path); err == nil {
			host := strings.ToLower(u.Host)
			if host == "" || host == "localhost" {
				pathStr := u.Path
				// Intercept Windows drive letters (e.g., /C:/) and strip the leading slash
				if len(pathStr) >= 3 && pathStr[0] == '/' && pathStr[2] == ':' {
					pathStr = pathStr[1:]
				}

				return filepath.Clean(pathStr)
			}
			// Remote authority – keep it as //host/path
			return filepath.Clean("//" + u.Host + u.Path)
		}
	}

	// Handle URL-based paths (s3://, gs://, etc.)
	if strings.Contains(path, "://") {
		return normalizeURLPath(path, cfg)
	}

	return normalizeNonURLPath(path)
}

func versionHintLocation(tableLocation string) string {
	if strings.Contains(tableLocation, "://") || strings.HasPrefix(tableLocation, "file:") {
		if joined, err := url.JoinPath(tableLocation, "metadata", "version-hint.text"); err == nil {
			return joined
		}
	}

	return filepath.Join(tableLocation, "metadata", "version-hint.text")
}

// normalizeURLPath normalizes URL-based file paths with scheme/authority equivalence.
//
// This function handles the complexities of cloud storage URIs where the same file
// can be referenced using different but semantically equivalent schemes and authorities.
//
// Examples of equivalent schemes (configured via equalSchemes):
//   - s3://bucket/path, s3a://bucket/path, s3n://bucket/path (all refer to S3)
//   - abfs://container@account.dfs.core.windows.net/path, abfss://container@account.dfs.core.windows.net/path (Azure)
//
// Examples of equivalent authorities (configured via equalAuthorities):
//   - s3://mybucket.s3.us-west-2.amazonaws.com/path vs s3://s3.us-west-2.amazonaws.com/mybucket/path
//   - Different regional endpoints that serve the same data
//
// Based on Apache Iceberg Java's DeleteOrphanFilesSparkAction.toFileURI() normalization (lines 542-548).
// https://github.com/apache/iceberg/blob/07c088fce9c54369864dcb6da16006e78206048b/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/DeleteOrphanFilesSparkAction.java#L1
func normalizeURLPath(path string, cfg *orphanCleanupConfig) string {
	parsedURL, err := url.Parse(path)
	if err != nil {
		return normalizeNonURLPath(path)
	}

	var equalSchemes map[string]string
	var equalAuthorities map[string]string
	if cfg != nil {
		equalSchemes = cfg.equalSchemes
		equalAuthorities = cfg.equalAuthorities
	}

	normalizedScheme := applySchemeEquivalence(parsedURL.Scheme, equalSchemes)
	normalizedAuthority := applyAuthorityEquivalence(parsedURL.Host, equalAuthorities)
	normalizedURL := &url.URL{
		Scheme: normalizedScheme,
		Host:   normalizedAuthority,
		Path:   filepath.Clean(parsedURL.Path),
	}

	return normalizedURL.String()
}

// normalizeNonURLPath provides basic path normalization for non-URL paths.
//
// Handles file system paths by:
// 1. Applying filepath.Clean() to resolve "..", ".", and redundant separators
// 2. Converting Windows-style backslashes to forward slashes for consistency
//
// This ensures that paths like "dir/./file", "dir//file", and "dir\file" (on Windows)
// all normalize to "dir/file" for consistent comparison.
//
// Uses filepath.ToSlash() equivalent logic to match Go's standard library approach.
func normalizeNonURLPath(path string) string {
	normalized := filepath.Clean(path)
	// We use this because to handle Windows paths
	// on all platforms.filepath.ToSlash() only convert the current OS separator, and
	// we need cross-platform support.
	return strings.ReplaceAll(normalized, "\\", "/")
}

// applySchemeEquivalence maps schemes to their equivalent canonical form.
//
// Common equivalences include:
//   - S3: s3://, s3a://, s3n:// → s3://
//   - Azure: abfs://, abfss:// → abfs://
//   - HDFS: hdfs://, hdfs+webhdfs:// → hdfs://
//
// Based on Apache Iceberg Java's flattenMap() (lines 392-403) and EQUAL_SCHEMES_DEFAULT (line 102).
// https://github.com/apache/iceberg/blob/07c088fce9c54369864dcb6da16006e78206048b/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/DeleteOrphanFilesSparkAction.java#L1
func applySchemeEquivalence(scheme string, equalSchemes map[string]string) string {
	if equalSchemes == nil {
		return scheme
	}
	if canonical, exists := equalSchemes[scheme]; exists {
		return canonical
	}

	// Check comma-separated lists (e.g., "s3,s3a,s3n" -> "s3")
	for schemes, canonical := range equalSchemes {
		if strings.Contains(schemes, ",") {
			for _, s := range strings.Split(schemes, ",") {
				if strings.TrimSpace(s) == scheme {
					return canonical
				}
			}
		}
	}

	return scheme
}

// applyAuthorityEquivalence maps authorities to their equivalent canonical form.
//
// Different cloud storage endpoints and authorities may serve the same logical storage,
// but appear different in URIs. This function normalizes these equivalent authorities
// to enable proper file matching during orphan cleanup.
//
// Common authority equivalences include:
//   - Regional S3 endpoints: s3.us-west-2.amazonaws.com, s3-us-west-2.amazonaws.com
//   - S3 path vs virtual-hosted style: bucket.s3.amazonaws.com vs s3.amazonaws.com/bucket
//   - Azure storage endpoints: account.dfs.core.windows.net, account.blob.core.windows.net
//   - Custom endpoints: minio.company.com, s3.company.local
//
// Based on Apache Iceberg Java's equalAuthorities logic (lines 546, 161-165, 392-403).
// https://github.com/apache/iceberg/blob/07c088fce9c54369864dcb6da16006e78206048b/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/actions/DeleteOrphanFilesSparkAction.java#L1
func applyAuthorityEquivalence(authority string, equalAuthorities map[string]string) string {
	if equalAuthorities == nil {
		return authority
	}

	if canonical, exists := equalAuthorities[authority]; exists {
		return canonical
	}

	for authorities, canonical := range equalAuthorities {
		if strings.Contains(authorities, ",") {
			for _, a := range strings.Split(authorities, ",") {
				if strings.TrimSpace(a) == authority {
					return canonical
				}
			}
		}
	}

	return authority
}

// checkPrefixMismatch detects and handles prefix mismatches between referenced files and filesystem files
func checkPrefixMismatch(referencedPath, filesystemPath string, cfg *orphanCleanupConfig) error {
	// Parse both paths as URLs to compare schemes and authorities
	refURL, refErr := url.Parse(referencedPath)
	fsURL, fsErr := url.Parse(filesystemPath)

	if refErr != nil || fsErr != nil {
		return nil
	}

	refScheme := applySchemeEquivalence(refURL.Scheme, cfg.equalSchemes)
	fsScheme := applySchemeEquivalence(fsURL.Scheme, cfg.equalSchemes)
	refAuth := applyAuthorityEquivalence(refURL.Host, cfg.equalAuthorities)
	fsAuth := applyAuthorityEquivalence(fsURL.Host, cfg.equalAuthorities)

	// Check for mismatches
	schemeMismatch := refScheme != fsScheme
	authMismatch := refAuth != fsAuth

	if !schemeMismatch && !authMismatch {
		return nil // No mismatch
	}

	switch cfg.prefixMismatchMode {
	case PrefixMismatchError:
		return fmt.Errorf("prefix mismatch detected: referenced=%s (scheme=%s, auth=%s) vs filesystem=%s (scheme=%s, auth=%s)",
			referencedPath, refScheme, refAuth, filesystemPath, fsScheme, fsAuth)
	case PrefixMismatchIgnore:
		return nil // Silently ignore mismatch
	case PrefixMismatchDelete:
		return nil // Allow deletion (treat as orphan)
	default:
		return fmt.Errorf("unknown prefix mismatch mode: %d", cfg.prefixMismatchMode)
	}
}

// PurgeFiles physically deletes all files under the table's warehouse location
// and any referenced files written outside the location root (e.g., via write.data.path
// or write.metadata.path properties).
//
// It operates on a best-effort basis. Errors from individual file deletions are
// collected and returned together. If files cannot be deleted (e.g. due to
// permission errors or missing paths), the errors are logged but the overall
// catalog drop operation should typically proceed so the catalog does not
// get out of sync with storage.
func (t Table) PurgeFiles(ctx context.Context) error {
	gcEnabled := t.Metadata().Properties().GetBool("gc.enabled", true)

	fs, err := t.FS(ctx)
	if err != nil {
		return fmt.Errorf("failed to load filesystem for table purge: %w", err)
	}

	var errs []error
	fileSet := make(map[string]string)
	location := t.metadata.Location()

	// 1. Walk the table location directory tree to capture all local files
	// Only walk the directory if gc.enabled=true to prevent accidental deletion
	// of unreferenced branched data files.
	if gcEnabled {
		if listable, ok := fs.(iceio.ListableIO); ok {
			walkErr := listable.WalkDir(location, func(path string, d stdfs.DirEntry, err error) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err != nil {
					if os.IsNotExist(err) || errors.Is(err, stdfs.ErrNotExist) {
						return nil
					}

					return err
				}
				if !d.IsDir() {
					fileSet[normalizeFilePath(path)] = path
				}

				return nil
			})
			if walkErr != nil && !os.IsNotExist(walkErr) && !errors.Is(walkErr, stdfs.ErrNotExist) {
				errs = append(errs, fmt.Errorf("failed walking directory %s: %w", location, walkErr))
			}
		}
	}

	// 2. Union in manifest-referenced and metadata files (which might be outside the table location)
	referencedFiles, refErr := t.getReferencedFiles(ctx, fs, runtime.GOMAXPROCS(0), false)
	if refErr != nil {
		return fmt.Errorf("failed to get referenced files: %w", refErr)
	}

	for path, isData := range referencedFiles {
		if !gcEnabled && isData {
			slog.WarnContext(ctx, "purge: skipping data file, gc.enabled=false", "path", path)

			continue
		}

		norm := normalizeFilePath(path)
		if _, ok := fileSet[norm]; !ok {
			fileSet[norm] = path
		}
	}

	// Convert to slice and sort for deterministic behavior
	files := make([]string, 0, len(fileSet))
	for _, orig := range fileSet {
		files = append(files, orig)
	}
	slices.Sort(files)

	if len(files) > 0 {
		if bulk, ok := fs.(iceio.BulkRemovableIO); ok {
			_, bulkErr := bulk.DeleteFiles(ctx, files)
			if bulkErr != nil {
				errs = append(errs, fmt.Errorf("bulk deletion failed: %w", bulkErr))
			}
		} else {
			for _, file := range files {
				if err := ctx.Err(); err != nil {
					errs = append(errs, err)

					break
				}
				if rmErr := fs.Remove(file); rmErr != nil && !os.IsNotExist(rmErr) {
					errs = append(errs, fmt.Errorf("failed to remove %s: %w", file, rmErr))
				}
			}
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}
