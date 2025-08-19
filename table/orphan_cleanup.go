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
	"fmt"
	stdfs "io/fs"
	"net/url"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	iceio "github.com/apache/iceberg-go/io"
)

type PrefixMismatchMode int

const (
	PrefixMismatchError PrefixMismatchMode = iota // default
	PrefixMismatchIgnore
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

// orphanCleanupConfig holds configuration for orphan file cleanup operations.
type orphanCleanupConfig struct {
	location           string
	olderThan          time.Time
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

func WithOlderThan(timestamp time.Time) OrphanCleanupOption {
	return func(cfg *orphanCleanupConfig) {
		cfg.olderThan = timestamp
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
	TotalSizeBytes      int64
}

func (t Table) DeleteOrphanFiles(ctx context.Context, opts ...OrphanCleanupOption) (OrphanCleanupResult, error) {
	cfg := &orphanCleanupConfig{
		location:           "",                           // empty means use table's data location
		olderThan:          time.Now().AddDate(0, 0, -3), // 3 days ago
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

func (t Table) executeOrphanCleanup(ctx context.Context, cfg *orphanCleanupConfig) (OrphanCleanupResult, error) {
	fs, err := t.fsF(ctx)
	if err != nil {
		return OrphanCleanupResult{}, fmt.Errorf("failed to get filesystem: %w", err)
	}

	scanLocation := cfg.location
	if scanLocation == "" {
		scanLocation = t.metadata.Location()
	}

	referencedFiles, err := t.getReferencedFiles(fs)
	if err != nil {
		return OrphanCleanupResult{}, fmt.Errorf("failed to get referenced files: %w", err)
	}

	allFiles, totalSize, err := t.scanFiles(fs, scanLocation, cfg)
	if err != nil {
		return OrphanCleanupResult{}, fmt.Errorf("failed to scan files: %w", err)
	}

	orphanFiles, err := identifyOrphanFiles(allFiles, referencedFiles, cfg)
	if err != nil {
		return OrphanCleanupResult{}, fmt.Errorf("failed to identify orphan files: %w", err)
	}

	result := OrphanCleanupResult{
		OrphanFileLocations: orphanFiles,
		TotalSizeBytes:      totalSize,
	}

	if cfg.dryRun {
		return result, nil
	}
	deletedFiles, err := deleteFiles(fs, orphanFiles, cfg)
	if err != nil {
		return OrphanCleanupResult{}, fmt.Errorf("failed to delete orphan files: %w", err)
	}

	result.DeletedFiles = deletedFiles

	return result, nil
}

// getReferencedFiles collects all files referenced by current snapshots
func (t Table) getReferencedFiles(fs iceio.IO) (map[string]bool, error) {
	referenced := make(map[string]bool)
	metadata := t.metadata

	for entry := range metadata.PreviousFiles() {
		referenced[entry.MetadataFile] = true
	}
	referenced[t.metadataLocation] = true

	// Add version hint file (for Hadoop-style tables)
	// Following Java's ReachableFileUtil.versionHintLocation() logic:
	versionHintPath := filepath.Join(metadata.Location(), "metadata", "version-hint.text")
	referenced[versionHintPath] = true

	// TODO: Add statistics files support once iceberg-go exposes statisticsFiles()

	for _, snapshot := range metadata.Snapshots() {
		if snapshot.ManifestList != "" {
			referenced[snapshot.ManifestList] = true
		}

		manifestFiles, err := snapshot.Manifests(fs)
		if err != nil {
			return nil, fmt.Errorf("failed to read manifests for snapshot %d: %w", snapshot.SnapshotID, err)
		}

		for _, manifest := range manifestFiles {
			referenced[manifest.FilePath()] = true

			entries, err := manifest.FetchEntries(fs, false)
			if err != nil {
				return nil, fmt.Errorf("failed to read manifest entries: %w", err)
			}

			for _, entry := range entries {
				referenced[entry.DataFile().FilePath()] = true
			}
		}
	}

	return referenced, nil
}

func (t Table) scanFiles(fs iceio.IO, location string, cfg *orphanCleanupConfig) ([]string, int64, error) {
	var allFiles []string
	var totalSize int64

	err := walkDirectory(fs, location, func(path string, info stdfs.FileInfo) error {
		if info.IsDir() {
			return nil
		}

		if !info.ModTime().Before(cfg.olderThan) {
			return nil // Skip files that are newer than or equal to the threshold
		}

		allFiles = append(allFiles, path)
		totalSize += info.Size()

		return nil
	})

	if err != nil {
		return nil, 0, err
	}

	return allFiles, totalSize, nil
}

// getBucket gets the Bucket field from blob storage - absolute minimal approach
func getBucket(fsys iceio.IO) stdfs.FS {
	v := reflect.ValueOf(fsys).Elem() // We know it's a pointer to struct

	return v.FieldByName("Bucket").Interface().(stdfs.FS)
}

// makeFileWalkFunc creates a WalkDirFunc that processes only files with path transformation
func makeFileWalkFunc(fn func(path string, info stdfs.FileInfo) error, pathTransform func(string) string) stdfs.WalkDirFunc {
	return func(path string, d stdfs.DirEntry, err error) error {
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

		return fn(pathTransform(path), info)
	}
}

func walkDirectory(fsys iceio.IO, root string, fn func(path string, info stdfs.FileInfo) error) error {
	switch v := fsys.(type) {
	case iceio.LocalFS:
		cleanRoot := strings.TrimPrefix(root, "file://")
		if cleanRoot == "" {
			cleanRoot = "."
		}

		return filepath.WalkDir(cleanRoot, makeFileWalkFunc(fn, func(path string) string {
			return path
		}))

	default:
		// For blob storage: direct field access since we know the structure
		bucket := getBucket(v)

		parsed, err := url.Parse(root)
		if err != nil {
			return fmt.Errorf("invalid URL %s: %w", root, err)
		}

		walkPath := strings.TrimPrefix(parsed.Path, "/")
		if walkPath == "" {
			walkPath = "."
		}

		// URL transform - reconstruct full URL path
		return stdfs.WalkDir(bucket, walkPath, makeFileWalkFunc(fn, func(path string) string {
			return parsed.Scheme + "://" + parsed.Host + "/" + path
		}))
	}
}

func identifyOrphanFiles(allFiles []string, referencedFiles map[string]bool, cfg *orphanCleanupConfig) ([]string, error) {
	normalizedReferencedFiles := make(map[string]string)
	for refPath := range referencedFiles {
		normalizedPath := normalizeFilePath(refPath, cfg)
		normalizedReferencedFiles[normalizedPath] = refPath
		// Also include the original path for direct lookup
		normalizedReferencedFiles[refPath] = refPath
	}

	var orphans []string

	for _, file := range allFiles {
		isOrphan, err := isFileOrphan(file, referencedFiles, normalizedReferencedFiles, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to determine if file %s is orphan: %w", file, err)
		}

		if isOrphan {
			orphans = append(orphans, file)
		}
	}

	return orphans, nil
}

func isFileOrphan(file string, referencedFiles map[string]bool, normalizedReferencedFiles map[string]string, cfg *orphanCleanupConfig) (bool, error) {
	normalizedFile := normalizeFilePath(file, cfg)

	if referencedFiles[file] || referencedFiles[normalizedFile] {
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

func deleteFiles(fs iceio.IO, orphanFiles []string, cfg *orphanCleanupConfig) ([]string, error) {
	if len(orphanFiles) == 0 {
		return nil, nil
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

	for _, file := range orphanFiles {
		err := deleteFunc(file)
		if err != nil {
			// Log warning but continue with other files
			fmt.Printf("Warning: failed to delete orphan file %s: %v\n", file, err)

			continue
		}

		deletedFiles = append(deletedFiles, file)
	}

	return deletedFiles, nil
}

func deleteFilesParallel(fs iceio.IO, orphanFiles []string, cfg *orphanCleanupConfig) ([]string, error) {
	deleteFunc := fs.Remove
	if cfg.deleteFunc != nil {
		deleteFunc = cfg.deleteFunc
	}

	var deletedFiles []string
	var mu sync.Mutex

	g := new(errgroup.Group)
	g.SetLimit(cfg.maxConcurrency)

	for _, file := range orphanFiles {
		file := file // capture loop variable
		g.Go(func() error {
			err := deleteFunc(file)
			if err != nil {
				// Log warning but don't fail the entire operation for individual file errors
				fmt.Printf("Warning: failed to delete orphan file %s: %v\n", file, err)

				return nil // Continue with other files
			}

			mu.Lock()
			deletedFiles = append(deletedFiles, file)
			mu.Unlock()

			return nil
		})
	}

	// Wait for all workers to complete
	if err := g.Wait(); err != nil {
		return deletedFiles, err
	}

	return deletedFiles, nil
}

// normalizeFilePath normalizes file paths for comparison by handling different
// path representations that might refer to the same file, with support for
// scheme/authority equivalence as specified in the configuration.
func normalizeFilePath(path string, cfg *orphanCleanupConfig) string {
	// Handle URL-based paths (s3://, gs://, etc.)
	if strings.Contains(path, "://") {
		return normalizeURLPath(path, cfg)
	} else {
		return normalizeNonURLPath(path)
	}
}

// normalizeURLPath normalizes URL-based file paths with scheme/authority equivalence
func normalizeURLPath(path string, cfg *orphanCleanupConfig) string {
	parsedURL, err := url.Parse(path)
	if err != nil {
		return normalizeNonURLPath(path)
	}

	normalizedScheme := applySchemeEquivalence(parsedURL.Scheme, cfg.equalSchemes)
	normalizedAuthority := applyAuthorityEquivalence(parsedURL.Host, cfg.equalAuthorities)
	normalizedURL := &url.URL{
		Scheme: normalizedScheme,
		Host:   normalizedAuthority,
		Path:   filepath.Clean(parsedURL.Path),
	}

	return normalizedURL.String()
}

// normalizeNonURLPath provides basic path normalization for non-URL paths
func normalizeNonURLPath(path string) string {
	normalized := filepath.Clean(path)

	return strings.ReplaceAll(normalized, "\\", "/")
}

// applySchemeEquivalence maps schemes to their equivalent canonical form
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

// applyAuthorityEquivalence maps authorities to their equivalent canonical form
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
