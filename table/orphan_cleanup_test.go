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
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrefixMismatchMode_String(t *testing.T) {
	tests := []struct {
		mode     PrefixMismatchMode
		expected string
	}{
		{PrefixMismatchError, "ERROR"},
		{PrefixMismatchIgnore, "IGNORE"},
		{PrefixMismatchDelete, "DELETE"},
		{PrefixMismatchMode(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.mode.String())
		})
	}
}

func TestOrphanCleanupOptions(t *testing.T) {
	cfg := &orphanCleanupConfig{}

	WithLocation("/test/location")(cfg)
	assert.Equal(t, "/test/location", cfg.location)

	testDuration := 24 * time.Hour
	WithFilesOlderThan(testDuration)(cfg)
	assert.Equal(t, testDuration, cfg.olderThan)

	WithDryRun(true)(cfg)
	assert.True(t, cfg.dryRun)

	deleteFunc := func(string) error { return nil }
	WithDeleteFunc(deleteFunc)(cfg)
	assert.NotNil(t, cfg.deleteFunc)

	WithMaxConcurrency(8)(cfg)
	assert.Equal(t, 8, cfg.maxConcurrency)

	WithPrefixMismatchMode(PrefixMismatchIgnore)(cfg)
	assert.Equal(t, PrefixMismatchIgnore, cfg.prefixMismatchMode)

	schemes := map[string]string{"s3,s3a,s3n": "s3"}
	WithEqualSchemes(schemes)(cfg)
	assert.Equal(t, schemes, cfg.equalSchemes)

	authorities := map[string]string{"host1,host2": "canonical"}
	WithEqualAuthorities(authorities)(cfg)
	assert.Equal(t, authorities, cfg.equalAuthorities)
}

func TestNormalizeFilePath(t *testing.T) {
	cfg := &orphanCleanupConfig{
		equalSchemes:     map[string]string{"s3,s3a,s3n": "s3"},
		equalAuthorities: map[string]string{"endpoint1,endpoint2": "canonical"},
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "local_path",
			input:    "/local/path/file.txt",
			expected: "/local/path/file.txt",
		},
		{
			name:     "windows_path",
			input:    "C:\\Windows\\path\\file.txt",
			expected: "C:/Windows/path/file.txt",
		},
		{
			name:     "s3_url",
			input:    "s3://bucket/path/file.txt",
			expected: "s3://bucket/path/file.txt",
		},
		{
			name:     "s3a_to_s3_scheme_equivalence",
			input:    "s3a://bucket/path/file.txt",
			expected: "s3://bucket/path/file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeFilePathWithConfig(tt.input, cfg)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNormalizeNonURLPath(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "unix_path",
			input:    "/path/to/file.txt",
			expected: "/path/to/file.txt",
		},
		{
			name:     "windows_path",
			input:    "C:\\Windows\\path\\file.txt",
			expected: "C:/Windows/path/file.txt",
		},
		{
			name:     "relative_path",
			input:    "./relative/path/../file.txt",
			expected: "relative/file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeNonURLPath(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestApplySchemeEquivalence(t *testing.T) {
	equalSchemes := map[string]string{
		"s3,s3a,s3n": "s3",
		"gs":         "gs",
	}

	tests := []struct {
		name     string
		scheme   string
		expected string
	}{
		{
			name:     "s3_unchanged",
			scheme:   "s3",
			expected: "s3",
		},
		{
			name:     "s3a_to_s3",
			scheme:   "s3a",
			expected: "s3",
		},
		{
			name:     "s3n_to_s3",
			scheme:   "s3n",
			expected: "s3",
		},
		{
			name:     "gs_unchanged",
			scheme:   "gs",
			expected: "gs",
		},
		{
			name:     "unknown_scheme",
			scheme:   "unknown",
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := applySchemeEquivalence(tt.scheme, equalSchemes)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestApplyAuthorityEquivalence(t *testing.T) {
	equalAuthorities := map[string]string{
		"host1,host2": "canonical",
		"single":      "single",
	}

	tests := []struct {
		name      string
		authority string
		expected  string
	}{
		{
			name:      "host1_to_canonical",
			authority: "host1",
			expected:  "canonical",
		},
		{
			name:      "host2_to_canonical",
			authority: "host2",
			expected:  "canonical",
		},
		{
			name:      "single_unchanged",
			authority: "single",
			expected:  "single",
		},
		{
			name:      "unknown_authority",
			authority: "unknown",
			expected:  "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := applyAuthorityEquivalence(tt.authority, equalAuthorities)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCheckPrefixMismatch(t *testing.T) {
	cfg := &orphanCleanupConfig{
		prefixMismatchMode: PrefixMismatchError,
		equalSchemes:       map[string]string{"s3,s3a,s3n": "s3"},
		equalAuthorities:   map[string]string{"host1,host2": "canonical"},
	}

	tests := []struct {
		name           string
		referencedPath string
		filesystemPath string
		mode           PrefixMismatchMode
		expectError    bool
		errorContains  string
	}{
		{
			name:           "no_mismatch",
			referencedPath: "s3://bucket/path/file.txt",
			filesystemPath: "s3://bucket/path/file.txt",
			mode:           PrefixMismatchError,
			expectError:    false,
		},
		{
			name:           "scheme_mismatch_error_mode",
			referencedPath: "s3://bucket/path/file.txt",
			filesystemPath: "gs://bucket/path/file.txt",
			mode:           PrefixMismatchError,
			expectError:    true,
			errorContains:  "prefix mismatch detected",
		},
		{
			name:           "scheme_mismatch_ignore_mode",
			referencedPath: "s3://bucket/path/file.txt",
			filesystemPath: "gs://bucket/path/file.txt",
			mode:           PrefixMismatchIgnore,
			expectError:    false,
		},
		{
			name:           "scheme_mismatch_delete_mode",
			referencedPath: "s3://bucket/path/file.txt",
			filesystemPath: "gs://bucket/path/file.txt",
			mode:           PrefixMismatchDelete,
			expectError:    false,
		},
		{
			name:           "equivalent_schemes",
			referencedPath: "s3://bucket/path/file.txt",
			filesystemPath: "s3a://bucket/path/file.txt",
			mode:           PrefixMismatchError,
			expectError:    false,
		},
		{
			name:           "non_url_paths",
			referencedPath: "/local/path/file.txt",
			filesystemPath: "/local/path/file.txt",
			mode:           PrefixMismatchError,
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCfg := *cfg
			testCfg.prefixMismatchMode = tt.mode

			err := checkPrefixMismatch(tt.referencedPath, tt.filesystemPath, &testCfg)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestIsFileOrphan(t *testing.T) {
	// PrefixMismatchError ensures that metadata files (value=false) found via
	// normalized lookup don't accidentally fall through to the prefix-mismatch
	// error path. This would happen if isFileOrphan used a bare map-value check
	// instead of the existence check (_, ok).
	cfg := &orphanCleanupConfig{
		prefixMismatchMode: PrefixMismatchError,
		equalSchemes:       map[string]string{"s3,s3a,s3n": "s3"},
		equalAuthorities:   map[string]string{"host-a,host-b": "host-a"},
	}

	referencedFiles := map[string]bool{
		"s3://bucket/data/file1.parquet":        true,
		"s3://host-a/metadata/v1.metadata.json": false, // metadata → value=false
		"s3://host-a/data/file1.parquet":        true,
		"/local/path/file2.parquet":             true,
	}

	tests := []struct {
		name         string
		file         string
		expectOrphan bool
	}{
		{
			name:         "referenced_file_exact_match",
			file:         "s3://bucket/data/file1.parquet",
			expectOrphan: false,
		},
		{
			name:         "referenced_file_scheme_equivalence",
			file:         "s3a://bucket/data/file1.parquet",
			expectOrphan: false,
		},
		{
			name:         "orphan_file",
			file:         "s3://bucket/data/orphan.parquet",
			expectOrphan: true,
		},
		{
			name:         "local_referenced_file",
			file:         "/local/path/file2.parquet",
			expectOrphan: false,
		},
		{
			name:         "local_orphan_file",
			file:         "/local/path/orphan.parquet",
			expectOrphan: true,
		},
		{
			name:         "metadata_exact_host_match",
			file:         "s3://host-a/metadata/v1.metadata.json",
			expectOrphan: false,
		},
		{
			name:         "metadata_equivalent_host",
			file:         "s3://host-b/metadata/v1.metadata.json",
			expectOrphan: false,
		},
		{
			name:         "data_equivalent_host",
			file:         "s3://host-b/data/file1.parquet",
			expectOrphan: false,
		},
	}
	normalizedReferencedFiles := make(map[string]string)
	for refPath := range referencedFiles {
		normalizedPath := normalizeFilePathWithConfig(refPath, cfg)
		normalizedReferencedFiles[normalizedPath] = refPath
		normalizedReferencedFiles[refPath] = refPath
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isOrphan, err := isFileOrphan(tt.file, referencedFiles, normalizedReferencedFiles, cfg)
			require.NoError(t, err)
			assert.Equal(t, tt.expectOrphan, isOrphan)
		})
	}
}

func TestNormalizeURLPath(t *testing.T) {
	cfg := &orphanCleanupConfig{
		equalSchemes:     map[string]string{"s3,s3a,s3n": "s3"},
		equalAuthorities: map[string]string{"host1,host2": "canonical"},
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple_s3_url",
			input:    "s3://bucket/path/file.txt",
			expected: "s3://bucket/path/file.txt",
		},
		{
			name:     "s3a_to_s3_conversion",
			input:    "s3a://bucket/path/file.txt",
			expected: "s3://bucket/path/file.txt",
		},
		{
			name:     "authority_equivalence",
			input:    "s3://host1/path/file.txt",
			expected: "s3://canonical/path/file.txt",
		},
		{
			name:     "complex_path_cleaning",
			input:    "s3://bucket/path/../other/./file.txt",
			expected: "s3://bucket/other/file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeURLPath(tt.input, cfg)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNormalizeURLPath_InvalidURL(t *testing.T) {
	cfg := &orphanCleanupConfig{}

	result := normalizeURLPath("not-a-valid-url", cfg)
	expected := normalizeNonURLPath("not-a-valid-url")
	assert.Equal(t, expected, result)
}

func TestOrphanCleanup_EdgeCases(t *testing.T) {
	t.Run("prefix_mismatch_unknown_mode", func(t *testing.T) {
		cfg := &orphanCleanupConfig{
			prefixMismatchMode: PrefixMismatchMode(999), // Invalid mode
		}

		err := checkPrefixMismatch("s3://bucket/file", "gs://bucket/file", cfg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown prefix mismatch mode")
	})

	t.Run("empty_scheme_authority_maps", func(t *testing.T) {
		// Test with nil maps
		result := applySchemeEquivalence("s3", nil)
		assert.Equal(t, "s3", result)

		result = applyAuthorityEquivalence("host", nil)
		assert.Equal(t, "host", result)
	})
}

func TestGetReferencedFiles_IncludesStatisticsFiles(t *testing.T) {
	const metaJSON = `{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": "s3://bucket/test/location",
  "last-sequence-number": 0,
  "last-updated-ms": 1602638573590,
  "last-column-id": 1,
  "current-schema-id": 0,
  "schemas": [
    {"type": "struct", "schema-id": 0, "fields": [{"id": 1, "name": "x", "required": true, "type": "long"}]}
  ],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id": 0, "fields": []}],
  "last-partition-id": 0,
  "default-sort-order-id": 0,
  "sort-orders": [{"order-id": 0, "fields": []}],
  "metadata-log": [],
  "snapshot-log": [],
  "statistics": [
    {
      "snapshot-id": 1,
      "statistics-path": "s3://bucket/stats/table-stats.puffin",
      "file-size-in-bytes": 1024,
      "file-footer-size-in-bytes": 512,
      "blob-metadata": []
    },
    {
      "snapshot-id": 2,
      "statistics-path": "",
      "file-size-in-bytes": 0,
      "file-footer-size-in-bytes": 0,
      "blob-metadata": []
    }
  ],
  "partition-statistics": [
    {
      "snapshot-id": 1,
      "statistics-path": "s3://bucket/stats/part-stats.puffin",
      "file-size-in-bytes": 512
    }
  ]
}`

	meta, err := ParseMetadataString(metaJSON)
	require.NoError(t, err)

	tbl := Table{
		metadata:         meta,
		metadataLocation: "s3://bucket/test/location/metadata/v1.metadata.json",
	}

	// No snapshots: FileIO is not used; statistics paths must still be referenced.
	refs, err := tbl.getReferencedFiles(context.Background(), nil, 1, true)
	require.NoError(t, err)

	assert.Contains(t, refs, normalizeFilePath("s3://bucket/stats/table-stats.puffin"))
	assert.Contains(t, refs, normalizeFilePath("s3://bucket/stats/part-stats.puffin"))
	assert.Contains(t, refs, normalizeFilePath(tbl.metadataLocation))
	assert.NotContains(t, refs, normalizeFilePath("s3://bucket/stats/not-referenced.puffin"))
	assert.NotContains(t, refs, "")
}

// mockBulkRemovableIO is a test double that implements BulkRemovableIO.
type mockBulkRemovableIO struct {
	bulkCalled bool
	bulkPaths  []string
}

func (m *mockBulkRemovableIO) Open(string) (io.File, error) {
	return nil, errors.New("not implemented")
}

func (m *mockBulkRemovableIO) Remove(string) error {
	return errors.New("Remove should not be called when BulkRemovableIO is available")
}

func (m *mockBulkRemovableIO) DeleteFiles(_ context.Context, paths []string) ([]string, error) {
	m.bulkCalled = true
	m.bulkPaths = paths

	return paths, nil
}

func TestDeleteFilesUsesBulkRemovableIO(t *testing.T) {
	mock := &mockBulkRemovableIO{}
	orphans := []string{"s3://bucket/data/orphan1.parquet", "s3://bucket/data/orphan2.parquet"}
	cfg := &orphanCleanupConfig{}

	deleted, err := deleteFiles(context.Background(), mock, orphans, cfg)
	require.NoError(t, err)
	assert.True(t, mock.bulkCalled)
	assert.Equal(t, orphans, mock.bulkPaths)
	assert.Equal(t, orphans, deleted)
}

func TestDeleteFilesWithCustomDeleteFunc(t *testing.T) {
	mock := &mockBulkRemovableIO{}

	var customDeleted []string
	cfg := &orphanCleanupConfig{
		deleteFunc: func(path string) error {
			customDeleted = append(customDeleted, path)

			return nil
		},
		maxConcurrency: 1,
	}
	orphans := []string{"s3://bucket/data/orphan1.parquet"}

	deleted, err := deleteFiles(context.Background(), mock, orphans, cfg)
	require.NoError(t, err)
	assert.False(t, mock.bulkCalled, "BulkRemovableIO should not be used when deleteFunc is set")
	assert.Equal(t, orphans, customDeleted)
	assert.Equal(t, orphans, deleted)
}

func TestDeleteFilesEmpty(t *testing.T) {
	deleted, err := deleteFiles(context.Background(), nil, nil, &orphanCleanupConfig{})
	require.NoError(t, err)
	assert.Nil(t, deleted)
}

// mockPlainIO implements only IO, without optional delete or listing capabilities.
type mockPlainIO struct {
	removed []string
}

func (m *mockPlainIO) Open(string) (io.File, error) {
	return nil, errors.New("not implemented")
}

func (m *mockPlainIO) Remove(name string) error {
	m.removed = append(m.removed, name)

	return nil
}

func TestDeleteFilesFallsBackToExistingBehavior(t *testing.T) {
	mock := &mockPlainIO{}
	orphans := []string{"s3://bucket/data/orphan1.parquet", "s3://bucket/data/orphan2.parquet"}
	cfg := &orphanCleanupConfig{maxConcurrency: 1}

	deleted, err := deleteFiles(context.Background(), mock, orphans, cfg)
	require.NoError(t, err)
	assert.ElementsMatch(t, orphans, mock.removed)
	assert.ElementsMatch(t, orphans, deleted)
}

func TestWalkDirectoryRequiresListableIO(t *testing.T) {
	err := walkDirectory(&mockPlainIO{}, "s3://bucket/data", func(string, stdfs.FileInfo) error {
		require.Fail(t, "walk callback should not run for non-listable IO")

		return nil
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not implement io.ListableIO")
}

type inMemoryCatalog struct {
	metadata Metadata
}

func (c *inMemoryCatalog) CommitTable(
	ctx context.Context,
	ident Identifier,
	reqs []Requirement,
	updates []Update,
) (Metadata, string, error) {
	meta, err := UpdateTableMetadata(c.metadata, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.metadata = meta

	return meta, "", nil
}

func (c *inMemoryCatalog) LoadTable(ctx context.Context, ident Identifier) (*Table, error) {
	return nil, nil
}

func TestGetReferencedFiles_OverwriteThenExpireExcludesTombstones(t *testing.T) {
	ctx := context.Background()
	tableLocation := t.TempDir()

	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	arrSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
	spec := *iceberg.UnpartitionedSpec

	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, tableLocation,
		iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)

	fs := io.LocalFS{}
	tbl := New(
		Identifier{"db", "tbl"},
		meta,
		tableLocation+"/metadata/v0.metadata.json",
		func(context.Context) (io.IO, error) { return fs, nil },
		&inMemoryCatalog{meta},
	)

	// Step 1: append id=1. Produces snapshot 1 with one ADDED entry for fileA.
	arrA, err := array.TableFromJSON(memory.DefaultAllocator, arrSchema, []string{`[{"id": 1}]`})
	require.NoError(t, err)
	defer arrA.Release()
	tbl, err = tbl.AppendTable(ctx, arrA, 1, nil)
	require.NoError(t, err)

	snap1 := tbl.CurrentSnapshot()
	require.NotNil(t, snap1)
	pathA := dataFilePathsFromSnapshot(t, snap1, fs, iceberg.EntryStatusADDED)
	require.Len(t, pathA, 1, "expected one ADDED data file after append")
	fileA := pathA[0]

	// Step 2: overwrite with id=2. Produces snapshot 2 whose manifest list
	// contains [added-fileB-manifest, deleted-fileA-manifest]. fileA still
	// lives in snapshot 1's manifest as ADDED at this point.
	arrB, err := array.TableFromJSON(memory.DefaultAllocator, arrSchema, []string{`[{"id": 2}]`})
	require.NoError(t, err)
	defer arrB.Release()
	tbl, err = tbl.OverwriteTable(ctx, arrB, 1, nil)
	require.NoError(t, err)
	require.Len(t, tbl.Metadata().Snapshots(), 2, "expected two snapshots after overwrite")

	pathB := dataFilePathsFromSnapshot(t, tbl.CurrentSnapshot(), fs, iceberg.EntryStatusADDED)
	require.Len(t, pathB, 1, "expected one ADDED data file after overwrite")
	fileB := pathB[0]

	// Step 3: expire snapshot 1, keeping only the overwrite snapshot.
	// WithPostCommit(false) keeps fileA on disk so the test only exercises
	// metadata reachability, not the side-effect of file removal.
	tx := tbl.NewTransaction()
	require.NoError(t, tx.ExpireSnapshots(
		WithRetainLast(1),
		WithOlderThan(0),
		WithPostCommit(false),
	))
	tbl, err = tx.Commit(ctx)
	require.NoError(t, err)
	require.Len(t, tbl.Metadata().Snapshots(), 1,
		"only the overwrite snapshot should remain after expiration")

	// fileA is now referenced only via a DELETED entry in the surviving
	// snapshot's tombstone manifest. The fix must exclude it.
	refs, err := tbl.getReferencedFiles(ctx, fs, 1, true)
	require.NoError(t, err)

	assert.Contains(t, refs, normalizeFilePath(fileB),
		"new live file (ADDED in surviving snapshot) must be in reference set")
	assert.NotContains(t, refs, normalizeFilePath(fileA),
		"overwritten file (only present as DELETED tombstone) must NOT be in reference set")
}

// dataFilePathsFromSnapshot returns the data-file paths referenced by the
// given snapshot's manifests, filtered to entries matching wantStatus.
func dataFilePathsFromSnapshot(
	t *testing.T,
	snap *Snapshot,
	fs io.IO,
	wantStatus iceberg.ManifestEntryStatus,
) []string {
	t.Helper()
	manifests, err := snap.Manifests(fs)
	require.NoError(t, err)

	var paths []string
	for _, m := range manifests {
		for e, err := range m.Entries(fs, false) {
			require.NoError(t, err)
			if e.Status() == wantStatus {
				paths = append(paths, e.DataFile().FilePath())
			}
		}
	}

	return paths
}

func TestGetReferencedFiles_SharedManifestReadOnce(t *testing.T) {
	// A manifest shared by two snapshots must be opened exactly once.
	// A regression that drops the dedup would open it twice, turning
	// O(unique_manifests) into O(snapshots × manifests_per_snapshot).
	const (
		dataPath      = "s3://bucket/data/file-1.parquet"
		manifestPath  = "s3://bucket/meta/manifest-shared.avro"
		manifestList1 = "s3://bucket/meta/snap-1.avro"
		manifestList2 = "s3://bucket/meta/snap-2.avro"
	)

	tio := newTrackingCallsIO()
	mf := writeManifest(t, tio.trackingIO, 1, 1, manifestPath, dataPath)
	writeManifestList(t, tio.trackingIO, 1, manifestList1, []iceberg.ManifestFile{mf})
	writeManifestList(t, tio.trackingIO, 2, manifestList2, []iceberg.ManifestFile{mf})
	tio.files[dataPath] = []byte("data")

	meta, err := ParseMetadataString(buildMetaJSON(metaJSONOpts{
		snapshots: fmt.Sprintf(
			`{"snapshot-id":1,"timestamp-ms":1000,"manifest-list":%q},`+
				`{"snapshot-id":2,"timestamp-ms":2000,"manifest-list":%q}`,
			manifestList1, manifestList2),
	}))
	require.NoError(t, err)

	tbl := New(Identifier{"ns", "tbl"}, meta, "metadata.json", testFSF(tio), nil)
	refs, err := tbl.getReferencedFiles(context.Background(), tio, 1, true)
	require.NoError(t, err)

	assert.Contains(t, refs, dataPath)
	assert.Contains(t, refs, manifestPath)
	assert.Contains(t, refs, manifestList1)
	assert.Contains(t, refs, manifestList2)

	assert.Equal(t, 1, tio.openCount[manifestPath],
		"shared manifest must be opened exactly once across snapshots")
}

func TestGetReferencedFiles_DisjointManifestsAllRead(t *testing.T) {
	// Two snapshots with disjoint manifests: both must be read,
	// and each data file must appear in the referenced set.
	const (
		dataPath1     = "s3://bucket/data/file-1.parquet"
		dataPath2     = "s3://bucket/data/file-2.parquet"
		manifestPath1 = "s3://bucket/meta/manifest-1.avro"
		manifestPath2 = "s3://bucket/meta/manifest-2.avro"
		manifestList1 = "s3://bucket/meta/snap-1.avro"
		manifestList2 = "s3://bucket/meta/snap-2.avro"
	)

	tio := newTrackingCallsIO()
	mf1 := writeManifest(t, tio.trackingIO, 1, 1, manifestPath1, dataPath1)
	mf2 := writeManifest(t, tio.trackingIO, 2, 2, manifestPath2, dataPath2)
	writeManifestList(t, tio.trackingIO, 1, manifestList1, []iceberg.ManifestFile{mf1})
	writeManifestList(t, tio.trackingIO, 2, manifestList2, []iceberg.ManifestFile{mf2})

	meta, err := ParseMetadataString(buildMetaJSON(metaJSONOpts{
		snapshots: fmt.Sprintf(
			`{"snapshot-id":1,"timestamp-ms":1000,"manifest-list":%q},`+
				`{"snapshot-id":2,"timestamp-ms":2000,"manifest-list":%q}`,
			manifestList1, manifestList2),
	}))
	require.NoError(t, err)

	tbl := New(Identifier{"ns", "tbl"}, meta, "metadata.json", testFSF(tio), nil)
	refs, err := tbl.getReferencedFiles(context.Background(), tio, 1, true)
	require.NoError(t, err)

	assert.Contains(t, refs, dataPath1)
	assert.Contains(t, refs, dataPath2)
	assert.Equal(t, 1, tio.openCount[manifestPath1])
	assert.Equal(t, 1, tio.openCount[manifestPath2])
}

func TestGetReferencedFiles_ManySnapshotsShareManifest(t *testing.T) {
	// Stress the dedup: 10 snapshots all reference the same manifest.
	// The manifest must be opened exactly once.
	const (
		dataPath     = "s3://bucket/data/file-1.parquet"
		manifestPath = "s3://bucket/meta/manifest-shared.avro"
	)

	tio := newTrackingCallsIO()
	mf := writeManifest(t, tio.trackingIO, 1, 1, manifestPath, dataPath)

	numSnapshots := 10
	var snapJSON []string
	for i := 1; i <= numSnapshots; i++ {
		listPath := fmt.Sprintf("s3://bucket/meta/snap-%d.avro", i)
		writeManifestList(t, tio.trackingIO, int64(i), listPath, []iceberg.ManifestFile{mf})
		snapJSON = append(snapJSON,
			fmt.Sprintf(`{"snapshot-id":%d,"timestamp-ms":%d,"manifest-list":%q}`,
				i, i*1000, listPath))
	}

	meta, err := ParseMetadataString(buildMetaJSON(metaJSONOpts{
		snapshots: strings.Join(snapJSON, ","),
	}))
	require.NoError(t, err)

	tbl := New(Identifier{"ns", "tbl"}, meta, "metadata.json", testFSF(tio), nil)
	refs, err := tbl.getReferencedFiles(context.Background(), tio, 4, true)
	require.NoError(t, err)

	assert.Contains(t, refs, dataPath)
	assert.Contains(t, refs, manifestPath)
	assert.Equal(t, 1, tio.openCount[manifestPath],
		"shared manifest must be opened exactly once even with %d snapshots", numSnapshots)
}
