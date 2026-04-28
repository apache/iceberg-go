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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

type HadoopCatalogTestSuite struct {
	suite.Suite
	warehouse string
	cat       *Catalog
}

func (s *HadoopCatalogTestSuite) SetupTest() {
	var err error
	s.warehouse, err = os.MkdirTemp("", "test_hadoop_*")
	s.Require().NoError(err)

	s.cat, err = NewCatalog("test", s.warehouse, nil)
	s.Require().NoError(err)
}

func (s *HadoopCatalogTestSuite) TearDownTest() {
	s.Require().NoError(os.RemoveAll(s.warehouse))
}

func TestHadoopCatalogTestSuite(t *testing.T) {
	suite.Run(t, new(HadoopCatalogTestSuite))
}

func (s *HadoopCatalogTestSuite) TestCatalogType() {
	s.Equal(catalog.Hadoop, s.cat.CatalogType())
}

func (s *HadoopCatalogTestSuite) TestRegistration() {
	cat, err := catalog.Load(context.Background(), "test", iceberg.Properties{
		"type":      "hadoop",
		"warehouse": s.warehouse,
	})
	s.Require().NoError(err)
	s.Equal(catalog.Hadoop, cat.CatalogType())
}

func (s *HadoopCatalogTestSuite) TestRegistrationMissingWarehouse() {
	_, err := catalog.Load(context.Background(), "test", iceberg.Properties{
		"type": "hadoop",
	})
	s.Require().Error(err)
	s.Contains(err.Error(), "warehouse")
}

func (s *HadoopCatalogTestSuite) TestNewCatalogTrimsTrailingSlash() {
	cat, err := NewCatalog("test", "/tmp/wh/", nil)
	s.Require().NoError(err)
	s.Equal("/tmp/wh", cat.warehouse)
}

func (s *HadoopCatalogTestSuite) TestNewCatalogStripsFilePrefix() {
	cat, err := NewCatalog("test", "file:///tmp/wh", nil)
	s.Require().NoError(err)
	s.Equal("/tmp/wh", cat.warehouse)

	cat, err = NewCatalog("test", "file:/tmp/wh", nil)
	s.Require().NoError(err)
	s.Equal("/tmp/wh", cat.warehouse)
}

func (s *HadoopCatalogTestSuite) TestNewCatalogRejectsNonFileScheme() {
	_, err := NewCatalog("test", "s3://bucket/path", nil)
	s.Require().Error(err)
	s.Contains(err.Error(), "unsupported warehouse scheme")

	_, err = NewCatalog("test", "hdfs://namenode/warehouse", nil)
	s.Require().Error(err)
	s.Contains(err.Error(), "unsupported warehouse scheme")
}

func (s *HadoopCatalogTestSuite) TestNamespaceToPathSingleLevel() {
	path := s.cat.namespaceToPath([]string{"db"})
	s.Equal(filepath.Join(s.warehouse, "db"), path)
}

func (s *HadoopCatalogTestSuite) TestNamespaceToPathNested() {
	path := s.cat.namespaceToPath([]string{"a", "b", "c"})
	s.Equal(filepath.Join(s.warehouse, "a", "b", "c"), path)
}

func (s *HadoopCatalogTestSuite) TestTableToPathSingleNamespace() {
	path := s.cat.tableToPath([]string{"ns", "tbl"})
	s.Equal(filepath.Join(s.warehouse, "ns", "tbl"), path)
}

func (s *HadoopCatalogTestSuite) TestTableToPathNestedNamespace() {
	path := s.cat.tableToPath([]string{"a", "b", "tbl"})
	s.Equal(filepath.Join(s.warehouse, "a", "b", "tbl"), path)
}

func (s *HadoopCatalogTestSuite) TestMetadataDir() {
	path := s.cat.metadataDir([]string{"ns", "tbl"})
	s.Equal(filepath.Join(s.warehouse, "ns", "tbl", "metadata"), path)
}

func (s *HadoopCatalogTestSuite) TestMetadataFilePath() {
	path := s.cat.metadataFilePath([]string{"ns", "tbl"}, 1)
	s.Equal(filepath.Join(s.warehouse, "ns", "tbl", "metadata", "v1.metadata.json"), path)

	path = s.cat.metadataFilePath([]string{"ns", "tbl"}, 42)
	s.Equal(filepath.Join(s.warehouse, "ns", "tbl", "metadata", "v42.metadata.json"), path)
}

func (s *HadoopCatalogTestSuite) TestVersionHintPath() {
	path := s.cat.versionHintPath([]string{"ns", "tbl"})
	s.Equal(filepath.Join(s.warehouse, "ns", "tbl", "metadata", "version-hint.text"), path)
}

func (s *HadoopCatalogTestSuite) TestDefaultTableLocation() {
	path := s.cat.defaultTableLocation([]string{"ns", "tbl"})
	s.Equal(filepath.Join(s.warehouse, "ns", "tbl"), path)
}

func (s *HadoopCatalogTestSuite) TestDefaultTableLocationNested() {
	path := s.cat.defaultTableLocation([]string{"a", "b", "tbl"})
	s.Equal(filepath.Join(s.warehouse, "a", "b", "tbl"), path)
}

func (s *HadoopCatalogTestSuite) TestVersionPatternMatches() {
	names := []string{
		"v1.metadata.json",
		"v42.metadata.json",
		"v100.metadata.json",
		"v1.gz.metadata.json",
		"v42.gz.metadata.json",
	}

	for _, name := range names {
		s.True(versionPattern.MatchString(name), "expected %s to match", name)
	}
}

func (s *HadoopCatalogTestSuite) TestVersionPatternRejects() {
	tests := []string{
		"00001-a1b2c3d4.metadata.json",
		"random.json",
		"v.metadata.json",
		"metadata.json",
		"v1.metadata.json.bak",
		"v-1.metadata.json",
	}

	for _, name := range tests {
		s.False(versionPattern.MatchString(name), "expected %s to not match", name)
	}
}

func (s *HadoopCatalogTestSuite) TestIsTableDirTrue() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "v1.metadata.json"), nil, 0o644))

	s.True(isTableDir(tableDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirFalseNoMetadataDir() {
	nsDir := filepath.Join(s.warehouse, "ns")
	s.Require().NoError(os.MkdirAll(nsDir, 0o755))

	s.False(isTableDir(nsDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirFalseEmptyMetadataDir() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))

	s.False(isTableDir(tableDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirTrueUUIDMetadata() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "00001-a1b2c3d4-e5f6-7890-abcd-ef1234567890.metadata.json"), nil, 0o644))

	s.True(isTableDir(tableDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirWithGzipMetadata() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "v1.gz.metadata.json"), nil, 0o644))

	s.True(isTableDir(tableDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirNonExistentPath() {
	s.False(isTableDir(filepath.Join(s.warehouse, "does", "not", "exist")))
}

func (s *HadoopCatalogTestSuite) createMetadataFile(ident table.Identifier, version int) {
	path := s.cat.metadataFilePath(ident, version)
	s.Require().NoError(os.MkdirAll(filepath.Dir(path), 0o755))
	s.Require().NoError(os.WriteFile(path, nil, 0o644))
}

func (s *HadoopCatalogTestSuite) TestReadWriteVersionHint() {
	ident := []string{"ns", "tbl"}
	s.Require().NoError(os.MkdirAll(s.cat.metadataDir(ident), 0o755))
	s.cat.writeVersionHint(ident, 42)
	s.Equal(42, s.cat.readVersionHint(ident))
}

func (s *HadoopCatalogTestSuite) TestReadVersionHintMissing() {
	s.Equal(0, s.cat.readVersionHint([]string{"ns", "tbl"}))
}

func (s *HadoopCatalogTestSuite) TestReadVersionHintCorrupt() {
	ident := []string{"ns", "tbl"}
	dir := s.cat.metadataDir(ident)
	s.Require().NoError(os.MkdirAll(dir, 0o755))
	s.Require().NoError(os.WriteFile(s.cat.versionHintPath(ident), []byte("garbage"), 0o644))
	s.Equal(0, s.cat.readVersionHint(ident))
}

func (s *HadoopCatalogTestSuite) TestWriteVersionHintCreatesFile() {
	ident := []string{"ns", "tbl"}
	s.Require().NoError(os.MkdirAll(s.cat.metadataDir(ident), 0o755))
	s.cat.writeVersionHint(ident, 7)
	data, err := os.ReadFile(s.cat.versionHintPath(ident))
	s.Require().NoError(err)
	s.Equal("7", string(data))
}

func (s *HadoopCatalogTestSuite) TestFindVersionFromHint() {
	ident := []string{"ns", "tbl"}
	s.createMetadataFile(ident, 1)
	s.cat.writeVersionHint(ident, 1)
	ver, err := s.cat.findVersion(ident)
	s.Require().NoError(err)
	s.Equal(1, ver)
}

func (s *HadoopCatalogTestSuite) TestFindVersionNoHint() {
	ident := []string{"ns", "tbl"}
	for i := 1; i <= 3; i++ {
		s.createMetadataFile(ident, i)
	}

	ver, err := s.cat.findVersion(ident)
	s.Require().NoError(err)
	s.Equal(3, ver)
}

func (s *HadoopCatalogTestSuite) TestFindVersionNoMetadata() {
	ident := []string{"ns", "tbl"}
	s.Require().NoError(os.MkdirAll(s.cat.metadataDir(ident), 0o755))
	_, err := s.cat.findVersion(ident)
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchTable)
}

func (s *HadoopCatalogTestSuite) TestFindVersionScanForward() {
	ident := []string{"ns", "tbl"}
	for i := 1; i <= 5; i++ {
		s.createMetadataFile(ident, i)
	}

	s.cat.writeVersionHint(ident, 2)
	ver, err := s.cat.findVersion(ident)
	s.Require().NoError(err)
	s.Equal(5, ver)
}

func (s *HadoopCatalogTestSuite) TestFindVersionNoMetadataDir() {
	_, err := s.cat.findVersion([]string{"ns", "tbl"})
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchTable)
}

func (s *HadoopCatalogTestSuite) TestFindVersionStaleHintPointsToDeletedVersion() {
	// hint=5, but only v1-v3 exist. Hint file points to a version whose
	// metadata was deleted. Should fall through to dir listing and find v3.
	ident := []string{"ns", "tbl"}
	for i := 1; i <= 3; i++ {
		s.createMetadataFile(ident, i)
	}

	s.cat.writeVersionHint(ident, 5)
	ver, err := s.cat.findVersion(ident)
	s.Require().NoError(err)
	s.Equal(3, ver)
}

func (s *HadoopCatalogTestSuite) TestReadVersionHintZeroValue() {
	ident := []string{"ns", "tbl"}
	s.Require().NoError(os.MkdirAll(s.cat.metadataDir(ident), 0o755))
	s.Require().NoError(os.WriteFile(s.cat.versionHintPath(ident), []byte("0"), 0o644))
	s.Equal(0, s.cat.readVersionHint(ident))
}

func (s *HadoopCatalogTestSuite) TestReadVersionHintWithWhitespace() {
	ident := []string{"ns", "tbl"}
	s.Require().NoError(os.MkdirAll(s.cat.metadataDir(ident), 0o755))
	s.Require().NoError(os.WriteFile(s.cat.versionHintPath(ident), []byte("  42\n"), 0o644))
	s.Equal(42, s.cat.readVersionHint(ident))
}

func (s *HadoopCatalogTestSuite) TestWriteVersionHintNoMetadataDir() {
	// metadata dir doesn't exist — writeVersionHint should not panic,
	// just log a warning silently.
	ident := []string{"nonexistent", "tbl"}
	s.NotPanics(func() {
		s.cat.writeVersionHint(ident, 1)
	})
}

func (s *HadoopCatalogTestSuite) TestFindVersionIgnoresTempFiles() {
	// metadata dir has both valid metadata files and orphaned temp files
	// from crashed commits. findVersion should ignore temp files.
	ident := []string{"ns", "tbl"}
	dir := s.cat.metadataDir(ident)
	s.Require().NoError(os.MkdirAll(dir, 0o755))
	s.createMetadataFile(ident, 1)
	s.createMetadataFile(ident, 2)
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "abc123-def456.metadata.json"), nil, 0o644))
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "abc123-version-hint.temp"), nil, 0o644))

	ver, err := s.cat.findVersion(ident)
	s.Require().NoError(err)
	s.Equal(2, ver)
}

func (s *HadoopCatalogTestSuite) TestFindVersionGzipOnlyWithHint() {
	// Table has only gzip metadata. Hint validation and scanForward
	// should recognize gzip-compressed metadata files.
	ident := []string{"ns", "tbl"}
	dir := s.cat.metadataDir(ident)
	s.Require().NoError(os.MkdirAll(dir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "v1.gz.metadata.json"), nil, 0o644))
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "v2.gz.metadata.json"), nil, 0o644))
	s.cat.writeVersionHint(ident, 1)

	ver, err := s.cat.findVersion(ident)
	s.Require().NoError(err)
	// Hint=1 validated via gzip path, scanForward finds v2.gz → returns 2
	s.Equal(2, ver)
}

func (s *HadoopCatalogTestSuite) TestFindVersionMixedGzipAndPlain() {
	// v1 and v2 are plain, v3 is gzip-compressed. scanForward must
	// check both formats to avoid returning a stale version.
	ident := []string{"ns", "tbl"}
	dir := s.cat.metadataDir(ident)
	s.Require().NoError(os.MkdirAll(dir, 0o755))
	s.createMetadataFile(ident, 1)
	s.createMetadataFile(ident, 2)
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "v3.gz.metadata.json"), nil, 0o644))
	s.cat.writeVersionHint(ident, 1)

	ver, err := s.cat.findVersion(ident)
	s.Require().NoError(err)
	s.Equal(3, ver)
}

// CreateNamespace tests

func (s *HadoopCatalogTestSuite) TestCreateNamespace() {
	err := s.cat.CreateNamespace(context.Background(), []string{"ns"}, nil)
	s.Require().NoError(err)

	info, err := os.Stat(filepath.Join(s.warehouse, "ns"))
	s.Require().NoError(err)
	s.True(info.IsDir())
}

func (s *HadoopCatalogTestSuite) TestCreateNamespaceAlreadyExists() {
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	err := s.cat.CreateNamespace(context.Background(), []string{"ns"}, nil)
	s.ErrorIs(err, catalog.ErrNamespaceAlreadyExists)
}

func (s *HadoopCatalogTestSuite) TestCreateNamespaceNested() {
	// Parent namespaces must exist first with atomic os.Mkdir.
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "a"), 0o755))
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "a", "b"), 0o755))

	err := s.cat.CreateNamespace(context.Background(), []string{"a", "b", "c"}, nil)
	s.Require().NoError(err)

	info, err := os.Stat(filepath.Join(s.warehouse, "a", "b", "c"))
	s.Require().NoError(err)
	s.True(info.IsDir())
}

func (s *HadoopCatalogTestSuite) TestCreateNamespaceNestedParentMissing() {
	// Creating a nested namespace without parent should fail.
	err := s.cat.CreateNamespace(context.Background(), []string{"a", "b", "c"}, nil)
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestCreateNamespaceWithProperties() {
	err := s.cat.CreateNamespace(context.Background(), []string{"ns"}, iceberg.Properties{"key": "val"})
	s.Require().Error(err)
	s.Contains(err.Error(), "properties are not supported")
}

func (s *HadoopCatalogTestSuite) TestCreateNamespaceNilProperties() {
	err := s.cat.CreateNamespace(context.Background(), []string{"ns"}, nil)
	s.Require().NoError(err)

	info, err := os.Stat(filepath.Join(s.warehouse, "ns"))
	s.Require().NoError(err)
	s.True(info.IsDir())
}

// DropNamespace tests

func (s *HadoopCatalogTestSuite) TestDropNamespace() {
	nsDir := filepath.Join(s.warehouse, "ns")
	s.Require().NoError(os.Mkdir(nsDir, 0o755))

	err := s.cat.DropNamespace(context.Background(), []string{"ns"})
	s.Require().NoError(err)

	_, err = os.Stat(nsDir)
	s.True(os.IsNotExist(err))
}

func (s *HadoopCatalogTestSuite) TestDropNamespaceNotExists() {
	err := s.cat.DropNamespace(context.Background(), []string{"nope"})
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestDropNamespaceNotEmptyWithTable() {
	nsDir := filepath.Join(s.warehouse, "ns")
	metaDir := filepath.Join(nsDir, "tbl", "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "v1.metadata.json"), nil, 0o644))

	err := s.cat.DropNamespace(context.Background(), []string{"ns"})
	s.ErrorIs(err, catalog.ErrNamespaceNotEmpty)
}

func (s *HadoopCatalogTestSuite) TestDropNamespaceNotEmptyWithChildNamespace() {
	nsDir := filepath.Join(s.warehouse, "ns")
	s.Require().NoError(os.MkdirAll(filepath.Join(nsDir, "child_ns"), 0o755))

	err := s.cat.DropNamespace(context.Background(), []string{"ns"})
	s.ErrorIs(err, catalog.ErrNamespaceNotEmpty)
}

// CheckNamespaceExists tests

func (s *HadoopCatalogTestSuite) TestCheckNamespaceExistsTrue() {
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	exists, err := s.cat.CheckNamespaceExists(context.Background(), []string{"ns"})
	s.Require().NoError(err)
	s.True(exists)
}

func (s *HadoopCatalogTestSuite) TestCheckNamespaceExistsFalse() {
	exists, err := s.cat.CheckNamespaceExists(context.Background(), []string{"nope"})
	s.Require().NoError(err)
	s.False(exists)
}

// ListNamespaces tests

func (s *HadoopCatalogTestSuite) TestListNamespacesRoot() {
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns1"), 0o755))
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns2"), 0o755))

	namespaces, err := s.cat.ListNamespaces(context.Background(), nil)
	s.Require().NoError(err)
	s.Len(namespaces, 2)
	s.Contains(namespaces, table.Identifier{"ns1"})
	s.Contains(namespaces, table.Identifier{"ns2"})
}

func (s *HadoopCatalogTestSuite) TestListNamespacesEmpty() {
	namespaces, err := s.cat.ListNamespaces(context.Background(), nil)
	s.Require().NoError(err)
	s.Empty(namespaces)
	s.NotNil(namespaces)
}

func (s *HadoopCatalogTestSuite) TestListNamespacesNested() {
	parentDir := filepath.Join(s.warehouse, "a")
	s.Require().NoError(os.MkdirAll(filepath.Join(parentDir, "child1"), 0o755))
	s.Require().NoError(os.MkdirAll(filepath.Join(parentDir, "child2"), 0o755))

	namespaces, err := s.cat.ListNamespaces(context.Background(), []string{"a"})
	s.Require().NoError(err)
	s.Len(namespaces, 2)
	s.Contains(namespaces, table.Identifier{"child1"})
	s.Contains(namespaces, table.Identifier{"child2"})
}

func (s *HadoopCatalogTestSuite) TestListNamespacesParentNotExists() {
	_, err := s.cat.ListNamespaces(context.Background(), []string{"nope"})
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestListNamespacesParentIsFile() {
	filePath := filepath.Join(s.warehouse, "a_file")
	s.Require().NoError(os.WriteFile(filePath, nil, 0o644))

	_, err := s.cat.ListNamespaces(context.Background(), []string{"a_file"})
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

// LoadNamespaceProperties tests

func (s *HadoopCatalogTestSuite) TestLoadNamespaceProperties() {
	nsDir := filepath.Join(s.warehouse, "ns")
	s.Require().NoError(os.Mkdir(nsDir, 0o755))

	props, err := s.cat.LoadNamespaceProperties(context.Background(), []string{"ns"})
	s.Require().NoError(err)
	s.Equal(iceberg.Properties{"location": "file://" + nsDir}, props)
}

func (s *HadoopCatalogTestSuite) TestLoadNamespacePropertiesNotExists() {
	_, err := s.cat.LoadNamespaceProperties(context.Background(), []string{"nope"})
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestLoadNamespacePropertiesFileNotDir() {
	filePath := filepath.Join(s.warehouse, "not_a_ns")
	s.Require().NoError(os.WriteFile(filePath, nil, 0o644))

	_, err := s.cat.LoadNamespaceProperties(context.Background(), []string{"not_a_ns"})
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

// UpdateNamespaceProperties test

func (s *HadoopCatalogTestSuite) TestUpdateNamespacePropertiesUnsupported() {
	_, err := s.cat.UpdateNamespaceProperties(context.Background(), []string{"ns"}, nil, nil)
	s.Require().Error(err)
	s.Contains(err.Error(), "not yet implemented")
}

func (s *HadoopCatalogTestSuite) TestDropNamespaceWithRegularFilesOnly() {
	// A namespace dir containing only regular files (no subdirectories)
	// should still return ErrNamespaceNotEmpty.
	nsDir := filepath.Join(s.warehouse, "ns")
	s.Require().NoError(os.Mkdir(nsDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(nsDir, "stray_file.txt"), nil, 0o644))

	err := s.cat.DropNamespace(context.Background(), []string{"ns"})
	s.ErrorIs(err, catalog.ErrNamespaceNotEmpty)
}

func (s *HadoopCatalogTestSuite) TestCheckNamespaceExistsFileNotDir() {
	// A file at the namespace path should return false (not a directory).
	filePath := filepath.Join(s.warehouse, "not_a_dir")
	s.Require().NoError(os.WriteFile(filePath, nil, 0o644))

	exists, err := s.cat.CheckNamespaceExists(context.Background(), []string{"not_a_dir"})
	s.Require().NoError(err)
	s.False(exists)
}

func (s *HadoopCatalogTestSuite) TestCreateNamespaceEmptyIdentifier() {
	err := s.cat.CreateNamespace(context.Background(), []string{}, nil)
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestDropNamespaceEmptyIdentifier() {
	err := s.cat.DropNamespace(context.Background(), []string{})
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestLoadNamespacePropertiesNested() {
	// Verify nested namespace returns the correct absolute path.
	nsDir := filepath.Join(s.warehouse, "a", "b", "c")
	s.Require().NoError(os.MkdirAll(nsDir, 0o755))

	props, err := s.cat.LoadNamespaceProperties(context.Background(), []string{"a", "b", "c"})
	s.Require().NoError(err)
	s.Equal("file://"+nsDir, props["location"])
}

func (s *HadoopCatalogTestSuite) TestListNamespacesMixedContent() {
	// Directory with tables, namespaces, and regular files.
	// Only non-table directories should appear.
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "child_ns"), 0o755))

	// Create a table dir
	tableDir := filepath.Join(s.warehouse, "my_table")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "v1.metadata.json"), nil, 0o644))

	// Regular file
	s.Require().NoError(os.WriteFile(filepath.Join(s.warehouse, "README.txt"), nil, 0o644))

	namespaces, err := s.cat.ListNamespaces(context.Background(), nil)
	s.Require().NoError(err)
	s.Len(namespaces, 1)
	s.Equal(table.Identifier{"child_ns"}, namespaces[0])
}

// Path validation tests

func (s *HadoopCatalogTestSuite) TestCreateNamespaceRejectsDotDot() {
	err := s.cat.CreateNamespace(context.Background(), []string{"a", ".."}, nil)
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestCreateNamespaceRejectsDot() {
	err := s.cat.CreateNamespace(context.Background(), []string{"."}, nil)
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestCreateNamespaceRejectsEmptyComponent() {
	err := s.cat.CreateNamespace(context.Background(), []string{"a", "", "b"}, nil)
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestCreateNamespaceRejectsPathSeparator() {
	err := s.cat.CreateNamespace(context.Background(), []string{"a/b"}, nil)
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestDropNamespaceRejectsDotDot() {
	err := s.cat.DropNamespace(context.Background(), []string{".."})
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestListNamespacesRejectsInvalidParent() {
	_, err := s.cat.ListNamespaces(context.Background(), []string{"a", ".."})
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestLoadNamespacePropertiesRejectsInvalid() {
	_, err := s.cat.LoadNamespaceProperties(context.Background(), []string{".."})
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestCheckNamespaceExistsRejectsInvalid() {
	_, err := s.cat.CheckNamespaceExists(context.Background(), []string{"a/b"})
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

// isTableDir interop tests

func (s *HadoopCatalogTestSuite) TestIsTableDirTrueVersionHintText() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "version-hint.text"), []byte("1"), 0o644))

	s.True(isTableDir(tableDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirTrueUUIDMetadataGzip() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "00001-a1b2c3d4-e5f6-7890-abcd-ef1234567890.gz.metadata.json"), nil, 0o644))

	s.True(isTableDir(tableDir))
}

func (s *HadoopCatalogTestSuite) TestIsTableDirFalseNonMatchingFiles() {
	tableDir := filepath.Join(s.warehouse, "ns", "tbl")
	metaDir := filepath.Join(tableDir, "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "random.json"), nil, 0o644))

	s.False(isTableDir(tableDir))
}

// Helper to create a fake table directory with a metadata file.
func (s *HadoopCatalogTestSuite) createFakeTable(ident table.Identifier) {
	metaDir := filepath.Join(s.cat.tableToPath(ident), "metadata")
	s.Require().NoError(os.MkdirAll(metaDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(metaDir, "v1.metadata.json"), []byte("{}"), 0o644))
}

// ListTables tests

func (s *HadoopCatalogTestSuite) TestListTablesEmpty() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	var tables []table.Identifier
	for ident, err := range s.cat.ListTables(ctx, []string{"ns"}) {
		s.Require().NoError(err)
		tables = append(tables, ident)
	}

	s.Empty(tables)
}

func (s *HadoopCatalogTestSuite) TestListTablesWithTables() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	s.createFakeTable([]string{"ns", "tbl1"})
	s.createFakeTable([]string{"ns", "tbl2"})

	var tables []table.Identifier
	for ident, err := range s.cat.ListTables(ctx, []string{"ns"}) {
		s.Require().NoError(err)
		tables = append(tables, ident)
	}

	s.Len(tables, 2)
	s.Contains(tables, table.Identifier{"ns", "tbl1"})
	s.Contains(tables, table.Identifier{"ns", "tbl2"})
}

func (s *HadoopCatalogTestSuite) TestListTablesNoNamespace() {
	ctx := context.Background()

	for _, err := range s.cat.ListTables(ctx, []string{"nope"}) {
		s.ErrorIs(err, catalog.ErrNoSuchNamespace)

		break
	}
}

func (s *HadoopCatalogTestSuite) TestListTablesEmptyIdentifier() {
	ctx := context.Background()

	for _, err := range s.cat.ListTables(ctx, []string{}) {
		s.Require().Error(err)
		s.Contains(err.Error(), "must not be empty")

		break
	}
}

func (s *HadoopCatalogTestSuite) TestListTablesMixedContent() {
	// Namespace with tables, child namespaces, and regular files.
	// Only table dirs should be returned.
	ctx := context.Background()
	nsDir := filepath.Join(s.warehouse, "ns")
	s.Require().NoError(os.Mkdir(nsDir, 0o755))

	s.createFakeTable([]string{"ns", "tbl1"})
	s.Require().NoError(os.Mkdir(filepath.Join(nsDir, "child_ns"), 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(nsDir, "stray_file.txt"), nil, 0o644))

	var tables []table.Identifier
	for ident, err := range s.cat.ListTables(ctx, []string{"ns"}) {
		s.Require().NoError(err)
		tables = append(tables, ident)
	}

	s.Len(tables, 1)
	s.Equal(table.Identifier{"ns", "tbl1"}, tables[0])
}

func (s *HadoopCatalogTestSuite) TestListTablesNestedNamespace() {
	ctx := context.Background()
	s.Require().NoError(os.MkdirAll(filepath.Join(s.warehouse, "a", "b"), 0o755))

	s.createFakeTable([]string{"a", "b", "tbl1"})

	var tables []table.Identifier
	for ident, err := range s.cat.ListTables(ctx, []string{"a", "b"}) {
		s.Require().NoError(err)
		tables = append(tables, ident)
	}

	s.Len(tables, 1)
	s.Equal(table.Identifier{"a", "b", "tbl1"}, tables[0])
}

// DropTable tests

func (s *HadoopCatalogTestSuite) TestDropTable() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))
	s.createFakeTable([]string{"ns", "tbl"})

	err := s.cat.DropTable(ctx, []string{"ns", "tbl"})
	s.Require().NoError(err)

	// Verify the table directory is completely removed.
	_, statErr := os.Stat(filepath.Join(s.warehouse, "ns", "tbl"))
	s.True(os.IsNotExist(statErr))
}

func (s *HadoopCatalogTestSuite) TestDropTableNotExists() {
	err := s.cat.DropTable(context.Background(), []string{"ns", "tbl"})
	s.ErrorIs(err, catalog.ErrNoSuchTable)
}

func (s *HadoopCatalogTestSuite) TestDropTableVerifyCleanup() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))
	s.createFakeTable([]string{"ns", "tbl"})

	// Add a data file to confirm everything is purged.
	dataDir := filepath.Join(s.warehouse, "ns", "tbl", "data")
	s.Require().NoError(os.MkdirAll(dataDir, 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(dataDir, "00000-0-0-00000.parquet"), []byte("data"), 0o644))

	err := s.cat.DropTable(ctx, []string{"ns", "tbl"})
	s.Require().NoError(err)

	_, statErr := os.Stat(filepath.Join(s.warehouse, "ns", "tbl"))
	s.True(os.IsNotExist(statErr))
}

func (s *HadoopCatalogTestSuite) TestDropTableShortIdentifier() {
	err := s.cat.DropTable(context.Background(), []string{"tbl"})
	s.Require().Error(err)
	s.Contains(err.Error(), "at least a namespace and table name")
}

// CreateTable tests

func (s *HadoopCatalogTestSuite) testSchema() *iceberg.Schema {
	return iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
}

func (s *HadoopCatalogTestSuite) TestCreateTable() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	tbl, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema())
	s.Require().NoError(err)
	s.NotNil(tbl)

	// Verify metadata directory and files exist
	metaDir := filepath.Join(s.warehouse, "ns", "tbl", "metadata")
	s.DirExists(metaDir)
	s.FileExists(filepath.Join(metaDir, "v1.metadata.json"))

	// Verify version hint
	data, err := os.ReadFile(filepath.Join(metaDir, "version-hint.text"))
	s.Require().NoError(err)
	s.Equal("1", string(data))

	// Verify metadata
	s.Equal(filepath.Join(metaDir, "v1.metadata.json"), tbl.MetadataLocation())
	s.Equal(filepath.Join(s.warehouse, "ns", "tbl"), tbl.Location())
}

func (s *HadoopCatalogTestSuite) TestCreateTableAndLoad() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	schema := s.testSchema()
	created, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, schema)
	s.Require().NoError(err)

	loaded, err := s.cat.LoadTable(ctx, []string{"ns", "tbl"})
	s.Require().NoError(err)

	s.Equal(created.Metadata().TableUUID(), loaded.Metadata().TableUUID())
	s.Equal(created.Location(), loaded.Location())
	s.Equal(len(created.Schema().Fields()), len(loaded.Schema().Fields()))
}

func (s *HadoopCatalogTestSuite) TestCreateTableCustomLocation() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	_, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema(),
		catalog.WithLocation("/some/other/path"))
	s.Require().Error(err)
	s.Contains(err.Error(), "custom table locations are not supported")
}

func (s *HadoopCatalogTestSuite) TestCreateTableSameLocationAllowed() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	loc := filepath.Join(s.warehouse, "ns", "tbl")
	tbl, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema(),
		catalog.WithLocation(loc))
	s.Require().NoError(err)
	s.Equal(loc, tbl.Location())
}

func (s *HadoopCatalogTestSuite) TestCreateTableNoNamespace() {
	ctx := context.Background()

	_, err := s.cat.CreateTable(ctx, []string{"nonexistent", "tbl"}, s.testSchema())
	s.ErrorIs(err, catalog.ErrNoSuchNamespace)
}

func (s *HadoopCatalogTestSuite) TestCreateTableAlreadyExists() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	_, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema())
	s.Require().NoError(err)

	_, err = s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema())
	s.ErrorIs(err, catalog.ErrTableAlreadyExists)
}

func (s *HadoopCatalogTestSuite) TestCreateTableWithPartitionSpec() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	spec := iceberg.NewPartitionSpec(
		iceberg.PartitionField{
			SourceIDs: []int{1},
			FieldID:   1000,
			Name:      "id_bucket",
			Transform: iceberg.BucketTransform{NumBuckets: 16},
		},
	)

	tbl, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema(),
		catalog.WithPartitionSpec(&spec))
	s.Require().NoError(err)
	s.False(tbl.Spec().IsUnpartitioned())
}

func (s *HadoopCatalogTestSuite) TestCreateTableWithSortOrder() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	sortOrder, err := table.NewSortOrder(1, []table.SortField{
		{
			SourceIDs: []int{1},
			Transform: iceberg.IdentityTransform{},
			Direction: table.SortASC,
			NullOrder: table.NullsFirst,
		},
	})
	s.Require().NoError(err)

	tbl, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema(),
		catalog.WithSortOrder(sortOrder))
	s.Require().NoError(err)
	s.Greater(tbl.SortOrder().Len(), 0)
}

func (s *HadoopCatalogTestSuite) TestCreateTableWithProperties() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	props := iceberg.Properties{
		"custom.key": "custom.value",
	}

	tbl, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema(),
		catalog.WithProperties(props))
	s.Require().NoError(err)
	s.Equal("custom.value", tbl.Properties()["custom.key"])
}

func (s *HadoopCatalogTestSuite) TestCreateTableShortIdentifier() {
	ctx := context.Background()

	_, err := s.cat.CreateTable(ctx, []string{"tbl"}, s.testSchema())
	s.Require().Error(err)
	s.Contains(err.Error(), "at least a namespace and table name")
}

func (s *HadoopCatalogTestSuite) TestDropTableNamespacePreserved() {
	// Dropping a table should not affect the parent namespace directory.
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))
	s.createFakeTable([]string{"ns", "tbl"})

	s.Require().NoError(s.cat.DropTable(ctx, []string{"ns", "tbl"}))

	// Namespace dir should still exist.
	info, err := os.Stat(filepath.Join(s.warehouse, "ns"))
	s.Require().NoError(err)
	s.True(info.IsDir())
}

// RenameTable tests

func (s *HadoopCatalogTestSuite) TestRenameTableUnsupported() {
	_, err := s.cat.RenameTable(context.Background(), []string{"ns", "old"}, []string{"ns", "new"})
	s.Require().Error(err)
	s.Contains(err.Error(), "not supported")
}

func (s *HadoopCatalogTestSuite) TestListTablesNamespaceIsFile() {
	ctx := context.Background()
	s.Require().NoError(os.WriteFile(filepath.Join(s.warehouse, "not_a_ns"), nil, 0o644))

	for _, err := range s.cat.ListTables(ctx, []string{"not_a_ns"}) {
		s.ErrorIs(err, catalog.ErrNoSuchNamespace)

		break
	}
}

func (s *HadoopCatalogTestSuite) TestListTablesIdentifierIsolation() {
	// Verify that yielded identifiers don't alias the namespace slice.
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))
	s.createFakeTable([]string{"ns", "tbl1"})
	s.createFakeTable([]string{"ns", "tbl2"})

	var tables []table.Identifier
	for ident, err := range s.cat.ListTables(ctx, []string{"ns"}) {
		s.Require().NoError(err)
		tables = append(tables, ident)
	}

	s.Len(tables, 2)
	// Each identifier should be independent — no aliasing.
	s.NotEqual(tables[0][len(tables[0])-1], tables[1][len(tables[1])-1])
}

// LoadTable tests

func (s *HadoopCatalogTestSuite) TestLoadTableNotExists() {
	ctx := context.Background()

	_, err := s.cat.LoadTable(ctx, []string{"ns", "tbl"})
	s.Require().Error(err)
	s.ErrorIs(err, catalog.ErrNoSuchTable)
}

func (s *HadoopCatalogTestSuite) TestLoadTableStaleHint() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	// Create table (version 1)
	_, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema())
	s.Require().NoError(err)

	// Manually set the hint to a stale value
	ident := []string{"ns", "tbl"}
	s.cat.writeVersionHint(ident, 99)

	// LoadTable should still succeed by falling back to dir listing
	tbl, err := s.cat.LoadTable(ctx, ident)
	s.Require().NoError(err)
	s.NotNil(tbl)
}

func (s *HadoopCatalogTestSuite) TestLoadTableShortIdentifier() {
	ctx := context.Background()

	_, err := s.cat.LoadTable(ctx, []string{"tbl"})
	s.Require().Error(err)
	s.Contains(err.Error(), "at least a namespace and table name")
}

// CheckTableExists tests

func (s *HadoopCatalogTestSuite) TestCheckTableExistsTrue() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	_, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema())
	s.Require().NoError(err)

	exists, err := s.cat.CheckTableExists(ctx, []string{"ns", "tbl"})
	s.Require().NoError(err)
	s.True(exists)
}

func (s *HadoopCatalogTestSuite) TestCheckTableExistsFalse() {
	exists, err := s.cat.CheckTableExists(context.Background(), []string{"ns", "tbl"})
	s.Require().NoError(err)
	s.False(exists)
}

func (s *HadoopCatalogTestSuite) TestCreateTableNestedNamespace() {
	ctx := context.Background()
	s.Require().NoError(os.MkdirAll(filepath.Join(s.warehouse, "a", "b"), 0o755))

	tbl, err := s.cat.CreateTable(ctx, []string{"a", "b", "tbl"}, s.testSchema())
	s.Require().NoError(err)
	s.Equal(filepath.Join(s.warehouse, "a", "b", "tbl"), tbl.Location())

	// Verify round-trip
	loaded, err := s.cat.LoadTable(ctx, []string{"a", "b", "tbl"})
	s.Require().NoError(err)
	s.Equal(tbl.Metadata().TableUUID(), loaded.Metadata().TableUUID())
}

func (s *HadoopCatalogTestSuite) TestCreateTableMetadataFormatVersion() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))

	tbl, err := s.cat.CreateTable(ctx, []string{"ns", "tbl"}, s.testSchema())
	s.Require().NoError(err)

	// Default format version should be V2
	s.Equal(2, tbl.Metadata().Version())
}

// CommitTable tests

func (s *HadoopCatalogTestSuite) createTestTable(ns, name string) *table.Table {
	ctx := context.Background()
	s.Require().NoError(os.MkdirAll(filepath.Join(s.warehouse, ns), 0o755))

	tbl, err := s.cat.CreateTable(ctx, []string{ns, name}, s.testSchema())
	s.Require().NoError(err)

	return tbl
}

func (s *HadoopCatalogTestSuite) TestCommitTableSingleUpdate() {
	ctx := context.Background()
	tbl := s.createTestTable("ns", "tbl")

	meta, metaLoc, err := s.cat.CommitTable(ctx, []string{"ns", "tbl"},
		[]table.Requirement{
			table.AssertTableUUID(tbl.Metadata().TableUUID()),
		},
		[]table.Update{
			table.NewSetPropertiesUpdate(iceberg.Properties{"test.key": "test.value"}),
		},
	)
	s.Require().NoError(err)

	// Version should have incremented to 2.
	s.Contains(metaLoc, "v2.metadata.json")
	s.Equal("test.value", meta.Properties()["test.key"])

	// File should exist on disk.
	s.FileExists(metaLoc)
}

func (s *HadoopCatalogTestSuite) TestCommitTableMultipleSequential() {
	ctx := context.Background()
	tbl := s.createTestTable("ns", "tbl")
	ident := []string{"ns", "tbl"}

	for i := 0; i < 3; i++ {
		loaded, err := s.cat.LoadTable(ctx, ident)
		s.Require().NoError(err)

		_, metaLoc, err := s.cat.CommitTable(ctx, ident,
			[]table.Requirement{
				table.AssertTableUUID(loaded.Metadata().TableUUID()),
			},
			[]table.Update{
				table.NewSetPropertiesUpdate(iceberg.Properties{
					fmt.Sprintf("iter.%d", i): "val",
				}),
			},
		)
		s.Require().NoError(err)
		s.Contains(metaLoc, fmt.Sprintf("v%d.metadata.json", i+2))
	}

	// Final load should have all properties.
	final, err := s.cat.LoadTable(ctx, ident)
	s.Require().NoError(err)
	s.Equal("val", final.Properties()["iter.0"])
	s.Equal("val", final.Properties()["iter.1"])
	s.Equal("val", final.Properties()["iter.2"])

	// Version hint should reflect the latest version.
	_ = tbl // suppress unused
	s.Equal(4, s.cat.readVersionHint(ident))
}

func (s *HadoopCatalogTestSuite) TestCommitTableNoChanges() {
	ctx := context.Background()
	tbl := s.createTestTable("ns", "tbl")

	meta, metaLoc, err := s.cat.CommitTable(ctx, []string{"ns", "tbl"},
		[]table.Requirement{
			table.AssertTableUUID(tbl.Metadata().TableUUID()),
		},
		nil, // no updates
	)
	s.Require().NoError(err)

	// Even with no updates, UpdateTableMetadata produces a new metadata
	// object (different timestamp), so a new version is written.
	s.Contains(metaLoc, "v2.metadata.json")
	s.Equal(tbl.Metadata().TableUUID(), meta.TableUUID())
}

func (s *HadoopCatalogTestSuite) TestCommitTableConflictDetection() {
	// Conflict detection relies on os.Stat checking whether the target
	// v{N+1}.metadata.json already exists before the atomic rename.
	// In a single-threaded test we cannot trigger the TOCTOU race
	// between findVersion and os.Stat. Instead, we verify that after
	// multiple sequential commits, the version sequence is contiguous
	// and no versions are skipped or duplicated.
	ctx := context.Background()
	s.createTestTable("ns", "tbl")
	ident := []string{"ns", "tbl"}

	for i := 2; i <= 5; i++ {
		_, metaLoc, err := s.cat.CommitTable(ctx, ident,
			nil,
			[]table.Update{
				table.NewSetPropertiesUpdate(iceberg.Properties{
					fmt.Sprintf("v%d", i): "committed",
				}),
			},
		)
		s.Require().NoError(err)
		s.Contains(metaLoc, fmt.Sprintf("v%d.metadata.json", i))
		s.FileExists(s.cat.metadataFilePath(ident, i))
	}
}

func (s *HadoopCatalogTestSuite) TestCommitTableRequirementFailure() {
	ctx := context.Background()
	s.createTestTable("ns", "tbl")

	wrongUUID := uuid.New()
	_, _, err := s.cat.CommitTable(ctx, []string{"ns", "tbl"},
		[]table.Requirement{
			table.AssertTableUUID(wrongUUID),
		},
		[]table.Update{
			table.NewSetPropertiesUpdate(iceberg.Properties{"k": "v"}),
		},
	)
	s.Require().Error(err)
	s.Contains(err.Error(), "UUID")
}

func (s *HadoopCatalogTestSuite) TestCommitTableLocationChange() {
	ctx := context.Background()
	s.createTestTable("ns", "tbl")

	_, _, err := s.cat.CommitTable(ctx, []string{"ns", "tbl"},
		nil,
		[]table.Update{
			table.NewSetLocationUpdate("/some/other/location"),
		},
	)
	s.Require().Error(err)
	s.Contains(err.Error(), "location cannot be changed")
}

func (s *HadoopCatalogTestSuite) TestCommitTableWriteMetadataLocation() {
	ctx := context.Background()
	s.createTestTable("ns", "tbl")

	_, _, err := s.cat.CommitTable(ctx, []string{"ns", "tbl"},
		nil,
		[]table.Update{
			table.NewSetPropertiesUpdate(iceberg.Properties{
				"write.metadata.location": "/custom/metadata/path",
			}),
		},
	)
	s.Require().Error(err)
	s.Contains(err.Error(), "write.metadata.location")
}

func (s *HadoopCatalogTestSuite) TestCommitTableNoOrphanedTempFiles() {
	ctx := context.Background()
	s.createTestTable("ns", "tbl")
	ident := []string{"ns", "tbl"}

	// Do several commits and verify no temp files are left behind.
	for i := 0; i < 3; i++ {
		_, _, err := s.cat.CommitTable(ctx, ident,
			nil,
			[]table.Update{
				table.NewSetPropertiesUpdate(iceberg.Properties{
					fmt.Sprintf("iter.%d", i): "val",
				}),
			},
		)
		s.Require().NoError(err)
	}

	metaDir := s.cat.metadataDir(ident)
	entries, err := os.ReadDir(metaDir)
	s.Require().NoError(err)

	for _, e := range entries {
		if !e.IsDir() && !versionPattern.MatchString(e.Name()) && e.Name() != "version-hint.text" {
			s.Failf("orphaned temp file", "found orphaned file: %s", e.Name())
		}
	}
}

func (s *HadoopCatalogTestSuite) TestCommitTableVersionHintUpdated() {
	ctx := context.Background()
	s.createTestTable("ns", "tbl")
	ident := []string{"ns", "tbl"}

	_, _, err := s.cat.CommitTable(ctx, ident,
		nil,
		[]table.Update{
			table.NewSetPropertiesUpdate(iceberg.Properties{"k": "v"}),
		},
	)
	s.Require().NoError(err)

	s.Equal(2, s.cat.readVersionHint(ident))
}

func (s *HadoopCatalogTestSuite) TestCommitTableCreateViaCommit() {
	ctx := context.Background()
	s.Require().NoError(os.Mkdir(filepath.Join(s.warehouse, "ns"), 0o755))
	ident := []string{"ns", "tbl"}

	loc := s.cat.defaultTableLocation(ident)
	meta, metaLoc, err := s.cat.CommitTable(ctx, ident,
		[]table.Requirement{
			table.AssertCreate(),
		},
		[]table.Update{
			table.NewAssignUUIDUpdate(uuid.New()),
			table.NewUpgradeFormatVersionUpdate(2),
			table.NewAddSchemaUpdate(s.testSchema()),
			table.NewSetCurrentSchemaUpdate(-1),
			table.NewSetLocationUpdate(loc),
		},
	)
	s.Require().NoError(err)
	s.Contains(metaLoc, "v1.metadata.json")
	s.Equal(loc, meta.Location())

	// Should be loadable.
	loaded, err := s.cat.LoadTable(ctx, ident)
	s.Require().NoError(err)
	s.Equal(meta.TableUUID(), loaded.Metadata().TableUUID())
}

func (s *HadoopCatalogTestSuite) TestCommitTableShortIdentifier() {
	_, _, err := s.cat.CommitTable(context.Background(), []string{"tbl"}, nil, nil)
	s.Require().Error(err)
	s.Contains(err.Error(), "at least a namespace and table name")
}
