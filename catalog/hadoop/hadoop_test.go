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
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
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